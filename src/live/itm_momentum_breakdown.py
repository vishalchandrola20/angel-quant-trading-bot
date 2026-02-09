"""
ITM Option Momentum Breakdown Strategy
Time Window: 9:15 to 10:00
Logic:
 - Put Setup: 3 consecutive Green candles -> Entry if Spot breaks 1st candle Low & ITM PE breaks 1st candle High.
 - Call Setup: 3 consecutive Red candles -> Entry if Spot breaks 1st candle High & ITM CE breaks 1st candle High.
Risk: Target +15 pts, SL -15 pts.
"""

from __future__ import annotations

import argparse
import logging
import math
import csv
import time as time_module
import json
import yaml
import os
from datetime import datetime, date, time as dt_time, timedelta
from pathlib import Path
from typing import Dict, Any, List
from dataclasses import asdict

from colorama import Fore, Style, init as colorama_init

from src.api.smartapi_client import AngelAPI
from src.market.contracts import find_option, OptionContract, get_next_expiry
from src.utils.telegram_bot import TelegramBot

# Initialize colorama
colorama_init(autoreset=True)

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)s.%(msecs)03d | %(message)s', datefmt='%Y-%m-%dT%H:%M:%S')

# --- Monkey-Patch for smartapi-python library bugs ---
try:
    from SmartApi.smartWebSocketV2 import SmartWebSocketV2

    def _patched_on_close(self, wsapp, close_status_code, close_msg):
        log.warning(f"event=WS_CLOSE | WebSocket connection closed. Code: {close_status_code}, Reason: {close_msg}")
        if self.on_close:
            self.on_close(wsapp)

    SmartWebSocketV2._on_close = _patched_on_close

except Exception as e:
    log.error("Failed to import or patch SmartWebSocketV2: %s", e)
    SmartWebSocketV2 = None
# --- End of Patch ---


class ITMMomentumStrategy:
    INDEX_CONFIG = {
        "NIFTY": {"token": "99926000", "exchange": "NSE", "options_exchange": "NFO", "strike_step": 50},
        "SENSEX": {"token": "99919000", "exchange": "BSE", "options_exchange": "BFO", "strike_step": 100}
    }

    def __init__(self, index_name: str = "NIFTY", trading_date: date | None = None, simulate_orders: bool = True):
        self.index_name = index_name.upper()
        if self.index_name not in self.INDEX_CONFIG:
            raise ValueError(f"Invalid index '{self.index_name}'")
        
        config = self.INDEX_CONFIG[self.index_name]
        self.index_token = config["token"]
        self.index_exchange = config["exchange"]
        self.options_exchange = config["options_exchange"]
        self.strike_step = config["strike_step"]
        
        self.trading_date = trading_date or date.today()
        self.simulate_orders = simulate_orders
        self.expiry = get_next_expiry(self.index_name, self.trading_date)
        
        self.api = AngelAPI()
        self.api.login()
        time_module.sleep(1)

        # Load Config
        self.config_file = Path("config/itm_momentum.yaml")
        self.last_config_mtime = 0
        self.sl_points = 10
        self.target_points = 15
        self.trailing_sl_tiers = []
        self.ce_setup_allowed = True
        self.pe_setup_allowed = True
        self.current_ce_setup_discard = False
        self.current_pe_setup_discard = False
        self.allowed_trading_hours = {h: True for h in range(9, 15)}
        self.slippage_buffer = 0.5
        self.telegram: TelegramBot | None = None
        self.ws = None
        self._load_config()

        # State Variables
        self.latest_ltp: Dict[str, float] = {}
        self.last_candle_check_minute = -1
        
        self.active_pe_setup: Dict[str, Any] | None = None
        self.active_ce_setup: Dict[str, Any] | None = None
        self.in_position = False
        self.position_info: Dict[str, Any] = {}
        self.closed_pnl = 0.0
        self.last_exit_time: datetime | None = None
        self.is_paused = False # Controls whether we scan for new trades
        self.max_daily_loss = 1500
        self.profit_lock_threshold = 3000
        self.profit_drawdown_limit = 1500
        self.profit_locking_active = False
        self.profit_protection_config = {}
        self.peak_closed_pnl = 0.0
        
        # State Persistence
        self.state_dir = Path("data/state")
        self.state_dir.mkdir(parents=True, exist_ok=True)
        
        self.subscribed_tokens = {self.index_token} # Always subscribe to Spot

        log.info(f"{Fore.CYAN}Strategy Initialized: {self.index_name} | Expiry: {self.expiry} | Mode: {'SIMULATION' if simulate_orders else 'LIVE'}{Style.RESET_ALL}")

        # Try to resume state
        self._load_state()

        if self.telegram:
            mode = "SIMULATION" if simulate_orders else "LIVE"
            self.telegram.send_message(f"🤖 <b>Bot Started</b>\nIndex: {self.index_name}\nMode: {mode}")

    def _load_config(self) -> bool:
        updated = False
        # Defaults
        default_lot = 65 if self.index_name == "NIFTY" else 20
        
        if not hasattr(self, 'lot_size'):
             self.lot_size = default_lot
             self.num_lots = 1
             self.quantity = self.lot_size * self.num_lots
        
        if self.config_file.exists():
            try:
                mtime = os.path.getmtime(self.config_file)
                if mtime > self.last_config_mtime:
                    with open(self.config_file, "r") as f:
                        config = yaml.safe_load(f)
                    
                    idx_config = config.get(self.index_name, {})
                    self.lot_size = idx_config.get("lot_size", self.lot_size)
                    self.num_lots = idx_config.get("num_lots", self.num_lots)
                    self.sl_points = idx_config.get("sl_points", self.sl_points)
                    self.target_points = idx_config.get("target_points", self.target_points)
                    self.max_daily_loss = idx_config.get("max_daily_loss", 1500)
                    self.slippage_buffer = idx_config.get("slippage_buffer", 0.5)
                    self.profit_lock_threshold = idx_config.get("profit_lock_threshold", 3000)
                    self.profit_drawdown_limit = idx_config.get("profit_drawdown_limit", 1500)
                    self.profit_protection_config = idx_config.get("profit_protection", {})
                    
                    self.trailing_sl_tiers = idx_config.get("trailing_sl_tiers", [])
                    if not self.trailing_sl_tiers:
                        act = idx_config.get("trailing_activation_points", 10)
                        dist = idx_config.get("trailing_distance_points", 10)
                        self.trailing_sl_tiers = [{"activation": act, "distance": dist}]

                    self.ce_setup_allowed = idx_config.get("ce_setup_allowed", self.ce_setup_allowed)
                    self.pe_setup_allowed = idx_config.get("pe_setup_allowed", self.pe_setup_allowed)
                    self.current_ce_setup_discard = idx_config.get("current_ce_setup_discard", False)
                    self.current_pe_setup_discard = idx_config.get("current_pe_setup_discard", False)
                    self.allowed_trading_hours = idx_config.get("allowed_trading_hours", self.allowed_trading_hours)
                    
                    # Load Telegram Config
                    creds_file = Path("config/credentials.yaml")
                    tg_token = ""
                    tg_chat = ""
                    tg_enabled = False
                    
                    if creds_file.exists():
                        log.info(f"Found credentials file: {creds_file}")
                        try:
                            with open(creds_file, "r") as cf:
                                creds = yaml.safe_load(cf) or {}
                                tg_creds = creds.get("TELEGRAM", {})
                                tg_token = tg_creds.get("bot_token", "")
                                tg_chat = str(tg_creds.get("chat_id", ""))
                                tg_enabled = tg_creds.get("enabled", False)
                        except Exception as e:
                            log.error(f"Error loading credentials: {e}")
                    else:
                        log.warning(f"Credentials file not found: {creds_file}")
                    
                    if tg_enabled:
                        if not tg_token or not tg_chat:
                            log.warning(f"⚠️ Telegram enabled but credentials missing! Token found: {bool(tg_token)}, ChatID found: {bool(tg_chat)}")
                        else:
                            log.info(f"✅ Telegram Alerts Enabled. Chat ID: {tg_chat}")
                    else:
                        log.info("ℹ️ Telegram Alerts are DISABLED in credentials.yaml.")
                    
                    # Only recreate bot if credentials changed to avoid killing the listener
                    if self.telegram:
                        if self.telegram.bot_token != tg_token or self.telegram.chat_id != tg_chat:
                            self.telegram.stop_listening()
                            self.telegram = TelegramBot(tg_token, tg_chat, tg_enabled)
                            # If strategy is already running, restart listener
                            if self.ws: 
                                self.telegram.start_listening(self._handle_telegram_command)
                        else:
                            self.telegram.enabled = tg_enabled
                    else:
                        self.telegram = TelegramBot(tg_token, tg_chat, tg_enabled)

                    self.quantity = self.lot_size * self.num_lots
                    self.last_config_mtime = mtime
                    log.info(f"Config Loaded: Qty={self.quantity}, SL={self.sl_points}, Target={self.target_points}, AllowedHours={list(self.allowed_trading_hours.keys())}")
                    
                    if self.telegram:
                        self.telegram.send_message(
                            f"⚙️ <b>Config Loaded ({self.trading_date})</b>\n"
                            f"SL: {self.sl_points} | Target: {self.target_points}\n"
                            f"Qty: {self.quantity} | Max Loss: {self.max_daily_loss}\n"
                            f"Profit Lock: {self.profit_lock_threshold} | Drawdown: {self.profit_drawdown_limit}"
                        )
                    updated = True
            except Exception as e:
                log.error(f"Error loading config: {e}")
        return updated

    def _get_state_file(self) -> Path:
        return self.state_dir / f"itm_state_{self.index_name}_{self.trading_date}.json"

    def _save_state(self):
        """Saves current strategy state to JSON."""
        def serialize_setup(setup):
            if not setup: return None
            s = setup.copy()
            s['contract'] = asdict(s['contract'])
            s['ts'] = s['ts'].isoformat()
            return s

        def serialize_pos(pos):
            if not pos: return {}
            p = pos.copy()
            if 'contract' in p:
                p['contract'] = asdict(p['contract'])
            return p

        state = {
            "active_pe_setup": serialize_setup(self.active_pe_setup),
            "active_ce_setup": serialize_setup(self.active_ce_setup),
            "in_position": self.in_position,
            "position_info": serialize_pos(self.position_info),
            "closed_pnl": self.closed_pnl,
            "is_paused": self.is_paused,
            "profit_locking_active": self.profit_locking_active,
            "peak_closed_pnl": self.peak_closed_pnl,
            # Note: We don't persist order_ids for pending setups across restarts for safety (risk of orphan orders)
            "subscribed_tokens": list(self.subscribed_tokens)
        }
        
        try:
            with open(self._get_state_file(), 'w') as f:
                json.dump(state, f, indent=4)
        except Exception as e:
            log.error(f"Failed to save state: {e}")

    def _load_state(self):
        """Loads strategy state from JSON."""
        state_file = self._get_state_file()
        if not state_file.exists(): return

        try:
            with open(state_file, 'r') as f:
                state = json.load(f)
            
            def deserialize_contract(d):
                return OptionContract(**d)

            def deserialize_setup(d):
                if not d: return None
                d['contract'] = deserialize_contract(d['contract'])
                d['ts'] = datetime.fromisoformat(d['ts'])
                return d

            def deserialize_pos(d):
                if not d: return {}
                if 'contract' in d:
                    d['contract'] = deserialize_contract(d['contract'])
                return d

            self.active_pe_setup = deserialize_setup(state.get("active_pe_setup"))
            self.active_ce_setup = deserialize_setup(state.get("active_ce_setup"))
            self.in_position = state.get("in_position", False)
            self.position_info = deserialize_pos(state.get("position_info"))
            self.closed_pnl = state.get("closed_pnl", 0.0)
            self.is_paused = state.get("is_paused", False)
            self.profit_locking_active = state.get("profit_locking_active", False)
            self.peak_closed_pnl = state.get("peak_closed_pnl", 0.0)
            
            saved_tokens = state.get("subscribed_tokens", [])
            self.subscribed_tokens.update(saved_tokens)

            log.info(f"{Fore.GREEN}State Resumed from {state_file}{Style.RESET_ALL}")
            if self.in_position:
                c = self.position_info['contract']
                log.info(f"Resumed Active Position: {c.symbol} | Entry: {self.position_info['entry_price']}")
            if self.active_pe_setup:
                log.info(f"Resumed PE Setup: {self.active_pe_setup['contract'].symbol}")
            if self.active_ce_setup:
                log.info(f"Resumed CE Setup: {self.active_ce_setup['contract'].symbol}")
            log.info(f"Resumed Stats: Closed PnL={self.closed_pnl}, Paused={self.is_paused}, ProfitLock={self.profit_locking_active}, PeakPnL={self.peak_closed_pnl}")

        except Exception as e:
            log.error(f"Failed to load state: {e}")

    def _is_trading_allowed(self, t: dt_time) -> bool:
        """Checks if the current time falls into an allowed trading hour slot."""
        # Slot logic: 9:15-10:15 is slot 9, 10:15-11:15 is slot 10, etc.
        # If minute >= 15, slot = hour. Else slot = hour - 1.
        slot = t.hour if t.minute >= 15 else t.hour - 1
        
        # Valid slots are 9 to 14 (covering 09:15 to 15:15)
        if slot < 9 or slot > 14:
            return False
            
        # Special case for Slot 14: Stop strictly at 15:00
        if slot == 14 and t.hour == 15:
            return False
            
        return self.allowed_trading_hours.get(slot, False)

    def _handle_telegram_command(self, command: str):
        """Callback for Telegram commands."""
        cmd = command.lower()
        if cmd in ["/stop", "stop"]:
            self.is_paused = True
            msg = "🛑 <b>Strategy PAUSED</b>\nNo new setups will be scanned or triggered.\nExisting positions will be managed."
            log.warning("Telegram command received: STOP")
            if self.telegram: self.telegram.send_message(msg)

            # Clear all active setups and cancel pending orders
            log.info("Clearing all active setups due to STOP command.")
            self._clear_setup("CE")
            self._clear_setup("PE")

            self._save_state()
            
            # Close WS if not in position to save resources
            if not self.in_position and self.ws and hasattr(self.ws, 'wsapp') and self.ws.wsapp:
                log.info("Pausing strategy: Closing WebSocket...")
                self.ws.wsapp.close()
        
        elif cmd in ["/start", "start"]:
            self.is_paused = False
            msg = "✅ <b>Strategy RESUMED</b>\nScanning for new setups..."
            log.info("Telegram command received: START")
            if self.telegram: self.telegram.send_message(msg)
            self._save_state()
            
        elif cmd in ["/status", "status"]:
            status = "🔴 PAUSED" if self.is_paused else "🟢 RUNNING"
            pos_status = f"In Position ({self.position_info.get('contract', {}).get('symbol', '')})" if self.in_position else "Scanning"
            mode = "SIM" if self.simulate_orders else "LIVE"
            msg = f"ℹ️ <b>STATUS REPORT [{mode}]</b>\nState: {status}\nActivity: {pos_status}\nClosed PnL: {self.closed_pnl:.2f}"
            if self.telegram: self.telegram.send_message(msg)

    def _cancel_setup_order(self, setup: dict) -> bool:
        """Cancels the pending SL-L order associated with a setup. Returns True if successful."""
        if not setup or 'order_id' not in setup or not setup['order_id']:
            return True
            
        order_id = setup['order_id']
        
        try:
            # Fetch status first to ensure we know what we are cancelling
            status_resp = self.api.get_order_status(order_id)
            current_status = status_resp.get('status', 'UNKNOWN') if status_resp else 'UNKNOWN'
            
            log.info(f"Cancelling pending setup order: {order_id} | Current Status: {current_status}")
            
            if current_status == 'complete':
                log.error(f"⚠️ CRITICAL: Setup order {order_id} was FILLED! Cannot cancel.")
                return False
            elif current_status in ['cancelled', 'rejected']:
                log.info(f"Order {order_id} is already {current_status}.")
                return True
            
            # SL-L orders are placed with STOPLOSS variety
            response = self.api.connection.cancelOrder(order_id, variety="STOPLOSS")
            log.info(f"✅ Cancel Response for {order_id}: {response}")
            return True
        except Exception as e:
            log.error(f"Failed to cancel setup order {order_id}: {e}")
            return False

    def _clear_setup(self, setup_type: str):
        """Clears the setup and cancels any pending orders."""
        if setup_type == "PE":
            if self.active_pe_setup:
                if not self.simulate_orders: self._cancel_setup_order(self.active_pe_setup)
                # Remove token from subscription list
                token = self.active_pe_setup['contract'].token
                if token in self.subscribed_tokens:
                    self.subscribed_tokens.remove(token)
                self.active_pe_setup = None
        elif setup_type == "CE":
            if self.active_ce_setup:
                if not self.simulate_orders: self._cancel_setup_order(self.active_ce_setup)
                # Remove token from subscription list
                token = self.active_ce_setup['contract'].token
                if token in self.subscribed_tokens:
                    self.subscribed_tokens.remove(token)
                self.active_ce_setup = None
        self._save_state()

    def _get_itm_strike(self, spot: float, option_type: str) -> int:
        """
        Returns the nearest ITM/ATM strike.
        CE = Floor(Spot), PE = Ceil(Spot)
        """
        strike = 0
        if option_type == "CE":
            strike = int(math.floor(spot / self.strike_step) * self.strike_step)
        else:
            strike = int(math.ceil(spot / self.strike_step) * self.strike_step)
        
        log.info(f"Strike Selection: Input Spot={spot}, Type={option_type}, Step={self.strike_step} -> Selected Strike={strike}")
        return strike

    def _fetch_last_n_candles(self, token: str, exchange: str, n: int = 3) -> List[List]:
        """Fetches the last N completed 1-minute candles."""
        now = datetime.now()
        # Fetch slightly more history to ensure we get N completed candles
        from_time = (now - timedelta(minutes=n + 5)).strftime("%Y-%m-%d %H:%M")
        to_time = now.strftime("%Y-%m-%d %H:%M")
        
        try:
            data = self.api.connection.getCandleData({
                "exchange": exchange, "symboltoken": token, "interval": "ONE_MINUTE",
                "fromdate": from_time, "todate": to_time
            })
            candles = data.get("data", [])
            if not candles: return []
            
            # Parse and sort by timestamp
            parsed_candles = []
            for c in candles:
                # c format: [timestamp, open, high, low, close, volume]
                ts = datetime.strptime(c[0], "%Y-%m-%dT%H:%M:%S%z").replace(tzinfo=None)
                parsed_candles.append({
                    "ts": ts, "open": float(c[1]), "high": float(c[2]), 
                    "low": float(c[3]), "close": float(c[4])
                })
            
            # Filter out the current incomplete candle (if any)
            # Usually API returns completed candles, but safe to check against current minute
            completed_candles = [c for c in parsed_candles if c["ts"].minute != now.minute]
            return completed_candles[-n:]
        except Exception as e:
            log.error(f"Error fetching candles: {e}")
            return []

    def _get_option_high_at_time(self, contract: OptionContract, target_time: datetime) -> float | None:
        """Fetches the High price of an option for a specific 1-minute candle."""
        from_str = target_time.strftime("%Y-%m-%d %H:%M")
        to_str = (target_time + timedelta(minutes=1)).strftime("%Y-%m-%d %H:%M")
        
        max_retries = 10
        for attempt in range(max_retries):
            try:
                data = self.api.connection.getCandleData({
                    "exchange": contract.exchange, "symboltoken": contract.token, "interval": "ONE_MINUTE",
                    "fromdate": from_str, "todate": to_str
                })
                candles = data.get("data", [])
                if candles:
                    return float(candles[0][2]) # High is index 2
            except Exception as e:
                log.warning(f"Attempt {attempt+1}/{max_retries}: Error fetching option history for {contract.symbol}: {e}")
            
            if attempt < max_retries - 1:
                time_module.sleep(1)
        
        log.error(f"Failed to fetch option high for {contract.symbol} after {max_retries} attempts.")
        return None

    def _get_prev_candle_open(self, contract) -> tuple[float, datetime] | None:
        """Fetches the previous completed candle's open price and time for the option."""
        try:
            # Fetch last 2 completed candles
            candles = self._fetch_last_n_candles(contract.token, contract.exchange, 2)
            if candles:
                # The last element is the most recent completed candle
                return candles[-1]['open'], candles[-1]['ts']
        except Exception as e:
            log.warning(f"Failed to fetch prev candle open: {e}")
        return None

    def _check_for_setup(self):
        """Analyzes the last 3 candles for the momentum pattern."""
        # Fetch 4 candles to check continuity (c0, c1, c2, c3)
        candles = self._fetch_last_n_candles(self.index_token, self.index_exchange, 4)
        if len(candles) < 3: return

        if len(candles) == 4:
            c0, c1, c2, c3 = candles[0], candles[1], candles[2], candles[3]
        else:
            c0 = None
            c1, c2, c3 = candles[0], candles[1], candles[2]
        
        # Avoid overlap with previous trade: Ensure the pattern starts AFTER the last exit
        if self.last_exit_time and c1['ts'] <= self.last_exit_time:
            return

        # Check 3 Green Candles (Put Setup)
        # Strict Trend Check: Higher Lows for Green sequence (Clean uptrend)
        is_clean_uptrend = c2['low'] >= c1['low'] and c3['low'] >= c2['low']
        
        if self.pe_setup_allowed and not self.current_pe_setup_discard and c1['close'] > c1['open'] and c2['close'] > c2['open'] and c3['close'] > c3['open'] and is_clean_uptrend:
            is_continuation = False
            if self.active_pe_setup:
                # If previous candle was also Green, it's a continuation.
                if c0 and c0['close'] > c0['open']:
                    is_continuation = True
                    log.info(f"  Continuation of Green leg. Ignoring update.")
            
            if not is_continuation:
                # New PE Setup detected (or replacing old one)
                self._initiate_setup("PE", c1, c3['close'])

        # Check 3 Red Candles (Call Setup)
        # Strict Trend Check: Lower Highs for Red sequence (Clean downtrend)
        is_clean_downtrend = c2['high'] <= c1['high'] and c3['high'] <= c2['high']
        
        if self.ce_setup_allowed and not self.current_ce_setup_discard and c1['close'] < c1['open'] and c2['close'] < c2['open'] and c3['close'] < c3['open'] and is_clean_downtrend:
            is_continuation = False
            if self.active_ce_setup:
                # If previous candle was also Red, it's a continuation.
                if c0 and c0['close'] < c0['open']:
                    is_continuation = True
                    log.info(f"  Continuation of Red leg. Ignoring update.")

            if not is_continuation:
                # New CE Setup detected (or replacing old one)
                self._initiate_setup("CE", c1, c3['close'])

    def _initiate_setup(self, setup_type: str, ref_candle: dict, current_spot: float):
        """Prepares the setup triggers."""
        # 1. Define Triggers
        spot_trigger = ref_candle['low'] if setup_type == "PE" else ref_candle['high']

        # 2. Select ITM Strike based on SPOT TRIGGER (Entry Level)
        strike = self._get_itm_strike(spot_trigger, setup_type)
        contract = find_option(self.index_name, strike, setup_type, self.expiry, self.trading_date)
        
        # 3. Get Option High during the Reference Candle (1st candle)
        opt_high = self._get_option_high_at_time(contract, ref_candle['ts'])
        
        if opt_high is None:
            log.error(f"Could not fetch history for {contract.symbol}. Setup aborted.")
            return

        setup_data = {
            "type": setup_type,
            "contract": contract,
            "spot_trigger": spot_trigger,
            "opt_trigger": opt_high,
            "ts": datetime.now(),
            "order_id": None # Will be populated for live orders
        }
        
        # --- HARDCORE ALGO: Pre-place Stop Loss Limit Order ---
        if not self.simulate_orders:
            try:
                # If replacing an existing setup of same type, cancel old order first
                if setup_type == "PE" and self.active_pe_setup:
                    if not self._cancel_setup_order(self.active_pe_setup):
                        log.warning("Skipping PE setup update: Could not cancel existing order. Retrying next tick.")
                        return
                elif setup_type == "CE" and self.active_ce_setup:
                    if not self._cancel_setup_order(self.active_ce_setup):
                        log.warning("Skipping CE setup update: Could not cancel existing order. Retrying next tick.")
                        return

                limit_price = round(opt_high + self.slippage_buffer, 1)
                trigger_price = round(opt_high, 1)
                
                log.info(f"Placing Pending SL-L Order: Trigger {trigger_price}, Limit {limit_price}")
                
                orderparams = {
                    "variety": "STOPLOSS",
                    "tradingsymbol": contract.symbol,
                    "symboltoken": contract.token,
                    "transactiontype": "BUY",
                    "exchange": contract.exchange,
                    "ordertype": "STOPLOSS_LIMIT",
                    "producttype": "INTRADAY",
                    "duration": "DAY",
                    "price": limit_price,
                    "triggerprice": trigger_price,
                    "quantity": self.quantity
                }
                order_id = self.api.connection.placeOrder(orderparams)
                setup_data['order_id'] = order_id
                log.info(f"Pending Order Placed: {order_id}")
            except Exception as e:
                log.error(f"Failed to place pending order: {e}")
                return # Don't arm setup if order failed
        
        if setup_type == "PE":
            # If replacing an existing setup, remove the old token from subscriptions
            if self.active_pe_setup and self.active_pe_setup['contract'].token != contract.token:
                old_token = self.active_pe_setup['contract'].token
                if old_token in self.subscribed_tokens:
                    self.subscribed_tokens.remove(old_token)
            self.active_pe_setup = setup_data
        else:
            if self.active_ce_setup and self.active_ce_setup['contract'].token != contract.token:
                old_token = self.active_ce_setup['contract'].token
                if old_token in self.subscribed_tokens:
                    self.subscribed_tokens.remove(old_token)
            self.active_ce_setup = setup_data
        
        # Subscribe to the option token for live monitoring
        self._subscribe_to_token(contract.token)
        
        setup_time_str = setup_data['ts'].strftime("%H:%M:%S")
        log.info(
            f"{Fore.GREEN}SETUP ARMED ({setup_type}) at {setup_time_str}: {contract.symbol} | "
            f"Spot Trigger: Break {'Below' if setup_type=='PE' else 'Above'} {spot_trigger} | "
            f"Option Trigger: Break Above {opt_high}{Style.RESET_ALL}"
        )
        
        if self.telegram:
            prefix = "[SIM] " if self.simulate_orders else ""
            msg = f"⚠️ <b>{prefix}SETUP ARMED: {self.index_name} {setup_type}</b>\n" \
                  f"Order ID: {setup_data.get('order_id', 'N/A')}\n" \
                  f"Symbol: {contract.symbol}\n" \
                  f"Spot Trigger: {spot_trigger}\n" \
                  f"Opt Trigger: {opt_high}\n" \
                  f"Time: {setup_time_str}"
            self.telegram.send_message(msg)
        self._save_state()

    def _subscribe_to_token(self, token: str):
        if token not in self.subscribed_tokens:
            self.subscribed_tokens.add(token)
            if self.ws and self.ws.wsapp and self.ws.wsapp.sock and self.ws.wsapp.sock.connected:
                # Exchange Type 1=NSE (Spot), 2=NFO (Opt)
                # We need to split tokens by exchange type
                spot_tokens = [t for t in self.subscribed_tokens if t == self.index_token]
                opt_tokens = [t for t in self.subscribed_tokens if t != self.index_token]
                
                req_list = []
                if spot_tokens:
                    req_list.append({"exchangeType": 1 if self.index_name=="NIFTY" else 3, "tokens": spot_tokens})
                if opt_tokens:
                    req_list.append({"exchangeType": 2 if self.index_name=="NIFTY" else 4, "tokens": opt_tokens})
                
                self.ws.subscribe("itm_strategy", 3, req_list)

    def _log_trade(self, contract, entry_price, exit_price, pnl, reason, max_points=0.0):
        """Logs the completed trade to a CSV file for the dashboard."""
        filename = "itm_trades_sim.csv" if self.simulate_orders else "itm_trades.csv"
        file_path = Path(f"data/live/{filename}")
        file_path.parent.mkdir(parents=True, exist_ok=True)
        file_exists = file_path.exists()
        
        try:
            with open(file_path, mode='a', newline='') as f:
                writer = csv.writer(f)
                if not file_exists:
                    writer.writerow(["Date", "Index", "Symbol", "Type", "Setup Time", "Entry Time", "Entry Price", "Exit Time", "Exit Price", "PnL", "Reason", "Max Points", "Quantity"])
                
                writer.writerow([
                    self.trading_date,
                    self.index_name,
                    contract.symbol,
                    contract.option_type,
                    self.position_info.get("setup_time", ""),
                    self.position_info.get("entry_time", ""),
                    entry_price,
                    datetime.now().strftime("%H:%M:%S"),
                    exit_price,
                    f"{pnl:.2f}",
                    reason,
                    f"{max_points:.2f}",
                    self.position_info.get("qty", 0)
                ])
        except Exception as e:
            log.error(f"Failed to log trade to CSV: {e}")

    def _wait_for_fill(self, order_id: str, timeout: int = 60) -> float | None:
        """Polls the order book to confirm fill and get actual price."""
        log.info(f"Polling for fill: Order ID {order_id}")
        start_time = time_module.time()
        
        while time_module.time() - start_time < timeout:
            try:
                # Fetch order status
                status = self.api.get_order_status(order_id)
                
                if status:
                    order_status = status.get('status') # 'complete', 'rejected', 'open', 'cancelled'
                    if order_status == 'complete':
                        avg_price = float(status.get('averageprice', 0.0))
                        log.info(f"✅ Order filled at {avg_price}")
                        return avg_price
                    elif order_status in ['rejected', 'cancelled']:
                        log.error(f"❌ Order {order_status}: {status.get('text')}")
                        return None
            except Exception as e:
                log.error(f"Polling Error: {e}")
            
            time_module.sleep(1)
            
        log.warning(f"⚠️ Order {order_id} not filled within {timeout}s timeout.")
        return None

    def _wait_for_fill_sim(self, contract, limit_price, timeout=60) -> float | None:
        """Simulates polling for a fill by checking LTP via REST API."""
        log.info(f"SIMULATION: Polling for fill for {contract.symbol} @ {limit_price}...")
        start_time = time_module.time()
        
        while time_module.time() - start_time < timeout:
            try:
                # Fetch current LTP via REST to avoid blocking WS issues
                curr_ltp = self.api.get_ltp(contract.exchange, contract.symbol, contract.token)
                if curr_ltp is not None and curr_ltp <= limit_price:
                    log.info(f"✅ SIMULATION: Order filled at {curr_ltp} (<= {limit_price})")
                    return limit_price
            except Exception as e:
                log.error(f"Simulation Polling Error: {e}")
            
            time_module.sleep(1)
            
        log.warning(f"⚠️ SIMULATION: Order not filled within {timeout}s timeout.")
        return None

    def _execute_entry(self, filled_price: float, setup: dict):
        """Transitions state to 'in_position' after an order is confirmed filled."""
        contract = setup['contract']
        setup_time_str = setup['ts'].strftime("%H:%M:%S")
        
        if self.simulate_orders:
            log.info(f"{Fore.GREEN}*** SIM ENTRY TRIGGERED *** {contract.symbol} @ {filled_price}{Style.RESET_ALL}")
        else:
            log.info(f"{Fore.GREEN}*** LIVE ENTRY CONFIRMED *** {contract.symbol} @ {filled_price} | Order: {setup.get('order_id')}{Style.RESET_ALL}")
        
        # Determine SL/Target based on Profit Protection Mode
        current_sl_pts = self.sl_points
        current_target_pts = self.target_points
        
        if self.profit_locking_active and self.profit_protection_config and self.profit_protection_config.get('enable_scalp_mode', True):
            current_sl_pts = self.profit_protection_config.get('sl_points', current_sl_pts)
            current_target_pts = self.profit_protection_config.get('target_points', current_target_pts)
            log.info(f"🛡️ Profit Protection Mode Active: Using Scalp SL={current_sl_pts}, Target={current_target_pts}")

        self.in_position = True
        self.position_info = {
            "contract": contract,
            "entry_price": filled_price,
            "sl": filled_price - current_sl_pts,
            "target": filled_price + current_target_pts,
            "qty": self.quantity,
            "highest_ltp": filled_price,
            "entry_time": datetime.now().strftime("%H:%M:%S"),
            "setup_time": setup_time_str,
        }
        
        # Clear setups: Just clear the triggered one (order filled), cancel the other (pending)
        if setup['type'] == 'PE':
            self.active_pe_setup = None
            self._clear_setup("CE")
        else:
            self.active_ce_setup = None
            self._clear_setup("PE")

        log.info(f"Position Active: Target {self.position_info['target']:.2f} | SL {self.position_info['sl']:.2f}")
        
        if self.telegram:
            prefix = "[SIM] " if self.simulate_orders else ""
            price_type = "Limit" if self.simulate_orders else "Filled"
            msg = f"🚀 <b>{prefix}ENTRY TRIGGERED: {self.index_name}</b>\n" \
                  f"Symbol: {contract.symbol}\n" \
                  f"Price: {filled_price} ({price_type})\n" \
                  f"Target: {self.position_info['target']:.2f}\n" \
                  f"SL: {self.position_info['sl']:.2f}"
            self.telegram.send_message(msg)
        self._save_state()

    def _execute_exit(self, ltp: float, reason: str):
        pos = self.position_info
        contract = pos['contract']
        exit_price = ltp
        
        if not self.simulate_orders:
            try:
                order_id = self.api.place_order(contract.symbol, contract.token, pos['qty'], "SELL", product_type="INTRADAY")
                log.info(f"Exit Order Placed: {order_id}")
                filled_price = self._wait_for_fill(order_id)
                if filled_price:
                    exit_price = filled_price
            except Exception as e:
                log.error(f"Exit order failed: {e}")

        pnl = (exit_price - pos['entry_price']) * pos['qty']
        
        exit_time_str = datetime.now().strftime("%H:%M:%S")
        log.info(f"{Fore.MAGENTA}*** EXIT TRIGGERED ({reason}) at {exit_time_str} *** {contract.symbol} @ {exit_price} | PNL: {pnl:.2f}{Style.RESET_ALL}")
        
        # Log trade to CSV for Dashboard
        max_points = pos.get('highest_ltp', pos['entry_price']) - pos['entry_price']
        self._log_trade(contract, pos['entry_price'], exit_price, pnl, reason, max_points)

        self.closed_pnl += pnl
        log.info(f"💰 Total Closed PnL: {self.closed_pnl:.2f}")
        self.in_position = False
        
        # --- Profit Maximization & Protection Logic ---
        if not self.profit_locking_active:
            # Check if we crossed the threshold to activate locking
            if self.closed_pnl >= self.profit_lock_threshold:
                self.profit_locking_active = True
                self.peak_closed_pnl = self.closed_pnl
                log.info(f"{Fore.GREEN}Profit Locking Activated! PnL ({self.closed_pnl:.2f}) >= Threshold ({self.profit_lock_threshold}). Peak set to {self.peak_closed_pnl:.2f}{Style.RESET_ALL}")
                if self.telegram:
                    prefix = "[SIM] " if self.simulate_orders else ""
                    self.telegram.send_message(f"💰 <b>{prefix}Profit Locking Activated</b>\nCurrent PnL: {self.closed_pnl:.2f}\nDrawdown Limit: {self.profit_drawdown_limit}")
        else:
            # Update peak if current PnL is higher
            if self.closed_pnl > self.peak_closed_pnl:
                self.peak_closed_pnl = self.closed_pnl
                log.info(f"{Fore.GREEN}New Peak PnL: {self.peak_closed_pnl:.2f}{Style.RESET_ALL}")
            
            # Check for drawdown from peak
            current_drawdown_limit = self.profit_drawdown_limit
            if self.profit_protection_config:
                current_drawdown_limit = self.profit_protection_config.get('drawdown_limit', current_drawdown_limit)
            
            drawdown = self.peak_closed_pnl - self.closed_pnl
            
            if drawdown >= current_drawdown_limit:
                log.warning(f"{Fore.RED}Profit drawdown limit reached. Peak: {self.peak_closed_pnl:.2f}, Current: {self.closed_pnl:.2f}, Drawdown: {drawdown:.2f} >= {current_drawdown_limit}. Stopping trading.{Style.RESET_ALL}")
                self.is_paused = True
                if self.telegram:
                    prefix = "[SIM] " if self.simulate_orders else ""
                    self.telegram.send_message(f"🛑 <b>{prefix}Profit Protection Hit</b>\nTrading stopped.\nPeak PnL: {self.peak_closed_pnl:.2f}\nCurrent PnL: {self.closed_pnl:.2f}\nLimit: {current_drawdown_limit}")

        # Check for max daily loss
        if self.closed_pnl <= -self.max_daily_loss:
            log.warning(f"{Fore.RED}Max daily loss limit reached ({self.closed_pnl:.2f} <= -{self.max_daily_loss}). Stopping trading for the day.{Style.RESET_ALL}")
            self.is_paused = True
            if self.telegram:
                prefix = "[SIM] " if self.simulate_orders else ""
                self.telegram.send_message(f"🛑 <b>{prefix}Max Daily Loss Reached</b>\nTrading stopped for the day.\nPnL: {self.closed_pnl:.2f}")
            
            # Close WS to enforce standby mode in run loop
            if self.ws and hasattr(self.ws, 'wsapp') and self.ws.wsapp:
                log.info("Max loss reached: Closing WebSocket to pause strategy.")
                self.ws.wsapp.close()

        self.last_exit_time = datetime.now()
        self.position_info = {}
        # Reset subscribed tokens to just Spot to save bandwidth/confusion
        self.subscribed_tokens = {self.index_token}
        # Note: In a robust system, we would unsubscribe from the option token here.

        if self.telegram:
            emoji = "✅" if pnl >= 0 else "❌"
            prefix = "[SIM] " if self.simulate_orders else ""
            msg = f"{emoji} <b>{prefix}EXIT: {self.index_name} ({reason})</b>\n" \
                  f"Symbol: {contract.symbol}\n" \
                  f"Exit Price: {exit_price}\n" \
                  f"PnL: {pnl:.2f}"
            self.telegram.send_message(msg)
        self._save_state()

    def _process_tick(self, token: str, ltp: float):
        # Check for dynamic config updates
        if self._load_config() and self.in_position:
            entry = self.position_info['entry_price']

            # Determine parameters based on Profit Protection Mode
            current_sl_pts = self.sl_points
            current_target_pts = self.target_points
            
            is_scalp_mode = self.profit_locking_active and self.profit_protection_config and self.profit_protection_config.get('enable_scalp_mode', True)

            if is_scalp_mode:
                current_sl_pts = self.profit_protection_config.get('sl_points', current_sl_pts)
                current_target_pts = self.profit_protection_config.get('target_points', current_target_pts)

            self.position_info['target'] = entry + current_target_pts
            
            # Recalculate SL based on new config and current high
            highest = self.position_info.get('highest_ltp', entry)
            peak_pts = highest - entry
            
            new_sl = entry - current_sl_pts # Base SL
            
            if not is_scalp_mode:
                # Find applicable tier
                active_tier = None
                for tier in sorted(self.trailing_sl_tiers, key=lambda x: x['activation'], reverse=True):
                    if peak_pts >= tier['activation']:
                        active_tier = tier
                        break
                
                if active_tier:
                    if 'distance' in active_tier:
                        trail_sl = highest - active_tier['distance']
                        new_sl = max(new_sl, trail_sl)
                    elif 'fix_sl' in active_tier:
                        fix_sl_level = entry + active_tier['fix_sl']
                        new_sl = max(new_sl, fix_sl_level)
                    # Candle open trail is handled in the main loop below, so we skip it here to avoid API calls
            
            self.position_info['sl'] = new_sl
            log.info(f"Position Limits Updated: Target {self.position_info['target']:.2f} | SL {self.position_info['sl']:.2f}")
        
        # Check for discard requests (regardless of position state, but mostly for armed setups)
        if self.current_ce_setup_discard and self.active_ce_setup:
            log.warning(f"{Fore.YELLOW}CONFIG REQUEST: Discarding active CE setup {self.active_ce_setup['contract'].symbol} as requested.{Style.RESET_ALL}")
            self._clear_setup("CE")

        if self.current_pe_setup_discard and self.active_pe_setup:
            log.warning(f"{Fore.YELLOW}CONFIG REQUEST: Discarding active PE setup {self.active_pe_setup['contract'].symbol} as requested.{Style.RESET_ALL}")
            self._clear_setup("PE")

        now = datetime.now()
        
        # 1. Check Time Window (Configurable Hourly Slots)
        # If outside allowed hours and not in position AND no active setups, stop processing (no new scans)
        has_active_setups = (self.active_pe_setup is not None) or (self.active_ce_setup is not None)
        if not self._is_trading_allowed(now.time()) and not self.in_position and not has_active_setups:
            return

        # Check for max daily loss
        if self.closed_pnl <= -self.max_daily_loss and not self.in_position:
            return

        # 2. Minute-based Candle Check (Only runs once per minute)
        if now.minute != self.last_candle_check_minute:
            if self.in_position:
                # Only fetch candle data if we actually have a tier that needs it
                uses_candle_trail = any(t.get('trail_type') == 'candle_open' for t in self.trailing_sl_tiers)
                
                if uses_candle_trail:
                    # Optimization: Fetch candle data once per minute for trailing logic
                    result = self._get_prev_candle_open(self.position_info['contract'])
                    if result:
                        self.position_info['prev_candle_open'] = result[0]
                        self.position_info['prev_candle_ts'] = result[1]
                        log.info(f"Updated Position Candle Data: Prev Open={result[0]} at {result[1].strftime('%H:%M')}")
            elif not self.is_paused and self._is_trading_allowed(now.time()):
                self._check_for_setup()
            self.last_candle_check_minute = now.minute

        # 3. Check Triggers (If Setup Active)
        if not self.in_position:
            if self.is_paused:
                return # Skip trigger checks if paused

            # --- TRIGGER CHECK (Both Sim and Live) ---
            # We monitor prices locally. If conditions are met, we assume the broker order (if live) 
            # has triggered or is about to trigger, and we verify it.
            
            spot_ltp = self.latest_ltp.get(self.index_token)
            
            # Filter out None setups first
            active_setups = [s for s in [self.active_pe_setup, self.active_ce_setup] if s is not None]
            
            for setup in active_setups:
                opt_ltp = self.latest_ltp.get(setup['contract'].token)
                
                if spot_ltp and opt_ltp:
                    # Check Price Conditions
                    spot_cond = (spot_ltp < setup['spot_trigger']) if setup['type'] == 'PE' else (spot_ltp > setup['spot_trigger'])
                    opt_cond = opt_ltp > setup['opt_trigger']
                    
                    # Determine if we should check for execution
                    trigger_met = False
                    if self.simulate_orders:
                        # Simulation: Strict adherence to strategy (Spot AND Option must trigger)
                        if spot_cond and opt_cond:
                            trigger_met = True
                    else:
                        # Live: The order is physically on the Option. If Option triggers, the broker executes.
                        # We MUST check order status if Option triggers, regardless of Spot.
                        if opt_cond:
                            trigger_met = True

                    if trigger_met:
                        log.info(f"{Fore.YELLOW}Local Trigger Detected: {setup['type']} ({setup['contract'].symbol}) Spot={spot_ltp} Opt={opt_ltp} > {setup['opt_trigger']}{Style.RESET_ALL}")
                        
                        if self.simulate_orders:
                            # In sim, we must wait/verify fill at the trigger price
                            fill_price = self._wait_for_fill_sim(setup['contract'], setup['opt_trigger'])
                            if fill_price:
                                self._execute_entry(fill_price, setup)
                                return
                        else:
                            # In LIVE, since we pre-placed the order, it should be filling now.
                            # We verify the status.
                            order_id = setup.get('order_id')
                            if order_id:
                                # Poll briefly for confirmation
                                fill_price = self._wait_for_fill(order_id, timeout=5)
                                if fill_price:
                                    self._execute_entry(fill_price, setup)
                                    return
                                else:
                                    log.warning(f"Local trigger met but order {order_id} not filled yet. Waiting...")
                                    # We don't cancel here; we let the order sit or next tick handle it.
                                    # If market moves away, we just continue monitoring.

        # 4. Manage Position (TP/SL)
        if self.in_position:
            pos = self.position_info
            if token == pos['contract'].token:
                state_changed = False
                # Update Highest LTP
                if ltp > pos.get('highest_ltp', pos['entry_price']):
                    pos['highest_ltp'] = ltp
                    peak_mtm = (ltp - pos['entry_price']) * pos['qty']
                    log.info(f"{Fore.CYAN}Peak MTM Updated: {peak_mtm:.2f} (LTP: {ltp}){Style.RESET_ALL}")
                    state_changed = True
                
                # Trailing SL Logic
                is_scalp_mode = self.profit_locking_active and self.profit_protection_config and self.profit_protection_config.get('enable_scalp_mode', True)

                if not is_scalp_mode:
                    peak_pts = pos['highest_ltp'] - pos['entry_price']

                    active_tier = None
                    for tier in sorted(self.trailing_sl_tiers, key=lambda x: x['activation'], reverse=True):
                        if peak_pts >= tier['activation']:
                            active_tier = tier
                            break
                    
                    if active_tier:
                        new_sl = pos['sl'] # Start with current SL
                        if 'distance' in active_tier:
                            trail_sl = pos['highest_ltp'] - active_tier['distance']
                            new_sl = max(new_sl, trail_sl)
                        elif 'fix_sl' in active_tier:
                            fix_sl_level = pos['entry_price'] + active_tier['fix_sl']
                            new_sl = max(new_sl, fix_sl_level)
                        elif active_tier.get('trail_type') == 'candle_open':
                            # Trail based on Previous Candle Open
                            # Use cached value to avoid API spam
                            prev_open = pos.get('prev_candle_open')
                            prev_ts = pos.get('prev_candle_ts')
                            
                            # Fallback if cache empty (e.g. right after entry)
                            if prev_open is None:
                                 result = self._get_prev_candle_open(pos['contract'])
                                 if result:
                                     prev_open, prev_ts = result
                                     pos['prev_candle_open'] = prev_open
                                     pos['prev_candle_ts'] = prev_ts
                            
                            if prev_open is not None:
                                # Check for minimum distance constraint (Buffer)
                                min_dist = active_tier.get('min_distance', 0)
                                current_high = pos.get('highest_ltp', pos['entry_price'])
                                fallback_buffer = active_tier.get('fallback_sl_buffer')
                                
                                if (current_high - prev_open) >= min_dist:
                                    potential_sl = max(new_sl, prev_open)
                                    log.info(f"Candle Open Trail (Peak {peak_pts:.2f}): Prev Open={prev_open} (Time: {prev_ts.strftime('%H:%M') if prev_ts else 'N/A'}) | Existing SL={new_sl} -> Result SL={potential_sl}")
                                    new_sl = potential_sl
                                elif fallback_buffer is not None:
                                    # Fallback: Trail by fixed buffer from high if candle open is too close
                                    fallback_sl = current_high - fallback_buffer
                                    potential_sl = max(new_sl, fallback_sl)
                                    log.info(f"Candle Open Fallback (Peak {peak_pts:.2f}): Buffer Not Met. Using Fallback SL={fallback_sl} | Existing SL={new_sl} -> Result SL={potential_sl}")
                                    new_sl = potential_sl

                        if new_sl > pos['sl']:
                            pos['sl'] = new_sl
                            log.info(f"{Fore.MAGENTA}Trailing SL Updated: {new_sl:.2f} (Peak Pts: {peak_pts:.2f}){Style.RESET_ALL}")
                            state_changed = True
                
                if state_changed:
                    self._save_state()

                current_pnl = (ltp - pos['entry_price']) * pos['qty']
                pnl_color = Fore.GREEN if current_pnl >= 0 else Fore.RED
                log.info(f"Position Monitor: {pos['contract'].symbol} LTP: {ltp} | PnL: {pnl_color}{current_pnl:.2f}{Style.RESET_ALL}")

                if ltp >= pos['target']:
                    self._execute_exit(ltp, "TARGET_HIT")
                elif ltp <= pos['sl']:
                    self._execute_exit(ltp, "STOP_LOSS_HIT")

    def _on_tick(self, payload: dict):
        try:
            token = str(payload.get("token") or payload.get("tk"))
            if not token: return
            
            # Ignore ticks for tokens we are no longer interested in
            if token not in self.subscribed_tokens:
                return

            ltp = float(payload.get("last_traded_price") or payload.get("ltp") or payload.get("lp"))
            exchange_type = payload.get("exchange_type")
            # Adjust scaling for Index/Options if needed (Angel sends integer * 100)
            if exchange_type in [1, 2, 3, 4]: 
                ltp /= 100.0
            
            self.latest_ltp[token] = ltp
            self._process_tick(token, ltp)
            
        except Exception as e:
            log.error(f"Tick Error: {e}")

    def _on_ws_message(self, ws, message):
        try:
            data = json.loads(message) if isinstance(message, (bytes, str)) else message
        except Exception: return
        if isinstance(data, list):
            for item in data: self._on_tick(item)
        elif isinstance(data, dict):
            self._on_tick(data)

    def run(self):
        log.info("Starting Strategy Loop...")
        
        if self.telegram:
            self.telegram.start_listening(self._handle_telegram_command)

        # Wait for start time
        now = datetime.now()
        start_time = dt_time(9, 15)
        if now.time() < start_time:
            wait_s = (datetime.combine(now.date(), start_time) - now).total_seconds()
            log.info(f"Waiting {wait_s:.0f}s for market open (9:15)...")
            time_module.sleep(wait_s)

        # --- Main Reconnection Loop ---
        while True:
            # Check for End of Day
            now = datetime.now()
            market_close_time = dt_time(15, 31) # Give a minute buffer after 15:30
            
            if now.time() >= market_close_time:
                log.info("Market closed (15:30). Exiting strategy script.")
                break

            # If paused and not in position, stay in standby mode (no WS connection)
            if self.is_paused and not self.in_position:
                time_module.sleep(1)
                continue

            try:
                if SmartWebSocketV2 is None:
                    log.error("SmartWebSocketV2 library not available. Exiting.")
                    break

                log.info("Connecting to WebSocket...")
                self.ws = SmartWebSocketV2(self.api.jwt_token, self.api.api_key, self.api.client_id, self.api.feed_token)
                
                def on_open(wsapp):
                    log.info("WebSocket Connected. Resubscribing to tracked tokens...")
                    # Subscribe to ALL tokens currently in the set (Spot + Active Options)
                    spot_tokens = [t for t in self.subscribed_tokens if t == self.index_token]
                    opt_tokens = [t for t in self.subscribed_tokens if t != self.index_token]
                    
                    req_list = []
                    if spot_tokens:
                        req_list.append({"exchangeType": 1 if self.index_name=="NIFTY" else 3, "tokens": spot_tokens})
                    if opt_tokens:
                        req_list.append({"exchangeType": 2 if self.index_name=="NIFTY" else 4, "tokens": opt_tokens})
                    
                    if req_list:
                        self.ws.subscribe("itm_resume", 3, req_list)

                self.ws.on_open = on_open
                self.ws.on_data = self._on_ws_message
                self.ws.on_error = lambda ws, err: log.error(f"WS Error: {err}")
                self.ws.connect()
                
                # If connect() returns, it means the connection closed cleanly or dropped
                log.warning("WebSocket connection dropped. Reconnecting in 5 seconds...")
                time_module.sleep(5)
                
            except KeyboardInterrupt:
                log.info("Stopped by user (Ctrl+C). Exiting loop.")
                break # <--- This ensures Ctrl+C kills the script immediately
            except Exception as e:
                log.error(f"Connection Error: {e}. Retrying in 10 seconds...")
                time_module.sleep(10)
            finally:
                # Clean up old websocket object before retrying
                if self.ws and hasattr(self.ws, 'wsapp') and self.ws.wsapp:
                    try:
                        self.ws.wsapp.close()
                    except Exception:
                        pass

        # Cleanup after loop exit (User stopped)
        if self.telegram:
            self.telegram.stop_listening()

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--index", default="NIFTY", help="NIFTY or SENSEX")
    parser.add_argument("--live", action="store_true", help="Execute real orders")
    args = parser.parse_args()

    strategy = ITMMomentumStrategy(
        index_name=args.index,
        simulate_orders=not args.live
    )
    strategy.run()

if __name__ == "__main__":
    main()