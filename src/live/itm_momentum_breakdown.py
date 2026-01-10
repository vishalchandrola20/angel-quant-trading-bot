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
import time as time_module
import json
import yaml
import os
from datetime import datetime, date, time as dt_time, timedelta
from pathlib import Path
from typing import Dict, Any, List

from colorama import Fore, Style, init as colorama_init

from src.api.smartapi_client import AngelAPI
from src.market.contracts import find_option, OptionContract, get_next_expiry

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
        self.trailing_activation_points = 10
        self.trailing_distance_points = 10
        self.ce_setup_allowed = True
        self.pe_setup_allowed = True
        self.current_ce_setup_discard = False
        self.current_pe_setup_discard = False
        self.allowed_trading_hours = {h: True for h in range(9, 15)}
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
        
        self.ws = None
        self.subscribed_tokens = {self.index_token} # Always subscribe to Spot

        log.info(f"{Fore.CYAN}Strategy Initialized: {self.index_name} | Expiry: {self.expiry} | Mode: {'SIMULATION' if simulate_orders else 'LIVE'}{Style.RESET_ALL}")

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
                    self.trailing_activation_points = idx_config.get("trailing_activation_points", self.trailing_activation_points)
                    self.trailing_distance_points = idx_config.get("trailing_distance_points", self.trailing_distance_points)
                    self.ce_setup_allowed = idx_config.get("ce_setup_allowed", self.ce_setup_allowed)
                    self.pe_setup_allowed = idx_config.get("pe_setup_allowed", self.pe_setup_allowed)
                    self.current_ce_setup_discard = idx_config.get("current_ce_setup_discard", False)
                    self.current_pe_setup_discard = idx_config.get("current_pe_setup_discard", False)
                    self.allowed_trading_hours = idx_config.get("allowed_trading_hours", self.allowed_trading_hours)
                    
                    self.quantity = self.lot_size * self.num_lots
                    self.last_config_mtime = mtime
                    log.info(f"Config Loaded: Qty={self.quantity}, SL={self.sl_points}, Target={self.target_points}, AllowedHours={list(self.allowed_trading_hours.keys())}")
                    updated = True
            except Exception as e:
                log.error(f"Error loading config: {e}")
        return updated

    def _is_trading_allowed(self, t: dt_time) -> bool:
        """Checks if the current time falls into an allowed trading hour slot."""
        # Slot logic: 9:15-10:15 is slot 9, 10:15-11:15 is slot 10, etc.
        # If minute >= 15, slot = hour. Else slot = hour - 1.
        slot = t.hour if t.minute >= 15 else t.hour - 1
        
        # Valid slots are 9 to 14 (covering 09:15 to 15:15)
        if slot < 9 or slot > 14:
            return False
            
        return self.allowed_trading_hours.get(slot, False)

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
        if self.pe_setup_allowed and c1['close'] > c1['open'] and c2['close'] > c2['open'] and c3['close'] > c3['open']:
            is_continuation = False
            if self.active_pe_setup:
                if c0 and c0['close'] > c0['open']:
                    is_continuation = True
            
            if not is_continuation:
                log.info(f"{Fore.YELLOW}Pattern Detected: 3 Green Candles (Spot {c3['close']}). Checking Put Setup...{Style.RESET_ALL}")
                self._initiate_setup("PE", c1, c3['close'])

        # Check 3 Red Candles (Call Setup)
        elif self.ce_setup_allowed and c1['close'] < c1['open'] and c2['close'] < c2['open'] and c3['close'] < c3['open']:
            is_continuation = False
            if self.active_ce_setup:
                if c0 and c0['close'] < c0['open']:
                    is_continuation = True
            
            if not is_continuation:
                log.info(f"{Fore.YELLOW}Pattern Detected: 3 Red Candles (Spot {c3['close']}). Checking Call Setup...{Style.RESET_ALL}")
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
            "ts": datetime.now()
        }
        
        if setup_type == "PE": self.active_pe_setup = setup_data
        else: self.active_ce_setup = setup_data
        
        # Subscribe to the option token for live monitoring
        self._subscribe_to_token(contract.token)
        
        setup_time_str = setup_data['ts'].strftime("%H:%M:%S")
        log.info(
            f"{Fore.GREEN}SETUP ARMED ({setup_type}) at {setup_time_str}: {contract.symbol} | "
            f"Spot Trigger: Break {'Below' if setup_type=='PE' else 'Above'} {spot_trigger} | "
            f"Option Trigger: Break Above {opt_high}{Style.RESET_ALL}"
        )

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

    def _execute_entry(self, ltp: float, setup: dict):
        contract = setup['contract']
        qty = self.quantity
        
        log.info(f"{Fore.GREEN}*** ENTRY TRIGGERED *** {contract.symbol} @ {ltp} | Spot crossed {setup['spot_trigger']}{Style.RESET_ALL}")
        
        if not self.simulate_orders:
            try:
                order_id = self.api.place_order(contract.symbol, contract.token, qty, "BUY", product_type="INTRADAY")
                log.info(f"Order placed: {order_id}")
                # In a real bot, we would poll for fill price. Here we assume fill at LTP for simplicity or add polling logic.
            except Exception as e:
                log.error(f"Order placement failed: {e}")
                return

        self.in_position = True
        self.position_info = {
            "contract": contract,
            "entry_price": ltp,
            "sl": ltp - self.sl_points,
            "target": ltp + self.target_points,
            "qty": qty,
            "highest_ltp": ltp,
        }
        # Clear ALL setups on entry
        self.active_pe_setup = None
        self.active_ce_setup = None
        log.info(f"Position Active: Target {self.position_info['target']:.2f} | SL {self.position_info['sl']:.2f}")

    def _execute_exit(self, ltp: float, reason: str):
        pos = self.position_info
        contract = pos['contract']
        pnl = (ltp - pos['entry_price']) * pos['qty']
        
        exit_time_str = datetime.now().strftime("%H:%M:%S")
        log.info(f"{Fore.MAGENTA}*** EXIT TRIGGERED ({reason}) at {exit_time_str} *** {contract.symbol} @ {ltp} | PNL: {pnl:.2f}{Style.RESET_ALL}")
        
        if not self.simulate_orders:
            try:
                self.api.place_order(contract.symbol, contract.token, pos['qty'], "SELL", product_type="INTRADAY")
            except Exception as e:
                log.error(f"Exit order failed: {e}")

        self.closed_pnl += pnl
        self.in_position = False
        self.last_exit_time = datetime.now()
        self.position_info = {}
        # Reset subscribed tokens to just Spot to save bandwidth/confusion
        self.subscribed_tokens = {self.index_token}
        # Note: In a robust system, we would unsubscribe from the option token here.

    def _process_tick(self, token: str, ltp: float):
        # Check for dynamic config updates
        if self._load_config() and self.in_position:
            entry = self.position_info['entry_price']
            self.position_info['target'] = entry + self.target_points
            
            # Recalculate SL based on new config and current high
            highest = self.position_info.get('highest_ltp', entry)
            peak_pts = highest - entry
            
            new_sl = entry - self.sl_points # Base SL
            
            if peak_pts >= self.trailing_activation_points:
                trail_sl = highest - self.trailing_distance_points
                new_sl = max(new_sl, trail_sl)
            
            self.position_info['sl'] = new_sl
            log.info(f"Position Limits Updated: Target {self.position_info['target']:.2f} | SL {self.position_info['sl']:.2f}")
        
        # Check for discard requests (regardless of position state, but mostly for armed setups)
        if self.current_ce_setup_discard and self.active_ce_setup:
            log.warning(f"{Fore.YELLOW}CONFIG REQUEST: Discarding active CE setup {self.active_ce_setup['contract'].symbol} as requested.{Style.RESET_ALL}")
            self.active_ce_setup = None
            self._save_state()

        if self.current_pe_setup_discard and self.active_pe_setup:
            log.warning(f"{Fore.YELLOW}CONFIG REQUEST: Discarding active PE setup {self.active_pe_setup['contract'].symbol} as requested.{Style.RESET_ALL}")
            self.active_pe_setup = None
            self._save_state()

        now = datetime.now()
        
        # 1. Check Time Window (Configurable Hourly Slots)
        # If outside allowed hours and not in position AND no active setups, stop processing (no new scans)
        has_active_setups = (self.active_pe_setup is not None) or (self.active_ce_setup is not None)
        if not self._is_trading_allowed(now.time()) and not self.in_position and not has_active_setups:
            return

        # 2. Minute-based Candle Check (Only runs once per minute)
        if now.minute != self.last_candle_check_minute:
            # Only look for new setups if we are NOT in a position
            if not self.in_position and self._is_trading_allowed(now.time()):
                self._check_for_setup()
            self.last_candle_check_minute = now.minute

        # 3. Check Triggers (If Setup Active)
        if not self.in_position:
            spot_ltp = self.latest_ltp.get(self.index_token)
            
            # Check both setups independently
            setups_to_check = []
            if self.active_pe_setup and self.pe_setup_allowed: setups_to_check.append(self.active_pe_setup)
            if self.active_ce_setup and self.ce_setup_allowed: setups_to_check.append(self.active_ce_setup)

            for setup in setups_to_check:
                
                opt_ltp = self.latest_ltp.get(setup['contract'].token)
                if spot_ltp and opt_ltp:
                    # Check Spot Condition
                    spot_condition = False
                    if setup['type'] == "PE":
                        # Put Setup: Spot breaks BELOW trigger (Low of C1)
                        if spot_ltp < setup['spot_trigger']: spot_condition = True
                    else:
                        # Call Setup: Spot breaks ABOVE trigger (High of C1)
                        if spot_ltp > setup['spot_trigger']: spot_condition = True
                    
                    # Check Option Condition
                    opt_condition = opt_ltp > setup['opt_trigger']
                    
                    if spot_condition and opt_condition:
                        self._execute_entry(opt_ltp, setup)
                        break # Stop checking other setups if entered

        # 4. Manage Position (TP/SL)
        if self.in_position:
            pos = self.position_info
            if token == pos['contract'].token:
                # Update Highest LTP
                if ltp > pos.get('highest_ltp', pos['entry_price']):
                    pos['highest_ltp'] = ltp
                    peak_mtm = (ltp - pos['entry_price']) * pos['qty']
                    log.info(f"{Fore.CYAN}Peak MTM Updated: {peak_mtm:.2f} (LTP: {ltp}){Style.RESET_ALL}")
                
                # Trailing SL Logic
                peak_pts = pos['highest_ltp'] - pos['entry_price']
                if peak_pts >= self.trailing_activation_points:
                    new_sl = pos['highest_ltp'] - self.trailing_distance_points
                    if new_sl > pos['sl']:
                        pos['sl'] = new_sl
                        log.info(f"{Fore.MAGENTA}Trailing SL Updated: {new_sl:.2f} (Peak Pts: {peak_pts:.2f}){Style.RESET_ALL}")

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
        
        # Wait for start time
        now = datetime.now()
        start_time = dt_time(9, 15)
        if now.time() < start_time:
            wait_s = (datetime.combine(now.date(), start_time) - now).total_seconds()
            log.info(f"Waiting {wait_s:.0f}s for market open (9:15)...")
            time_module.sleep(wait_s)

        try:
            self.ws = SmartWebSocketV2(self.api.jwt_token, self.api.api_key, self.api.client_id, self.api.feed_token)
            
            def on_open(wsapp):
                log.info("WebSocket Connected. Subscribing to Spot...")
                # Initial subscription to Spot Index
                req = [{"exchangeType": 1 if self.index_name=="NIFTY" else 3, "tokens": [self.index_token]}]
                self.ws.subscribe("itm_init", 3, req)

            self.ws.on_open = on_open
            self.ws.on_data = self._on_ws_message
            self.ws.on_error = lambda ws, err: log.error(f"WS Error: {err}")
            self.ws.connect()
            
        except KeyboardInterrupt:
            log.info("Stopped by user.")
        except Exception as e:
            log.exception(f"Fatal Error: {e}")
        finally:
            if self.ws and self.ws.wsapp:
                self.ws.wsapp.close()

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