"""
Live WebSocket streamer for Iron Condor strategy with live order placement and execution handling.
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import time as time_module
from datetime import datetime, date, time as dt_time, timedelta
from pathlib import Path
from typing import Dict, Any
from collections import defaultdict

from colorama import Fore, Style, init as colorama_init

from src.api.smartapi_client import AngelAPI
from src.data_pipeline.nifty_first_15m import get_index_first_15m_close
from src.market.contracts import find_option, OptionContract
from src.strategy.strike_selection import get_single_ce_pe_strikes

# Initialize colorama
colorama_init(autoreset=True)

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)s.%(msecs)03d | %(message)s', datefmt='%Y-%m-%dT%H:%M:%S')

# --- Monkey-Patch for smartapi-python library bugs ---
try:
    from SmartApi.smartWebSocketV2 import SmartWebSocketV2

    log.info("Patching SmartWebSocketV2 to fix library bugs...")

    def _patched_on_close(self, wsapp, close_status_code, close_msg):
        log.warning(f"event=WS_CLOSE | WebSocket connection closed. Code: {close_status_code}, Reason: {close_msg}")
        if self.on_close:
            self.on_close(wsapp)

    SmartWebSocketV2._on_close = _patched_on_close

except Exception as e:
    log.error("Failed to import or patch SmartWebSocketV2: %s", e)
    SmartWebSocketV2 = None
# --- End of Patch ---


class IronCondorLive:
    INDEX_CONFIG = {
        "NIFTY": {
            "lot_size": 150,
            "token": "99926000",
            "exchange": "NSE",
            "options_exchange": "NFO",
            "exchange_type": 1,
            "options_exchange_type": 2,
            "strike_step": 50,
            "spot_proximity_exit_points": 40,
            "take_profit_per_lot": 20.0,
            "absolute_stop_loss_per_lot": 20.0,
            "trailing_activation_mtm_per_lot": 8.0, # 1000 / 50 lots
            "trailing_sl_reversal_pct": 0.70,
        },

        "SENSEX": {
            "lot_size": 60,
            "token": "99919000",
            "exchange": "BSE",
            "options_exchange": "BFO",
            "exchange_type": 3,
            "options_exchange_type": 4,
            "strike_step": 100,
            "spot_proximity_exit_points": 60,
            "take_profit_per_lot": 50.0,
            "absolute_stop_loss_per_lot": 50.0,
            "trailing_activation_mtm_per_lot": 18, # 1000 / 60 lots
            "trailing_sl_reversal_pct": 0.70,
        }
    }

    def __init__(
            self,
            index_name: str = "NIFTY",
            trading_date: date | None = None,
            expiry: str | None = None,
            vwap_recalc_interval_minutes: int = 5,
            simulate_orders: bool = True,
    ):
        self.index_name = index_name.upper()
        if self.index_name not in self.INDEX_CONFIG:
            raise ValueError(f"Invalid index '{self.index_name}'. Must be one of {list(self.INDEX_CONFIG.keys())}")

        config = self.INDEX_CONFIG[self.index_name]
        self.lot_size = config["lot_size"]
        self.index_token = config["token"]
        self.index_exchange = config["exchange"]
        self.options_exchange = config["options_exchange"]
        self.exchange_type = config["exchange_type"]
        self.options_exchange_type = config["options_exchange_type"]
        self.strike_step = config["strike_step"]
        self.spot_proximity_exit_points = config["spot_proximity_exit_points"]
        take_profit_per_lot = config["take_profit_per_lot"]
        absolute_stop_loss_per_lot = config["absolute_stop_loss_per_lot"]
        trailing_activation_mtm_per_lot = config["trailing_activation_mtm_per_lot"]
        self.trailing_sl_reversal_pct = config["trailing_sl_reversal_pct"]
        self.trailing_activation_mtm = trailing_activation_mtm_per_lot * self.lot_size


        self.trading_date = trading_date or date.today()
        self.expiry = expiry
        self.simulate_orders = simulate_orders
        self.vwap_recalc_interval = timedelta(minutes=vwap_recalc_interval_minutes)
        
        self.take_profit_points = take_profit_per_lot * self.lot_size
        self.absolute_stop_loss = absolute_stop_loss_per_lot * self.lot_size

        self.api = AngelAPI()
        self.api.login()
        time_module.sleep(1)

        self.short_ce_contract: OptionContract | None = None
        self.short_pe_contract: OptionContract | None = None
        self.long_ce_contract: OptionContract | None = None
        self.long_pe_contract: OptionContract | None = None

        self.latest_ltp: Dict[str, float] = {}
        self.individual_leg_vwap_accumulators: Dict[str, Dict[str, float]] = {}
        self.next_vwap_update_time: dt_time | None = None
        self.in_position = False
        self.trading_active = True
        self.trailing_sl_active = False
        self.peak_mtm = 0.0
        self.entry_info: Dict[str, Any] = {}
        self.exit_info: Dict[str, Any] = {}

        self.ws = None

        # Initialize all state variables
        self._reset_state()

        log.info(
            f"Strategy configured with TP: {self.take_profit_points:.2f} ({take_profit_per_lot}/lot), "
            f"Abs SL: {self.absolute_stop_loss:.2f} ({absolute_stop_loss_per_lot}/lot)"
        )


    def _ensure_csv(self):
        self.closed_pnl = 0.0
        if not self.csv_path.exists():
            with self.csv_path.open("w", newline="") as f:
                writer = csv.writer(f)
                writer.writerow([
                    "ts", "net_credit", "vwap", "in_position", "event", "details"
                ])

    def _reset_state(self):
        """Resets all strategy state variables."""
        self.individual_leg_vwap_accumulators: Dict[str, Dict[str, float]] = {
            "s_ce": {"cum_pv": 0.0, "cum_vol": 0.0},
            "s_pe": {"cum_pv": 0.0, "cum_vol": 0.0},
            "l_ce": {"cum_pv": 0.0, "cum_vol": 0.0},
            "l_pe": {"cum_pv": 0.0, "cum_vol": 0.0},
        }

        self.in_position = False
        self.trading_active = True # Flag to control the main trading loop
        self.trailing_sl_active = False
        self.peak_mtm = 0.0

        self.events_dir = Path("data/live")
        self.events_dir.mkdir(parents=True, exist_ok=True)
        self.csv_path = self.events_dir / f"iron_condor_events_{self.trading_date}.csv"
        self._ensure_csv()

    def log_event(self, ts: datetime, event, details="", net_credit=None, vwap=None):
        row = [
            ts.isoformat(),
            f"{net_credit:.2f}" if net_credit is not None else "",
            f"{vwap:.4f}" if vwap is not None else "",
            int(self.in_position),
            event,
            details,
        ]
        with self.csv_path.open("a", newline="") as f:
            csv.writer(f).writerow(row)
        log.info("event=%s | %s", event, details)

    def _get_historical_candles(self, contract: OptionContract, from_dt: datetime, to_dt: datetime) -> list:
        time_module.sleep(0.5) # Rate limit API calls
        # Add 1 minute to to_dt to ensure the current (incomplete) bar is included in the fetch window
        to_dt_adjusted = to_dt + timedelta(minutes=1)
        from_str, to_str = from_dt.strftime("%Y-%m-%d %H:%M"), to_dt_adjusted.strftime("%Y-%m-%d %H:%M")
        log.info(f"Fetching historical candles for {contract.symbol} from {from_str} to {to_str}")
        try:
            payload = {
                "exchange": contract.exchange,
                "symboltoken": contract.token,
                "interval": "ONE_MINUTE",
                "fromdate": from_str,
                "todate": to_str,
            }
            candle_data = self.api.connection.getCandleData(payload)
            return candle_data.get("data", []) or []
        except Exception as e:
            log.error(f"Failed to fetch historical candles for {contract.symbol}: {e}")
            return []

    def prepare_contracts(self):
        spot, spot_candle_end_time = get_index_first_15m_close(self.index_name, self.trading_date)
        strikes_info = get_single_ce_pe_strikes(spot, spot_candle_end_time, self.index_name, trading_date=self.trading_date, strike_step=self.strike_step)
        short_ce_strike, short_pe_strike, long_ce_strike, long_pe_strike = \
            strikes_info["ce_strike"], strikes_info["pe_strike"], strikes_info["long_ce_strike"], strikes_info["long_pe_strike"]

        self.short_ce_contract = find_option(self.index_name, short_ce_strike, "CE", self.expiry, self.trading_date)
        self.short_pe_contract = find_option(self.index_name, short_pe_strike, "PE", self.expiry, self.trading_date)
        self.long_ce_contract = find_option(self.index_name, long_ce_strike, "CE", self.expiry, self.trading_date)
        self.long_pe_contract = find_option(self.index_name, long_pe_strike, "PE", self.expiry, self.trading_date)
        
        # Initial VWAP calculation will be triggered on the first tick after 9:30
        log.info("Contracts prepared. VWAP will be calculated on the first tick after 9:30 AM.")

    def _get_strategy_vwap(self) -> float:
        """Calculates the combined strategy VWAP from individual leg VWAPs."""
        def get_leg_vwap(key):
            acc = self.individual_leg_vwap_accumulators[key]
            return acc["cum_pv"] / acc["cum_vol"] if acc["cum_vol"] > 0 else 0.0

        s_ce_vwap = get_leg_vwap("s_ce")
        s_pe_vwap = get_leg_vwap("s_pe")
        l_ce_vwap = get_leg_vwap("l_ce")
        l_pe_vwap = get_leg_vwap("l_pe")
        return (s_ce_vwap + s_pe_vwap) - (l_ce_vwap + l_pe_vwap)

    def _recalculate_vwap_from_history(self, current_dt: datetime):
        """Fetches all 1-min bars from day start and recalculates VWAP for all legs."""
        log.info(f"event=VWAP_RECALC | Triggered at {current_dt.time()}. Fetching fresh 1-min bars.")
        start_of_day = datetime.combine(self.trading_date, dt_time(9, 15))

        # Fetch historical data for all legs
        hist_bars = {
            "s_ce": self._get_historical_candles(self.short_ce_contract, start_of_day, current_dt),
            "s_pe": self._get_historical_candles(self.short_pe_contract, start_of_day, current_dt),
            "l_ce": self._get_historical_candles(self.long_ce_contract, start_of_day, current_dt),
            "l_pe": self._get_historical_candles(self.long_pe_contract, start_of_day, current_dt),
        }
        time_module.sleep(0.5) # Pause after burst of API calls

        if not all(hist_bars.values()):
            log.error("event=VWAP_RECALC_FAIL | Failed to fetch bars for one or more legs. VWAP not updated.")
            return

        # Reset accumulators
        for key in self.individual_leg_vwap_accumulators:
            self.individual_leg_vwap_accumulators[key] = {"cum_pv": 0.0, "cum_vol": 0.0}

        # Repopulate accumulators with the full history
        num_bars = len(hist_bars["s_ce"])
        for i in range(num_bars):
            for key, bars in hist_bars.items():
                if i < len(bars):
                    bar_data = bars[i]
                    o, h, l, c, v = [float(x or 0) for x in bar_data[1:]]
                    self.individual_leg_vwap_accumulators[key]["cum_pv"] += ((o + h + l + c) / 4) * v
                    self.individual_leg_vwap_accumulators[key]["cum_vol"] += v
        
        log.info(f"event=VWAP_RECALC_SUCCESS | VWAP updated using {num_bars} bars.")

    def _check_and_resume_position(self):
        log.info("Checking for existing open positions...")
        # In simulation mode, we never look at the real trade book. PNL starts at 0.
        if not self.simulate_orders:
            self.closed_pnl = self._calculate_closed_pnl()
            log.info(f"Found initial closed PNL from trade book: {self.closed_pnl:.2f}")

        open_positions = self.api.get_open_positions()
        if not open_positions:
            log.info("No open positions found.")
            return

        s_ce, s_pe, l_ce, l_pe = None, None, None, None
        for pos in open_positions:
            symbol = pos.get("tradingsymbol")
            log.info(f"Checking position: {symbol}, sellqty: {pos.get('sellqty')}, buyqty: {pos.get('buyqty')}")
            if int(pos.get('sellqty', 0)) > int(pos.get('buyqty', 0)): # Net short
                if symbol == self.short_ce_contract.symbol: s_ce = pos
                elif symbol == self.short_pe_contract.symbol: s_pe = pos
            elif int(pos.get('buyqty', 0)) > int(pos.get('sellqty', 0)): # Net long
                if symbol == self.long_ce_contract.symbol: l_ce = pos
                elif symbol == self.long_pe_contract.symbol: l_pe = pos
        
        if all([s_ce, s_pe, l_ce, l_pe]):
            log.warning("event=RESUME_POSITION | Found existing open Iron Condor. Resuming management.")
            self.in_position = True
            
            # Fetch current index price to use as reference for PNL change display
            try:
                index_ltp = self.api.get_ltp(self.index_exchange, self.index_name, self.index_token)
                self.latest_ltp[self.index_token] = index_ltp
            except Exception as e:
                log.error(f"Failed to fetch index LTP during resume: {e}")
                index_ltp = 0.0

            self.entry_info = {
                "ts": datetime.now().isoformat(),
                "short_ce_entry": float(s_ce['sellavgprice']),
                "short_pe_entry": float(s_pe['sellavgprice']),
                "long_ce_entry": float(l_ce['buyavgprice']),
                "long_pe_entry": float(l_pe['buyavgprice']),
                "nifty_entry_price": index_ltp,
            }
            self.short_ce_stop = self.entry_info["short_ce_entry"] * 2.20
            self.short_pe_stop = self.entry_info["short_pe_entry"] * 2.20
            self.trailing_sl_active = False # Reset for new position
            self.peak_mtm = 0.0
            log.info(f"Resumed with entry prices and SLs.")
        else:
            log.info("No complete Iron Condor position found to resume.")

    def _calculate_closed_pnl(self) -> float:
        trade_book = self.api.get_trade_book()
        if not trade_book:
            return 0.0

        condor_symbols = {
            self.short_ce_contract.symbol,
            self.short_pe_contract.symbol,
            self.long_ce_contract.symbol,
            self.long_pe_contract.symbol,
        }

        # Group trades by fill ID to identify related transactions
        fills = defaultdict(list)
        for trade in trade_book:
            if trade.get('tradingsymbol') in condor_symbols:
                fills[trade['fillid']].append(trade)

        pnl = 0.0
        for fill_id, trades_in_fill in fills.items():
            # A complete condor transaction (entry or exit) should have 4 trades.
            if len(trades_in_fill) == 4:
                for trade in trades_in_fill:
                    price = float(trade['fillprice'])
                    qty = int(trade['fillsize'])
                    if trade['transactiontype'] == 'BUY':
                        pnl -= price * qty
                    elif trade['transactiontype'] == 'SELL':
                        pnl += price * qty
        
        return pnl

    def _on_tick(self, payload: dict):
        try:
            token = str(payload.get("token") or payload.get("tk"))
            if not token: return

            ltp = float(payload.get("last_traded_price") or payload.get("ltp") or payload.get("lp"))
            # Angel sends prices as integers (e.g. 123.45 is sent as 12345)
            # Exchange Type: 1=NSE_IDX, 2=NFO_OPT
            exchange_type = payload.get("exchange_type")
            if exchange_type in [1, 2, 3, 4]: # Handle NIFTY and SENSEX
                ltp /= 100.0
            vol = float(payload.get("volume_trade_for_the_day") or payload.get("volume") or payload.get("v") or 0)
        except (TypeError, ValueError): return
        self.latest_ltp[token] = ltp
        self._process_strategy_on_tick(token, ltp, vol)

    def _on_ws_message(self, ws, message):
        try:
            data = json.loads(message) if isinstance(message, (bytes, str)) else message
        except Exception: return
        if isinstance(data, list):
            for item in data: self._on_tick(item)
        elif isinstance(data, dict) and ("token" in data or "tk" in data):
            self._on_tick(data)

    def _close_ws(self):
        if self.ws and hasattr(self.ws, 'wsapp') and self.ws.wsapp:
            self.ws.wsapp.close()

    def _poll_order_status(self, order_id: str, timeout=10) -> dict | None:
        for _ in range(timeout * 2): # Check every 0.5s
            time_module.sleep(0.5)
            status = self.api.get_order_status(order_id)
            if status and status.get("status") == "complete":
                return status
        return None

    def _execute_entry(self, current_net_credit: float, current_vwap: float):
        log.info(f"event=ORDER_ENTRY_INIT | Triggered at NetCredit={current_net_credit:.2f}, VWAP={current_vwap:.2f}")

        if self.simulate_orders:
            log.warning("SIMULATION: Would have placed BUY orders for Long CE/PE and SELL orders for Short CE/PE.")
            sim_sc_entry = self.latest_ltp.get(self.short_ce_contract.token, 0.0)
            sim_sp_entry = self.latest_ltp.get(self.short_pe_contract.token, 0.0)
            sim_lc_entry = self.latest_ltp.get(self.long_ce_contract.token, 0.0)
            sim_lp_entry = self.latest_ltp.get(self.long_pe_contract.token, 0.0)
            sim_index_entry = self.latest_ltp.get(self.index_token, 0.0)
            
            if sim_index_entry == 0.0:
                try:
                    sim_index_entry = self.api.get_ltp(self.index_exchange, self.index_name, self.index_token)
                    self.latest_ltp[self.index_token] = sim_index_entry
                except Exception:
                    pass

            log.info(
                f"event=ORDER_ENTRY_INIT | Triggered at NetCredit={current_net_credit:.2f}, VWAP={current_vwap:.2f} | "
                f"Prices: S_CE={sim_sc_entry:.2f}, S_PE={sim_sp_entry:.2f}, L_CE={sim_lc_entry:.2f}, L_PE={sim_lp_entry:.2f}"
            )

            self.in_position = True
            self.entry_info = {
                "ts": datetime.now().isoformat(),
                "short_ce_entry": sim_sc_entry, "short_pe_entry": sim_sp_entry,
                "long_ce_entry": sim_lc_entry, "long_pe_entry": sim_lp_entry,
                "nifty_entry_price": sim_index_entry,
            }
            log.info(f"{Fore.GREEN}SIMULATION: ENTRY CONFIRMED | Iron Condor position entered at {self.index_name} {sim_index_entry:.2f}.")
            return
        
        try:
            lc_id = self.api.place_order(self.long_ce_contract.symbol, self.long_ce_contract.token, self.lot_size, "BUY")
            lp_id = self.api.place_order(self.long_pe_contract.symbol, self.long_pe_contract.token, self.lot_size, "BUY")
        except Exception as e:
            log.error(f"Failed to place one or both BUY orders: {e}"); return

        lc_details = self._poll_order_status(lc_id)
        lp_details = self._poll_order_status(lp_id)

        if not lc_details or not lp_details:
            log.error("One or both BUY orders failed to confirm. Aborting entry."); return

        try:
            sc_id = self.api.place_order(self.short_ce_contract.symbol, self.short_ce_contract.token, self.lot_size, "SELL")
            sp_id = self.api.place_order(self.short_pe_contract.symbol, self.short_pe_contract.token, self.lot_size, "SELL")
        except Exception as e:
            log.error(f"Failed to place one or both SELL orders: {e}. Exiting bought legs.")
            self._execute_exit(exit_reason="SELL_LEG_FAILED"); return

        sc_details = self._poll_order_status(sc_id)
        sp_details = self._poll_order_status(sp_id)

        if not sc_details or not sp_details:
            log.error("One or both SELL orders failed. Exiting all legs.")
            self._execute_exit(exit_reason="PARTIAL_SELL_FAILED"); return

        index_entry_price = self.api.get_ltp(self.index_exchange, self.index_name, self.index_token)

        self.in_position = True
        self.entry_info = {
            "ts": datetime.now().isoformat(),
            "short_ce_entry": float(sc_details['averageprice']), "short_pe_entry": float(sp_details['averageprice']),
            "long_ce_entry": float(lc_details['averageprice']), "long_pe_entry": float(lp_details['averageprice']),
            "nifty_entry_price": index_entry_price,
        }
        self.short_ce_stop = self.entry_info["short_ce_entry"] * 1.70
        self.short_pe_stop = self.entry_info["short_pe_entry"] * 1.70
        log.info(f"{Fore.GREEN}event=ENTRY_CONFIRMED | Iron Condor position entered at {self.index_name} {index_entry_price}.")
        self.trailing_sl_active = False # Reset for new position
        self.peak_mtm = 0.0

    def _execute_exit(self, exit_reason="STRATEGY_EXIT"):
        log.info(f"event=ORDER_EXIT_INIT | Triggered by: {exit_reason}")

        if self.simulate_orders:
            log.warning("SIMULATION: Would have placed exit orders for all 4 legs.")
            if self.in_position: # Only update closed PNL if there was an open position
                entry_net_credit = (self.entry_info['short_ce_entry'] + self.entry_info['short_pe_entry']) - \
                                   (self.entry_info['long_ce_entry'] + self.entry_info['long_pe_entry'])
                current_net_credit = (self.latest_ltp.get(self.short_ce_contract.token, 0) + self.latest_ltp.get(self.short_pe_contract.token, 0)) - \
                                     (self.latest_ltp.get(self.long_ce_contract.token, 0) + self.latest_ltp.get(self.long_pe_contract.token, 0))
                simulated_open_pnl = (entry_net_credit - current_net_credit) * self.lot_size
                self.closed_pnl += simulated_open_pnl
            self.in_position = False
            self.trading_active = False # Stop trading for the day
            log.info(f"{Fore.GREEN}SIMULATION: EXIT CONFIRMED | All 4 exit orders simulated. New Closed PNL: {self.closed_pnl:.2f}. Reason={exit_reason}{Style.RESET_ALL}")
            self._close_ws()
            return

        # --- Real Order Placement ---
        self.api.place_order(self.short_ce_contract.symbol, self.short_ce_contract.token, self.lot_size, "BUY")
        self.api.place_order(self.short_pe_contract.symbol, self.short_pe_contract.token, self.lot_size, "BUY")
        self.api.place_order(self.long_ce_contract.symbol, self.long_ce_contract.token, self.lot_size, "SELL")
        self.api.place_order(self.long_pe_contract.symbol, self.long_pe_contract.token, self.lot_size, "SELL")
        
        self.in_position = False
        self.trading_active = False # Stop trading for the day
        time_module.sleep(2) # Give time for orders to be updated in tradebook
        self.closed_pnl = self._calculate_closed_pnl()
        log.info(f"event=EXIT_CONFIRMED | All 4 exit orders placed. New Closed PNL: {self.closed_pnl:.2f}. Reason={exit_reason}")
        self._close_ws()

    def _process_strategy_on_tick(self, updated_token: str, updated_ltp: float, updated_vol: float):
        now = datetime.now()
        if not self.trading_active:
            return

        if self.in_position and now.time() >= dt_time(14, 50):
            self._execute_exit(exit_reason="EOD"); return

        current_time = now.time()
        if current_time < dt_time(9, 30): return
        # --- Periodic VWAP Recalculation ---
        if self.next_vwap_update_time is None or current_time >= self.next_vwap_update_time:
            self._recalculate_vwap_from_history(now)
            # Set the next update time based on the current time.
            self.next_vwap_update_time = (now + self.vwap_recalc_interval).time()

        s_ce_ltp = self.latest_ltp.get(self.short_ce_contract.token)
        s_pe_ltp = self.latest_ltp.get(self.short_pe_contract.token)
        l_ce_ltp = self.latest_ltp.get(self.long_ce_contract.token)
        l_pe_ltp = self.latest_ltp.get(self.long_pe_contract.token)
        nifty_ltp = self.latest_ltp.get(self.index_token)

        if not all([s_ce_ltp, s_pe_ltp, l_ce_ltp, l_pe_ltp, nifty_ltp]): return

        net_credit = (s_ce_ltp + s_pe_ltp) - (l_ce_ltp + l_pe_ltp)
        vwap = self._get_strategy_vwap()

        if not self.in_position:
            log.info(f"reached here to take entry {self.in_position}")
            # Simplified Entry: Enter on the first valid tick after 9:30 AM if not already in a position.
            self._execute_entry(net_credit, vwap)
        else:
            # --- Position Management ---
            entry_net_credit = (self.entry_info['short_ce_entry'] + self.entry_info['short_pe_entry']) - \
                               (self.entry_info['long_ce_entry'] + self.entry_info['long_pe_entry'])
            
            open_pnl = (entry_net_credit - net_credit) * self.lot_size
            total_pnl = open_pnl + self.closed_pnl
            
            nifty_change = nifty_ltp - self.entry_info.get("nifty_entry_price", nifty_ltp)
            nifty_color = Fore.GREEN if nifty_change >= 0 else Fore.RED
            nifty_str = f"{self.index_name}: {nifty_ltp:.2f} ({nifty_color}{nifty_change:+.2f}{Style.RESET_ALL})"

            pnl_color = Fore.GREEN if total_pnl >= 0 else Fore.RED
            log.info(f"event=PNL_UPDATE | NetCredit={net_credit:.2f}, VWAP={vwap:.2f} | Open PNL: {open_pnl:+.2f}, Closed PNL: {self.closed_pnl:+.2f}, Total PNL: {pnl_color}{total_pnl:+.2f}{Style.RESET_ALL} | {nifty_str}")

            if total_pnl >= self.take_profit_points:
                self._execute_exit(exit_reason="TAKE_PROFIT")
            elif total_pnl <= -self.absolute_stop_loss:
                self._execute_exit(exit_reason="STOP_LOSS_ABS")
            # New condition: Spot price near short strikes
            elif nifty_ltp <= (self.short_pe_contract.strike + self.spot_proximity_exit_points) or \
                nifty_ltp >= (self.short_ce_contract.strike - self.spot_proximity_exit_points):
                self._execute_exit(exit_reason="SPOT_NEAR_STRIKE")
            
            # Trailing Stop Loss on Profit
            if not self.trailing_sl_active and total_pnl >= self.trailing_activation_mtm:
                self.trailing_sl_active = True
                self.peak_mtm = total_pnl
                log.info(f"{Fore.MAGENTA}Trailing SL activated at PNL: {total_pnl:.2f}{Style.RESET_ALL}")
            
            if self.trailing_sl_active:
                new_peak_mtm = max(self.peak_mtm, total_pnl)
                if new_peak_mtm > self.peak_mtm:
                    log.info(f"{Fore.MAGENTA}Peak MTM updated to:: {new_peak_mtm:.2f}{Style.RESET_ALL}")
                    self.peak_mtm = new_peak_mtm

                trailing_stop_level = self.peak_mtm * self.trailing_sl_reversal_pct
                if total_pnl <= trailing_stop_level:
                    log.info(f"{Fore.MAGENTA}Trailing SL triggered. Current PNL {total_pnl:.2f} <= 70% of Peak PNL {self.peak_mtm:.2f}{Style.RESET_ALL}")
                    self._execute_exit(exit_reason="TRAILING_STOP_LOSS")

    def run(self):
        self.prepare_contracts()
        self._check_and_resume_position()

        if self.in_position:
            log.info("Starting WebSocket to monitor existing position.")
        else:
            log.info("Starting WebSocket for new entry.")

        try:
            self.ws = SmartWebSocketV2(self.api.jwt_token, self.api.api_key, self.api.client_id, self.api.feed_token)
            token_list = [{"exchangeType": self.options_exchange_type, "tokens": [
                self.short_ce_contract.token, self.short_pe_contract.token,
                self.long_ce_contract.token, self.long_pe_contract.token
            ]}, {"exchangeType": self.exchange_type, "tokens": [self.index_token]}]
            
            def on_open(wsapp):
                log.info("event=WS_OPEN | Subscribing to tokens: %s", token_list)
                self.ws.subscribe(f"condor_{self.trading_date}", 3, token_list) # Use mode 3 for SNAP_QUOTE

            self.ws.on_open = on_open
            self.ws.on_data = self._on_ws_message
            self.ws.on_error = lambda ws, err: log.error("event=WS_ERROR | %s", err)
            self.ws.on_close = lambda wsapp: log.info("event=PUBLIC_WS_CLOSE | Public on_close handler called.")
            self.ws.connect()
        except Exception as e:
            log.exception("event=FATAL | Failed to start WebSocket. Err: %s", e)
        finally:
            self._close_ws()

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--index", default="NIFTY", help="Index to trade: NIFTY or SENSEX (default: NIFTY)")
    parser.add_argument("--expiry", help="Optional expiry string like 27NOV2025")
    parser.add_argument("--date", help="Trading date YYYY-MM-DD (default: today)")
    parser.add_argument("--vwap-interval", type=int, default=5, help="Interval in minutes to recalculate VWAP (default: 5)")
    parser.add_argument("--simulate-orders", action="store_true", default=True, help="If set, no real orders will be placed.")
    parser.add_argument("--live", dest="simulate_orders", action="store_false", help="Place real orders.")
    args = parser.parse_args()

    trading_date = datetime.strptime(args.date, "%Y-%m-%d").date() if args.date else date.today()
    s = IronCondorLive(index_name=args.index, trading_date=trading_date, expiry=args.expiry,
                       vwap_recalc_interval_minutes=args.vwap_interval, simulate_orders=args.simulate_orders)
    s.run()

if __name__ == "__main__":
    main()
