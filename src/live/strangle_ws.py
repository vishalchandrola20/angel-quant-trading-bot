"""
Live WebSocket streamer for Strangle strategy with live order placement and execution handling.
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import time
from datetime import datetime, date, time as dt_time
from pathlib import Path
from typing import Dict, Any

from colorama import Fore, Style, init as colorama_init

from src.api.smartapi_client import AngelAPI
from src.data_pipeline.nifty_first_15m import get_nifty_first_15m_close
from src.market.contracts import find_nifty_option, OptionContract
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


class StrangleLive:
    NIFTY_LOT_SIZE = 75

    def __init__(
            self,
            trading_date: date | None = None,
            expiry: str | None = None,
            take_profit_points: float = 1200.0,
    ):
        self.trading_date = trading_date or date.today()
        self.expiry = expiry
        self.take_profit_points = take_profit_points

        self.api = AngelAPI()
        self.api.login()
        time.sleep(1)

        self.ce_contract: OptionContract | None = None
        self.pe_contract: OptionContract | None = None
        self.latest_ltp: Dict[str, float] = {}
        self.cum_pv = 0.0
        self.cum_vol = 0.0
        self.seen_sum_above_vwap = False
        self.in_position = False
        self.entry_info: Dict[str, Any] = {}
        self.exit_info: Dict[str, Any] = {}
        self.ws = None

        self.events_dir = Path("data/live")
        self.events_dir.mkdir(parents=True, exist_ok=True)
        self.csv_path = self.events_dir / f"strangle_events_{self.trading_date}.csv"
        self._ensure_csv()

    def _ensure_csv(self):
        if not self.csv_path.exists():
            with self.csv_path.open("w", newline="") as f:
                writer = csv.writer(f)
                writer.writerow([
                    "ts", "ce_symbol", "pe_symbol", "ce_ltp", "pe_ltp",
                    "sum_price", "vwap", "in_position", "event", "details"
                ])

    def log_event(self, ts: datetime, event, details="", ce_ltp=None, pe_ltp=None, vwap=None):
        ce_token_str = str(self.ce_contract.token) if self.ce_contract else ""
        pe_token_str = str(self.pe_contract.token) if self.pe_contract else ""
        
        ce_ltp = ce_ltp if ce_ltp is not None else self.latest_ltp.get(ce_token_str)
        pe_ltp = pe_ltp if pe_ltp is not None else self.latest_ltp.get(pe_token_str)
        sum_price = (ce_ltp or 0) + (pe_ltp or 0)

        row = [
            ts.isoformat(),
            self.ce_contract.symbol if self.ce_contract else "",
            self.pe_contract.symbol if self.pe_contract else "",
            f"{ce_ltp:.2f}" if ce_ltp is not None else "",
            f"{pe_ltp:.2f}" if pe_ltp is not None else "",
            f"{sum_price:.2f}" if sum_price > 0 else "",
            f"{vwap:.4f}" if vwap is not None else "",
            int(self.in_position),
            event,
            details,
        ]
        with self.csv_path.open("a", newline="") as f:
            csv.writer(f).writerow(row)

        log.info("event=%s | %s", event, details)

    def _get_historical_candles(self, contract: OptionContract, from_dt: datetime, to_dt: datetime) -> list:
        """Helper to fetch historical 1-minute candles."""
        from_str = from_dt.strftime("%Y-%m-%d %H:%M")
        to_str = to_dt.strftime("%Y-%m-%d %H:%M")
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
        """
        Selects contracts and pre-fills VWAP with historical data from 9:15 AM using OHLC/4.
        """
        first15 = get_nifty_first_15m_close(self.trading_date)
        strikes_info = get_single_ce_pe_strikes(first15)
        ce_strike, pe_strike = strikes_info["ce_strike"], strikes_info["pe_strike"]
        log.info(f"event=SETUP | Initial strikes: CE={ce_strike}, PE={pe_strike}")

        self.ce_contract = find_nifty_option(ce_strike, "CE", self.expiry, self.trading_date)
        self.pe_contract = find_nifty_option(pe_strike, "PE", self.expiry, self.trading_date)
        
        now = datetime.now()
        start_of_day = datetime.combine(self.trading_date, dt_time(9, 15))
        
        if now > start_of_day:
            log.info("event=VWAP_INIT | Pre-filling VWAP from 9:15 AM to current time using OHLC/4.")
            ce_hist_bars = self._get_historical_candles(self.ce_contract, start_of_day, now)
            pe_hist_bars = self._get_historical_candles(self.pe_contract, start_of_day, now)

            if ce_hist_bars and pe_hist_bars:
                for ce_bar, pe_bar in zip(ce_hist_bars, pe_hist_bars):
                    ce_open, ce_high, ce_low, ce_close, ce_vol = float(ce_bar[1]), float(ce_bar[2]), float(ce_bar[3]), float(ce_bar[4]), float(ce_bar[5] or 0)
                    pe_open, pe_high, pe_low, pe_close, pe_vol = float(pe_bar[1]), float(pe_bar[2]), float(pe_bar[3]), float(pe_bar[4]), float(pe_bar[5] or 0)
                    
                    combined_open = ce_open + pe_open
                    combined_high = ce_high + pe_high
                    combined_low = ce_low + pe_low
                    combined_close = ce_close + pe_close
                    combined_vol = ce_vol + pe_vol
                    
                    ohlc4_price = (combined_open + combined_high + combined_low + combined_close) / 4
                    price_volume = ohlc4_price * combined_vol
                    
                    self.cum_pv += price_volume
                    self.cum_vol += combined_vol
                
                if self.cum_vol > 0:
                    initial_vwap = self.cum_pv / self.cum_vol
                    log.info(f"event=VWAP_INIT | VWAP initialized to {initial_vwap:.2f} with {len(ce_hist_bars)} historical bars.")
                else:
                    log.warning("event=VWAP_INIT | No volume in historical data. VWAP starts at 0.")
            else:
                log.warning("event=VWAP_INIT | Could not fetch historical data. VWAP will start from now.")

        log.info("event=SETUP | Final contracts: CE=%s, PE=%s", self.ce_contract.symbol, self.pe_contract.symbol)

    def _check_and_resume_position(self):
        """Checks for open positions at startup and resumes managing them."""
        log.info("Checking for existing open positions...")
        open_positions = self.api.get_open_positions()
        if not open_positions:
            log.info("No open positions found.")
            return

        ce_pos, pe_pos = None, None
        for pos in open_positions:
            log.info(f"Checking position: {pos.get('tradingsymbol')}, sellqty: {pos.get('sellqty')}, buyqty: {pos.get('buyqty')}")
            is_net_short = int(pos.get('sellqty', 0)) > int(pos.get('buyqty', 0))
            
            if is_net_short:
                if pos.get("tradingsymbol") == self.ce_contract.symbol:
                    ce_pos = pos
                elif pos.get("tradingsymbol") == self.pe_contract.symbol:
                    pe_pos = pos
        
        if ce_pos and pe_pos:
            log.warning("event=RESUME_POSITION | Found existing open strangle position. Resuming management.")
            self.in_position = True
            
            actual_ce_entry_price = float(ce_pos['sellavgprice'])
            actual_pe_entry_price = float(pe_pos['sellavgprice'])

            self.entry_info = {
                "ts": datetime.now().isoformat(),
                "ce_entry": actual_ce_entry_price,
                "pe_entry": actual_pe_entry_price,
            }
            self.ce_stop = actual_ce_entry_price * 1.70
            self.pe_stop = actual_pe_entry_price * 1.70
            
            log.info(f"Resumed with entry prices: CE={actual_ce_entry_price:.2f}, PE={actual_pe_entry_price:.2f}")
            log.info(f"Resumed with SL levels: CE={self.ce_stop:.2f}, PE={self.pe_stop:.2f}")
        else:
            log.info("No matching strangle position found to resume.")

    def _on_tick(self, payload: dict):
        try:
            token = str(payload.get("token") or payload.get("tk"))
            if not token: return

            ltp_val = payload.get("last_traded_price") or payload.get("ltp") or payload.get("lp")
            if ltp_val is None: return

            ltp = float(ltp_val)
            if payload.get("exchange_type") == 2: ltp /= 100.0

            vol = float(payload.get("volume") or payload.get("v") or 0)
        except (TypeError, ValueError):
            return

        self.latest_ltp[token] = ltp
        self._process_strategy_on_tick(token, ltp, vol)

    def _on_ws_message(self, ws, message):
        try:
            data = json.loads(message) if isinstance(message, (bytes, str)) else message
        except Exception:
            return

        if isinstance(data, list):
            for item in data: self._on_tick(item)
        elif isinstance(data, dict):
            if "token" in data or "tk" in data:
                self._on_tick(data)

    def _close_ws(self):
        """Safely close the websocket connection."""
        if self.ws and hasattr(self.ws, 'wsapp') and self.ws.wsapp:
            log.info("event=SHUTDOWN | Closing WebSocket connection.")
            self.ws.wsapp.close()
        self.ws = None

    def _poll_order_status(self, order_id: str) -> dict | None:
        """Polls for order completion and returns the final order details."""
        for i in range(10):  # Poll for 10 seconds
            time.sleep(1)
            order_status = self.api.get_order_status(order_id)
            if order_status and order_status.get("status") == "complete":
                log.info(f"Order {order_id} completed.")
                return order_status
            log.info(f"Waiting for order {order_id} to complete... Status: {order_status.get('status') if order_status else 'N/A'}")
        log.error(f"Order {order_id} did not complete in time.")
        return None

    def _execute_entry(self, current_sum_price: float, current_vwap: float):
        """Places entry orders and confirms execution."""
        log.info(f"event=ORDER_ENTRY_INIT | Triggered at sum_price={current_sum_price:.2f}, vwap={current_vwap:.2f}")
        
        # --- Place CE SELL Order ---
        try:
            ce_order_id = self.api.place_order(
                tradingsymbol=self.ce_contract.symbol,
                symbol_token=self.ce_contract.token,
                quantity=self.NIFTY_LOT_SIZE,
                transaction_type="SELL",
            )
            log.info(f"event=ORDER_CE_SELL | Order ID: {ce_order_id}")
        except Exception as e:
            log.error(f"{Fore.RED}event=ORDER_CE_SELL_FAILED | Failed to place CE SELL order: {e}")
            return

        # --- Place PE SELL Order ---
        try:
            pe_order_id = self.api.place_order(
                tradingsymbol=self.pe_contract.symbol,
                symbol_token=self.pe_contract.token,
                quantity=self.NIFTY_LOT_SIZE,
                transaction_type="SELL",
            )
            log.info(f"event=ORDER_PE_SELL | Order ID: {pe_order_id}")
        except Exception as e:
            log.error(f"{Fore.RED}event=ORDER_PE_SELL_FAILED | Failed to place PE SELL order: {e}")
            log.warning("event=ORDER_PE_SELL_FAILED | PE order failed. Attempting to exit the CE leg to avoid naked position.")
            self._execute_exit(ce_only=True, exit_reason="PE_ORDER_FAILED") # Exit the leg that was entered
            return

        # --- Confirm Execution and Get Prices ---
        ce_order_details = self._poll_order_status(ce_order_id)
        pe_order_details = self._poll_order_status(pe_order_id)

        if not ce_order_details or not pe_order_details:
            log.error("event=ORDER_CONFIRM_FAILED | One or both entry orders did not complete. Attempting to exit all.")
            self._execute_exit(ce_only=bool(ce_order_details), pe_only=bool(pe_order_details), exit_reason="PARTIAL_ENTRY_FAILED")
            return

        actual_ce_entry_price = float(ce_order_details["averageprice"])
        actual_pe_entry_price = float(pe_order_details["averageprice"])

        self.in_position = True
        self.entry_info = {
            "ts": datetime.now().isoformat(),
            "ce_entry": actual_ce_entry_price,
            "pe_entry": actual_pe_entry_price,
            "entry_sum_price": current_sum_price,
            "entry_vwap": current_vwap,
        }
        self.ce_stop = actual_ce_entry_price * 1.70
        self.pe_stop = actual_pe_entry_price * 1.70
        
        entry_details = (f"LIVE ENTRY CONFIRMED | CE Price: {actual_ce_entry_price:.2f}, PE Price: {actual_pe_entry_price:.2f} "
                         f"(SLs: CE={self.ce_stop:.2f}, PE={self.pe_stop:.2f}) | "
                         f"Trigger Sum={current_sum_price:.2f}, Trigger VWAP={current_vwap:.2f}")
        log.info(f"{Fore.GREEN}event=ENTRY_CONFIRMED | {entry_details}")
        self.log_event(datetime.now(), "ENTRY_CONFIRMED", entry_details)

    def _execute_exit(self, ce_only=False, pe_only=False, exit_reason="STRATEGY_EXIT"):
        """Places BUY orders to exit the position."""
        log.info(f"event=ORDER_EXIT_INIT | Triggered by: {exit_reason}")
        
        ce_exit_price = self.latest_ltp.get(str(self.ce_contract.token), 0.0)
        pe_exit_price = self.latest_ltp.get(str(self.pe_contract.token), 0.0)

        if not ce_only:
            try:
                pe_order_id = self.api.place_order(
                    tradingsymbol=self.pe_contract.symbol,
                    symbol_token=self.pe_contract.token,
                    quantity=self.NIFTY_LOT_SIZE,
                    transaction_type="BUY",
                )
                log.info(f"{Fore.GREEN}event=ORDER_PE_BUY | Order ID: {pe_order_id}")
            except Exception as e:
                log.error(f"{Fore.RED}event=ORDER_PE_BUY_FAILED | Failed to place PE BUY order: {e}")

        if not pe_only:
            try:
                ce_order_id = self.api.place_order(
                    tradingsymbol=self.ce_contract.symbol,
                    symbol_token=self.ce_contract.token,
                    quantity=self.NIFTY_LOT_SIZE,
                    transaction_type="BUY",
                )
                log.info(f"{Fore.GREEN}event=ORDER_CE_BUY | Order ID: {ce_order_id}")
            except Exception as e:
                log.error(f"{Fore.RED}event=ORDER_CE_BUY_FAILED | Failed to place CE BUY order: {e}")
        
        self.in_position = False
        self.exit_info = {
            "ts": datetime.now().isoformat(),
            "ce_exit": ce_exit_price,
            "pe_exit": pe_exit_price,
            "reason": exit_reason,
        }
        log.info(f"event=EXIT_CONFIRMED | CE Exit={ce_exit_price:.2f}, PE Exit={pe_exit_price:.2f}, Reason={exit_reason}")
        self.log_event(datetime.now(), "EXIT_CONFIRMED", json.dumps(self.exit_info))
        self._close_ws()

    def _process_strategy_on_tick(self, updated_token: str, updated_ltp: float, updated_vol: float):
        now = datetime.now()
        
        if self.in_position and now.time() >= dt_time(14, 50):
            log.info("EOD trigger. Exiting position.")
            self._execute_exit(exit_reason="EOD")
            return

        ce_token = str(self.ce_contract.token)
        pe_token = str(self.pe_contract.token)
        ce_ltp = self.latest_ltp.get(ce_token)
        pe_ltp = self.latest_ltp.get(pe_token)

        if ce_ltp is None or pe_ltp is None: return

        sum_price = ce_ltp + pe_ltp
        price_volume = sum_price * updated_vol
        
        self.cum_pv += price_volume
        self.cum_vol += updated_vol
        
        vwap = self.cum_pv / self.cum_vol if self.cum_vol > 0 else sum_price

        if not self.in_position:
            if not self.seen_sum_above_vwap:
                if sum_price > vwap:
                    self.seen_sum_above_vwap = True
                    log.info(f"{Fore.CYAN}event=CONDITION_MET | price={sum_price:.2f} > vwap={vwap:.2f}. Armed for entry.")
            else:
                if sum_price <= vwap:
                    log.info(f"{Fore.YELLOW}event=ENTRY_TRIGGERED | price={sum_price:.2f} <= vwap={vwap:.2f}. Executing entry.")
                    self._execute_entry(sum_price, vwap)
        else:
            entry_ce = self.entry_info.get("ce_entry", 0)
            entry_pe = self.entry_info.get("pe_entry", 0)
            pnl_ce = (entry_ce - ce_ltp) * self.NIFTY_LOT_SIZE
            pnl_pe = (entry_pe - pe_ltp) * self.NIFTY_LOT_SIZE
            total_pnl = pnl_ce + pnl_pe

            pnl_color = Fore.GREEN if total_pnl >= 0 else Fore.RED
            pnl_details = f"Total PNL: {pnl_color}{total_pnl:+.2f}{Style.RESET_ALL}"
            log.info("event=PNL_UPDATE | %s", pnl_details)

            ce_stop_price = getattr(self, "ce_stop", float("inf"))
            pe_stop_price = getattr(self, "pe_stop", float("inf"))

            if total_pnl >= self.take_profit_points:
                self._execute_exit(exit_reason="TAKE_PROFIT")
            elif ce_ltp >= ce_stop_price or pe_ltp >= pe_stop_price:
                reason = "CE_SL_HIT" if ce_ltp >= ce_stop_price else "PE_SL_HIT"
                log.info(f"{Fore.RED}event=EXIT_SL | {reason}. Exiting position.")
                self._execute_exit(exit_reason=reason)

    def run(self):
        self.prepare_contracts()
        self._check_and_resume_position()

        if SmartWebSocketV2 is None:
            log.error("SmartWebSocketV2 not available. Cannot run.")
            return

        try:
            self.ws = SmartWebSocketV2(self.api.jwt_token, self.api.api_key, self.api.client_id, self.api.feed_token)

            exch_type = 2  # NFO
            token_list = [{"exchangeType": exch_type, "tokens": [self.ce_contract.token, self.pe_contract.token]}]
            correlation_id = f"strangle_{self.trading_date}"
            mode = 1  # LTP stream

            def on_open(wsapp):
                log.info("event=WS_OPEN | Subscribing to tokens: %s", token_list)
                self.ws.subscribe(correlation_id, mode, token_list)

            self.ws.on_open = on_open
            self.ws.on_data = self._on_ws_message
            self.ws.on_error = lambda ws, err: log.error("event=WS_ERROR | %s", err)
            self.ws.on_close = lambda wsapp: log.info("event=PUBLIC_WS_CLOSE | Public on_close handler called.")

            log.info("event=CONNECT | Starting WebSocket...")
            self.ws.connect()

        except KeyboardInterrupt:
            log.info("event=INTERRUPT | User interrupted the process.")
        except Exception as e:
            log.exception("event=FATAL | Failed to start WebSocket. Err: %s", e)
        finally:
            self._close_ws()

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--expiry", help="Optional expiry string like 27NOV2025")
    parser.add_argument("--date", help="Trading date YYYY-MM-DD (default: today)")
    args = parser.parse_args()

    trading_date = datetime.strptime(args.date, "%Y-%m-%d").date() if args.date else date.today()
    
    s = StrangleLive(trading_date=trading_date, expiry=args.expiry)
    s.run()

if __name__ == "__main__":
    main()
