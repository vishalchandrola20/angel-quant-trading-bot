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
from collections import defaultdict

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
        self.closed_pnl = 0.0
        self.ws = None

        self.events_dir = Path("data/live")
        self.events_dir.mkdir(parents=True, exist_ok=True)
        self.csv_path = self.events_dir / f"strangle_events_{self.trading_date}.csv"
        self._ensure_csv()
        log.info(f"Strategy configured with Take Profit: {self.take_profit_points}")

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
            time.sleep(1)
            pe_hist_bars = self._get_historical_candles(self.pe_contract, start_of_day, now)

            if ce_hist_bars and pe_hist_bars:
                for ce_bar, pe_bar in zip(ce_hist_bars, pe_hist_bars):
                    ce_open, ce_high, ce_low, ce_close, ce_vol = [float(x or 0) for x in ce_bar[1:]]
                    pe_open, pe_high, pe_low, pe_close, pe_vol = [float(x or 0) for x in pe_bar[1:]]
                    
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

    def _check_and_resume_position(self):
        """Checks for open positions at startup and resumes managing them."""
        log.info("Checking for existing open positions...")
        self.closed_pnl = self._calculate_closed_pnl()
        log.info(f"Found initial closed PNL: {self.closed_pnl:.2f}")

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
            
            self.entry_info = {
                "ts": datetime.now().isoformat(),
                "ce_entry": float(ce_pos['sellavgprice']),
                "pe_entry": float(pe_pos['sellavgprice']),
            }
            self.ce_stop = self.entry_info["ce_entry"] * 1.70
            self.pe_stop = self.entry_info["pe_entry"] * 1.70
            log.info(f"Resumed with entry prices and SLs.")
        else:
            log.info("No matching strangle position found to resume.")

    def _calculate_closed_pnl(self) -> float:
        trade_book = self.api.get_trade_book()
        if not trade_book:
            return 0.0

        pnl = 0.0
        trades = defaultdict(list)
        for trade in trade_book:
            trades[trade['tradingsymbol']].append(trade)

        for symbol, symbol_trades in trades.items():
            buys = sorted([t for t in symbol_trades if t['transactiontype'] == 'BUY'], key=lambda x: x['fillid'])
            sells = sorted([t for t in symbol_trades if t['transactiontype'] == 'SELL'], key=lambda x: x['fillid'])

            while buys and sells:
                buy = buys.pop(0)
                sell = sells.pop(0)
                pnl += (float(sell['fillprice']) - float(buy['fillprice'])) * int(buy['fillsize'])
        
        return pnl

    def _on_tick(self, payload: dict):
        try:
            token = str(payload.get("token") or payload.get("tk"))
            if not token: return
            ltp = float(payload.get("last_traded_price") or payload.get("ltp") or payload.get("lp"))
            if payload.get("exchange_type") == 2: ltp /= 100.0
            vol = float(payload.get("volume") or payload.get("v") or 0)
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
        for _ in range(timeout):
            time.sleep(1)
            status = self.api.get_order_status(order_id)
            if status and status.get("status") == "complete":
                return status
        return None

    def _execute_entry(self, current_sum_price: float, current_vwap: float):
        log.info(f"event=ORDER_ENTRY_INIT | Triggered at sum_price={current_sum_price:.2f}, vwap={current_vwap:.2f}")
        
        try:
            ce_order_id = self.api.place_order(self.ce_contract.symbol, self.ce_contract.token, self.NIFTY_LOT_SIZE, "SELL")
            pe_order_id = self.api.place_order(self.pe_contract.symbol, self.pe_contract.token, self.NIFTY_LOT_SIZE, "SELL")
        except Exception as e:
            log.error(f"Failed to place one or both SELL orders: {e}"); return

        ce_details = self._poll_order_status(ce_order_id)
        pe_details = self._poll_order_status(pe_order_id)

        if not ce_details or not pe_details:
            log.error("One or both entry orders failed. Exiting all legs."); 
            self._execute_exit(ce_only=bool(ce_details), pe_only=bool(pe_details), exit_reason="PARTIAL_ENTRY_FAILED"); return

        self.in_position = True
        self.entry_info = {
            "ts": datetime.now().isoformat(),
            "ce_entry": float(ce_details['averageprice']), "pe_entry": float(pe_details['averageprice']),
        }
        self.ce_stop = self.entry_info["ce_entry"] * 1.70
        self.pe_stop = self.entry_info["pe_entry"] * 1.70
        log.info(f"{Fore.GREEN}event=ENTRY_CONFIRMED | Strangle position entered.")

    def _execute_exit(self, ce_only=False, pe_only=False, exit_reason="STRATEGY_EXIT"):
        log.info(f"event=ORDER_EXIT_INIT | Triggered by: {exit_reason}")
        if not ce_only:
            self.api.place_order(self.pe_contract.symbol, self.pe_contract.token, self.NIFTY_LOT_SIZE, "BUY")
        if not pe_only:
            self.api.place_order(self.ce_contract.symbol, self.ce_contract.token, self.NIFTY_LOT_SIZE, "BUY")
        
        self.in_position = False
        time.sleep(2) # Give time for orders to be updated in tradebook
        self.closed_pnl = self._calculate_closed_pnl()
        log.info(f"event=EXIT_CONFIRMED | Exit orders placed. New Closed PNL: {self.closed_pnl:.2f}. Reason={exit_reason}")
        self._close_ws()

    def _process_strategy_on_tick(self, updated_token: str, updated_ltp: float, updated_vol: float):
        now = datetime.now()
        if self.in_position and now.time() >= dt_time(14, 50):
            self._execute_exit(exit_reason="EOD"); return

        ce_ltp = self.latest_ltp.get(self.ce_contract.token)
        pe_ltp = self.latest_ltp.get(self.pe_contract.token)

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
                    log.info(f"{Fore.CYAN}event=CONDITION_MET | price={sum_price:.2f} > vwap={vwap:.2f}. Armed.")
            else:
                if sum_price <= vwap:
                    self._execute_entry(sum_price, vwap)
        else:
            open_pnl = ((self.entry_info['ce_entry'] - ce_ltp) + (self.entry_info['pe_entry'] - pe_ltp)) * self.NIFTY_LOT_SIZE
            total_pnl = open_pnl + self.closed_pnl
            
            pnl_color = Fore.GREEN if total_pnl >= 0 else Fore.RED
            log.info(f"event=PNL_UPDATE | Open PNL: {open_pnl:+.2f}, Closed PNL: {self.closed_pnl:+.2f}, Total PNL: {pnl_color}{total_pnl:+.2f}{Style.RESET_ALL}")

            if total_pnl >= self.take_profit_points:
                self._execute_exit(exit_reason="TAKE_PROFIT")
            elif ce_ltp >= self.ce_stop or pe_ltp >= self.pe_stop:
                self._execute_exit(exit_reason="STOP_LOSS_PCT")

    def run(self):
        self.prepare_contracts()
        self._check_and_resume_position()

        if self.in_position:
            log.info("Starting WebSocket to monitor existing position.")
        else:
            log.info("Starting WebSocket for new entry.")

        try:
            self.ws = SmartWebSocketV2(self.api.jwt_token, self.api.api_key, self.api.client_id, self.api.feed_token)
            token_list = [{"exchangeType": 2, "tokens": [self.ce_contract.token, self.pe_contract.token]}]
            
            def on_open(wsapp):
                log.info("event=WS_OPEN | Subscribing to tokens: %s", token_list)
                self.ws.subscribe(f"strangle_{self.trading_date}", 1, token_list)

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
    parser.add_argument("--expiry", help="Optional expiry string like 27NOV2025")
    parser.add_argument("--date", help="Trading date YYYY-MM-DD (default: today)")
    args = parser.parse_args()
    trading_date = datetime.strptime(args.date, "%Y-%m-%d").date() if args.date else date.today()
    s = StrangleLive(trading_date=trading_date, expiry=args.expiry)
    s.run()

if __name__ == "__main__":
    main()
