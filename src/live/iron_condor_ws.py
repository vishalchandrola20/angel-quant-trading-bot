"""
Live WebSocket streamer for Iron Condor strategy with live order placement and execution handling.
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


class IronCondorLive:
    NIFTY_LOT_SIZE = 150

    def __init__(
            self,
            trading_date: date | None = None,
            expiry: str | None = None,
            take_profit_per_lot: float = 9.0,
            absolute_stop_loss_per_lot: float = 26.67,
    ):
        self.trading_date = trading_date or date.today()
        self.expiry = expiry
        
        self.take_profit_points = take_profit_per_lot * self.NIFTY_LOT_SIZE
        self.absolute_stop_loss = absolute_stop_loss_per_lot * self.NIFTY_LOT_SIZE

        self.api = AngelAPI()
        self.api.login()
        time.sleep(1)

        self.short_ce_contract: OptionContract | None = None
        self.short_pe_contract: OptionContract | None = None
        self.long_ce_contract: OptionContract | None = None
        self.long_pe_contract: OptionContract | None = None
        
        self.latest_ltp: Dict[str, float] = {}
        self.cum_pv = 0.0
        self.cum_vol = 0.0
        self.seen_net_credit_above_vwap = False
        self.in_position = False
        self.entry_info: Dict[str, Any] = {}
        self.exit_info: Dict[str, Any] = {}
        self.closed_pnl = 0.0
        self.ws = None

        self.events_dir = Path("data/live")
        self.events_dir.mkdir(parents=True, exist_ok=True)
        self.csv_path = self.events_dir / f"iron_condor_events_{self.trading_date}.csv"
        self._ensure_csv()
        log.info(
            f"Strategy configured with TP: {self.take_profit_points:.2f} ({take_profit_per_lot}/lot), "
            f"Abs SL: {self.absolute_stop_loss:.2f} ({absolute_stop_loss_per_lot}/lot)"
        )


    def _ensure_csv(self):
        if not self.csv_path.exists():
            with self.csv_path.open("w", newline="") as f:
                writer = csv.writer(f)
                writer.writerow([
                    "ts", "net_credit", "vwap", "in_position", "event", "details"
                ])

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
        first15 = get_nifty_first_15m_close(self.trading_date)
        strikes_info = get_single_ce_pe_strikes(first15)
        
        short_ce_strike, short_pe_strike = strikes_info["ce_strike"], strikes_info["pe_strike"]
        long_ce_strike = short_ce_strike + 8 * 50
        long_pe_strike = short_pe_strike - 8 * 50

        self.short_ce_contract = find_nifty_option(short_ce_strike, "CE", self.expiry, self.trading_date)
        self.short_pe_contract = find_nifty_option(short_pe_strike, "PE", self.expiry, self.trading_date)
        self.long_ce_contract = find_nifty_option(long_ce_strike, "CE", self.expiry, self.trading_date)
        self.long_pe_contract = find_nifty_option(long_pe_strike, "PE", self.expiry, self.trading_date)
        
        now = datetime.now()
        start_of_day = datetime.combine(self.trading_date, dt_time(9, 15))
        
        if now > start_of_day:
            log.info("event=VWAP_INIT | Pre-filling VWAP from 9:15 AM to current time using OHLC/4.")
            short_ce_hist = self._get_historical_candles(self.short_ce_contract, start_of_day, now)
            time.sleep(1)
            short_pe_hist = self._get_historical_candles(self.short_pe_contract, start_of_day, now)
            time.sleep(1)
            long_ce_hist = self._get_historical_candles(self.long_ce_contract, start_of_day, now)
            time.sleep(1)
            long_pe_hist = self._get_historical_candles(self.long_pe_contract, start_of_day, now)

            if all([short_ce_hist, short_pe_hist, long_ce_hist, long_pe_hist]):
                for s_ce, s_pe, l_ce, l_pe in zip(short_ce_hist, short_pe_hist, long_ce_hist, long_pe_hist):
                    s_ce_o, s_ce_h, s_ce_l, s_ce_c, s_ce_v = [float(x or 0) for x in s_ce[1:]]
                    s_pe_o, s_pe_h, s_pe_l, s_pe_c, s_pe_v = [float(x or 0) for x in s_pe[1:]]
                    l_ce_o, l_ce_h, l_ce_l, l_ce_c, l_ce_v = [float(x or 0) for x in l_ce[1:]]
                    l_pe_o, l_pe_h, l_pe_l, l_pe_c, l_pe_v = [float(x or 0) for x in l_pe[1:]]

                    net_credit_open = (s_ce_o + s_pe_o) - (l_ce_o + l_pe_o)
                    net_credit_high = (s_ce_h + s_pe_h) - (l_ce_l + l_pe_l)
                    net_credit_low = (s_ce_l + s_pe_l) - (l_ce_h + l_pe_h)
                    net_credit_close = (s_ce_c + s_pe_c) - (l_ce_c + l_pe_c)
                    combined_vol = s_ce_v + s_pe_v + l_ce_v + l_pe_v
                    
                    ohlc4_price = (net_credit_open + net_credit_high + net_credit_low + net_credit_close) / 4
                    price_volume = ohlc4_price * combined_vol
                    
                    self.cum_pv += price_volume
                    self.cum_vol += combined_vol
                
                if self.cum_vol > 0:
                    initial_vwap = self.cum_pv / self.cum_vol
                    log.info(f"event=VWAP_INIT | VWAP initialized to {initial_vwap:.2f} with {len(short_ce_hist)} historical bars.")

    def _check_and_resume_position(self):
        log.info("Checking for existing open positions...")
        #self.closed_pnl = self._calculate_closed_pnl()
        log.info(f"Found initial closed PNL: {self.closed_pnl:.2f}")

        open_positions = self.api.get_open_positions()
        if not open_positions:
            log.info("No open positions found.")
            return

        s_ce, s_pe, l_ce, l_pe = None, None, None, None
        for pos in open_positions:
            symbol = pos.get("tradingsymbol")
            if int(pos.get('sellqty', 0)) > int(pos.get('buyqty', 0)): # Net short
                if symbol == self.short_ce_contract.symbol: s_ce = pos
                elif symbol == self.short_pe_contract.symbol: s_pe = pos
            elif int(pos.get('buyqty', 0)) > int(pos.get('sellqty', 0)): # Net long
                if symbol == self.long_ce_contract.symbol: l_ce = pos
                elif symbol == self.long_pe_contract.symbol: l_pe = pos
        
        if all([s_ce, s_pe, l_ce, l_pe]):
            log.warning("event=RESUME_POSITION | Found existing open Iron Condor. Resuming management.")
            self.in_position = True
            
            self.entry_info = {
                "ts": datetime.now().isoformat(),
                "short_ce_entry": float(s_ce['sellavgprice']),
                "short_pe_entry": float(s_pe['sellavgprice']),
                "long_ce_entry": float(l_ce['buyavgprice']),
                "long_pe_entry": float(l_pe['buyavgprice']),
            }
            self.short_ce_stop = self.entry_info["short_ce_entry"] * 2.20
            self.short_pe_stop = self.entry_info["short_pe_entry"] * 2.20
            log.info(f"Resumed with entry prices and SLs.")
        else:
            log.info("No complete Iron Condor position found to resume.")

    def _calculate_closed_pnl(self) -> float:
        trade_book = self.api.get_trade_book()
        log.info(trade_book)
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
                pnl += (float(sell['fillprice']) - float(buy['fillprice'])) * int(buy['fillquantity'])
        
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

    def _execute_entry(self, current_net_credit: float, current_vwap: float):
        log.info(f"event=ORDER_ENTRY_INIT | Triggered at NetCredit={current_net_credit:.2f}, VWAP={current_vwap:.2f}")
        
        try:
            lc_id = self.api.place_order(self.long_ce_contract.symbol, self.long_ce_contract.token, self.NIFTY_LOT_SIZE, "BUY")
            lp_id = self.api.place_order(self.long_pe_contract.symbol, self.long_pe_contract.token, self.NIFTY_LOT_SIZE, "BUY")
        except Exception as e:
            log.error(f"Failed to place one or both BUY orders: {e}"); return

        lc_details = self._poll_order_status(lc_id)
        lp_details = self._poll_order_status(lp_id)

        if not lc_details or not lp_details:
            log.error("One or both BUY orders failed to confirm. Aborting entry."); return

        try:
            sc_id = self.api.place_order(self.short_ce_contract.symbol, self.short_ce_contract.token, self.NIFTY_LOT_SIZE, "SELL")
            sp_id = self.api.place_order(self.short_pe_contract.symbol, self.short_pe_contract.token, self.NIFTY_LOT_SIZE, "SELL")
        except Exception as e:
            log.error(f"Failed to place one or both SELL orders: {e}. Exiting bought legs.")
            self._execute_exit(exit_reason="SELL_LEG_FAILED"); return

        sc_details = self._poll_order_status(sc_id)
        sp_details = self._poll_order_status(sp_id)

        if not sc_details or not sp_details:
            log.error("One or both SELL orders failed. Exiting all legs.")
            self._execute_exit(exit_reason="PARTIAL_SELL_FAILED"); return

        self.in_position = True
        self.entry_info = {
            "ts": datetime.now().isoformat(),
            "short_ce_entry": float(sc_details['averageprice']), "short_pe_entry": float(sp_details['averageprice']),
            "long_ce_entry": float(lc_details['averageprice']), "long_pe_entry": float(lp_details['averageprice']),
        }
        self.short_ce_stop = self.entry_info["short_ce_entry"] * 1.70
        self.short_pe_stop = self.entry_info["short_pe_entry"] * 1.70
        log.info(f"{Fore.GREEN}event=ENTRY_CONFIRMED | Iron Condor position entered.")

    def _execute_exit(self, exit_reason="STRATEGY_EXIT"):
        log.info(f"event=ORDER_EXIT_INIT | Triggered by: {exit_reason}")
        self.api.place_order(self.short_ce_contract.symbol, self.short_ce_contract.token, self.NIFTY_LOT_SIZE, "BUY")
        self.api.place_order(self.short_pe_contract.symbol, self.short_pe_contract.token, self.NIFTY_LOT_SIZE, "BUY")
        self.api.place_order(self.long_ce_contract.symbol, self.long_ce_contract.token, self.NIFTY_LOT_SIZE, "SELL")
        self.api.place_order(self.long_pe_contract.symbol, self.long_pe_contract.token, self.NIFTY_LOT_SIZE, "SELL")
        
        self.in_position = False
        time.sleep(2) # Give time for orders to be updated in tradebook
        self.closed_pnl = self._calculate_closed_pnl()
        log.info(f"event=EXIT_CONFIRMED | All 4 exit orders placed. New Closed PNL: {self.closed_pnl:.2f}. Reason={exit_reason}")
        self._close_ws()

    def _process_strategy_on_tick(self, updated_token: str, updated_ltp: float, updated_vol: float):
        now = datetime.now()
        if self.in_position and now.time() >= dt_time(14, 50):
            self._execute_exit(exit_reason="EOD"); return

        s_ce_ltp = self.latest_ltp.get(self.short_ce_contract.token)
        s_pe_ltp = self.latest_ltp.get(self.short_pe_contract.token)
        l_ce_ltp = self.latest_ltp.get(self.long_ce_contract.token)
        l_pe_ltp = self.latest_ltp.get(self.long_pe_contract.token)

        if not all([s_ce_ltp, s_pe_ltp, l_ce_ltp, l_pe_ltp]): return

        net_credit = (s_ce_ltp + s_pe_ltp) - (l_ce_ltp + l_pe_ltp)
        price_volume = net_credit * updated_vol
        self.cum_pv += price_volume
        self.cum_vol += updated_vol
        vwap = self.cum_pv / self.cum_vol if self.cum_vol > 0 else net_credit

        if not self.in_position:
            if not self.seen_net_credit_above_vwap:
                if net_credit > vwap:
                    self.seen_net_credit_above_vwap = True
                    log.info(f"{Fore.CYAN}event=CONDITION_MET | NetCredit={net_credit:.2f} > VWAP={vwap:.2f}. Armed.")
            else:
                if net_credit <= vwap:
                    self._execute_entry(net_credit, vwap)
        else:
            entry_net_credit = (self.entry_info['short_ce_entry'] + self.entry_info['short_pe_entry']) - \
                               (self.entry_info['long_ce_entry'] + self.entry_info['long_pe_entry'])
            
            open_pnl = (entry_net_credit - net_credit) * self.NIFTY_LOT_SIZE
            total_pnl = open_pnl + self.closed_pnl
            
            pnl_color = Fore.GREEN if total_pnl >= 0 else Fore.RED
            log.info(f"event=PNL_UPDATE | Open PNL: {open_pnl:+.2f}, Closed PNL: {self.closed_pnl:+.2f}, Total PNL: {pnl_color}{total_pnl:+.2f}{Style.RESET_ALL}")

            if total_pnl >= self.take_profit_points:
                self._execute_exit(exit_reason="TAKE_PROFIT")
            elif total_pnl <= -self.absolute_stop_loss:
                self._execute_exit(exit_reason="STOP_LOSS_ABS")
            elif s_ce_ltp >= self.short_ce_stop or s_pe_ltp >= self.short_pe_stop:
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
            token_list = [{"exchangeType": 2, "tokens": [
                self.short_ce_contract.token, self.short_pe_contract.token,
                self.long_ce_contract.token, self.long_pe_contract.token
            ]}]
            
            def on_open(wsapp):
                log.info("event=WS_OPEN | Subscribing to tokens: %s", token_list)
                self.ws.subscribe(f"condor_{self.trading_date}", 1, token_list)

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
    s = IronCondorLive(trading_date=trading_date, expiry=args.expiry)
    s.run()

if __name__ == "__main__":
    main()
