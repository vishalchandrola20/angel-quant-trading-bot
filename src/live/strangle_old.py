"""
Live WebSocket streamer for Strangle strategy (mocked order placement).

How it works:
1) Compute today's first 15-min NIFTY close using existing helper.
2) Compute CE (ceil->+100) and PE (floor->-100) strikes with your rules.
3) Resolve option contracts via scrip master (auto-expiry if not provided).
4) Use SmartApi WebSocket for tick subscription to both tokens.
5) Aggregate ticks into 1-minute candles (open/high/low/close/volume).
6) Compute running VWAP on (CE+PE) sum and apply your entry/exit rules.
7) Log events and export CSV to data/live/strangle_events_<YYYY-MM-DD>.csv
8) Order placement calls are present but commented out for safe testing.

Run:
    source .venv/bin/activate
    python -m src.live.strangle_ws --expiry 27NOV2025   # optional --expiry
"""
from __future__ import annotations

import argparse
import csv
import json
import logging
import time
from collections import deque, defaultdict
from dataclasses import dataclass
from datetime import datetime, date, timedelta
from pathlib import Path
from typing import Dict, Any

from src.api.smartapi_client import AngelAPI
from src.data_pipeline.nifty_first_15m import get_nifty_first_15m_close
from src.strategy.strike_selection import get_single_ce_pe_strikes
from src.market.contracts import find_nifty_option

# If your SmartApi package exports SmartWebSocket under a different name, adjust import below.
try:
    from SmartApi.smartApiWebsocket import SmartWebSocket  # typical name in smartapi-python
except Exception:
    SmartWebSocket = None

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


@dataclass
class Tick:
    ts: float  # epoch seconds
    ltp: float
    volume: float


@dataclass
class Candle:
    open: float
    high: float
    low: float
    close: float
    volume: float
    ts_start: datetime


class StrangleLive:
    def __init__(self, trading_date: date | None = None, expiry: str | None = None, interval_seconds: int = 60):
        self.trading_date = trading_date or date.today()
        self.expiry = expiry  # if None, auto-pick next expiry
        self.interval_seconds = interval_seconds  # candle aggregation window (60 = 1-min)
        self.api = AngelAPI()
        self.api.login()
        time.sleep(1)
        if self.api.mock:
            raise RuntimeError("AngelAPI is in MOCK mode — cannot run live websocket streamer.")

        self.ce_contract = None
        self.pe_contract = None

        # per-token tick buffers
        self.tick_buffers: Dict[str, deque[Tick]] = defaultdict(lambda: deque())

        # per-token latest LTP fallback
        self.latest_ltp: Dict[str, float] = {}

        # running VWAP accumulators
        self.cum_pv = 0.0
        self.cum_vol = 0.0

        # strategy state
        self.seen_sum_above_vwap = False
        self.in_position = False
        self.entry_info: Dict[str, Any] = {}
        self.exit_info: Dict[str, Any] = {}

        # CSV logging
        self.events_dir = Path("data/live")
        self.events_dir.mkdir(parents=True, exist_ok=True)
        self.csv_path = self.events_dir / f"strangle_events_{self.trading_date}.csv"
        self._ensure_csv()

        # Candle holders: keyed by token -> current candle
        self.current_candle: Dict[str, Candle] = {}

        # Combined sum candles list for VWAP plotting/backtest
        self.sum_candles: list[dict] = []

    def _ensure_csv(self):
        if not self.csv_path.exists():
            with self.csv_path.open("w", newline="") as f:
                writer = csv.writer(f)
                writer.writerow(
                    [
                        "ts",
                        "ce_symbol",
                        "pe_symbol",
                        "ce_ltp",
                        "pe_ltp",
                        "sum_price",
                        "vwap",
                        "in_position",
                        "event",
                        "details",
                    ]
                )

    def log_event(self, ts: datetime, ce_sym, pe_sym, ce_ltp, pe_ltp, vwap, in_position, event, details=""):
        row = [
            ts.isoformat(),
            ce_sym,
            pe_sym,
            f"{ce_ltp:.2f}" if ce_ltp is not None else "",
            f"{pe_ltp:.2f}" if pe_ltp is not None else "",
            f"{(ce_ltp or 0) + (pe_ltp or 0):.2f}" if ce_ltp is not None or pe_ltp is not None else "",
            f"{vwap:.4f}" if vwap is not None else "",
            int(in_position),
            event,
            details,
        ]
        with self.csv_path.open("a", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(row)

        log.info("%s | event=%s | details=%s", ts.isoformat(), event, details)

    def prepare_contracts(self):
        # 1) Get 1st 15m close (for today we may need to wait until 09:30+15m if running early)
        first15 = get_nifty_first_15m_close(self.trading_date)
        strikes_info = get_single_ce_pe_strikes(first15)
        ce_strike = strikes_info["ce_strike"]
        pe_strike = strikes_info["pe_strike"]

        log.info("First15 close = %s ; CE strike=%s ; PE strike=%s", first15, ce_strike, pe_strike)

        # 2) Resolve tokens (auto expiry if needed)
        self.ce_contract = find_nifty_option(ce_strike, "CE", expiry_str=self.expiry, trading_date=self.trading_date)
        self.pe_contract = find_nifty_option(pe_strike, "PE", expiry_str=self.expiry, trading_date=self.trading_date)

        log.info("Resolved CE: %s token=%s ; PE: %s token=%s",
                 self.ce_contract.symbol, self.ce_contract.token, self.pe_contract.symbol, self.pe_contract.token)

    # --- Candle aggregation (1-min) from ticks ---
    def _start_new_candle(self, token: str, ts_start: datetime, first_price: float, first_vol: float) -> Candle:
        return Candle(open=first_price, high=first_price, low=first_price, close=first_price, volume=first_vol, ts_start=ts_start)

    def _add_tick_to_candle(self, token: str, tick: Tick):
        c = self.current_candle.get(token)
        if c is None:
            # align ts_start to floor of interval seconds
            ts_dt = datetime.fromtimestamp(tick.ts)
            floored = ts_dt - timedelta(seconds=ts_dt.second % self.interval_seconds,
                                        microseconds=ts_dt.microsecond)
            c = self._start_new_candle(token, floored, tick.ltp, tick.volume)
            self.current_candle[token] = c
            return

        # if tick belongs to next candle window, flush will happen by caller
        # else update current candle
        c.high = max(c.high, tick.ltp)
        c.low = min(c.low, tick.ltp)
        c.close = tick.ltp
        c.volume += tick.volume

    def _flush_and_get_combined(self):
        """
        Flush current candles for both CE & PE (if available) and produce combined sum candle
        Returns combined dict: {"ts": ts_start_iso, "ce_close", "pe_close", "sum", "vwap", "ce_vol","pe_vol"}
        """
        ce_token = self.ce_contract.token
        pe_token = self.pe_contract.token

        ce_c = self.current_candle.pop(ce_token, None)
        pe_c = self.current_candle.pop(pe_token, None)

        # if one leg missing, use last known ltp (fallback)
        ce_close = ce_c.close if ce_c else (self.latest_ltp.get(ce_token) or 0.0)
        pe_close = pe_c.close if pe_c else (self.latest_ltp.get(pe_token) or 0.0)
        ce_vol = ce_c.volume if ce_c else 0.0
        pe_vol = pe_c.volume if pe_c else 0.0

        price = ce_close + pe_close
        vol = ce_vol + pe_vol if (ce_vol + pe_vol) > 0 else 1.0

        # running VWAP
        self.cum_pv += price * vol
        self.cum_vol += vol
        vwap = self.cum_pv / self.cum_vol if self.cum_vol > 0 else price

        rec = {
            "ts": (ce_c.ts_start if ce_c else datetime.now()).isoformat(),
            "ce_close": ce_close,
            "pe_close": pe_close,
            "sum_price": price,
            "vwap": vwap,
            "ce_volume": ce_vol,
            "pe_volume": pe_vol,
        }
        self.sum_candles.append(rec)
        return rec

    # --- Tick handling & websocket callbacks ---
    def _on_tick(self, payload: dict):
        """
        Expected tick shape varies by SmartAPI version. Typical: {'t': token, 'ltp': 123.4, 'volume': 45, 'timestamp': '...'}
        Print payload to adapt if needed.
        """
        try:
            # adapt fields depending on provider
            token = str(payload.get("token") or payload.get("t") or payload.get("symboltoken") or payload.get("tk"))
            ltp = float(payload.get("last_price") or payload.get("ltp") or payload.get("lt"))
            vol = float(payload.get("volume") or payload.get("v") or 0)
            ts_epoch = time.time()
        except Exception as e:
            log.warning("Unexpected tick format: %s", payload)
            return

        # update latest ltp
        self.latest_ltp[token] = ltp

        tick = Tick(ts=ts_epoch, ltp=ltp, volume=vol)
        # add to token candle buffer
        self._add_tick_to_candle(token, tick)

    def _on_ws_message(self, ws, message):
        """
        Called by the SmartWebSocket wrapper; message may be JSON string or dict.
        We parse and route tick(s) to _on_tick. Different SmartAPI versions send different envelopes.
        """
        try:
            data = message
            if isinstance(message, (bytes, str)):
                data = json.loads(message)
        except Exception:
            data = message

        # SmartAPI often sends list of ticks under 'data' or as a list directly.
        if isinstance(data, dict) and "data" in data:
            payload = data["data"]
            # payload could be a list or single dict
            if isinstance(payload, list):
                for item in payload:
                    self._on_tick(item)
            elif isinstance(payload, dict):
                self._on_tick(payload)
            else:
                log.debug("Unknown data payload from websocket: %s", payload)
        elif isinstance(data, list):
            for item in data:
                self._on_tick(item)
        else:
            log.debug("Unhandled websocket message: %s", data)

    def _subscribe_tokens(self, ws):
        """
        Subscribe using websocket's subscription call. Different versions vary:
        Example payload (SmartApi typical): {"a":"subscribe","v":[<token1>,<token2>]}
        Or using ws.subscribe([...]) method if wrapper provides it.
        """
        tokens = [int(self.ce_contract.token), int(self.pe_contract.token)]
        log.info("Subscribing to tokens: %s", tokens)

        # If wrapper has 'subscribe' method:
        try:
            if hasattr(ws, "subscribe"):
                ws.subscribe(tokens)
                return
        except Exception:
            pass

        # fallback: send JSON message
        try:
            msg = json.dumps({"a": "subscribe", "v": tokens})
            ws.send(msg)
        except Exception:
            log.exception("Failed to subscribe via raw send. Please adapt to your SmartWebSocket wrapper.")

    # --- Main run loop: open websocket, subscribe, aggregate into interval windows, run strategy ---
    def run(self):
        # Ensure contracts resolved
        self.prepare_contracts()

        # Build websocket connection using SmartWebSocket if available
        if SmartWebSocket is None:
            log.warning("SmartWebSocket class not available in SmartApi package on this env. Falling back to polling LTP every 5s.")
            self._poll_loop_fallback()
            return

        # obtain feed token (api wrapper provides getfeedToken in many SDKs)
        try:
            feed_token = self.api.connection.getfeedToken()
        except Exception as e:
            log.exception("Failed to get feed token from SmartAPI: %s", e)
            feed_token = None

        # WebSocket URL (SmartAPI typically uses ws:// or wss:// with feed token)
        # The wrapper's SmartWebSocket usually handles url and login using client code & feed token.
        try:
            ws = SmartWebSocket(self.api.api_key, self.api.client_id, feed_token)  # wrapper-specific
            ws.on_message = self._on_ws_message
            ws.connect()
            self._subscribe_tokens(ws)
        except Exception as e:
            log.exception("Failed to construct SmartWebSocket (wrapper differences). Falling back to polling LTP. Err: %s", e)
            self._poll_loop_fallback()
            return

        # Aggregation loop: flush every interval_seconds
        try:
            next_flush = (datetime.now() + timedelta(seconds=self.interval_seconds)).replace(microsecond=0)
            while True:
                time.sleep(0.5)
                now = datetime.now()
                if now >= next_flush:
                    rec = self._flush_and_get_combined()
                    # make decision (VWAP vs sum)
                    ce_ltp = rec["ce_close"]
                    pe_ltp = rec["pe_close"]
                    price = rec["sum_price"]
                    vwap = rec["vwap"]

                    # log per-candle
                    self.log_event(now, self.ce_contract.symbol, self.pe_contract.symbol, ce_ltp, pe_ltp, vwap, self.in_position, "CANDLE", json.dumps(rec))

                    # Strategy logic
                    if not self.in_position:
                        if not self.seen_sum_above_vwap:
                            if price > vwap:
                                self.seen_sum_above_vwap = True
                        else:
                            if price <= vwap:
                                # ENTRY
                                self.in_position = True
                                entry_ce = ce_ltp
                                entry_pe = pe_ltp
                                self.entry_info = {
                                    "ts": now.isoformat(),
                                    "ce_entry": entry_ce,
                                    "pe_entry": entry_pe,
                                }

                                # commented order placement (mock)
                                # order_payload_ce = {...}  # build SmartAPI place order payload
                                # self.api.connection.placeOrder(order_payload_ce)
                                # order_payload_pe = {...}
                                # self.api.connection.placeOrder(order_payload_pe)

                                self.log_event(now, self.ce_contract.symbol, self.pe_contract.symbol, entry_ce, entry_pe, vwap, True, "ENTRY", "Orders mocked (commented)")

                                # set stops
                                self.ce_stop = entry_ce * 1.70
                                self.pe_stop = entry_pe * 1.70

                    else:
                        # monitor SL
                        ce_cur = ce_ltp
                        pe_cur = pe_ltp
                        if ce_cur >= getattr(self, "ce_stop", 1e12) or pe_cur >= getattr(self, "pe_stop", 1e12):
                            # STOP LOSS -> exit both legs
                            self.in_position = False
                            exit_ce = ce_cur
                            exit_pe = pe_cur
                            self.exit_info = {"ts": now.isoformat(), "ce_exit": exit_ce, "pe_exit": exit_pe, "reason": "STOP_LOSS"}

                            # commented order placement for exit
                            # self.api.connection.placeOrder({ ... sell cover ... })

                            self.log_event(now, self.ce_contract.symbol, self.pe_contract.symbol, exit_ce, exit_pe, vwap, False, "EXIT_SL", json.dumps(self.exit_info))
                            # we break the loop after SL (or continue monitoring, choose break)
                            break

                    next_flush = now + timedelta(seconds=self.interval_seconds)
        except KeyboardInterrupt:
            log.info("User interrupted. Closing websocket.")
            try:
                ws.close()
            except Exception:
                pass
        finally:
            # If still in position at EOD (or script end), do EOD exit
            if self.in_position:
                last_ts = datetime.now()
                last_ce = self.latest_ltp.get(self.ce_contract.token, 0.0)
                last_pe = self.latest_ltp.get(self.pe_contract.token, 0.0)
                self.in_position = False
                self.exit_info = {"ts": last_ts.isoformat(), "ce_exit": last_ce, "pe_exit": last_pe, "reason": "EOD"}
                # commented order placement
                # self.api.connection.placeOrder({ ... })
                self.log_event(last_ts, self.ce_contract.symbol, self.pe_contract.symbol, last_ce, last_pe, None, False, "EXIT_EOD", json.dumps(self.exit_info))

    # Fallback polling loop if websocket unavailable
    def _poll_loop_fallback(self, poll_interval: float = 5.0):
        log.info("Starting fallback polling loop (ltpData every %ss)", poll_interval)
        while True:
            try:
                ce_ltp = self.api.get_ltp(self.ce_contract.exchange, self.ce_contract.symbol, self.ce_contract.token)
                pe_ltp = self.api.get_ltp(self.pe_contract.exchange, self.pe_contract.symbol, self.pe_contract.token)
                now = datetime.now()
                price = ce_ltp + pe_ltp

                # simple VWAP update using 1 tick as volume=1 (approx)
                vol = 1.0
                self.cum_pv += price * vol
                self.cum_vol += vol
                vwap = self.cum_pv / self.cum_vol if self.cum_vol > 0 else price

                self.log_event(now, self.ce_contract.symbol, self.pe_contract.symbol, ce_ltp, pe_ltp, vwap, self.in_position, "POLL", "")

                # strategy same as above
                if not self.in_position:
                    if not self.seen_sum_above_vwap and price > vwap:
                        self.seen_sum_above_vwap = True
                    elif self.seen_sum_above_vwap and price <= vwap:
                        # ENTRY
                        self.in_position = True
                        entry_ce = ce_ltp
                        entry_pe = pe_ltp
                        self.entry_info = {"ts": now.isoformat(), "ce_entry": entry_ce, "pe_entry": entry_pe}
                        # mocked order placement
                        # self.api.connection.placeOrder(...)
                        self.log_event(now, self.ce_contract.symbol, self.pe_contract.symbol, entry_ce, entry_pe, vwap, True, "ENTRY", "Orders mocked (commented)")
                        self.ce_stop = entry_ce * 1.70
                        self.pe_stop = entry_pe * 1.70
                else:
                    if ce_ltp >= getattr(self, "ce_stop", 1e12) or pe_ltp >= getattr(self, "pe_stop", 1e12):
                        # STOP LOSS -> exit
                        self.in_position = False
                        exit_ce = ce_ltp
                        exit_pe = pe_ltp
                        self.exit_info = {"ts": now.isoformat(), "ce_exit": exit_ce, "pe_exit": exit_pe, "reason": "STOP_LOSS"}
                        # mocked exit order
                        # self.api.connection.placeOrder(...)
                        self.log_event(now, self.ce_contract.symbol, self.pe_contract.symbol, exit_ce, exit_pe, vwap, False, "EXIT_SL", json.dumps(self.exit_info))
                        break

                time.sleep(poll_interval)
            except KeyboardInterrupt:
                log.info("User interrupted polling loop.")
                break
            except Exception:
                log.exception("Polling loop error; sleeping and retrying.")
                time.sleep(poll_interval)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--expiry", help="Optional expiry string like 27NOV2025 (auto-picks if omitted)")
    parser.add_argument("--date", help="Trading date YYYY-MM-DD (default: today)")
    parser.add_argument("--interval", type=int, default=60, help="Aggregation interval seconds (default=60)")
    args = parser.parse_args()

    trading_date = datetime.strptime(args.date, "%Y-%m-%d").date() if args.date else None

    s = StrangleLive(trading_date=trading_date, expiry=args.expiry, interval_seconds=args.interval)
    s.run()


if __name__ == "__main__":
    main()
