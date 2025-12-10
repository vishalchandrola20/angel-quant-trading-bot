# src/backtest/vwap_ce_pe_strategy.py
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, date, time
import time as time_module

from src.api.smartapi_client import AngelAPI
from src.data_pipeline.nifty_first_15m import get_nifty_first_15m_close
from src.strategy.strike_selection import get_single_ce_pe_strikes
from src.market.contracts import find_nifty_option
from pathlib import Path
import csv


log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

LOT_SIZE = 75

@dataclass
class Bar:
    ts: str
    ce_close: float
    pe_close: float
    ce_volume: float
    pe_volume: float

    @property
    def sum_price(self) -> float:
        return self.ce_close + self.pe_close

    @property
    def sum_volume(self) -> float:
        # if volume is zero or missing, we fallback to 1 to avoid div-by-zero
        v = (self.ce_volume or 0) + (self.pe_volume or 0)
        return v if v > 0 else 1.0


def _fetch_intraday_bars_for_ce_pe(
        trading_date: date,
        bar_interval: str = "FIVE_MINUTE",
        expiry_str: str | None = None,
) -> tuple[list[Bar], str, str]:
    """
    1. Get first 15m close & CE/PE strikes (your custom logic)
    2. Resolve CE & PE option contracts (auto-expiry if not provided)
    3. Fetch intraday candles for CE & PE from 09:30 to 15:15
    """

    # 1) First 15m close & strikes
    first15_close = get_nifty_first_15m_close(trading_date)
    strikes_info = get_single_ce_pe_strikes(first15_close)

    ce_strike = strikes_info["ce_strike"]
    pe_strike = strikes_info["pe_strike"]

    log.info("Backtest date=%s first15_close=%s", trading_date, first15_close)
    log.info("CE strike=%s | PE strike=%s", ce_strike, pe_strike)

    # 2) Resolve contracts using Scrip Master (auto expiry if expiry_str is None)
    ce_contract = find_nifty_option(ce_strike, "CE", expiry_str=expiry_str, trading_date=trading_date)
    pe_contract = find_nifty_option(pe_strike, "PE", expiry_str=expiry_str, trading_date=trading_date)

    # 3) Fetch intraday candles from SmartAPI
    api = AngelAPI()
    api.login()
    time_module.sleep(1)
    if api.mock:
        raise RuntimeError("AngelAPI in MOCK mode; cannot backtest with real data.")

    start_dt = datetime.combine(trading_date, time(9, 30))
    end_dt = datetime.combine(trading_date, time(15, 15)) # Changed to 15:15

    from_str = start_dt.strftime("%Y-%m-%d %H:%M")
    to_str = end_dt.strftime("%Y-%m-%d %H:%M")

    log.info("Fetching CE candles %s (%s) %s→%s", ce_contract.symbol, ce_contract.token, from_str, to_str)
    ce_res = api.connection.getCandleData(
        {
            "exchange": ce_contract.exchange,
            "symboltoken": ce_contract.token,
            "interval": bar_interval,
            "fromdate": from_str,
            "todate": to_str,
        }
    )

    log.info("Fetching PE candles %s (%s) %s→%s", pe_contract.symbol, pe_contract.token, from_str, to_str)
    pe_res = api.connection.getCandleData(
        {
            "exchange": pe_contract.exchange,
            "symboltoken": pe_contract.token,
            "interval": bar_interval,
            "fromdate": from_str,
            "todate": to_str,
        }
    )

    ce_bars_raw = ce_res.get("data") or []
    pe_bars_raw = pe_res.get("data") or []

    if not ce_bars_raw or not pe_bars_raw:
        raise RuntimeError("No candle data returned for CE/PE in backtest window")

    # assume both lists have same timestamps and length
    bars: list[Bar] = []
    for ce_row, pe_row in zip(ce_bars_raw, pe_bars_raw):
        ts_ce, o1, h1, l1, c1, v1 = ce_row
        ts_pe, o2, h2, l2, c2, v2 = pe_row
        # you could assert ts_ce == ts_pe here if needed
        bars.append(
            Bar(
                ts=str(ts_ce),
                ce_close=float(c1),
                pe_close=float(c2),
                ce_volume=float(v1 or 0),
                pe_volume=float(v2 or 0),
            )
        )

    return bars, ce_contract.symbol, pe_contract.symbol


def run_vwap_strangle_strategy_for_day(
        trading_date: date,
        bar_interval: str = "ONE_MINUTE",
        expiry_str: str | None = None,
        stop_loss_pct: float = 0.70,
        absolute_stop_loss: float = 2000.0,
        export_csv: bool = True,
):
    """
    Your strategy:
      - Use CE+PE sum as 'price'
      - Compute running VWAP on (sum_price, sum_volume)
      - Entry condition:
          * Wait until sum > VWAP at least once
          * Then, when sum comes back down to VWAP or below => SELL both CE & PE
      - Exit condition (whichever comes first):
          * Per-leg SL: 70% (if either leg goes +70% over entry)
          * Absolute SL: Total loss reaches 2000
          * EOD: Exit at last bar (~3 pm)
      - Also: export per-bar data (VWAP vs sum) to CSV for visualization
    """

    bars, ce_symbol, pe_symbol = _fetch_intraday_bars_for_ce_pe(
        trading_date, bar_interval, expiry_str
    )

    cum_pv = 0.0
    cum_vol = 0.0

    seen_sum_above_vwap = False
    in_position = False

    entry_index = None
    entry_ce = entry_pe = None
    exit_index = None
    exit_ce = exit_pe = None
    exit_reason = None

    ce_stop = pe_stop = None

    records: list[dict] = []

    for i, bar in enumerate(bars):
        price = bar.sum_price
        volume = bar.sum_volume

        cum_pv += price * volume
        cum_vol += volume
        vwap = cum_pv / cum_vol if cum_vol > 0 else price

        entry_flag = False
        exit_flag = False
        current_reason = ""

        # Only check for entry if we haven't already exited a trade today
        if not in_position and exit_index is None:
            if not seen_sum_above_vwap:
                if price > vwap:
                    seen_sum_above_vwap = True
            else:
                if price <= vwap:
                    in_position = True
                    entry_index = i
                    entry_ce = bar.ce_close
                    entry_pe = bar.pe_close

                    ce_stop = entry_ce * (1 + stop_loss_pct)
                    pe_stop = entry_pe * (1 + stop_loss_pct)

                    entry_flag = True
                    current_reason = "ENTRY"

                    print(
                        f"ENTRY at {bar.ts}: "
                        f"{ce_symbol}={entry_ce:.2f}, {pe_symbol}={entry_pe:.2f}, "
                        f"Sum={price:.2f}, VWAP={vwap:.2f}"
                    )
        
        # If we are in a position, check for exits
        if in_position:
            ce_p = bar.ce_close
            pe_p = bar.pe_close
            
            pnl_ce = (entry_ce - ce_p) * LOT_SIZE
            pnl_pe = (entry_pe - pe_p) * LOT_SIZE
            total_pnl = pnl_ce + pnl_pe

            temp_exit_reason = None
            if ce_p >= ce_stop or pe_p >= pe_stop:
                temp_exit_reason = "STOP_LOSS_PCT"
            elif total_pnl <= -absolute_stop_loss:
                temp_exit_reason = "STOP_LOSS_ABS"

            if temp_exit_reason:
                in_position = False # We are now out of the position
                exit_index = i
                exit_ce = ce_p
                exit_pe = pe_p
                exit_reason = temp_exit_reason
                exit_flag = True
                current_reason = exit_reason

                print(
                    f"EXIT ({exit_reason}) at {bar.ts}: "
                    f"{ce_symbol}={exit_ce:.2f}, {pe_symbol}={exit_pe:.2f}, Total PNL={total_pnl:.2f}"
                )

        records.append(
            {
                "ts": bar.ts,
                "ce_close": bar.ce_close,
                "pe_close": bar.pe_close,
                "sum_price": price,
                "ce_volume": bar.ce_volume,
                "pe_volume": bar.pe_volume,
                "vwap": vwap,
                "in_position": int(in_position),
                "entry_flag": int(entry_flag),
                "exit_flag": int(exit_flag),
                "reason": current_reason,
            }
        )

    # If a trade was entered but not exited by SL, it's an EOD exit
    if entry_index is not None and exit_index is None:
        last_bar = bars[-1]
        exit_index = len(bars) - 1
        exit_ce = last_bar.ce_close
        exit_pe = last_bar.pe_close
        exit_reason = "EOD"

        records[-1]["exit_flag"] = 1
        records[-1]["reason"] = exit_reason

        print(
            f"EXIT (EOD) at {last_bar.ts}: "
            f"{ce_symbol}={exit_ce:.2f}, {pe_symbol}={exit_pe:.2f}"
        )

    if entry_index is None:
        print("No entry signal for the day (VWAP pattern never met).")

    if entry_index is not None and exit_index is not None:
        pnl_ce_per_share = entry_ce - exit_ce
        pnl_pe_per_share = entry_pe - exit_pe
        
        total_pnl_per_lot = (pnl_ce_per_share + pnl_pe_per_share) * LOT_SIZE

        print("\n--- Day Summary ---")
        print(f"Date: {trading_date}")
        print(f"CE symbol: {ce_symbol}")
        print(f"PE symbol: {pe_symbol}")
        print(f"Entry bar index: {entry_index} at price CE={entry_ce:.2f}, PE={entry_pe:.2f}")
        print(f"Exit bar index: {exit_index} at price CE={exit_ce:.2f}, PE={exit_pe:.2f}")
        print(f"Exit reason: {exit_reason}")
        print(f"PNL CE (per share): {pnl_ce_per_share:.2f}")
        print(f"PNL PE (per share): {pnl_pe_per_share:.2f}")
        print(f"Total PNL (1 lot of {LOT_SIZE}): {total_pnl_per_lot:.2f}")

    if export_csv:
        out_dir = Path("data/processed/strangle")
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / f"vwap_backtest_{trading_date}.csv"

        fieldnames = [
            "ts", "ce_close", "pe_close", "sum_price", "vwap",
            "ce_volume", "pe_volume", "in_position", "entry_flag",
            "exit_flag", "reason",
        ]

        with out_path.open("w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for row in records:
                writer.writerow(row)

        print(f"Bar-by-bar data exported to {out_path}")
