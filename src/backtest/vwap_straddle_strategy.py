# src/backtest/vwap_straddle_strategy.py
from __future__ import annotations
import csv
from dataclasses import dataclass
from datetime import datetime, date, time
from pathlib import Path
import logging

from src.api.smartapi_client import AngelAPI
from src.data_pipeline.nifty_first_15m import get_nifty_first_15m_close
from src.strategy.strike_selection import get_atm_strike_custom
from src.market.contracts import find_nifty_option

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


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
        v = (self.ce_volume or 0) + (self.pe_volume or 0)
        return v if v > 0 else 1.0


def _fetch_intraday_bars_for_atm_straddle(
        trading_date: date,
        bar_interval: str = "ONE_MINUTE",
        expiry_str: str | None = None,
) -> tuple[list[Bar], str, str]:
    """
    Resolve ATM strike, then fetch CE/PE intraday candles for that ATM strike
    """
    first15_close = get_nifty_first_15m_close(trading_date)
    atm = get_atm_strike_custom(first15_close)

    ce_strike = atm
    pe_strike = atm

    log.info("Backtest date=%s first15_close=%s ATM=%s", trading_date, first15_close, atm)

    # resolve option contracts (CE and PE at same strike)
    ce_contract = find_nifty_option(ce_strike, "CE", expiry_str=expiry_str, trading_date=trading_date)
    pe_contract = find_nifty_option(pe_strike, "PE", expiry_str=expiry_str, trading_date=trading_date)

    api = AngelAPI()
    api.login()
    if api.mock:
        raise RuntimeError("AngelAPI in MOCK mode; cannot backtest with real data.")

    start_dt = datetime.combine(trading_date, time(9, 30))
    end_dt = datetime.combine(trading_date, time(15, 0))

    from_str = start_dt.strftime("%Y-%m-%d %H:%M")
    to_str = end_dt.strftime("%Y-%m-%d %H:%M")

    log.info("Fetching CE candles %s token=%s", ce_contract.symbol, ce_contract.token)
    ce_res = api.connection.getCandleData(
        {
            "exchange": ce_contract.exchange,
            "symboltoken": ce_contract.token,
            "interval": bar_interval,
            "fromdate": from_str,
            "todate": to_str,
        }
    )
    log.info("Fetching PE candles %s token=%s", pe_contract.symbol, pe_contract.token)
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
        raise RuntimeError("No candle data returned for ATM CE/PE in backtest window")

    bars: list[Bar] = []
    for ce_row, pe_row in zip(ce_bars_raw, pe_bars_raw):
        ts_ce, o1, h1, l1, c1, v1 = ce_row
        ts_pe, o2, h2, l2, c2, v2 = pe_row
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


def run_vwap_straddle_strategy_for_day(
        trading_date: date,
        bar_interval: str = "ONE_MINUTE",
        expiry_str: str | None = None,
        stop_loss_pct: float = 1,
        export_csv: bool = True,
):
    """
    VWAP straddle strategy backtest (SELL ATM CE & ATM PE).
    Same mechanics as your existing VWAP strangle strategy but both legs at ATM.
    """
    bars, ce_symbol, pe_symbol = _fetch_intraday_bars_for_atm_straddle(trading_date, bar_interval, expiry_str)

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

        # Entry rule: wait for sum > VWAP, then when sum <= VWAP -> SELL both legs
        if not in_position:
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
                        f"ENTRY at {bar.ts}: {ce_symbol}={entry_ce:.2f}, {pe_symbol}={entry_pe:.2f}, Sum={price:.2f}, VWAP={vwap:.2f}"
                    )
        else:
            # After entry: check per-leg SL
            ce_p = bar.ce_close
            pe_p = bar.pe_close

            if ce_p >= ce_stop or pe_p >= pe_stop:
                in_position = False
                exit_index = i
                exit_ce = ce_p
                exit_pe = pe_p
                exit_reason = "STOP_LOSS"
                exit_flag = True
                current_reason = exit_reason

                print(f"EXIT (SL) at {bar.ts}: {ce_symbol}={exit_ce:.2f}, {pe_symbol}={exit_pe:.2f}")
                # record and break after this bar

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

        if exit_flag and exit_reason == "STOP_LOSS":
            break

    # EOD exit if still in position
    if in_position:
        last_bar = bars[-1]
        in_position = False
        exit_index = len(bars) - 1
        exit_ce = last_bar.ce_close
        exit_pe = last_bar.pe_close
        exit_reason = "EOD"

        records[-1]["exit_flag"] = 1
        records[-1]["reason"] = exit_reason

        print(f"EXIT (EOD) at {last_bar.ts}: {ce_symbol}={exit_ce:.2f}, {pe_symbol}={exit_pe:.2f}")

    # If no entry, print and still export CSV
    if entry_index is None:
        print("No entry signal for the day (VWAP pattern never met).")

    # Compute P&L if we traded
    if entry_index is not None and exit_index is not None:
        pnl_ce = entry_ce - exit_ce
        pnl_pe = entry_pe - exit_pe
        total_pnl = pnl_ce + pnl_pe

        print("\n--- Day Summary ---")
        print(f"Date: {trading_date}")
        print(f"CE symbol: {ce_symbol}")
        print(f"PE symbol: {pe_symbol}")
        print(f"Entry bar index: {entry_index} at price CE={entry_ce:.2f}, PE={entry_pe:.2f}")
        print(f"Exit bar index: {exit_index} at price CE={exit_ce:.2f}, PE={exit_pe:.2f}")
        print(f"Exit reason: {exit_reason}")
        print(f"PNL CE: {pnl_ce:.2f}")
        print(f"PNL PE: {pnl_pe:.2f}")
        print(f"Total PNL (per lot): {total_pnl:.2f}")

    # Export CSV
    if export_csv:
        out_dir = Path("data/processed/straddle/100_sl")
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / f"vwap_straddle_backtest_{trading_date}.csv"

        fieldnames = [
            "ts",
            "ce_close",
            "pe_close",
            "sum_price",
            "vwap",
            "ce_volume",
            "pe_volume",
            "in_position",
            "entry_flag",
            "exit_flag",
            "reason",
        ]

        with out_path.open("w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for row in records:
                writer.writerow(row)

        print(f"Bar-by-bar data exported to {out_path}")
