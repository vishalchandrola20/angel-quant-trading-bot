# src/backtest/vwap_strangle_5_percent_strategy.py
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, date, time
import time as time_module

from src.api.smartapi_client import AngelAPI
from src.data_pipeline.nifty_first_15m import get_nifty_first_15m_close
from src.strategy.strike_selection import get_single_ce_pe_strikes
from src.market.contracts import find_nifty_option, OptionContract
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
        v = (self.ce_volume or 0) + (self.pe_volume or 0)
        return v if v > 0 else 1.0


def _get_bars_for_contract(api: AngelAPI, contract: OptionContract, from_str: str, to_str: str, bar_interval: str) -> list:
    log.info(f"Fetching candles for {contract.symbol} ({contract.token}) from {from_str} to {to_str}")
    res = api.connection.getCandleData({
        "exchange": contract.exchange,
        "symboltoken": contract.token,
        "interval": bar_interval,
        "fromdate": from_str,
        "todate": to_str,
    })
    return res.get("data") or []


def _fetch_intraday_bars_for_ce_pe(
        trading_date: date,
        bar_interval: str = "ONE_MINUTE",
        expiry_str: str | None = None,
) -> tuple[list[Bar], str, str]:
    """
    Fetches intraday bars, with a special rule to adjust strikes if the initial
    price difference between CE and PE is > 40.
    """
    first15_close = get_nifty_first_15m_close(trading_date)
    strikes_info = get_single_ce_pe_strikes(first15_close)
    ce_strike, pe_strike = strikes_info["ce_strike"], strikes_info["pe_strike"]

    log.info(f"Initial strikes: CE={ce_strike}, PE={pe_strike}")

    api = AngelAPI()
    api.login()
    time_module.sleep(1)
    if api.mock:
        raise RuntimeError("AngelAPI in MOCK mode; cannot backtest with real data.")

    start_dt = datetime.combine(trading_date, time(9, 30))
    end_dt = datetime.combine(trading_date, time(15, 15))
    from_str, to_str = start_dt.strftime("%Y-%m-%d %H:%M"), end_dt.strftime("%Y-%m-%d %H:%M")

    # 1. Fetch data for initial strikes
    ce_contract = find_nifty_option(ce_strike, "CE", expiry_str, trading_date)
    pe_contract = find_nifty_option(pe_strike, "PE", expiry_str, trading_date)
    
    ce_bars_raw = _get_bars_for_contract(api, ce_contract, from_str, to_str, bar_interval)
    pe_bars_raw = _get_bars_for_contract(api, pe_contract, from_str, to_str, bar_interval)

    if not ce_bars_raw or not pe_bars_raw:
        raise RuntimeError("No initial candle data returned for CE/PE.")

    # 2. Check the price of the first bar
    first_ce_price = float(ce_bars_raw[0][4])
    first_pe_price = float(pe_bars_raw[0][4])
    price_diff = abs(first_ce_price - first_pe_price)

    log.info(f"Initial prices at 09:30: CE={first_ce_price:.2f}, PE={first_pe_price:.2f}, Diff={price_diff:.2f}")

    # 3. Adjust if necessary
    if price_diff > 40:
        log.warning(f"Price difference > 40. Adjusting cheaper leg.")
        if first_ce_price < first_pe_price:
            # CE is cheaper, move strike down by 100 (2 strikes)
            new_ce_strike = ce_strike - 100
            log.info(f"CE is cheaper. New CE strike: {new_ce_strike}")
            ce_contract = find_nifty_option(new_ce_strike, "CE", expiry_str, trading_date)
            ce_bars_raw = _get_bars_for_contract(api, ce_contract, from_str, to_str, bar_interval)
        else:
            # PE is cheaper, move strike up by 100 (2 strikes)
            new_pe_strike = pe_strike + 100
            log.info(f"PE is cheaper. New PE strike: {new_pe_strike}")
            pe_contract = find_nifty_option(new_pe_strike, "PE", expiry_str, trading_date)
            pe_bars_raw = _get_bars_for_contract(api, pe_contract, from_str, to_str, bar_interval)
        
        if not ce_bars_raw or not pe_bars_raw:
            raise RuntimeError("No data returned for adjusted CE/PE leg.")

    # 4. Combine final bars
    bars: list[Bar] = []
    for ce_row, pe_row in zip(ce_bars_raw, pe_bars_raw):
        bars.append(Bar(
            ts=str(ce_row[0]),
            ce_close=float(ce_row[4]),
            pe_close=float(pe_row[4]),
            ce_volume=float(ce_row[5] or 0),
            pe_volume=float(pe_row[5] or 0),
        ))

    return bars, ce_contract.symbol, pe_contract.symbol


def run_vwap_strangle_5_percent_strategy_for_day(
        trading_date: date,
        bar_interval: str = "ONE_MINUTE",
        expiry_str: str | None = None,
        stop_loss_pct: float = 0.70,
        absolute_stop_loss: float = 2000.0,
        vwap_band_pct: float = 0.05,
        export_csv: bool = True,
):
    """
    Strategy:
      - Price is CE+PE sum.
      - Entry:
          * SELL strangle as soon as price > VWAP * (1 + vwap_band_pct).
      - Exit:
          * Per-leg SL or absolute loss SL.
          * EOD exit.
    """

    bars, ce_symbol, pe_symbol = _fetch_intraday_bars_for_ce_pe(
        trading_date, bar_interval, expiry_str
    )

    cum_pv = 0.0
    cum_vol = 0.0
    in_position = False
    entry_index = exit_index = None
    entry_ce = entry_pe = exit_ce = exit_pe = None
    exit_reason = None
    ce_stop = pe_stop = None
    records: list[dict] = []

    for i, bar in enumerate(bars):
        price = bar.sum_price
        volume = bar.sum_volume
        cum_pv += price * volume
        cum_vol += volume
        vwap = cum_pv / cum_vol if cum_vol > 0 else price
        
        vwap_upper = vwap * (1 + vwap_band_pct)
        vwap_lower = vwap * (1 - vwap_band_pct)

        entry_flag = exit_flag = False
        current_reason = ""

        if not in_position and exit_index is None:
            if price > vwap_upper:
                in_position = True
                entry_index = i
                entry_ce, entry_pe = bar.ce_close, bar.pe_close
                ce_stop, pe_stop = entry_ce * (1 + stop_loss_pct), entry_pe * (1 + stop_loss_pct)
                entry_flag, current_reason = True, "ENTRY"
                print(
                    f"ENTRY at {bar.ts}: price={price:.2f} > vwap_upper={vwap_upper:.2f} | "
                    f"CE_Entry={entry_ce:.2f}, PE_Entry={entry_pe:.2f}"
                )
        
        if in_position:
            ce_p, pe_p = bar.ce_close, bar.pe_close
            total_pnl = ((entry_ce - ce_p) + (entry_pe - pe_p)) * LOT_SIZE
            
            temp_exit_reason = None
            if ce_p >= ce_stop or pe_p >= pe_stop:
                temp_exit_reason = "STOP_LOSS_PCT"
            elif total_pnl <= -absolute_stop_loss:
                temp_exit_reason = "STOP_LOSS_ABS"

            if temp_exit_reason:
                in_position = False
                exit_index, exit_ce, exit_pe = i, ce_p, pe_p
                exit_reason, exit_flag, current_reason = temp_exit_reason, True, temp_exit_reason
                print(
                    f"EXIT ({exit_reason}) at {bar.ts}: PNL={total_pnl:.2f} | "
                    f"CE_Exit={exit_ce:.2f}, PE_Exit={exit_pe:.2f}"
                )

        records.append({
            "ts": bar.ts, "ce_close": bar.ce_close, "pe_close": bar.pe_close,
            "sum_price": price, "vwap": vwap, "vwap_upper": vwap_upper, "vwap_lower": vwap_lower,
            "in_position": int(in_position), "entry_flag": int(entry_flag),
            "exit_flag": int(exit_flag), "reason": current_reason,
        })

    if entry_index is not None and exit_index is None:
        last_bar = bars[-1]
        exit_index, exit_ce, exit_pe, exit_reason = len(bars) - 1, last_bar.ce_close, last_bar.pe_close, "EOD"
        records[-1].update({"exit_flag": 1, "reason": exit_reason})
        print(f"EXIT (EOD) at {last_bar.ts}: CE_Exit={exit_ce:.2f}, PE_Exit={exit_pe:.2f}")

    if entry_index is not None:
        pnl_per_lot = ((entry_ce - exit_ce) + (entry_pe - exit_pe)) * LOT_SIZE
        print("\n--- Day Summary ---")
        print(f"Date: {trading_date}, PNL: {pnl_per_lot:.2f}, Reason: {exit_reason}")

    if export_csv:
        out_dir = Path("data/processed/strangle_5_pct")
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / f"vwap_backtest_{trading_date}.csv"
        
        with out_path.open("w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=records[0].keys())
            writer.writeheader()
            writer.writerows(records)
        print(f"Data exported to {out_path}")
