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
    ce_open: float
    ce_high: float
    ce_low: float
    ce_close: float
    ce_volume: float
    pe_open: float
    pe_high: float
    pe_low: float
    pe_close: float
    pe_volume: float

    @property
    def combined_open(self) -> float:
        return self.ce_open + self.pe_open
    
    @property
    def combined_high(self) -> float:
        return self.ce_high + self.pe_high

    @property
    def combined_low(self) -> float:
        return self.ce_low + self.pe_low

    @property
    def combined_close(self) -> float:
        return self.ce_close + self.pe_close

    @property
    def combined_volume(self) -> float:
        v = (self.ce_volume or 0) + (self.pe_volume or 0)
        return v if v > 0 else 1.0


def _fetch_intraday_bars_for_ce_pe(
        trading_date: date,
        bar_interval: str = "FIVE_MINUTE",
        expiry_str: str | None = None,
) -> tuple[list[Bar], str, str]:
    """
    1. Get first 15m close & CE/PE strikes
    2. Resolve contracts
    3. Fetch intraday candles for CE & PE
    """
    first15_close = get_nifty_first_15m_close(trading_date)
    strikes_info = get_single_ce_pe_strikes(first15_close)
    ce_strike, pe_strike = strikes_info["ce_strike"], strikes_info["pe_strike"]

    log.info(f"Backtest date={trading_date}, first15_close={first15_close}")
    log.info(f"CE strike={ce_strike} | PE strike={pe_strike}")

    ce_contract = find_nifty_option(ce_strike, "CE", expiry_str=expiry_str, trading_date=trading_date)
    pe_contract = find_nifty_option(pe_strike, "PE", expiry_str=expiry_str, trading_date=trading_date)

    api = AngelAPI()
    api.login()
    time_module.sleep(1)
    if api.mock:
        raise RuntimeError("AngelAPI in MOCK mode; cannot backtest with real data.")

    start_dt = datetime.combine(trading_date, time(9, 30))
    end_dt = datetime.combine(trading_date, time(15, 15))
    from_str, to_str = start_dt.strftime("%Y-%m-%d %H:%M"), end_dt.strftime("%Y-%m-%d %H:%M")

    ce_res = api.connection.getCandleData({"exchange": ce_contract.exchange, "symboltoken": ce_contract.token, "interval": bar_interval, "fromdate": from_str, "todate": to_str})
    pe_res = api.connection.getCandleData({"exchange": pe_contract.exchange, "symboltoken": pe_contract.token, "interval": bar_interval, "fromdate": from_str, "todate": to_str})

    ce_bars_raw, pe_bars_raw = ce_res.get("data") or [], pe_res.get("data") or []
    if not ce_bars_raw or not pe_bars_raw:
        raise RuntimeError("No candle data returned for CE/PE in backtest window")

    bars: list[Bar] = []
    for ce_row, pe_row in zip(ce_bars_raw, pe_bars_raw):
        bars.append(Bar(
            ts=str(ce_row[0]),
            ce_open=float(ce_row[1]), ce_high=float(ce_row[2]), ce_low=float(ce_row[3]), ce_close=float(ce_row[4]), ce_volume=float(ce_row[5] or 0),
            pe_open=float(pe_row[1]), pe_high=float(pe_row[2]), pe_low=float(pe_row[3]), pe_close=float(pe_row[4]), pe_volume=float(pe_row[5] or 0),
        ))
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
    Your strategy using OHLC/4 VWAP.
    """
    bars, ce_symbol, pe_symbol = _fetch_intraday_bars_for_ce_pe(trading_date, bar_interval, expiry_str)

    cum_pv, cum_vol = 0.0, 0.0
    seen_sum_above_vwap = False
    in_position = False
    entry_index = exit_index = None
    entry_ce = entry_pe = exit_ce = exit_pe = None
    exit_reason = None
    ce_stop = pe_stop = None
    records: list[dict] = []

    for i, bar in enumerate(bars):
        ohlc4_price = (bar.combined_open + bar.combined_high + bar.combined_low + bar.combined_close) / 4
        volume = bar.combined_volume
        
        cum_pv += ohlc4_price * volume
        cum_vol += volume
        vwap = cum_pv / cum_vol if cum_vol > 0 else ohlc4_price

        entry_flag = exit_flag = False
        current_reason = ""

        if not in_position and exit_index is None:
            if not seen_sum_above_vwap:
                if bar.combined_close > vwap:
                    seen_sum_above_vwap = True
                    log.info(f"ARMED at {bar.ts}: Combined Close={bar.combined_close:.2f} > VWAP={vwap:.2f}")
            else:
                if bar.combined_close <= vwap:
                    in_position = True
                    entry_index = i
                    entry_ce, entry_pe = bar.ce_close, bar.pe_close
                    ce_stop, pe_stop = entry_ce * (1 + stop_loss_pct), entry_pe * (1 + stop_loss_pct)
                    entry_flag, current_reason = True, "ENTRY"
                    log.info(f"ENTRY at {bar.ts}: Combined Close={bar.combined_close:.2f} <= VWAP={vwap:.2f} | CE_Entry={entry_ce:.2f}, PE_Entry={entry_pe:.2f}")
        
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
                log.info(f"EXIT ({exit_reason}) at {bar.ts}: PNL={total_pnl:.2f} | CE_Exit={exit_ce:.2f}, PE_Exit={exit_pe:.2f}")

        records.append({
            "ts": bar.ts, "ce_close": bar.ce_close, "pe_close": bar.pe_close,
            "sum_price": bar.combined_close, "vwap": vwap,
            "in_position": int(in_position), "entry_flag": int(entry_flag),
            "exit_flag": int(exit_flag), "reason": current_reason,
        })

    if entry_index is not None and exit_index is None:
        last_bar = bars[-1]
        exit_index, exit_ce, exit_pe, exit_reason = len(bars) - 1, last_bar.ce_close, last_bar.pe_close, "EOD"
        records[-1].update({"exit_flag": 1, "reason": exit_reason})
        log.info(f"EXIT (EOD) at {last_bar.ts}: CE_Exit={exit_ce:.2f}, PE_Exit={exit_pe:.2f}")

    if entry_index is None:
        log.info("No entry signal for the day (VWAP pattern never met).")

    if entry_index is not None:
        pnl_per_lot = ((entry_ce - exit_ce) + (entry_pe - exit_pe)) * LOT_SIZE
        print(f"\n--- Day Summary ---\nDate: {trading_date}, PNL: {pnl_per_lot:.2f}, Reason: {exit_reason}")

    if export_csv:
        out_dir = Path("data/processed/strangle")
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / f"vwap_backtest_{trading_date}.csv"
        
        with out_path.open("w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=records[0].keys())
            writer.writeheader()
            writer.writerows(records)
        print(f"Data exported to {out_path}")
