# src/backtest/vwap_ce_pe_strategy.py
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, date, time
import time as time_module

from src.api.smartapi_client import AngelAPI
from src.data_pipeline.nifty_first_15m import get_index_first_15m_close
from src.strategy.strike_selection import get_single_ce_pe_strikes
from src.market.contracts import find_option
from pathlib import Path
import csv


log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

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
        "trailing_activation_mtm_per_lot": 20, # 1000 / 60 lots
        "trailing_sl_reversal_pct": 0.70,
    }
}


@dataclass
class Bar:
    ts: str
    short_ce_open: float
    short_ce_high: float
    short_ce_low: float
    short_ce_close: float
    short_ce_volume: float
    short_pe_open: float
    short_pe_high: float
    short_pe_low: float
    short_pe_close: float
    short_pe_volume: float
    long_ce_open: float
    long_ce_high: float
    long_ce_low: float
    long_ce_close: float
    long_ce_volume: float
    long_pe_open: float
    long_pe_high: float
    long_pe_low: float
    long_pe_close: float
    long_pe_volume: float

    @property
    def net_credit_open(self) -> float:
        return (self.short_ce_open + self.short_pe_open) - (self.long_ce_open + self.long_pe_open)

    @property
    def net_credit_high(self) -> float:
        return (self.short_ce_high + self.short_pe_high) - (self.long_ce_low + self.long_pe_low)

    @property
    def net_credit_low(self) -> float:
        return (self.short_ce_low + self.short_pe_low) - (self.long_ce_high + self.long_pe_high)

    @property
    def net_credit_close(self) -> float:
        return (self.short_ce_close + self.short_pe_close) - (self.long_ce_close + self.long_pe_close)

    @property
    def combined_volume(self) -> float:
        v = (self.short_ce_volume + self.short_pe_volume + self.long_ce_volume + self.long_pe_volume)
        return v if v > 0 else 1.0


def _fetch_intraday_bars_for_iron_condor(
        trading_date: date,
        index_name: str = "NIFTY",
        bar_interval: str = "ONE_MINUTE",
        expiry_str: str | None = None,
) -> tuple[list[Bar], str, str, str, str]:
    index_name = index_name.upper()
    if index_name not in INDEX_CONFIG:
        raise ValueError(f"Unsupported index '{index_name}' for backtesting.")

    config = INDEX_CONFIG[index_name]
    strike_step = config["strike_step"]

    spot, spot_candle_end_time = get_index_first_15m_close(index_name, trading_date)
    strikes_info = get_single_ce_pe_strikes(spot, spot_candle_end_time, index_name=index_name, trading_date=trading_date, strike_step=strike_step)
    short_ce_strike, short_pe_strike, long_ce_strike, long_pe_strike = \
        strikes_info["ce_strike"], strikes_info["pe_strike"], strikes_info["long_ce_strike"], strikes_info["long_pe_strike"]

    log.info(f"Strikes: Short CE={short_ce_strike}, Short PE={short_pe_strike}, Long CE={long_ce_strike}, Long PE={long_pe_strike}")

    short_ce_contract = find_option(index_name, short_ce_strike, "CE", expiry_str, trading_date)
    short_pe_contract = find_option(index_name, short_pe_strike, "PE", expiry_str, trading_date)
    long_ce_contract = find_option(index_name, long_ce_strike, "CE", expiry_str, trading_date)
    long_pe_contract = find_option(index_name, long_pe_strike, "PE", expiry_str, trading_date)

    api = AngelAPI()
    api.login()
    time_module.sleep(1)

    start_dt = datetime.combine(trading_date, time(9, 15))
    end_dt = datetime.combine(trading_date, time(15, 15))
    from_str, to_str = start_dt.strftime("%Y-%m-%d %H:%M"), end_dt.strftime("%Y-%m-%d %H:%M")

    def get_bars(contract):
        res = api.connection.getCandleData({"exchange": contract.exchange, "symboltoken": contract.token, "interval": bar_interval, "fromdate": from_str, "todate": to_str})
        return res.get("data") or []

    short_ce_bars_raw = get_bars(short_ce_contract)
    time_module.sleep(1)
    short_pe_bars_raw = get_bars(short_pe_contract)
    time_module.sleep(1)
    long_ce_bars_raw = get_bars(long_ce_contract)
    time_module.sleep(1)
    long_pe_bars_raw = get_bars(long_pe_contract)
    time_module.sleep(1)

    if not all([short_ce_bars_raw, short_pe_bars_raw, long_ce_bars_raw, long_pe_bars_raw]):
        raise RuntimeError("No candle data returned for one or more legs.")

    bars: list[Bar] = []
    for short_ce, short_pe, long_ce, long_pe in zip(short_ce_bars_raw, short_pe_bars_raw, long_ce_bars_raw, long_pe_bars_raw):
        bars.append(Bar(
            ts=str(short_ce[0]),
            short_ce_open=float(short_ce[1]), short_ce_high=float(short_ce[2]), short_ce_low=float(short_ce[3]), short_ce_close=float(short_ce[4]), short_ce_volume=float(short_ce[5] or 0),
            short_pe_open=float(short_pe[1]), short_pe_high=float(short_pe[2]), short_pe_low=float(short_pe[3]), short_pe_close=float(short_pe[4]), short_pe_volume=float(short_pe[5] or 0),
            long_ce_open=float(long_ce[1]), long_ce_high=float(long_ce[2]), long_ce_low=float(long_ce[3]), long_ce_close=float(long_ce[4]), long_ce_volume=float(long_ce[5] or 0),
            long_pe_open=float(long_pe[1]), long_pe_high=float(long_pe[2]), long_pe_low=float(long_pe[3]), long_pe_close=float(long_pe[4]), long_pe_volume=float(long_pe[5] or 0),
        ))
    return bars, short_ce_contract.symbol, short_pe_contract.symbol, long_ce_contract.symbol, long_pe_contract.symbol


def run_iron_condor_strategy_for_day(
        trading_date: date,
        index_name: str = "NIFTY",
        bar_interval: str = "ONE_MINUTE",
        expiry_str: str | None = None,
        export_csv: bool = True):
    log.info("--- Iron Condor Strategy ---")
    log.info(f"trading_date={trading_date}")

    index_name = index_name.upper()
    config = INDEX_CONFIG.get(index_name, INDEX_CONFIG["NIFTY"])
    lot_size = config["lot_size"]
    absolute_stop_loss = config["absolute_stop_loss_per_lot"] * lot_size
    take_profit_points = config["take_profit_per_lot"] * lot_size
    trailing_activation_mtm = config["trailing_activation_mtm_per_lot"] * lot_size
    trailing_sl_reversal_pct = config["trailing_sl_reversal_pct"]


    bars, short_ce_sym, short_pe_sym, long_ce_sym, long_pe_sym = _fetch_intraday_bars_for_iron_condor(trading_date, index_name, bar_interval, expiry_str)

    cum_pv, cum_vol = 0.0, 0.0
    in_position = False
    entry_index = exit_index = None
    entry_bar = None
    exit_bar = None
    exit_reason = None
    short_ce_stop_price = None
    short_pe_stop_price = None
    trailing_sl_active = False
    peak_mtm = 0.0
    records: list[dict] = []

    for i, bar in enumerate(bars):
        bar_time = datetime.strptime(bar.ts, "%Y-%m-%dT%H:%M:%S%z").time()

        ohlc4_price = (bar.net_credit_open + bar.net_credit_high + bar.net_credit_low + bar.net_credit_close) / 4
        volume = bar.combined_volume
        
        cum_pv += ohlc4_price * volume
        cum_vol += volume
        vwap = cum_pv / cum_vol if cum_vol > 0 else ohlc4_price

        entry_flag = exit_flag = False
        current_reason = ""

        if bar_time >= time(9, 30):
            if not in_position and exit_index is None:
                # Simplified entry: enter on the first valid bar (9:30)
                if entry_index is None:
                    in_position = True
                    entry_index = i
                    entry_bar = bar
                    trailing_sl_active = False # Reset on new entry
                    peak_mtm = 0.0
                    entry_flag, current_reason = True, "ENTRY"

                    log.info(
                        f"ENTRY at {bar.ts}: Net Credit={bar.net_credit_close:.2f} | "
                        f"Short CE={bar.short_ce_close:.2f}), "
                        f"Short PE={bar.short_pe_close:.2f}), "
                        f"Long CE={bar.long_ce_close:.2f}, Long PE={bar.long_pe_close:.2f}"
                    )
            
            if in_position:
                current_net_credit = bar.net_credit_close
                entry_net_credit = entry_bar.net_credit_close
                total_pnl = (entry_net_credit - current_net_credit) * lot_size
                
                temp_exit_reason = None
                if total_pnl >= take_profit_points:
                    temp_exit_reason = "TAKE_PROFIT"
                elif total_pnl <= -absolute_stop_loss:
                    temp_exit_reason = "STOP_LOSS_ABS"
                
                # Trailing Stop Loss on Profit
                if not trailing_sl_active and total_pnl >= trailing_activation_mtm:
                    trailing_sl_active = True
                    peak_mtm = total_pnl
                    log.info(f"Trailing SL activated at PNL: {total_pnl:.2f}")
                
                if trailing_sl_active:
                    new_peak_mtm = max(peak_mtm, total_pnl)
                    if new_peak_mtm > peak_mtm:
                        log.info(f"Peak MTM updated to: {new_peak_mtm:.2f}")
                        peak_mtm = new_peak_mtm

                    trailing_stop_level = peak_mtm * trailing_sl_reversal_pct
                    if total_pnl <= trailing_stop_level:
                        temp_exit_reason = "TRAILING_STOP_LOSS"

                if temp_exit_reason:
                    in_position = False
                    exit_index = i
                    exit_bar = bar
                    exit_reason, exit_flag, current_reason = temp_exit_reason, True, temp_exit_reason
                    log.info(
                        f"EXIT ({exit_reason}) at {bar.ts}: PNL={total_pnl:.2f} | Net Credit={current_net_credit:.2f} | "
                        f"Short CE={bar.short_ce_close:.2f}, Short PE={bar.short_pe_close:.2f}, "
                        f"Long CE={bar.long_ce_close:.2f}, Long PE={bar.long_pe_close:.2f}"
                    )

        records.append({
            "ts": bar.ts, "net_credit_close": bar.net_credit_close, "vwap": vwap,
            "in_position": int(in_position), "entry_flag": int(entry_flag),
            "exit_flag": int(exit_flag), "reason": current_reason,
        })

    if entry_index is not None and exit_index is None:
        last_bar = bars[-1]
        exit_index, exit_bar, exit_reason = len(bars) - 1, last_bar, "EOD"
        records[-1].update({"exit_flag": 1, "reason": exit_reason})
        log.info(f"EXIT (EOD) at {last_bar.ts}: Net Credit={last_bar.net_credit_close:.2f}")

    if entry_index is not None:
        pnl_per_lot = (entry_bar.net_credit_close - exit_bar.net_credit_close) * lot_size
        print(f"\n--- Day Summary ---\nDate: {trading_date}, PNL: {pnl_per_lot:.2f}, Reason: {exit_reason}")

    if export_csv:
        out_dir = Path(f"data/processed/iron_condor/{index_name.lower()}")
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / f"iron_condor_backtest_{trading_date}.csv"
        
        with out_path.open("w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=records[0].keys())
            writer.writeheader()
            writer.writerows(records)
        print(f"Data exported to {out_path}")

# --- Restored Strangle Method ---
@dataclass
class StrangleBar:
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
) -> tuple[list[StrangleBar], str, str]:
    spot, _ = get_index_first_15m_close("NIFTY", trading_date)
    strikes_info = get_single_ce_pe_strikes(spot, _, index_name="NIFTY")
    ce_strike, pe_strike = strikes_info["ce_strike"], strikes_info["pe_strike"]

    log.info(f"Backtest date={trading_date}")
    log.info(f"CE strike={ce_strike} | PE strike={pe_strike}")

    ce_contract = find_option("NIFTY", ce_strike, "CE", expiry_str=expiry_str, trading_date=trading_date)
    pe_contract = find_option("NIFTY", pe_strike, "PE", expiry_str=expiry_str, trading_date=trading_date)

    api = AngelAPI()
    api.login()
    time_module.sleep(1)
    if api.mock:
        raise RuntimeError("AngelAPI in MOCK mode; cannot backtest with real data.")

    start_dt = datetime.combine(trading_date, time(9, 15))
    end_dt = datetime.combine(trading_date, time(15, 15))
    from_str, to_str = start_dt.strftime("%Y-%m-%d %H:%M"), end_dt.strftime("%Y-%m-%d %H:%M")

    ce_res = api.connection.getCandleData({"exchange": ce_contract.exchange, "symboltoken": ce_contract.token, "interval": bar_interval, "fromdate": from_str, "todate": to_str})
    pe_res = api.connection.getCandleData({"exchange": pe_contract.exchange, "symboltoken": pe_contract.token, "interval": bar_interval, "fromdate": from_str, "todate": to_str})

    ce_bars_raw, pe_bars_raw = ce_res.get("data") or [], pe_res.get("data") or []
    if not ce_bars_raw or not pe_bars_raw:
        raise RuntimeError("No candle data returned for CE/PE in backtest window")

    bars: list[StrangleBar] = []
    for ce_row, pe_row in zip(ce_bars_raw, pe_bars_raw):
        bars.append(StrangleBar(
            ts=str(ce_row[0]),
            ce_open=float(ce_row[1]), ce_high=float(ce_row[2]), ce_low=float(ce_row[3]), ce_close=float(ce_row[4]), ce_volume=float(ce_row[5] or 0),
            pe_open=float(pe_row[1]), pe_high=float(pe_row[2]), pe_low=float(pe_row[3]), pe_close=float(pe_row[4]), pe_volume=float(pe_row[5] or 0),
        ))
    return bars, ce_contract.symbol, pe_contract.symbol

def run_vwap_strangle_strategy_for_day(
        trading_date: date,
        index_name: str = "NIFTY", bar_interval: str = "ONE_MINUTE",
        expiry_str: str | None = None,
        stop_loss_pct: float = 0.70,
        absolute_stop_loss: float = 2000.0,
        take_profit_points: float = 3000.0,
        export_csv: bool = True,
        LOT_SIZE=75):
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
        bar_time = datetime.strptime(bar.ts, "%Y-%m-%dT%H:%M:%S%z").time()
        
        ohlc4_price = (bar.combined_open + bar.combined_high + bar.combined_low + bar.combined_close) / 4
        volume = bar.combined_volume
        
        cum_pv += ohlc4_price * volume
        cum_vol += volume
        vwap = cum_pv / cum_vol if cum_vol > 0 else ohlc4_price

        entry_flag = exit_flag = False
        current_reason = ""

        if bar_time >= time(9, 30):
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
                total_pnl = ((entry_ce - ce_p) + (entry_pe - pe_p)) * 75 # Assuming NIFTY lot size for now
                
                temp_exit_reason = None
                if total_pnl >= take_profit_points:
                    temp_exit_reason = "TAKE_PROFIT"
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
        pnl_per_lot = ((entry_ce - exit_ce) + (entry_pe - exit_pe)) * 75 # Assuming NIFTY lot size for now
        print(f"\n--- Day Summary ---\nDate: {trading_date}, PNL: {pnl_per_lot:.2f}, Reason: {exit_reason}")

    if export_csv:
        out_dir = Path("data/processed/strangle")
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / f"vwap_backtest_{trading_date}.csv"
        
        with out_path.open("w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=records[0].keys())
            writer.writeheader()
            for row in records:
                writer.writerow(row)
        print(f"Data exported to {out_path}")
