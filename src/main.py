# src/main.py
import argparse
import time as time_module # Import time_module for sleep
from datetime import datetime, date, time # Import time from datetime

from src.data_pipeline.option_chain import fetch_and_save
from src.data_pipeline.nifty_first_15m import get_nifty_first_15m_close
from src.strategy.strike_selection import get_single_ce_pe_strikes
from src.api.smartapi_client import AngelAPI # Import AngelAPI
from src.market.contracts import find_nifty_option # Import find_nifty_option


from src.backtest.vwap_ce_pe_strategy import run_vwap_strangle_strategy_for_day
from src.backtest.vwap_straddle_strategy import run_vwap_straddle_strategy_for_day
from src.backtest.vwap_strangle_5_percent_strategy import run_vwap_strangle_5_percent_strategy_for_day



from src.market.ltp_stream import (
    stream_ce_pe_ltp_for_first_15m,
    backtest_ce_pe_intraday_for_day,
)



def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--task",
        choices=[
            "option_chain",
            "nifty_first_15m",
            "nifty_first_15m_strikes",
            "stream_ce_pe_ltp",
            "backtest_ce_pe_intraday",
            "backtest_vwap_strangle",
            "backtest_vwap_straddle",
            "backtest_vwap_strangle_5_percent",
            "calculate_vwap_until", # New task
        ],
        default="option_chain",
    )

    parser.add_argument("--symbol", default="NIFTY")
    parser.add_argument("--date", help="Trading date in YYYY-MM-DD (for NIFTY tasks)")
    parser.add_argument("--expiry", help="Optional expiry like 27FEB2025; if omitted, auto-selected.")
    parser.add_argument("--interval", type=float, default=5.0, help="Polling interval in seconds for LTP stream")
    parser.add_argument("--time", help="Target time in HH:MM for calculate_vwap_until task") # New argument
    args = parser.parse_args()

    if args.task == "option_chain":
        out = fetch_and_save(args.symbol)
        print("Saved option chain:", out)

    elif args.task == "nifty_first_15m":
        trading_date = datetime.strptime(args.date, "%Y-%m-%d").date() if args.date else None
        close = get_nifty_first_15m_close(trading_date)
        print("NIFTY first 15m close:", close)

    elif args.task == "nifty_first_15m_strikes":
        trading_date = datetime.strptime(args.date, "%Y-%m-%d").date() if args.date else None
        close = get_nifty_first_15m_close(trading_date)
        info = get_single_ce_pe_strikes(close)
        print(f"NIFTY first 15m close: {info['spot']}")
        print(f"ATM strike (custom rule): {info['atm']}")
        print(f"CE strike: {info['ce_strike']}")
        print(f"PE strike: {info['pe_strike']}")

    elif args.task == "stream_ce_pe_ltp":
        trading_date = datetime.strptime(args.date, "%Y-%m-%d").date() if args.date else None
        expiry_str = args.expiry or None
        stream_ce_pe_ltp_for_first_15m(trading_date, expiry_str=expiry_str, interval_sec=args.interval)

    elif args.task == "backtest_ce_pe_intraday":
        if not args.date:
            raise SystemExit("--date is required for backtest_ce_pe_intraday")
        trading_date = datetime.strptime(args.date, "%Y-%m-%d").date()

        # expiry optional: if provided, we backtest that exact expiry; if not, auto-pick
        expiry_str = args.expiry or None

        backtest_ce_pe_intraday_for_day(trading_date, bar_interval="FIVE_MINUTE",expiry_str=expiry_str)

    elif args.task == "backtest_vwap_strangle":
        if not args.date:
            raise SystemExit("--date is required for backtest_vwap_strategy")
        trading_date = datetime.strptime(args.date, "%Y-%m-%d").date()
        expiry_str = args.expiry or None  # if None -> auto-select next available expiry

        run_vwap_strangle_strategy_for_day(
            trading_date=trading_date,
            bar_interval="ONE_MINUTE",
            expiry_str=expiry_str,
            stop_loss_pct=0.70,
        )

    elif args.task == "backtest_vwap_straddle":
        if not args.date:
            raise SystemExit("--date is required for backtest_vwap_straddle")
        trading_date = datetime.strptime(args.date, "%Y-%m-%d").date()
        expiry_str = args.expiry or None

        run_vwap_straddle_strategy_for_day(
            trading_date=trading_date,
            bar_interval="ONE_MINUTE",
            expiry_str=expiry_str,
            stop_loss_pct=0.70,
            export_csv=True,
    )

    elif args.task == "backtest_vwap_strangle_5_percent":
        if not args.date:
            raise SystemExit("--date is required for backtest_vwap_strangle_5_percent")
        trading_date = datetime.strptime(args.date, "%Y-%m-%d").date()
        expiry_str = args.expiry or None

        run_vwap_strangle_5_percent_strategy_for_day(
            trading_date=trading_date,
            bar_interval="ONE_MINUTE",
            expiry_str=expiry_str,
            stop_loss_pct=0.70,
            absolute_stop_loss=2000.0,
            vwap_band_pct=0.15, # Changed to 15%
            export_csv=True,
        )
    
    elif args.task == "calculate_vwap_until":
        if not args.date or not args.time:
            raise SystemExit("--date and --time are required for calculate_vwap_until")
        
        trading_date = datetime.strptime(args.date, "%Y-%m-%d").date()
        target_time = datetime.strptime(args.time, "%H:%M").time()
        target_dt = datetime.combine(trading_date, target_time)

        # Determine initial strikes
        first15_close = get_nifty_first_15m_close(trading_date)
        strikes_info = get_single_ce_pe_strikes(first15_close)
        ce_strike, pe_strike = strikes_info["ce_strike"], strikes_info["pe_strike"]

        # Resolve contracts
        expiry_str = args.expiry or None
        ce_contract = find_nifty_option(ce_strike, "CE", expiry_str, trading_date)
        pe_contract = find_nifty_option(pe_strike, "PE", expiry_str, trading_date)

        api = AngelAPI()
        api.login()
        time_module.sleep(1)
        if api.mock:
            raise RuntimeError("AngelAPI in MOCK mode; cannot fetch real data.")

        start_of_day = datetime.combine(trading_date, time(9, 15))

        # Fetch historical candles
        ce_hist_res = api.connection.getCandleData({
            "exchange": ce_contract.exchange,
            "symboltoken": ce_contract.token,
            "interval": "ONE_MINUTE",
            "fromdate": start_of_day.strftime("%Y-%m-%d %H:%M"),
            "todate": target_dt.strftime("%Y-%m-%d %H:%M"),
        })
        pe_hist_res = api.connection.getCandleData({
            "exchange": pe_contract.exchange,
            "symboltoken": pe_contract.token,
            "interval": "ONE_MINUTE",
            "fromdate": start_of_day.strftime("%Y-%m-%d %H:%M"),
            "todate": target_dt.strftime("%Y-%m-%d %H:%M"),
        })

        ce_hist_bars = ce_hist_res.get("data") or []
        pe_hist_bars = pe_hist_res.get("data") or []
        if not ce_hist_bars or not pe_hist_bars:
            print(f"No historical data found for {trading_date} up to {args.time}.")
            return

        ce_cum_pv, ce_cum_vol = 0.0, 0.0
        pe_cum_pv, pe_cum_vol = 0.0, 0.0

        print(f"\n--- VWAP Calculation (Close Price) for {trading_date} up to {args.time} ---")
        header = (
            f"{'Time':<8} | {'CE_O':>7} {'CE_H':>7} {'CE_L':>7} {'CE_C':>7} {'CE_V':>10} | "
            f"{'PE_O':>7} {'PE_H':>7} {'PE_L':>7} {'PE_C':>7} {'PE_V':>10} | "
            f"{'CE_VWAP':>10} {'PE_VWAP':>10}"
        )
        print(header)
        print("-" * len(header))

        for ce_bar, pe_bar in zip(ce_hist_bars, pe_hist_bars):
            bar_time = datetime.strptime(ce_bar[0], "%Y-%m-%dT%H:%M:%S%z").strftime("%H:%M")
            
            ce_open, ce_high, ce_low, ce_close, ce_vol = float(ce_bar[1]), float(ce_bar[2]), float(ce_bar[3]), float(ce_bar[4]), float(ce_bar[5] or 0)
            pe_open, pe_high, pe_low, pe_close, pe_vol = float(pe_bar[1]), float(pe_bar[2]), float(pe_bar[3]), float(pe_bar[4]), float(pe_bar[5] or 0)

            # CE VWAP Calculation using Close Price
            ce_pv = ce_close * ce_vol
            ce_cum_pv += ce_pv
            ce_cum_vol += ce_vol
            ce_vwap = ce_cum_pv / ce_cum_vol if ce_cum_vol > 0 else ce_close

            # PE VWAP Calculation using Close Price
            pe_pv = pe_close * pe_vol
            pe_cum_pv += pe_pv
            pe_cum_vol += pe_vol
            pe_vwap = pe_cum_pv / pe_cum_vol if pe_cum_vol > 0 else pe_close

            print(
                f"{bar_time:<8} | {ce_open:>7.2f} {ce_high:>7.2f} {ce_low:>7.2f} {ce_close:>7.2f} {ce_vol:>10.0f} | "
                f"{pe_open:>7.2f} {pe_high:>7.2f} {pe_low:>7.2f} {pe_close:>7.2f} {pe_vol:>10.0f} | "
                f"{ce_vwap:>10.2f} {pe_vwap:>10.2f}"
            )

        print("-" * len(header))
        print(f"Final CE VWAP at {args.time}: {ce_vwap:.2f}")
        print(f"Final PE VWAP at {args.time}: {pe_vwap:.2f}")


if __name__ == "__main__":
    main()
