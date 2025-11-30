# src/main.py
import argparse

from src.data_pipeline.option_chain import fetch_and_save
from src.data_pipeline.nifty_first_15m import get_nifty_first_15m_close
from src.strategy.strike_selection import get_single_ce_pe_strikes
from datetime import datetime
from src.backtest.vwap_ce_pe_strategy import run_vwap_ce_pe_strategy_for_day


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
            "backtest_vwap_strategy",
        ],
        default="option_chain",
    )

    parser.add_argument("--symbol", default="NIFTY")
    parser.add_argument("--date", help="Trading date in YYYY-MM-DD (for NIFTY tasks)")
    parser.add_argument("--expiry", help="Optional expiry like 27FEB2025; if omitted, auto-selected.")
    parser.add_argument("--interval", type=float, default=5.0, help="Polling interval in seconds for LTP stream")
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

        backtest_ce_pe_intraday_for_day(trading_date, bar_interval="ONE_MINUTE",expiry_str=expiry_str)

    elif args.task == "backtest_vwap_strategy":
        if not args.date:
            raise SystemExit("--date is required for backtest_vwap_strategy")
        trading_date = datetime.strptime(args.date, "%Y-%m-%d").date()
        expiry_str = args.expiry or None  # if None -> auto-select next available expiry

        run_vwap_ce_pe_strategy_for_day(
            trading_date=trading_date,
            bar_interval="ONE_MINUTE",
            expiry_str=expiry_str,
            stop_loss_pct=0.70,
        )



if __name__ == "__main__":
    main()
