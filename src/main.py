# src/main.py
import argparse
from datetime import datetime

from src.data_pipeline.option_chain import fetch_and_save
from src.data_pipeline.nifty_first_15m import get_nifty_first_15m_close
from src.strategy.strike_selection import  get_single_ce_pe_strikes


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--symbol", default="NIFTY", help="Symbol to get option chain for")
    parser.add_argument(
        "--task",
        choices=["option_chain", "nifty_first_15m", "nifty_first_15m_strikes"],
        default="option_chain",
    )
    parser.add_argument("--date", help="Trading date in YYYY-MM-DD (for nifty_first_15m*)")
    args = parser.parse_args()

    if args.task == "option_chain":
        out = fetch_and_save(args.symbol)
        print("Saved option chain:", out)

    elif args.task == "nifty_first_15m":
        if args.date:
            trading_date = datetime.strptime(args.date, "%Y-%m-%d").date()
        else:
            trading_date = None
        close = get_nifty_first_15m_close(trading_date)
        print("NIFTY first 15m close:", close)

    elif args.task == "nifty_first_15m_strikes":
        if args.date:
            trading_date = datetime.strptime(args.date, "%Y-%m-%d").date()
        else:
            trading_date = None

        close = get_nifty_first_15m_close(trading_date)
        info = get_single_ce_pe_strikes(close)

        print(f"NIFTY first 15m close: {info['spot']}")
        print(f"ATM strike (custom rule): {info['atm']}")
        print(f"CE base (ceil to 50): {info['ce_base']}")
        print(f"PE base (floor to 50): {info['pe_base']}")
        print(f"CE strike (base + 100): {info['ce_strike']}")
        print(f"PE strike (base - 100): {info['pe_strike']}")



if __name__ == "__main__":
    main()
