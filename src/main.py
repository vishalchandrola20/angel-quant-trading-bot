# src/main.py
import argparse
import time as time_module # Import time_module for sleep
from datetime import datetime, time # Import time from datetime

from src.data_pipeline.option_chain import fetch_and_save
from src.data_pipeline.nifty_first_15m import get_nifty_first_15m_close
from src.strategy.strike_selection import get_single_ce_pe_strikes
from src.api.smartapi_client import AngelAPI # Import AngelAPI
from src.market.contracts import find_nifty_option # Import find_nifty_option


from src.backtest.vwap_ce_pe_strategy import run_iron_condor_strategy_for_day, run_vwap_strangle_strategy_for_day
from src.backtest.vwap_straddle_strategy import run_vwap_straddle_strategy_for_day



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
            "backtest_iron_condor",
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

    elif args.task == "backtest_iron_condor":
        if not args.date:
            raise SystemExit("--date is required for backtest_iron_condor")
        trading_date = datetime.strptime(args.date, "%Y-%m-%d").date()
        expiry_str = args.expiry or None

        run_iron_condor_strategy_for_day(
            trading_date=trading_date,
            bar_interval="ONE_MINUTE",
            expiry_str=expiry_str,
            absolute_stop_loss=4000.0,
            take_profit_points=1400.0,
            export_csv=True,
        )
    
    elif args.task == "calculate_vwap_until":
        if not args.date or not args.time:
            raise SystemExit("--date and --time are required for calculate_vwap_until")
        
        trading_date = datetime.strptime(args.date, "%Y-%m-%d").date()
        target_time = datetime.strptime(args.time, "%H:%M").time()
        target_dt = datetime.combine(trading_date, target_time)

        first15_close = get_nifty_first_15m_close(trading_date)
        strikes_info = get_single_ce_pe_strikes(first15_close)
        short_ce_strike, short_pe_strike = strikes_info["ce_strike"], strikes_info["pe_strike"]
        long_ce_strike = short_ce_strike + 8 * 50
        long_pe_strike = short_pe_strike - 8 * 50

        expiry_str = args.expiry or None
        short_ce_contract = find_nifty_option(short_ce_strike, "CE", expiry_str, trading_date)
        short_pe_contract = find_nifty_option(short_pe_strike, "PE", expiry_str, trading_date)
        long_ce_contract = find_nifty_option(long_ce_strike, "CE", expiry_str, trading_date)
        long_pe_contract = find_nifty_option(long_pe_strike, "PE", expiry_str, trading_date)

        api = AngelAPI()
        api.login()
        time_module.sleep(1)

        start_of_day = datetime.combine(trading_date, time(9, 15))

        def get_bars(contract):
            res = api.connection.getCandleData({"exchange": contract.exchange, "symboltoken": contract.token, "interval": "ONE_MINUTE", "fromdate": start_of_day.strftime("%Y-%m-%d %H:%M"), "todate": target_dt.strftime("%Y-%m-%d %H:%M")})
            return res.get("data") or []

        short_ce_bars = get_bars(short_ce_contract)
        time_module.sleep(1)
        short_pe_bars = get_bars(short_pe_contract)
        time_module.sleep(1)
        long_ce_bars = get_bars(long_ce_contract)
        time_module.sleep(1)
        long_pe_bars = get_bars(long_pe_contract)

        if not all([short_ce_bars, short_pe_bars, long_ce_bars, long_pe_bars]):
            print(f"No historical data found for one or more legs on {trading_date} up to {args.time}.")
            return

        cum_pv_ohlc4, cum_vol_ohlc4 = 0.0, 0.0
        cum_pv_close, cum_vol_close = 0.0, 0.0
        cum_pv_hlc3, cum_vol_hlc3 = 0.0, 0.0

        print(f"\n--- Iron Condor VWAP Comparison for {trading_date} up to {args.time} ---")
        header = f"{'Time':<8} | {'Net Credit Close':>18} {'OHLC4_VWAP':>12} {'CLOSE_VWAP':>12} {'HLC3_VWAP':>12}"
        print(header)
        print("-" * len(header))

        for short_ce, short_pe, long_ce, long_pe in zip(short_ce_bars, short_pe_bars, long_ce_bars, long_pe_bars):
            bar_time = datetime.strptime(short_ce[0], "%Y-%m-%dT%H:%M:%S%z").strftime("%H:%M")
            
            short_ce_o, short_ce_h, short_ce_l, short_ce_c, short_ce_v = [float(x or 0) for x in short_ce[1:]]
            short_pe_o, short_pe_h, short_pe_l, short_pe_c, short_pe_v = [float(x or 0) for x in short_pe[1:]]
            long_ce_o, long_ce_h, long_ce_l, long_ce_c, long_ce_v = [float(x or 0) for x in long_ce[1:]]
            long_pe_o, long_pe_h, long_pe_l, long_pe_c, long_pe_v = [float(x or 0) for x in long_pe[1:]]

            net_credit_open = (short_ce_o + short_pe_o) - (long_ce_o + long_pe_o)
            net_credit_high = (short_ce_h + short_pe_h) - (long_ce_l + long_pe_l)
            net_credit_low = (short_ce_l + short_pe_l) - (long_ce_h + long_pe_h)
            net_credit_close = (short_ce_c + short_pe_c) - (long_ce_c + long_pe_c)
            
            combined_vol = short_ce_v + short_pe_v + long_ce_v + long_pe_v
            
            # OHLC4 VWAP
            ohlc4_price = (net_credit_open + net_credit_high + net_credit_low + net_credit_close) / 4
            cum_pv_ohlc4 += ohlc4_price * combined_vol
            cum_vol_ohlc4 += combined_vol
            ohlc4_vwap = cum_pv_ohlc4 / cum_vol_ohlc4 if cum_vol_ohlc4 > 0 else ohlc4_price

            # Close VWAP
            cum_pv_close += net_credit_close * combined_vol
            cum_vol_close += combined_vol
            close_vwap = cum_pv_close / cum_vol_close if cum_vol_close > 0 else net_credit_close

            # HLC3 VWAP
            hlc3_price = (net_credit_high + net_credit_low + net_credit_close) / 3
            cum_pv_hlc3 += hlc3_price * combined_vol
            cum_vol_hlc3 += combined_vol
            hlc3_vwap = cum_pv_hlc3 / cum_vol_hlc3 if cum_vol_hlc3 > 0 else hlc3_price

            print(f"{bar_time:<8} | {net_credit_close:>18.2f} {ohlc4_vwap:>12.2f} {close_vwap:>12.2f} {hlc3_vwap:>12.2f}")

        print("-" * len(header))
        print(f"Final OHLC4 VWAP at {args.time}: {ohlc4_vwap:.2f}")
        print(f"Final Close VWAP at {args.time}: {close_vwap:.2f}")
        print(f"Final HLC3 VWAP at {args.time}: {hlc3_vwap:.2f}")


if __name__ == "__main__":
    main()
