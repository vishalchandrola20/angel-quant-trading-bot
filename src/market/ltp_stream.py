# src/market/ltp_stream.py
from __future__ import annotations

import logging
import time
from datetime import date

from src.api.smartapi_client import AngelAPI
from src.data_pipeline.nifty_first_15m import get_nifty_first_15m_close
from src.strategy.strike_selection import get_single_ce_pe_strikes
from src.market.contracts import find_nifty_option

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


def stream_ce_pe_ltp_for_first_15m(
        trading_date: date | None,
        expiry_str: str | None = None,
        interval_sec: float = 5.0,
):
    """
    Workflow:
      1. Get NIFTY first 15m close for trading_date
      2. Compute ATM, CE/PE strikes (custom rules)
      3. Resolve CE & PE option contracts from Scrip Master
      4. Continuously poll LTP for both via SmartAPI.ltpData every interval_sec seconds
    """
    # 1) First 15m close
    close = get_nifty_first_15m_close(trading_date)
    strikes_info = get_single_ce_pe_strikes(close)

    spot = strikes_info["spot"]
    atm = strikes_info["atm"]
    ce_strike = strikes_info["ce_strike"]
    pe_strike = strikes_info["pe_strike"]

    log.info("NIFTY first 15m close: %s", spot)
    log.info("ATM (custom): %s", atm)
    log.info("CE strike: %s | PE strike: %s", ce_strike, pe_strike)

    # 2) Resolve contracts via Scrip Master
    # Auto expiry if not provided
    ce_contract = find_nifty_option(ce_strike, "CE", expiry_str=expiry_str, trading_date=trading_date)
    pe_contract = find_nifty_option(pe_strike, "PE", expiry_str=expiry_str, trading_date=trading_date)

    # 3) Login and start polling LTP
    api = AngelAPI()
    api.login()
    if api.mock:
        raise RuntimeError("AngelAPI ended in MOCK mode; cannot stream real LTP.")

    log.info("Starting LTP stream every %.1f seconds ... Ctrl+C to stop.", interval_sec)

    while True:
        try:
            ce_ltp = api.get_ltp(ce_contract.exchange, ce_contract.symbol, ce_contract.token)
            pe_ltp = api.get_ltp(pe_contract.exchange, pe_contract.symbol, pe_contract.token)
            total_premium = ce_ltp + pe_ltp

            print(
                f"[{time.strftime('%H:%M:%S')}] "
                f"{ce_contract.symbol} LTP={ce_ltp:.2f} | "
                f"{pe_contract.symbol} LTP={pe_ltp:.2f} | "
                f"Sum={total_premium:.2f}"
            )

            # TODO: here you'll later plug your VWAP comparison & buy/sell decision logic

            time.sleep(interval_sec)
        except KeyboardInterrupt:
            log.info("Stopped LTP streaming by user.")
            break
        except Exception as e:
            log.exception("Error while fetching LTP, will retry after a pause...")
            time.sleep(interval_sec)

from datetime import datetime, time, date
...

def backtest_ce_pe_intraday_for_day(
        trading_date: date,
        bar_interval: str = "FIVE_MINUTE",   # or "ONE_MINUTE" if you want finer granularity
        expiry_str: str | None = None,
):
    """
    Backtest mode:
      - Uses given trading_date (must be < today ideally)
      - Computes first 15m close, CE/PE strikes
      - Auto-selects expiry
      - Fetches intraday candles for CE & PE from 09:30 to 15:00/15:30
      - Prints each bar as if 'streaming' through the day
    """
    close = get_nifty_first_15m_close(trading_date)
    strikes_info = get_single_ce_pe_strikes(close)

    spot = strikes_info["spot"]
    atm = strikes_info["atm"]
    ce_strike = strikes_info["ce_strike"]
    pe_strike = strikes_info["pe_strike"]

    log.info("Backtest NIFTY date=%s, first 15m close=%s, ATM=%s", trading_date, spot, atm)
    log.info("CE strike: %s | PE strike: %s", ce_strike, pe_strike)

    # Auto pick expiry for that day
    ce_contract = find_nifty_option(ce_strike, "CE", expiry_str=expiry_str, trading_date=trading_date)
    pe_contract = find_nifty_option(pe_strike, "PE", expiry_str=expiry_str, trading_date=trading_date)

    api = AngelAPI()
    api.login()
    if api.mock:
        raise RuntimeError("AngelAPI in MOCK mode; cannot backtest with real data.")

    # Time window: 09:30 to 15:00 (you can make it 15:30 if you want)
    start_dt = datetime.combine(trading_date, time(9, 30))
    end_dt = datetime.combine(trading_date, time(15, 0))

    from_str = start_dt.strftime("%Y-%m-%d %H:%M")
    to_str = end_dt.strftime("%Y-%m-%d %H:%M")

    log.info("Fetching CE candles %s %s from %s to %s", ce_contract.symbol, ce_contract.token, from_str, to_str)
    ce_res = api.connection.getCandleData(
        {
            "exchange": ce_contract.exchange,
            "symboltoken": ce_contract.token,
            "interval": bar_interval,
            "fromdate": from_str,
            "todate": to_str,
        }
    )
    log.info("Fetching PE candles %s %s from %s to %s", pe_contract.symbol, pe_contract.token, from_str, to_str)
    pe_res = api.connection.getCandleData(
        {
            "exchange": pe_contract.exchange,
            "symboltoken": pe_contract.token,
            "interval": bar_interval,
            "fromdate": from_str,
            "todate": to_str,
        }
    )

    ce_bars = ce_res.get("data") or []
    pe_bars = pe_res.get("data") or []

    if not ce_bars or not pe_bars:
        raise RuntimeError("No candle data returned for CE/PE in backtest window")

    # Assume both lists have same timestamps; loop and print
    for ce_row, pe_row in zip(ce_bars, pe_bars):
        ts_ce, o1, h1, l1, c1, v1 = ce_row
        ts_pe, o2, h2, l2, c2, v2 = pe_row

        # (Optionally assert ts_ce == ts_pe here)
        ce_close = float(c1)
        pe_close = float(c2)
        total_premium = ce_close + pe_close

        print(
            f"{ts_ce} | {ce_contract.symbol} C={ce_close:.2f} | "
            f"{pe_contract.symbol} C={pe_close:.2f} | Sum={total_premium:.2f}"
        )
        # Later: plug strategy logic here (e.g., VWAP comparison, simulated trades)

