# src/strategy/strike_selection.py
from __future__ import annotations
import math
import logging
import time
from datetime import date, datetime, time as dt_time

from src.api.smartapi_client import AngelAPI
from src.market.contracts import find_nifty_option

log = logging.getLogger(__name__)


def get_atm_strike_custom(spot: float) -> int:
    """
    ATM = nearest 50 using 25/75 style rule (effectively nearest 50):
      - < 25  -> ...00
      - > 75  -> next ...00
      - else  -> ...50
    """
    spot_int = int(round(spot))
    base_100 = (spot_int // 100) * 100
    last_two = spot_int % 100

    if last_two < 25:
        atm = base_100          # ...00
    elif last_two > 75:
        atm = base_100 + 100    # next ...00
    else:
        atm = base_100 + 50     # ...50

    return atm


def floor_to_50(spot: float) -> int:
    """Round down to nearest 50."""
    return int(math.floor(spot / 50.0) * 50)


def ceil_to_50(spot: float) -> int:
    """Round up to nearest 50."""
    return int(math.ceil(spot / 50.0) * 50)


def get_single_ce_pe_strikes(spot: float, trading_date: date | None = None) -> dict:
    """
    From spot, compute initial strikes, then adjust based on the 9:30 AM price difference.
    """
    if trading_date is None:
        trading_date = date.today()

    atm = get_atm_strike_custom(spot)
    ce_base = ceil_to_50(spot)
    pe_base = floor_to_50(spot)

    ce_strike = ce_base + 100
    pe_strike = pe_base - 100

    log.info(f"Initial strikes: CE={ce_strike}, PE={pe_strike}")

    # --- Price-based Adjustment ---
    try:
        api = AngelAPI()
        api.login()
        time.sleep(1)

        ce_contract = find_nifty_option(ce_strike, "CE", trading_date=trading_date)
        pe_contract = find_nifty_option(pe_strike, "PE", trading_date=trading_date)

        start_dt = datetime.combine(trading_date, dt_time(9, 30))
        end_dt = datetime.combine(trading_date, dt_time(9, 31))
        from_str, to_str = start_dt.strftime("%Y-%m-%d %H:%M"), end_dt.strftime("%Y-%m-%d %H:%M")
        
        def get_historical_price(contract):
            res = api.connection.getCandleData({"exchange": contract.exchange, "symboltoken": contract.token, "interval": "ONE_MINUTE", "fromdate": from_str, "todate": to_str})
            log.info(f"Historical data for {contract.symbol} at 9:30: {res.get('data')}")
            if res and res.get("data"):
                return float(res["data"][0][4]) # Close price of the 9:30 candle
            raise RuntimeError(f"No 9:30 historical data for {contract.symbol}")

        ce_ltp = get_historical_price(ce_contract)
        pe_ltp = get_historical_price(pe_contract)

        log.info(f"9:30 prices for adjustment check: CE={ce_ltp:.2f}, PE={pe_ltp:.2f}")

        price_diff_pct = abs(ce_ltp - pe_ltp) / max(ce_ltp, pe_ltp)

        if price_diff_pct > 0.30:
            log.warning(f"Price difference > 30% ({price_diff_pct:.1%}). Adjusting cheaper leg.")
            if ce_ltp < pe_ltp:
                ce_strike -= 50 # Move 1 strike closer
                log.info(f"CE is cheaper. New CE strike: {ce_strike}")
            else:
                pe_strike += 50 # Move 1 strike closer
                log.info(f"PE is cheaper. New PE strike: {pe_strike}")
        else:
            log.info("Price difference is within limits. No adjustment needed.")

    except Exception as e:
        log.error(f"Could not perform price-based strike adjustment: {e}. Using initial strikes.")

    return {
        "spot": float(spot),
        "atm": atm,
        "ce_base": ce_base,
        "pe_base": pe_base,
        "ce_strike": ce_strike,
        "pe_strike": pe_strike,
    }


if __name__ == "__main__":
    test_spot = 26229.65
    # For testing historical dates, you need to provide a date
    # info = get_single_ce_pe_strikes(test_spot, trading_date=date(2023, 12, 8))
    info = get_single_ce_pe_strikes(test_spot)
    print(info)
