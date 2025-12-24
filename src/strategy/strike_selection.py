# src/strategy/strike_selection.py
from __future__ import annotations
import math
import logging
import time
from datetime import date, datetime, timedelta

from src.api.smartapi_client import AngelAPI
from src.market.contracts import find_option, get_next_expiry

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


def floor_to_strike_step(spot: float, index_name: str = "NIFTY") -> int:
    """Round down to the nearest strike step (50 for NIFTY, 100 for SENSEX)."""
    step = 100 if index_name.upper() == "SENSEX" else 50
    return int(math.floor(spot / step) * step)


def ceil_to_strike_step(spot: float, index_name: str = "NIFTY") -> int:
    """Round up to the nearest strike step (50 for NIFTY, 100 for SENSEX)."""
    step = 100 if index_name.upper() == "SENSEX" else 50
    return int(math.ceil(spot / step) * step)

def _adjust_strikes_for_delta(trading_date: date, ce_strike: int, pe_strike: int) -> tuple[int, int]:
    try:
        api = AngelAPI()
        api.login()
        time.sleep(1)

        expiry_str_for_greeks = get_next_expiry("NIFTY", trading_date)
        greeks_data = api.get_option_greeks("NIFTY", expiry_date=expiry_str_for_greeks)
        
        if greeks_data and greeks_data.get("status"):
            greeks_list = greeks_data.get("data", [])

            # Convert strikePrice and delta to numeric types for proper sorting and comparison
            for item in greeks_list:
                item["strikePrice"] = int(float(item.get("strikePrice", 0)))
                item["delta"] = float(item.get("delta", 0))

            ce_options = sorted([item for item in greeks_list if item.get("optionType") == "CE"], key=lambda x: x.get("strikePrice", 0))
            pe_options = sorted([item for item in greeks_list if item.get("optionType") == "PE"], key=lambda x: x.get("strikePrice", 0), reverse=True)

            for option in ce_options:
                log.info(f"Strike : {option["strikePrice"]} : Delta : {option["delta"]:.2f}")
                if option.get("strikePrice", 0) >= ce_strike and option.get("delta", 1) <= 0.25:
                    ce_strike = option["strikePrice"]

                    log.info(f"Found CE strike {ce_strike} with delta {option['delta']:.2f}")
                    break
            
            for option in pe_options:
                if option.get("strikePrice", 0) <= pe_strike and option.get("delta", -1) >= -0.25:
                    pe_strike = option["strikePrice"]
                    log.info(f"Found PE strike {pe_strike} with delta {option['delta']:.2f}")
                    break
        else:
            log.error(f"Failed to fetch greeks data for delta selection: {greeks_data}")

    except Exception as e:
        log.error(f"Could not perform delta-based strike selection: {e}")
    
    return ce_strike, pe_strike


def get_single_ce_pe_strikes(spot: float, spot_candle_end_time: datetime, index_name: str = "NIFTY", trading_date: date | None = None, strike_step: int = 50) -> dict:
    """
    From spot, compute initial strikes, then adjust based on the 9:30 AM price difference.
    """
    if trading_date is None:
        trading_date = date.today()

    ce_base = ceil_to_strike_step(spot, index_name)
    pe_base = floor_to_strike_step(spot, index_name)

    # Determine strike offset based on index and days to expiry
    if index_name.upper() == "SENSEX":
        expiry_str = get_next_expiry(index_name, trading_date)
        expiry_date = datetime.strptime(expiry_str, "%d%b%Y").date()
        days_to_expiry = (expiry_date - trading_date).days

        if days_to_expiry == 0:
            strike_offset = 400
            hedge_offset = 500
        elif days_to_expiry in [1, 2]:
            strike_offset = 600
            hedge_offset = 800
        elif days_to_expiry == 3:
            strike_offset = 700
            hedge_offset = 700
        else:  # More than 3 days
            strike_offset = 800
            hedge_offset = 700
        log.info(f"SENSEX expiry is in {days_to_expiry} days. Selected strike offset: {strike_offset}, hedge offset: {hedge_offset}")
    else:  # Default to NIFTY
        strike_offset = 150
        hedge_offset = 400

    ce_strike = ce_base + strike_offset
    pe_strike = pe_base - strike_offset

    log.info(f"Initial strikes: CE={ce_strike}, PE={pe_strike}")

    # --- Price-based Adjustment ---
    try:
        api = AngelAPI()
        api.login()
        time.sleep(1)

        ce_contract = find_option(index_name, ce_strike, "CE", trading_date=trading_date)
        pe_contract = find_option(index_name, pe_strike, "PE", trading_date=trading_date)

        start_dt = spot_candle_end_time
        end_dt = start_dt + timedelta(minutes=1)
        from_str, to_str = start_dt.strftime("%Y-%m-%d %H:%M"), end_dt.strftime("%Y-%m-%d %H:%M")
        
        def get_historical_price(contract):
            res = api.connection.getCandleData({"exchange": contract.exchange, "symboltoken": contract.token, "interval": "ONE_MINUTE", "fromdate": from_str, "todate": to_str})
            log.info(f"Historical data for {contract.symbol} at 9:30: {res.get('data')}")
            if res and res.get("data"):
                return float(res["data"][0][4]) # Close price of the 9:30 candle
            raise RuntimeError(f"No 9:30 historical data for {contract.symbol}")

        ce_ltp = get_historical_price(ce_contract)
        pe_ltp = get_historical_price(pe_contract)

        log.info(f"Prices for adjustment check: CE Strike={ce_contract.strike}, Price={ce_ltp:.2f} | PE Strike={pe_contract.strike}, Price={pe_ltp:.2f}")

        price_diff_pct = abs(ce_ltp - pe_ltp) / max(ce_ltp, pe_ltp)

        if price_diff_pct > 0.30:
            strike_step = 100 if index_name.upper() == "SENSEX" else 50
            log.warning(f"Price difference > 30% ({price_diff_pct:.1%}). Adjusting cheaper leg.")
            if ce_ltp < pe_ltp:
                ce_strike -= strike_step # Move 1 strike closer
                log.info(f"CE is cheaper. New CE strike: {ce_strike}")
            else:
                pe_strike += strike_step # Move 1 strike closer
                log.info(f"PE is cheaper. New PE strike: {pe_strike}")
        else:
            log.info("Price difference is within limits. No adjustment needed.")

    except Exception as e:
        log.error(f"Could not perform price-based strike adjustment: {e}. Using initial strikes.")

    # Calculate long strikes based on the (potentially adjusted) short strikes and the index's strike_step
    long_ce_strike = ce_strike + hedge_offset
    long_pe_strike = pe_strike - hedge_offset

    return {
        "spot": float(spot),
        "ce_base": ce_base,
        "pe_base": pe_base,
        "ce_strike": ce_strike,
        "pe_strike": pe_strike,
        "long_ce_strike": long_ce_strike,
        "long_pe_strike": long_pe_strike,
    }


if __name__ == "__main__":
    test_spot = 26229.65
    # For testing historical dates, you need to provide a date
    # info = get_single_ce_pe_strikes(test_spot, trading_date=date(2023, 12, 8))
    info = get_single_ce_pe_strikes(test_spot)
    print(info)
