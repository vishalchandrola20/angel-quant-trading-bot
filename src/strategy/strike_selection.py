# src/strategy/strike_selection.py
from __future__ import annotations
import math
import logging
import time
from datetime import date, datetime, timedelta

from colorama import Fore, Style

from src.api.smartapi_client import AngelAPI
from src.market.contracts import find_option, get_next_expiry

log = logging.getLogger(__name__)


def _get_trading_days_to_expiry(trading_date: date, expiry_date: date) -> int:
    """Calculates the number of trading days between trading_date and expiry_date."""
    # This calculation is for future days, so we start from the day after trading_date
    current_date = trading_date + timedelta(days=1)
    trading_days = 0
    while current_date <= expiry_date:
        # Monday is 0 and Sunday is 6. We only count weekdays.
        if current_date.weekday() < 5:
            trading_days += 1
        current_date += timedelta(days=1)
    return trading_days


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
    global get_historical_price
    if trading_date is None:
        trading_date = date.today()

    ce_base = ceil_to_strike_step(spot, index_name)
    pe_base = floor_to_strike_step(spot, index_name)

    # Determine strike offset based on index and days to expiry
    if index_name.upper() == "SENSEX":
        expiry_str = get_next_expiry(index_name, trading_date)
        expiry_date = datetime.strptime(expiry_str, "%d%b%Y").date()        
        dte = _get_trading_days_to_expiry(trading_date, expiry_date)

        if dte <= 1:
            strike_offset = 800
            hedge_offset = 1000
        elif dte <= 3:
            strike_offset = 900
            hedge_offset = 1000
        else:  # More than 3 days
            strike_offset = 1000
            hedge_offset = 1000
        log.info(f"SENSEX expiry is in {dte} trading days. Selected strike offset: {strike_offset}, hedge offset: {hedge_offset}")
    else:  # Default to NIFTY
        expiry_str = get_next_expiry(index_name, trading_date)
        expiry_date = datetime.strptime(expiry_str, "%d%b%Y").date()

        # Calculate trading days to expiry, excluding weekends
        dte = _get_trading_days_to_expiry(trading_date, expiry_date)

        if dte <= 1:  # 0 and 1 DTE (e.g., Tuesday, Monday for a Tuesday expiry)
            strike_offset = 150
            hedge_offset = 400
        elif dte <= 3:  # 2 and 3 DTE (e.g., Friday, Thursday)
            strike_offset = 200
            hedge_offset = 400
        else:  # 4+ DTE
            strike_offset = 250
            hedge_offset = 400
        log.info(f"NIFTY expiry is in {dte} trading days. Selected strike offset: {strike_offset}, hedge offset: {hedge_offset}")

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
            time.sleep(0.5)
            res = api.connection.getCandleData({"exchange": contract.exchange, "symboltoken": contract.token, "interval": "ONE_MINUTE", "fromdate": from_str, "todate": to_str})
            if res and res.get("data"):
                return float(res["data"][0][4]) # Close price of the 9:30 candle
            raise RuntimeError(f"No 9:30 historical data for {contract.symbol}")

        ce_ltp = get_historical_price(ce_contract)
        pe_ltp = get_historical_price(pe_contract)

        log.info(f"{Fore.YELLOW}Prices for adjustment check: CE Strike={ce_contract.strike}, Price={ce_ltp:.2f} | PE Strike={pe_contract.strike}, Price={pe_ltp:.2f}{Style.RESET_ALL}")

        price_diff_pct = abs(ce_ltp - pe_ltp) / max(ce_ltp, pe_ltp)

        if price_diff_pct > 0.30:
            strike_step = 100 if index_name.upper() == "SENSEX" else 50
            log.warning(f"{Fore.YELLOW}Price difference > 30% ({price_diff_pct:.1%}). Adjusting cheaper leg.{Style.RESET_ALL}")
            if ce_ltp < pe_ltp:
                ce_strike -= strike_step # Move 1 strike closer
                log.info(f"CE is cheaper. New CE strike: {ce_strike}")
            else:
                pe_strike += strike_step # Move 1 strike closer
                log.info(f"PE is cheaper. New PE strike: {pe_strike}")
        else:
            log.info(f"{Fore.GREEN}Price difference is within limits. No adjustment needed.{Style.RESET_ALL}")

    except Exception as e:
        log.error(f"Could not perform price-based strike adjustment: {e}. Using initial strikes.")

    # Determine min_net_credit based on DTE
    if index_name.upper() == "SENSEX":
        if dte <= 1: min_net_credit = 75
        elif dte <= 3: min_net_credit = 90
        else: min_net_credit = 105
    else: # NIFTY
        if dte <= 1: min_net_credit = 30
        elif dte <= 3: min_net_credit = 36
        else: min_net_credit = 42

    log.info(f"DTE is {dte}. Minimum required net credit set to: {min_net_credit}")

    # --- Net Credit-based Adjustment ---
    min_strike_distance = 600 if index_name.upper() == "SENSEX" else 100
    max_adjust_loops = 5  # Safety break to prevent infinite loops

    for i in range(max_adjust_loops):
        try:
            # Find contracts for the current strikes to check their price
            temp_ce_contract = find_option(index_name, ce_strike, "CE", trading_date=trading_date)
            temp_pe_contract = find_option(index_name, pe_strike, "PE", trading_date=trading_date)
            temp_long_ce_contract = find_option(index_name, ce_strike + hedge_offset, "CE", trading_date=trading_date)
            temp_long_pe_contract = find_option(index_name, pe_strike - hedge_offset, "PE", trading_date=trading_date)

            # Get historical prices for the current strikes
            ce_price = get_historical_price(temp_ce_contract)
            pe_price = get_historical_price(temp_pe_contract)
            long_ce_price = get_historical_price(temp_long_ce_contract)
            long_pe_price = get_historical_price(temp_long_pe_contract)

            current_net_credit = (ce_price + pe_price) - (long_ce_price + long_pe_price)
            log.info(f"{Fore.CYAN}Credit Check (Loop {i+1}): Strikes CE={ce_strike}/PE={pe_strike} -> Net Credit = {current_net_credit:.2f}{Style.RESET_ALL}")

            if current_net_credit >= min_net_credit:
                log.info(f"{Fore.GREEN}Net credit {current_net_credit:.2f} is >= minimum required {min_net_credit}. Strikes finalized.{Style.RESET_ALL}")
                break  # Exit loop if credit is sufficient
            else:
                log.warning(f"Net credit {current_net_credit:.2f} is below minimum {min_net_credit}. Adjusting strikes closer.")

                # Check if the next adjustment would violate the minimum strike distance
                next_ce_strike = ce_strike - strike_step
                next_pe_strike = pe_strike + strike_step

                if (next_ce_strike - ce_base) < min_strike_distance or (pe_base - next_pe_strike) < min_strike_distance:
                    log.error(f"{Fore.RED}Cannot adjust further to meet min credit. Next adjustment would breach min strike distance of {min_strike_distance} points.{Style.RESET_ALL}")
                    break  # Stop adjusting

                ce_strike = next_ce_strike
                pe_strike = next_pe_strike

        except Exception as e:
            log.error(f"Error during net credit adjustment loop: {e}. Halting adjustments.")
            break
    else: # This 'else' belongs to the 'for' loop, runs if the loop completes without a 'break'
        log.error(f"Could not achieve minimum net credit of {min_net_credit} after {max_adjust_loops} attempts. Using last calculated strikes.")

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
    # For testing, we need a trading_date and a spot_candle_end_time
    test_trading_date = date.today()
    # Let's assume the spot price is from the 9:30 candle for this test
    test_spot_candle_end_time = datetime.combine(test_trading_date, datetime.min.time()).replace(hour=9, minute=30)

    info = get_single_ce_pe_strikes(
        spot=test_spot,
        spot_candle_end_time=test_spot_candle_end_time,
        trading_date=test_trading_date
    )
    print(info)
