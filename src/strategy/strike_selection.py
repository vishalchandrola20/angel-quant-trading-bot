# src/strategy/strike_selection.py
from __future__ import annotations
import math
import logging
import time
from datetime import date, datetime, timedelta
from typing import Callable

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


def _adjust_for_price_difference(
    ce_strike: int,
    pe_strike: int,
    index_name: str,
    trading_date: date,
    get_historical_price: Callable,
    expiry_str: str | None
) -> tuple[int, int]:
    """Adjusts strikes based on the price difference of the initial options."""
    ce_contract = find_option(index_name, ce_strike, "CE", expiry_str, trading_date)
    pe_contract = find_option(index_name, pe_strike, "PE", expiry_str, trading_date)

    ce_ltp = get_historical_price(ce_contract)
    pe_ltp = get_historical_price(pe_contract)

    log.info(f"{Fore.YELLOW}Prices for adjustment check: CE Strike={ce_contract.strike}, Price={ce_ltp:.2f} | PE Strike={pe_contract.strike}, Price={pe_ltp:.2f}{Style.RESET_ALL}")

    price_diff_pct = abs(ce_ltp - pe_ltp) / max(ce_ltp, pe_ltp)

    if price_diff_pct > 0.25:
        strike_step = 100 if index_name.upper() == "SENSEX" else 50
        log.warning(f"{Fore.YELLOW}Price difference > 30% ({price_diff_pct:.1%}). Adjusting cheaper leg.{Style.RESET_ALL}")
        if ce_ltp < pe_ltp:
            ce_strike -= strike_step
            new_ce_contract = find_option(index_name, ce_strike, "CE", expiry_str, trading_date)
            new_ce_price = get_historical_price(new_ce_contract)
            new_price_diff_pct = abs(new_ce_price - pe_ltp) / max(new_ce_price, pe_ltp)
            log.info(f"{Fore.CYAN}CE is cheaper. New CE strike: {ce_strike} with Price={new_ce_price:.2f}. New diff: {new_price_diff_pct:.1%}{Style.RESET_ALL}")

            if new_price_diff_pct > 0.25:
                log.warning(f"{Fore.YELLOW}Difference still > 30%. Adjusting expensive leg (PE) away.{Style.RESET_ALL}")
                pe_strike -= strike_step
                final_pe_contract = find_option(index_name, pe_strike, "PE", expiry_str, trading_date)
                final_pe_price = get_historical_price(final_pe_contract)
                final_diff_pct = abs(new_ce_price - final_pe_price) / max(new_ce_price, final_pe_price)
                log.info(f"{Fore.CYAN}PE moved away. New PE strike: {pe_strike} with Price={final_pe_price:.2f}. Final diff: {final_diff_pct:.1%}{Style.RESET_ALL}")
        else:
            pe_strike += strike_step
            new_pe_contract = find_option(index_name, pe_strike, "PE", expiry_str, trading_date)
            new_pe_price = get_historical_price(new_pe_contract)
            new_price_diff_pct = abs(ce_ltp - new_pe_price) / max(ce_ltp, new_pe_price)
            log.info(f"{Fore.CYAN}PE is cheaper. New PE strike: {pe_strike} with Price={new_pe_price:.2f}. New diff: {new_price_diff_pct:.1%}{Style.RESET_ALL}")

            if new_price_diff_pct > 0.25:
                log.warning(f"{Fore.YELLOW}Difference still > 30%. Adjusting expensive leg (CE) away.{Style.RESET_ALL}")
                ce_strike += strike_step
                final_ce_contract = find_option(index_name, ce_strike, "CE", expiry_str, trading_date)
                final_ce_price = get_historical_price(final_ce_contract)
                final_diff_pct = abs(final_ce_price - new_pe_price) / max(final_ce_price, new_pe_price)
                log.info(f"{Fore.CYAN}CE moved away. New CE strike: {ce_strike} with Price={final_ce_price:.2f}. Final diff: {final_diff_pct:.1%}{Style.RESET_ALL}")
    else:
        log.info(f"{Fore.GREEN}Price difference is within limits. No adjustment needed.{Style.RESET_ALL}")
    
    return ce_strike, pe_strike


def get_single_ce_pe_strikes(spot: float, spot_candle_end_time: datetime, index_name: str = "NIFTY", trading_date: date | None = None, strike_step: int = 50, expiry_str: str | None = None) -> dict:
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
        final_expiry_str = expiry_str or get_next_expiry(index_name, trading_date)
        expiry_date = datetime.strptime(final_expiry_str, "%d%b%Y").date()
        dte = _get_trading_days_to_expiry(trading_date, expiry_date)

        if dte <= 1:
            strike_offset = 800
            hedge_offset = 1000
        elif dte <= 3:
            strike_offset = 1100
            hedge_offset = 700
        else:  # More than 3 days
            strike_offset = 1200
            hedge_offset = 700
        log.info(f"SENSEX expiry is in {dte} trading days. Selected strike offset: {strike_offset}, hedge offset: {hedge_offset}")
    else:  # Default to NIFTY
        final_expiry_str = expiry_str or get_next_expiry(index_name, trading_date)
        expiry_date = datetime.strptime(final_expiry_str, "%d%b%Y").date()

        # Calculate trading days to expiry, excluding weekends
        dte = _get_trading_days_to_expiry(trading_date, expiry_date)

        if dte <= 1:  # 0 and 1 DTE (e.g., Tuesday, Monday for a Tuesday expiry)
            strike_offset = 200
            hedge_offset = 400
        elif dte <= 3:  # 2 and 3 DTE (e.g., Friday, Thursday)
            strike_offset = 350
            hedge_offset = 400
        else:  # 4+ DTE
            strike_offset = 400
            hedge_offset = 400
        log.info(f"NIFTY expiry is in {dte} trading days. Selected strike offset: {strike_offset}, hedge offset: {hedge_offset}")

    ce_strike = ce_base + strike_offset
    pe_strike = pe_base - strike_offset

    log.info(f"Initial strikes: CE={ce_strike}, PE={pe_strike}")

    # --- Price and Credit Adjustments ---
    try:
        api = AngelAPI()
        api.login()
        time.sleep(1)

        start_dt = spot_candle_end_time
        end_dt = start_dt + timedelta(minutes=1)
        from_str, to_str = start_dt.strftime("%Y-%m-%d %H:%M"), end_dt.strftime("%Y-%m-%d %H:%M")
        
        def get_historical_price(contract):
            max_retries = 5
            for attempt in range(max_retries):
                try:
                    time.sleep(0.5)  # Keep the rate limit sleep
                    res = api.connection.getCandleData({"exchange": contract.exchange, "symboltoken": contract.token, "interval": "ONE_MINUTE", "fromdate": from_str, "todate": to_str})

                    # Case 1: Successful response with data
                    if res and res.get("status") and res.get("data"):
                        return float(res["data"][0][4])  # Success

                    # Case 2: Successful response but NO data (illiquid option)
                    if res and res.get("status") is True and not res.get("data"):
                        return None  # Signal illiquidity to the caller

                    log.warning(f"Attempt {attempt + 1}/{max_retries}: API error for {contract.symbol}. Response: {res}")

                except Exception as e:
                    log.warning(f"Attempt {attempt + 1}/{max_retries}: API call failed for {contract.symbol}: {e}")

                if attempt < max_retries - 1:
                    log.info("Waiting 2 seconds before retrying...")
                    time.sleep(2)

            # If loop finishes without returning, it means all retries failed.
            raise RuntimeError(f"Failed to get historical data for {contract.symbol} after {max_retries} attempts (hard failure).")

        ce_strike, pe_strike = _adjust_for_price_difference(ce_strike, pe_strike, index_name, trading_date, get_historical_price, final_expiry_str)

        # Determine min_net_credit based on DTE
        if index_name.upper() == "SENSEX":
            if dte <= 1: min_net_credit = 50
            elif dte <= 3: min_net_credit = 75
            else: min_net_credit = 90
        else: # NIFTY
            if dte <= 1: min_net_credit = 20
            elif dte <= 3: min_net_credit = 30
            else: min_net_credit = 30

        log.info(f"DTE is {dte}. Minimum required net credit set to: {min_net_credit}")

        # --- Net Credit-based Adjustment ---
        min_strike_distance = 900 if index_name.upper() == "SENSEX" else 200
        max_adjust_loops = 5  # Safety break to prevent infinite loops
        
        # Initialize final long strikes with default values
        final_long_ce_strike = ce_strike + hedge_offset
        final_long_pe_strike = pe_strike - hedge_offset

        for i in range(max_adjust_loops):
            # Find contracts for the current strikes to check their price
            temp_ce_contract = find_option(index_name, ce_strike, "CE", final_expiry_str, trading_date)
            temp_pe_contract = find_option(index_name, pe_strike, "PE", final_expiry_str, trading_date)

            # Get historical prices for the current strikes
            ce_price = get_historical_price(temp_ce_contract)
            pe_price = get_historical_price(temp_pe_contract)

            # --- Hedge Leg Liquidity Search ---
            # For Long CE
            current_long_ce_strike = ce_strike + hedge_offset
            long_ce_price = None
            for step in range(5):
                temp_long_ce_contract = find_option(index_name, current_long_ce_strike, "CE", final_expiry_str, trading_date)
                price = get_historical_price(temp_long_ce_contract)
                if price is not None:
                    long_ce_price = price
                    if step > 0: log.info(f"{Fore.GREEN}Found liquid hedge for CE at strike {current_long_ce_strike} with price {price:.2f}{Style.RESET_ALL}")
                    break
                log.warning(f"No data for hedge CE at {current_long_ce_strike}. Moving one step closer.")
                current_long_ce_strike -= strike_step
            if long_ce_price is None:
                log.error(f"{Fore.RED}Could not find a liquid hedge for CE. Using 0.0 price.{Style.RESET_ALL}")
                long_ce_price = 0.0
            final_long_ce_strike = current_long_ce_strike

            # For Long PE
            current_long_pe_strike = pe_strike - hedge_offset
            long_pe_price = None
            for step in range(5):
                temp_long_pe_contract = find_option(index_name, current_long_pe_strike, "PE", final_expiry_str, trading_date)
                price = get_historical_price(temp_long_pe_contract)
                if price is not None:
                    long_pe_price = price
                    if step > 0: log.info(f"{Fore.GREEN}Found liquid hedge for PE at strike {current_long_pe_strike} with price {price:.2f}{Style.RESET_ALL}")
                    break
                log.warning(f"No data for hedge PE at {current_long_pe_strike}. Moving one step closer.")
                current_long_pe_strike += strike_step
            if long_pe_price is None:
                log.error(f"{Fore.RED}Could not find a liquid hedge for PE. Using 0.0 price.{Style.RESET_ALL}")
                long_pe_price = 0.0
            final_long_pe_strike = current_long_pe_strike

            current_net_credit = (ce_price + pe_price) - (long_ce_price + long_pe_price)
            log.info(f"{Fore.CYAN}Credit Check (Loop {i+1}): Strikes CE={ce_strike}/PE={pe_strike} -> Net Credit = {current_net_credit:.2f}{Style.RESET_ALL}")

            if current_net_credit >= min_net_credit:
                log.info(f"{Fore.GREEN}Net credit {current_net_credit:.2f} is >= minimum required {min_net_credit}. Strikes finalized for now.{Style.RESET_ALL}")
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
        else: # This 'else' belongs to the 'for' loop, runs if the loop completes without a 'break'
            log.error(f"Could not achieve minimum net credit of {min_net_credit} after {max_adjust_loops} attempts. Using last calculated strikes.")

        # --- Final Price Skew Check ---
        log.info("Performing final price skew check after credit adjustment...")
        ce_strike, pe_strike = _adjust_for_price_difference(ce_strike, pe_strike, index_name, trading_date, get_historical_price, final_expiry_str)

    except Exception as e:
        log.error(f"Could not perform strike adjustments: {e}. Halting strike selection.")
        raise

    # Calculate long strikes based on the (potentially adjusted) short strikes and the index's strike_step
    long_ce_strike = final_long_ce_strike
    long_pe_strike = final_long_pe_strike

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
