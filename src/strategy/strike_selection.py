# src/strategy/strike_selection.py
from __future__ import annotations
import math


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


def get_single_ce_pe_strikes(spot: float) -> dict:
    """
    From spot, compute:
      - atm_strike  = nearest 50 (custom rule)
      - ce_base     = ceil_to_50(spot)
      - pe_base     = floor_to_50(spot)
      - ce_strike   = ce_base + 100
      - pe_strike   = pe_base - 100
    """
    atm = get_atm_strike_custom(spot)

    ce_base = ceil_to_50(spot)
    pe_base = floor_to_50(spot)

    ce_strike = ce_base + 100
    pe_strike = pe_base - 100

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
    info = get_single_ce_pe_strikes(test_spot)
    print(info)
