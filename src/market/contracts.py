# src/market/contracts.py
from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path
from datetime import datetime, date


import requests

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

SCRIP_MASTER_URL = "https://margincalculator.angelone.in/OpenAPI_File/files/OpenAPIScripMaster.json"
SCRIP_CACHE_FILE = Path("data/OpenAPIScripMaster.json")


@dataclass
class OptionContract:
    symbol: str        # tradingsymbol
    token: str         # symboltoken
    strike: int
    option_type: str   # "CE" or "PE"
    expiry: str
    exchange: str      # e.g. "NFO"


def load_scrip_master(force_download: bool = False) -> list[dict]:
    """
    Load the Angel One Scrip Master JSON.
    - Downloads once and caches to data/OpenAPIScripMaster.json
    - Later runs read from local file unless force_download=True
    """
    SCRIP_CACHE_FILE.parent.mkdir(parents=True, exist_ok=True)

    if not SCRIP_CACHE_FILE.exists() or force_download:
        log.info("Downloading Scrip Master from Angel One...")
        resp = requests.get(SCRIP_MASTER_URL, timeout=30)
        resp.raise_for_status()
        SCRIP_CACHE_FILE.write_bytes(resp.content)
        log.info("Scrip Master saved to %s", SCRIP_CACHE_FILE)

    import json

    with SCRIP_CACHE_FILE.open() as f:
        data = json.load(f)

    # data is usually a list of dicts; you can print one row to inspect fields if needed.
    return data


def find_option(
        index_name: str,
        strike: int,
        option_type: str,
        expiry_str: str | None = None,
        trading_date: date | None = None,
) -> OptionContract:
    """
    Find an option contract in Scrip Master for:
      - index_name: "NIFTY" or "SENSEX"
      - given strike (e.g. 26350)
      - option_type: "CE" or "PE"
      - expiry_str: exact expiry like '26JUN2029'; if None, auto-select next expiry based on trading_date.
    """
    option_type = option_type.upper()
    if option_type not in ("CE", "PE"):
        raise ValueError("option_type must be 'CE' or 'PE'")

    index_name = index_name.upper()
    if index_name not in ["NIFTY", "SENSEX"]:
        raise ValueError(f"Unsupported index for option lookup: {index_name}")

    options_exchange = "NFO" if index_name == "NIFTY" else "BFO"

    if expiry_str is None:
        expiry_str = get_next_expiry(index_name, trading_date)

    data = load_scrip_master()
    candidates = []

    for row in data:
        row_name = row.get("name") or ""
        exch_seg = row.get("exch_seg") or ""
        instrumenttype = row.get("instrumenttype") or ""
        symbol = row.get("symbol") or ""
        token = row.get("token") or ""
        expiry = row.get("expiry") or ""

        if row_name != index_name:
            continue
        if exch_seg != options_exchange:
            continue
        if "OPT" not in instrumenttype.upper():
            continue
        if not symbol.endswith(option_type):
            continue

        # Strike is scaled by 100
        strike_field = row.get("strike") or row.get("strikeprice") or 0
        try:
            strike_value_raw = float(strike_field)
            strike_value = int(strike_value_raw / 100)   # convert back to real strike
        except Exception:
            continue

        if strike_value != strike:
            continue

        if expiry != expiry_str:
            continue

        candidates.append(
            OptionContract(
                symbol=symbol,
                token=str(token),
                strike=strike_value,
                option_type=option_type,
                expiry=str(expiry),
                exchange=exch_seg,
            )
        )

    if not candidates:
        raise RuntimeError(
            f"No {index_name} option found for strike={strike}, type={option_type}, expiry={expiry_str}"
        )

    c = candidates[0]
    log.info("Resolved %s %s %s as symbol=%s, token=%s, expiry=%s", index_name, c.option_type, c.strike, c.symbol, c.token, c.expiry)
    return c

def get_next_expiry(index_name: str, trading_date: date | None = None) -> str:
    """
    Auto-select next option expiry (>= trading_date) from Scrip Master for a given index.
    index_name: "NIFTY" or "SENSEX"
    trading_date: date for which you want to trade/backtest.
                  If None, uses today's date.
    Returns expiry string as in Scrip Master, e.g. '26JUN2029'.
    """
    index_name = index_name.upper()
    if index_name not in ["NIFTY", "SENSEX"]:
        raise ValueError(f"Unsupported index for expiry lookup: {index_name}")

    options_exchange = "NFO" if index_name == "NIFTY" else "BFO"

    if trading_date is None:
        trading_date = date.today()

    data = load_scrip_master()
    expiries: list[tuple[date, str]] = []

    for row in data:
        if row.get("name") != index_name:
            continue
        if row.get("exch_seg") != options_exchange:
            continue
        instrumenttype = row.get("instrumenttype") or ""
        if "OPT" not in instrumenttype.upper():
            continue

        expiry_str = row.get("expiry")
        if not expiry_str:
            continue

        try:
            # expiry like '26JUN2029'
            dt = datetime.strptime(expiry_str, "%d%b%Y").date()
        except Exception:
            continue

        if dt >= trading_date:
            expiries.append((dt, expiry_str))

    if not expiries:
        raise RuntimeError(f"No {index_name} option expiry found on or after {trading_date}")

    dt, expiry_str = min(expiries, key=lambda x: x[0])
    log.info("Auto-selected %s expiry %s for trading date %s", index_name, expiry_str, trading_date)
    return expiry_str
