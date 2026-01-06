# src/data_pipeline/upstox_option_chain.py
import csv
import logging
from datetime import datetime
from pathlib import Path

from src.api.upstox_client import UpstoxAPI

log = logging.getLogger(__name__)

OUT_DIR = Path("data/raw")
OUT_DIR.mkdir(parents=True, exist_ok=True)


def save_upstox_option_chain_csv(option_chain_data, symbol="NIFTY"):
    """Saves a simple CSV from the Upstox option chain data."""
    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    out_file = OUT_DIR / f"upstox_{symbol}_option_chain_{ts}.csv"
    with open(out_file, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["strike", "expiry", "side", "lastPrice", "openInterest", "iv"])
        for contract in option_chain_data:
            writer.writerow([
                contract.get("strike_price"),
                contract.get("expiry"),
                contract.get("option_type"),
                contract.get("ltp"),
                contract.get("oi"),
                contract.get("iv"),
            ])
    log.info(f"Saved Upstox option chain snapshot to {out_file}")
    return out_file


def fetch_and_save_upstox_chain(index_name: str, expiry_date: str):
    """
    Main function to fetch option chain from Upstox and save it.
    index_name: "NIFTY" or "SENSEX"
    expiry_date: "YYYY-MM-DD"
    """
    api = UpstoxAPI()
    if not api.login():
        return

    # Map index name to Upstox instrument key
    instrument_map = {
        "NIFTY": "NSE_INDEX|Nifty 50",
        "SENSEX": "BSE_INDEX|SENSEX",
    }
    instrument_key = instrument_map.get(index_name.upper())
    if not instrument_key:
        log.error(f"Unsupported index for Upstox: {index_name}")
        return

    option_data = api.get_option_chain(instrument_key, expiry_date)
    if option_data:
        save_upstox_option_chain_csv(option_data, symbol=index_name)