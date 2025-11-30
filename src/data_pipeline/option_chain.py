# src/data_pipeline/option_chain.py
"""
Fetch option chain via AngelAPI wrapper and save CSV snapshot.
If AngelAPI is in mock mode, a small sample CSV will still be created so you can test flows.
"""
import csv
from datetime import datetime
from pathlib import Path
from src.api.smartapi_client import AngelAPI
import logging

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


OUT_DIR = Path("data/raw")
OUT_DIR.mkdir(parents=True, exist_ok=True)


def save_option_chain_csv(option_chain, symbol="NIFTY"):
    """
    option_chain: list of dicts (each strike)
    Saves a simple CSV: strike, expiry, side, lastPrice, openInterest, iv
    """
    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    out_file = OUT_DIR / f"{symbol}_option_chain_{ts}.csv"
    with open(out_file, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["strike", "expiry", "side", "lastPrice", "openInterest", "iv"])
        for strike_row in option_chain:
            strike = strike_row.get("strikePrice")
            expiry = strike_row.get("expiryDate")
            # CE
            ce = strike_row.get("CE")
            if ce:
                writer.writerow([strike, expiry, "CE", ce.get("lastPrice"), ce.get("openInterest"), ce.get("impliedVolatility")])
            # PE
            pe = strike_row.get("PE")
            if pe:
                writer.writerow([strike, expiry, "PE", pe.get("lastPrice"), pe.get("openInterest"), pe.get("impliedVolatility")])

    log.info(f"Saved option chain snapshot to {out_file}")
    return out_file


def fetch_and_save(symbol="NIFTY"):
    api = AngelAPI()
    logged_in = api.login()
    option_chain = api.get_option_chain(symbol)
    # option_chain may be in various shapes depending on API; our smart wrapper returns list-of-dicts
    csv_path = save_option_chain_csv(option_chain, symbol)
    return csv_path


if __name__ == "__main__":
    path = fetch_and_save("NIFTY")
    print("Output:", path)
