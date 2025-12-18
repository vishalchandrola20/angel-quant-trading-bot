# src/data_pipeline/nifty_first_15m.py
from datetime import datetime, date, time as dt_time, timedelta
import logging
import time as time_module

from src.api.smartapi_client import AngelAPI

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

INDEX_CONFIG = {
    "NIFTY": {
        "token": "99926000",
        "exchange": "NSE",
    },
    "SENSEX": {
        "token": "99919000",
        "exchange": "BSE",
    }
}


def get_index_first_15m_close(index_name: str, trading_date: date | None = None) -> tuple[float, datetime]:
    """
    Returns the close price and end time of the latest completed 15-minute candle.
    - If trading_date is None, uses today's date.
    - Waits until 9:30 AM if started early.
    """
    index_name = index_name.upper()
    if index_name not in INDEX_CONFIG:
        raise ValueError(f"Unsupported index: {index_name}. Supported are {list(INDEX_CONFIG.keys())}")

    trading_date = trading_date or date.today()
    now = datetime.now()

    # Always target the first 15-minute candle of the day (9:15 - 9:30)
    first_candle_end_time = datetime.combine(trading_date, dt_time(9, 30))

    # If running before the first candle is complete, wait.
    if now < first_candle_end_time:
        wait_seconds = (first_candle_end_time - now).total_seconds()
        log.info(f"Current time is before {first_candle_end_time.strftime('%H:%M')}. Waiting for {wait_seconds:.0f} seconds...")
        time_module.sleep(wait_seconds)

    # Set the time window to always be the first 15-minute candle
    start_dt = datetime.combine(trading_date, dt_time(9, 15))
    end_dt = datetime.combine(trading_date, dt_time(9, 30))
    start_dt = end_dt - timedelta(minutes=15)

    from_str = start_dt.strftime("%Y-%m-%d %H:%M")
    to_str = end_dt.strftime("%Y-%m-%d %H:%M")

    log.info(f"Fetching {index_name} 15m candle for {trading_date} ({from_str} → {to_str})")

    api = AngelAPI()
    api.login()
    time_module.sleep(1)
    if api.mock:
        raise RuntimeError("AngelAPI is in MOCK mode; cannot fetch real NIFTY candles.")

    config = INDEX_CONFIG[index_name]
    params = {
        "exchange": config["exchange"],
        "symboltoken": config["token"],
        "interval": "FIFTEEN_MINUTE",
        "fromdate": from_str,
        "todate": to_str,
    }

    res = api.connection.getCandleData(params)
    candles = res.get("data") or []
    if not candles:
        raise RuntimeError(f"No candle data returned for {index_name} in range {from_str} → {to_str}")

    first = candles[0]
    ts, o, h, l, c, vol = first
    log.info(f"Latest 15m candle for {index_name} on {trading_date} ({from_str} -> {to_str}): O={o}, H={h}, L={l}, C={c}, V={vol}")
    return float(c), end_dt
