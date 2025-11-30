# src/data_pipeline/nifty_first_15m.py
from datetime import datetime, date, time
import logging

from src.api.smartapi_client import AngelAPI

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

NIFTY_TOKEN = "99926000"
NIFTY_EXCHANGE = "NSE"


def get_nifty_first_15m_close(trading_date: date | None = None) -> float:
    """
    Returns the first 15-minute candle close of NIFTY for the given trading_date.
    If trading_date is None, uses today's date.
    """

    if trading_date is None:
        trading_date = datetime.now().date()

    # 09:15 → 09:30 window for first 15m candle
    start_dt = datetime.combine(trading_date, time(9, 15))
    end_dt = datetime.combine(trading_date, time(9, 30))

    from_str = start_dt.strftime("%Y-%m-%d %H:%M")
    to_str = end_dt.strftime("%Y-%m-%d %H:%M")

    log.info(f"Fetching NIFTY first 15m candle for {trading_date} ({from_str} → {to_str})")

    api = AngelAPI()
    api.login()
    if api.mock:
        raise RuntimeError("AngelAPI is in MOCK mode; cannot fetch real NIFTY candles.")

    params = {
        "exchange": NIFTY_EXCHANGE,
        "symboltoken": NIFTY_TOKEN,
        "interval": "FIFTEEN_MINUTE",
        "fromdate": from_str,
        "todate": to_str,
    }

    res = api.connection.getCandleData(params)
    # Expected format: {"status": True, "data": [[ts, o, h, l, c, vol], ...]}
    candles = res.get("data") or []
    if not candles:
        raise RuntimeError(f"No candle data returned for NIFTY in range {from_str} → {to_str}")

    first = candles[0]
    ts, o, h, l, c, vol = first
    log.info(f"First 15m candle for {trading_date}: O={o}, H={h}, L={l}, C={c}, V={vol}")
    return float(c)
