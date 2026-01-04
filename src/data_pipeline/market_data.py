import logging
from datetime import datetime, timedelta

log = logging.getLogger(__name__)

def get_latest_candle_close(api_client, exchange: str, token: str, symbol_name: str, query_time: datetime | None = None) -> float:
    """
    Fetches the close of the most recent completed 1-minute candle for the index.
    Falls back to LTP if candle data is unavailable.
    """
    now = query_time or datetime.now()
    # Fetch last 10 minutes to ensure we get at least one completed candle
    from_time = (now - timedelta(minutes=10)).strftime("%Y-%m-%d %H:%M")
    to_time = now.strftime("%Y-%m-%d %H:%M")
    
    try:
        payload = {
            "exchange": exchange,
            "symboltoken": token,
            "interval": "ONE_MINUTE",
            "fromdate": from_time,
            "todate": to_time,
        }
        # Accessing the underlying SmartConnect object via .connection
        data = api_client.connection.getCandleData(payload)
        candles = data.get("data", [])
        
        if candles:
            # candles format: [timestamp, open, high, low, close, volume]
            last_candle = candles[-1]
            close_price = float(last_candle[4])
            log.info(f"Selected Spot Price from latest candle ({last_candle[0]}): {close_price}")
            return close_price
        
        log.warning(f"No candle data found for {symbol_name}. Falling back to LTP.")
    except Exception as e:
        log.error(f"Error fetching index candle: {e}. Falling back to LTP.")
        
    return api_client.get_ltp(exchange, symbol_name, token)