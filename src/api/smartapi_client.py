# src/api/smartapi_client.py
"""
SmartAPI client wrapper.
- Reads config/credentials.yaml (not committed)
- Tries to login to SmartAPI. If credentials aren't present or login fails,
  the class can be used in "mock" mode for local testing.
"""
import os
import yaml
import pyotp
import logging
import uuid

try:
    from SmartApi import SmartConnect
except Exception:
    SmartConnect = None  # we'll detect this at runtime

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


class AngelAPI:
    def __init__(self, config_path="config/credentials.yaml", mock_if_missing=True):
        self.config_path = config_path
        self.connection = None
        self.jwt_token = None
        self.feed_token = None
        self.mock = False

        if not os.path.exists(config_path):
            log.warning(f"Credentials file not found at {config_path}. Running in MOCK mode.")
            self.mock = mock_if_missing
            return

        with open(config_path) as f:
            creds = yaml.safe_load(f).get("angel", {})
        required = ("api_key", "client_id", "password", "totp_secret")
        if not all(k in creds for k in required):
            log.warning("credentials.yaml missing required keys. Running in MOCK mode.")
            self.mock = mock_if_missing
            return

        if SmartConnect is None:
            log.warning("smartapi-python not installed or import failed. Running in MOCK mode.")
            self.mock = mock_if_missing
            return

        self.api_key = creds["api_key"]
        self.client_id = creds["client_id"]
        self.password = creds["password"]
        self.totp_secret = creds["totp_secret"]

        self.connection = SmartConnect(api_key=self.api_key)

    def login(self):
        """
        Attempt SmartAPI login. On success, sets jwt_token and feed_token.
        If in mock mode, just returns True.
        """
        if self.mock:
            log.info("MOCK login successful.")
            return True

        try:
            totp = pyotp.TOTP(self.totp_secret).now()
            data = self.connection.generateSession(self.client_id, self.password, totp)
            self.jwt_token = data["data"]["jwtToken"]
            self.feed_token = self.connection.getfeedToken()
            log.info("✅ Logged in to Angel SmartAPI")
            return True
        except Exception as e:
            log.exception("SmartAPI login failed — switching to MOCK mode.")
            self.mock = True
            return False

    def place_order(
        self,
        tradingsymbol: str,
        symbol_token: str,
        quantity: int,
        transaction_type: str,
        order_type: str = "MARKET",
        product_type: str = "INTRADAY",
        exchange: str = "NFO",
    ) -> str:
        """
        Places an order and returns the order ID.
        Raises a RuntimeError if the order placement fails.
        """
        if self.mock:
            order_id = str(uuid.uuid4())
            log.info(f"MOCK order placed for {tradingsymbol}. MOCK order ID: {order_id}")
            return order_id

        order_params = {
            "variety": "NORMAL",
            "tradingsymbol": tradingsymbol,
            "symboltoken": symbol_token,
            "transactiontype": transaction_type,
            "exchange": exchange,
            "ordertype": order_type,
            "producttype": product_type,
            "duration": "DAY",
            "quantity": quantity,
        }
        
        order_id = self.connection.placeOrder(order_params)
        
        if order_id:
            log.info(f"Successfully placed order for {tradingsymbol}. Order ID: {order_id}")
            return order_id
        else:
            raise RuntimeError(f"Order placement failed for {tradingsymbol}. No order ID returned.")

    def get_order_book(self) -> list[dict] | None:
        """
        Fetches the entire order book for the day.
        """
        if self.mock:
            log.info("MOCK get_order_book returning empty list.")
            return []
        
        try:
            response = self.connection.orderBook()
            if response and response.get("status"):
                return response.get("data")
            else:
                log.error(f"Failed to fetch order book: {response.get('message', 'Unknown error')}")
                return None
        except Exception as e:
            log.exception("Exception while fetching order book.")
            return None

    def get_order_status(self, order_id: str) -> dict | None:
        """
        Fetches the status and details of a specific order by its ID.
        Returns the order details dictionary if found, otherwise None.
        """
        if self.mock:
            log.info(f"MOCK get_order_status for {order_id}")
            return {"status": "complete", "averageprice": 100.0, "filledshares": 75, "orderid": order_id}

        order_book = self.get_order_book()
        if order_book:
            for order in order_book:
                if order.get("orderid") == order_id:
                    return order
        return None

    def get_open_positions(self) -> list[dict] | None:
        """
        Fetches all open positions.
        """
        if self.mock:
            log.info("MOCK get_open_positions returning empty list.")
            return []
        
        try:
            response = self.connection.position()
            log.info(f"Open positions response: {response}")
            if response and response.get("status"):
                return response.get("data")
            else:
                log.error(f"Failed to fetch open positions: {response.get('message', 'Unknown error')}")
                return None
        except Exception as e:
            log.exception("Exception while fetching open positions.")
            return None

    def get_trade_book(self) -> list[dict] | None:
        """
        Fetches the trade book for the day.
        """
        if self.mock:
            log.info("MOCK get_trade_book returning empty list.")
            return []
        
        try:
            response = self.connection.tradeBook()
            if response and response.get("status"):
                return response.get("data")
            else:
                log.error(f"Failed to fetch trade book: {response.get('message', 'Unknown error')}")
                return None
        except Exception as e:
            log.exception("Exception while fetching trade book.")
            return None

    def get_ltp(self, exchange: str, tradingsymbol: str, symboltoken: str | int) -> float:
        """
        Fetch LTP using SmartAPI ltpData for a single instrument.
        Returns the last traded price as float.
        """
        if self.mock or self.connection is None:
            raise RuntimeError("AngelAPI is in MOCK mode or not logged in; cannot fetch real LTP.")

        resp = self.connection.ltpData(
            exchange=exchange,
            tradingsymbol=tradingsymbol,
            symboltoken=str(symboltoken),
        )
        data = resp.get("data") or {}
        ltp = data.get("ltp")
        if ltp is None:
            raise RuntimeError(f"No LTP in response for {tradingsymbol} / {symboltoken}: {resp}")
        return float(ltp)

if __name__ == "__main__":
    api = AngelAPI()
    api.login()
    if not api.mock:
        try:
            positions = api.get_open_positions()
            print("Open Positions:", positions)
            
            order_book = api.get_order_book()
            print("Order Book:", order_book)

            trade_book = api.get_trade_book()
            print("Trade Book:", trade_book)

        except Exception as e:
            print(e)
    else:
        print("Running in MOCK mode. Cannot place real orders.")
