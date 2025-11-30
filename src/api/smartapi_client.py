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

    # Example wrapper for future SmartAPI calls
    def get_option_chain(self, symbol="NIFTY"):
        """
        Try to fetch option chain via SmartAPI wrapper.
        NOTE: The exact method/endpoint depends on SmartAPI SDK version.
        If the SDK has a specific helper method, replace the call below.
        If running in mock mode, returns sample data.
        """
        if self.mock:
            log.info("Returning MOCK option chain data.")
            return self._mock_option_chain(symbol)

        # Replace the following with the correct SmartAPI method if available.
        # Many SDKs expose a method to fetch option chains; if not, you can
        # use connection.getData or connection.get('...') to call REST endpoints.
        try:
            # example: if SDK exposes get_option_chain - this is a placeholder
            if hasattr(self.connection, "getOptionChain"):
                return self.connection.getOptionChain({"symbol": symbol})
            # fallback: try a generic 'get_option_chain' name
            if hasattr(self.connection, "get_option_chain"):
                return self.connection.get_option_chain(symbol)
            # If neither exists, raise to go to fallback message below.
            raise AttributeError("SmartAPI client has no get_option_chain method")
        except Exception as e:
            log.exception("Could not get option chain from SmartAPI. Returning MOCK data.")
            self.mock = True
            return self._mock_option_chain(symbol)

    @staticmethod
    def _mock_option_chain(symbol):
        """
        Return a tiny example option chain as a list of dicts.
        This is intentionally small — used only so other code can run locally.
        """
        return [
            {
                "strikePrice": 22000,
                "expiryDate": "2025-11-27",
                "CE": {"lastPrice": 120.5, "openInterest": 25000, "impliedVolatility": 14.2},
                "PE": {"lastPrice": 110.0, "openInterest": 27000, "impliedVolatility": 13.8},
            },
            {
                "strikePrice": 22100,
                "expiryDate": "2025-11-27",
                "CE": {"lastPrice": 95.75, "openInterest": 18000, "impliedVolatility": 14.0},
                "PE": {"lastPrice": 130.25, "openInterest": 22000, "impliedVolatility": 14.5},
            },
        ]

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
        # Expected format (simplified):
        # {"status": True, "data": {"ltp": 123.45, ...}, ...}
        data = resp.get("data") or {}
        ltp = data.get("ltp")
        if ltp is None:
            raise RuntimeError(f"No LTP in response for {tradingsymbol} / {symboltoken}: {resp}")
        return float(ltp)





if __name__ == "__main__":
    api = AngelAPI()
    api.login()
    oc = api.get_option_chain("NIFTY")
    print(oc)
