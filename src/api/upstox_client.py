# src/api/upstox_client.py
import logging
import requests
import yaml
from pathlib import Path
import webbrowser

log = logging.getLogger(__name__)

class UpstoxAPI:
    def __init__(self, config_path="config/credentials.yaml"):
        self.api_key = None
        self.api_secret = None
        self.redirect_uri = None
        self.access_token = None
        self.token_file = Path("data/state/upstox_access_token.yaml")

        creds = self._load_credentials(config_path)
        if creds:
            self.api_key = creds.get("api_key")
            self.api_secret = creds.get("api_secret")
            self.redirect_uri = creds.get("redirect_uri")

        if not all([self.api_key, self.api_secret, self.redirect_uri]):
            log.error("Upstox credentials (api_key, api_secret, redirect_uri) not found. Cannot proceed.")
            raise ValueError("Upstox credentials missing.")

    def _load_credentials(self, config_path):
        config_file = Path(config_path)
        if not config_file.exists():
            log.error(f"Credentials file not found at {config_path}")
            return None
        with open(config_file) as f:
            return yaml.safe_load(f).get("upstox")

    def _save_access_token(self):
        self.token_file.parent.mkdir(parents=True, exist_ok=True)
        with open(self.token_file, 'w') as f:
            yaml.dump({"access_token": self.access_token}, f)
        log.info(f"Upstox access token saved to {self.token_file}")

    def _load_access_token(self):
        if self.token_file.exists():
            with open(self.token_file, 'r') as f:
                data = yaml.safe_load(f)
                self.access_token = data.get("access_token")
                log.info("Loaded existing Upstox access token.")
                return True
        return False

    def login(self):
        if self._load_access_token():
            return True

        # --- Interactive login flow ---
        auth_url = (
            f"https://api.upstox.com/v2/login/authorization/dialog?"
            f"response_type=code&client_id={self.api_key}&redirect_uri={self.redirect_uri}"
        )
        print("\n--- Upstox First-Time Login ---")
        print("1. A login URL will be opened in your browser.")
        print("2. Log in to your Upstox account and grant access.")
        print("3. You will be redirected to a blank page. Copy the ENTIRE URL from your browser's address bar.")
        print("4. Paste the full URL here when prompted.")
        input("Press Enter to open the login page...")

        webbrowser.open(auth_url)

        redirected_url = input("\nPaste the full redirected URL here: ")
        try:
            auth_code = redirected_url.split('code=')[1]
        except IndexError:
            log.error("Could not find 'code=' in the provided URL. Please try again.")
            return False

        # --- Exchange auth code for access token ---
        url = "https://api.upstox.com/v2/login/authorization/token"
        headers = {
            'accept': 'application/json',
            'Api-Version': '2.0',
            'Content-Type': 'application/x-www-form-urlencoded'
        }
        data = {
            'code': auth_code,
            'client_id': self.api_key,
            'client_secret': self.api_secret,
            'redirect_uri': self.redirect_uri,
            'grant_type': 'authorization_code'
        }
        response = requests.post(url, headers=headers, data=data)
        if response.status_code == 200:
            self.access_token = response.json().get("access_token")
            self._save_access_token()
            log.info("✅ Successfully obtained and saved Upstox access token.")
            return True
        else:
            log.error(f"Failed to get access token: {response.status_code} - {response.text}")
            return False

    def get_option_chain(self, instrument_key: str, expiry_date: str):
        if not self.access_token:
            log.error("Not logged in. Please run the login flow first.")
            return None

        url = "https://api.upstox.com/v2/market-quote/option-chain"
        headers = {
            'accept': 'application/json',
            'Api-Version': '2.0',
            'Authorization': f'Bearer {self.access_token}'
        }
        params = {'instrument_key': instrument_key, 'expiry_date': expiry_date}

        log.info(f"Fetching Upstox option chain for {instrument_key} on {expiry_date}...")
        response = requests.get(url, headers=headers, params=params)
        if response.status_code == 200:
            return response.json().get("data", [])
        else:
            log.error(f"Failed to fetch option chain: {response.status_code} - {response.text}")
            return None