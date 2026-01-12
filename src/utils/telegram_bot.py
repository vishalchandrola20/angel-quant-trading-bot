import logging
import requests
import threading
import time

log = logging.getLogger(__name__)

class TelegramBot:
    def __init__(self, bot_token: str, chat_id: str, enabled: bool = True):
        self.bot_token = bot_token
        self.chat_id = str(chat_id) # Ensure string for comparison
        self.enabled = enabled
        self.base_url = f"https://api.telegram.org/bot{self.bot_token}/sendMessage"
        self.polling_url = f"https://api.telegram.org/bot{self.bot_token}/getUpdates"
        self.last_update_id = 0
        self.stop_event = threading.Event()
        self.startup_time = int(time.time())

    def send_message(self, message: str):
        if not self.enabled or not self.bot_token or not self.chat_id:
            if not self.enabled:
                log.info("Telegram: Message skipped (Disabled)")
            elif not self.bot_token or not self.chat_id:
                log.warning("Telegram: Message skipped (Missing Credentials)")
            return

        try:
            log.info(f"Telegram: Sending message to {self.chat_id}...")
            payload = {
                "chat_id": self.chat_id,
                "text": message,
                "parse_mode": "HTML"
            }
            # Short timeout to prevent blocking the trading loop
            response = requests.post(self.base_url, json=payload, timeout=3)
            if response.status_code != 200:
                log.error(f"Telegram send failed: {response.text}")
            else:
                log.info("Telegram: Message sent successfully.")
        except Exception as e:
            log.error(f"Telegram error: {e}")

    def _poll(self, callback):
        """Internal polling loop."""
        while not self.stop_event.is_set():
            try:
                params = {"offset": self.last_update_id + 1, "timeout": 10}
                response = requests.get(self.polling_url, params=params, timeout=15)
                
                if response.status_code == 200:
                    data = response.json()
                    if data.get("ok"):
                        for result in data.get("result", []):
                            self.last_update_id = result["update_id"]
                            message = result.get("message", {})
                            
                            # Ignore stale messages sent before bot started
                            msg_date = message.get("date", 0)
                            if msg_date < self.startup_time:
                                continue

                            # Security Check: Verify Sender ID
                            sender_id = str(message.get("chat", {}).get("id", ""))
                            text = message.get("text", "").strip()
                            
                            if sender_id == self.chat_id:
                                callback(text)
                            else:
                                log.warning(f"Ignored command from unauthorized chat_id: {sender_id}")
            except Exception as e:
                log.error(f"Telegram polling error: {e}")
                time.sleep(5) # Wait before retrying on error
            
            time.sleep(0.5)

    def start_listening(self, command_callback):
        """Starts a background thread to listen for commands."""
        if not self.enabled or not self.bot_token:
            return
        
        self.stop_event.clear()
        self.thread = threading.Thread(target=self._poll, args=(command_callback,), daemon=True)
        self.thread.start()
        log.info("Telegram bot listening for commands...")

    def stop_listening(self):
        """Stops the polling thread."""
        self.stop_event.set()