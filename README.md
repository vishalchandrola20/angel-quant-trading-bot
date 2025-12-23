# 📈 Angel Quant Options Trading Bot

A quantitative options trading bot built using **Angel One SmartAPI**, designed to analyze volatility, backtest strategies, and automate execution in Indian equity derivatives (NIFTY / SENSEX).

---

## 🚀 Project Goals

- Backtest data-driven options strategies
- Automate trades based on volatility + Greeks
- Manage risks with proper position sizing + SL/hedging
- Deploy an algo bot with real-time monitoring

---

## 🧱 Tech Stack

| Component | Tools |
|------------|-------|
| Language | Python 3.10+ |
| Broker API | Angel One SmartAPI |
| Data | NSEpy, Opstra (IV reference) |
| Backtesting | pandas, numpy, custom engine |
| Deployment | Local scheduler / Cloud automation (future) |

---

## 📦 Project Structure
```
angel-quant-bot/
│
├── config/
│   ├── credentials.yaml  # API key + login secrets (ignored from Git)
│   └── settings.yaml     # Strategy config & parameters
│
├── data/
│   ├── raw/              # Raw option chain & OHLC data
│   ├── processed/        # Cleaned data for backtesting
│   └── logs/             # Trading logs & actions
│
├── src/
│   ├── main.py             # Main entry to run the bot
│   ├── api/smartapi_client.py # API wrapper (login + orders + data)
│   ├── strategy/           # Strategies (IV, delta-neutral)
│   ├── backtest/           # Backtesting engine
│   ├── trading/            # Order execution & risk mgmt
│   └── utils/              # Helpers, indicators, logger
│
├── tests/                  # Unit tests (TDD for safety)
│
├── requirements.txt        # Dependencies
├── README.md               # You are here ✅
└── .gitignore              # Keeps repo clean and secure
```


---

## 🛠️ Setup Instructions

### 1️⃣ Clone & enter project folder

```bash
git clone https://github.com/<your_github>/angel-quant-bot.git
cd angel-quant-bot
```
### 2️⃣ Setup virtual environment
```bash
python3 -m venv .venv
source .venv/bin/activate
```
### 3️⃣ Install dependencies
```bash
pip install -r requirements.txt
```
### 4️⃣ Add Angel One Credentials
```bash
Create: config/credentials.yaml
(⚠️ remain private — already in .gitignore)

angel:
  api_key: "YOUR_API_KEY"
  client_id: "YOUR_CLIENT_ID"
  password: "YOUR_PASSWORD"
  totp_secret: "YOUR_TOTP_SECRET"

```

### ▶️ Run Project
```bash
python src/main.py

```

If SmartAPI login works → ✅ setup success!

### 🔒 Security Notes
* Never commit API secrets or TOTP codes
* Use gitignore to protect sensitive files
* Prefer paper trading before going live

### 🤝 Contributing
Pull requests and feature suggestions are welcome.  
Please open an issue before major changes.

### ⚠️ Disclaimer
This project is for educational and research purposes only.  
Trading involves financial risk — use capital cautiously.

Happy Algo Trading! 💹🤖
