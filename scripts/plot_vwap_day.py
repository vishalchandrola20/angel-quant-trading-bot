# scripts/plot_vwap_day.py
import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path
import sys


def plot_vwap_for_day(trading_date: str):
    csv_path = Path("data/processed") / f"vwap_backtest_{trading_date}.csv"
    if not csv_path.exists():
        print(f"CSV not found: {csv_path}")
        sys.exit(1)

    df = pd.read_csv(csv_path, parse_dates=["ts"])

    fig, ax = plt.subplots(figsize=(12, 6))

    ax.plot(df["ts"], df["sum_price"], label="CE+PE Sum")
    ax.plot(df["ts"], df["vwap"], label="VWAP (Sum)", linestyle="--")

    # Mark entry / exit bars
    entries = df[df["entry_flag"] == 1]
    exits = df[df["exit_flag"] == 1]

    ax.scatter(entries["ts"], entries["sum_price"], marker="^", s=80, label="Entry", zorder=3)
    ax.scatter(exits["ts"], exits["sum_price"], marker="v", s=80, label="Exit", zorder=3)

    ax.set_title(f"VWAP vs CE+PE Sum - {trading_date}")
    ax.set_xlabel("Time")
    ax.set_ylabel("Price")
    ax.grid(True)
    ax.legend()

    fig.autofmt_xdate()
    plt.tight_layout()
    plt.show()


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python scripts/plot_vwap_day.py YYYY-MM-DD")
        sys.exit(1)

    plot_vwap_for_day(sys.argv[1])
