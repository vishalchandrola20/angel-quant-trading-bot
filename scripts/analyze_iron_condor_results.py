from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
from colorama import Fore, Style, init as colorama_init

# Initialize colorama
colorama_init(autoreset=True)

CONDOR_DIR = Path("data/processed/iron_condor")
OUT_PATH = Path("data/processed/iron_condor_summary.csv")


def analyze_single_day(csv_path: Path) -> dict | None:
    """
    Reads one iron condor CSV and returns a summary dict.
    """
    df = pd.read_csv(csv_path)

    if "ts" not in df.columns:
        raise ValueError(f"'ts' column not found in {csv_path}")

    df["ts"] = pd.to_datetime(df["ts"])
    trading_date = df["ts"].dt.date.iloc[0]

    if "entry_flag" not in df.columns or "exit_flag" not in df.columns:
        raise ValueError(f"'entry_flag' or 'exit_flag' missing in {csv_path}")

    entry_rows = df[df["entry_flag"] == 1]
    if entry_rows.empty:
        print(f"[{trading_date}] No entry found in {csv_path.name}, skipping.")
        return None

    entry_row = entry_rows.iloc[0]
    entry_idx = entry_row.name
    entry_time = entry_row["ts"]
    entry_net_credit = float(entry_row["net_credit_close"])

    exit_rows = df[df["exit_flag"] == 1]
    if exit_rows.empty:
        exit_row = df.iloc[-1]
        exit_reason = "NO_FLAG_EOD"
    else:
        exit_row = exit_rows.iloc[0]
        exit_reason = exit_row.get("reason", "UNKNOWN")

    exit_idx = exit_row.name
    exit_time = exit_row["ts"]
    exit_net_credit = float(exit_row["net_credit_close"])

    # For a short credit spread, profit is entry credit - exit credit
    total_pnl = entry_net_credit - exit_net_credit

    # --- In-Trade P&L ---
    trade_slice = df.loc[entry_idx:exit_idx].copy()
    trade_slice["pnl_bar"] = entry_net_credit - trade_slice["net_credit_close"]
    
    max_profit_trade_row = trade_slice.loc[trade_slice["pnl_bar"].idxmax()]
    max_profit_trade_time = max_profit_trade_row["ts"]
    max_profit_trade = max_profit_trade_row["pnl_bar"]
    max_loss_trade = trade_slice["pnl_bar"].min()

    # --- Full-Day P&L (relative to entry) ---
    df["pnl_bar_day"] = entry_net_credit - df["net_credit_close"]
    
    max_profit_day_row = df.loc[df["pnl_bar_day"].idxmax()]
    max_profit_day_time = max_profit_day_row["ts"]
    max_profit_day = max_profit_day_row["pnl_bar_day"]

    max_loss_day_row = df.loc[df["pnl_bar_day"].idxmin()]
    max_loss_day_time = max_loss_day_row["ts"]
    max_loss_day = max_loss_day_row["pnl_bar_day"]

    summary = {
        "date": trading_date,
        "entry_time": entry_time,
        "exit_time": exit_time,
        "exit_reason": exit_reason,
        "total_pnl": total_pnl,
        "max_profit_during_trade": max_profit_trade,
        "max_profit_time_during_trade": max_profit_trade_time,
        "max_loss_during_trade": max_loss_trade,
        "max_profit_during_day": max_profit_day,
        "max_profit_time_during_day": max_profit_day_time,
        "max_loss_during_day": max_loss_day,
        "max_loss_time_during_day": max_loss_day_time,
    }

    return summary


def main():
    if not CONDOR_DIR.exists():
        print(f"Iron Condor folder not found: {CONDOR_DIR}")
        sys.exit(1)

    csv_files = sorted(CONDOR_DIR.glob("*.csv"))
    if not csv_files:
        print(f"No CSV files found in {CONDOR_DIR}")
        sys.exit(0)

    print(f"Found {len(csv_files)} Iron Condor CSVs under {CONDOR_DIR}")

    summaries = []
    for csv_path in csv_files:
        try:
            summary = analyze_single_day(csv_path)
            if summary is not None:
                summaries.append(summary)
        except Exception as e:
            print(f"Error processing {csv_path.name}: {e}")

    if not summaries:
        print("No valid trades found in any CSVs.")
        sys.exit(0)

    summary_df = pd.DataFrame(summaries)
    summary_df.sort_values("date", inplace=True)

    # --- Manual Print with Fixed Width and Color ---
    print("\n=== Iron Condor Strategy Summary ===")
    
    header = (
        f"{'date':<12}"
        f"{'pnl':>10}"
        f"{'during_trade':>20}"
        f"{'trade_max_t':>12}"
        f"{'during_day':>20}"
        f"{'day_max_p_t':>12}"
        f"{'day_max_l_t':>12}"
        f"{'entry':>8}"
        f"{'exit':>8}"
        f"{'reason':>15}"
    )
    print(header)
    print("-" * len(header))

    for _, row in summary_df.iterrows():
        pnl_val = row['total_pnl']
        pnl_color = Fore.GREEN if pnl_val >= 0 else Fore.RED
        pnl_str = f"{pnl_color}{pnl_val:+.2f}{Style.RESET_ALL}"

        during_trade_str = f"{row['max_profit_during_trade']:+.2f} / {row['max_loss_during_trade']:+.2f}"
        during_day_str = f"{row['max_profit_during_day']:+.2f} / {row['max_loss_during_day']:+.2f}"

        print(
            f"{str(row['date']):<12}"
            f"{pnl_str:>18}" # Adjusted padding for color codes
            f"{during_trade_str:>20}"
            f"{row['max_profit_time_during_trade'].strftime('%H:%M'):>12}"
            f"{during_day_str:>20}"
            f"{row['max_profit_time_during_day'].strftime('%H:%M'):>12}"
            f"{row['max_loss_time_during_day'].strftime('%H:%M'):>12}"
            f"{row['entry_time'].strftime('%H:%M'):>8}"
            f"{row['exit_time'].strftime('%H:%M'):>8}"
            f"{row['exit_reason']:>15}"
        )

    # --- Print Aggregate Stats ---
    total_pnl_val = summary_df["total_pnl"].sum()
    max_day_profit = summary_df["total_pnl"].max()
    max_day_loss = summary_df["total_pnl"].min()
    win_rate = (summary_df["total_pnl"] > 0).mean() * 100.0

    print("\n=== Aggregate Stats ===")
    pnl_color = Fore.GREEN if total_pnl_val >=0 else Fore.RED
    print(f"Total P&L (per lot): {pnl_color}{total_pnl_val:+.2f}{Style.RESET_ALL}")
    print(f"Max single-day profit: {max_day_profit:.2f}")
    print(f"Max single-day loss: {max_day_loss:.2f}")
    print(f"Win rate: {win_rate:.1f}%")

    # Save the original, unformatted data to CSV
    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    summary_df.to_csv(OUT_PATH, index=False)
    print(f"\nSummary saved to {OUT_PATH}")


if __name__ == "__main__":
    main()
