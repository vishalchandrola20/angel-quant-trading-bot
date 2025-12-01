from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd


STRANGLE_DIR = Path("data/processed/strangle")
OUT_PATH = Path("data/processed/strangle_summary.csv")


def analyze_single_day(csv_path: Path) -> dict | None:
    """
    Reads one strangle CSV and returns a summary dict:
    - date
    - entry_time, exit_time
    - exit_reason
    - pnl_ce, pnl_pe, total_pnl
    - max_profit (best M2M during trade)
    - max_loss (worst M2M during trade)
    If no trade (no entry_flag), returns None.
    """
    df = pd.read_csv(csv_path)

    if "ts" not in df.columns:
        raise ValueError(f"'ts' column not found in {csv_path}")

    # parse timestamps
    df["ts"] = pd.to_datetime(df["ts"])

    # determine trading date as date of first bar
    trading_date = df["ts"].dt.date.iloc[0]

    # find entry & exit rows
    if "entry_flag" not in df.columns or "exit_flag" not in df.columns:
        raise ValueError(f"'entry_flag' or 'exit_flag' missing in {csv_path}")

    entry_rows = df[df["entry_flag"] == 1]
    if entry_rows.empty:
        # no trade that day
        print(f"[{trading_date}] No entry found in {csv_path.name}, skipping.")
        return None

    entry_row = entry_rows.iloc[0]
    entry_idx = entry_row.name
    entry_time = entry_row["ts"]
    entry_ce = float(entry_row["ce_close"])
    entry_pe = float(entry_row["pe_close"])

    exit_rows = df[df["exit_flag"] == 1]
    if exit_rows.empty:
        # if no explicit exit_flag, assume exit at last bar
        exit_row = df.iloc[-1]
        exit_reason = "NO_FLAG_EOD"
    else:
        exit_row = exit_rows.iloc[0]
        exit_reason = exit_row.get("reason", "UNKNOWN")

    exit_idx = exit_row.name
    exit_time = exit_row["ts"]
    exit_ce = float(exit_row["ce_close"])
    exit_pe = float(exit_row["pe_close"])

    # realized P&L for short CE & PE (entry - exit)
    pnl_ce = entry_ce - exit_ce
    pnl_pe = entry_pe - exit_pe
    total_pnl = pnl_ce + pnl_pe

    # intraday M2M from entry to exit
    trade_slice = df.loc[entry_idx:exit_idx].copy()

    # if for some reason ce_close/pe_close missing
    if "ce_close" not in trade_slice.columns or "pe_close" not in trade_slice.columns:
        raise ValueError(f"'ce_close' or 'pe_close' missing in {csv_path}")

    trade_slice["ce_close"] = trade_slice["ce_close"].astype(float)
    trade_slice["pe_close"] = trade_slice["pe_close"].astype(float)

    # mark-to-market per bar for the combined short strangle
    # P&L per bar = (entry_ce - current_ce) + (entry_pe - current_pe)
    trade_slice["pnl_ce_bar"] = entry_ce - trade_slice["ce_close"]
    trade_slice["pnl_pe_bar"] = entry_pe - trade_slice["pe_close"]
    trade_slice["total_pnl_bar"] = trade_slice["pnl_ce_bar"] + trade_slice["pnl_pe_bar"]

    max_profit = trade_slice["total_pnl_bar"].max()   # best M2M
    max_loss = trade_slice["total_pnl_bar"].min()     # worst M2M (likely negative)

    # optional: infer symbols if present in CSV
    ce_symbol = df.columns[df.columns.str.lower().str.contains("ce_symbol")].tolist()
    pe_symbol = df.columns[df.columns.str.lower().str.contains("pe_symbol")].tolist()

    summary = {
        "date": trading_date,
        "csv_file": csv_path.name,
        "entry_time": entry_time,
        "exit_time": exit_time,
        "exit_reason": exit_reason,
        "entry_ce": entry_ce,
        "entry_pe": entry_pe,
        "exit_ce": exit_ce,
        "exit_pe": exit_pe,
        "pnl_ce": pnl_ce,
        "pnl_pe": pnl_pe,
        "total_pnl": total_pnl,
        "max_profit_during_trade": max_profit,
        "max_loss_during_trade": max_loss,
    }

    return summary


def main():
    if not STRANGLE_DIR.exists():
        print(f"Strangle folder not found: {STRANGLE_DIR}")
        sys.exit(1)

    csv_files = sorted(STRANGLE_DIR.glob("*.csv"))
    if not csv_files:
        print(f"No CSV files found in {STRANGLE_DIR}")
        sys.exit(0)

    print(f"Found {len(csv_files)} strangle CSVs under {STRANGLE_DIR}")

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

    # compute some overall stats
    total_pnl = summary_df["total_pnl"].sum()
    max_day_profit = summary_df["total_pnl"].max()
    max_day_loss = summary_df["total_pnl"].min()
    win_rate = (summary_df["total_pnl"] > 0).mean() * 100.0

    print("\n=== Strangle Strategy Summary ===")
    print(summary_df[["date", "total_pnl", "max_profit_during_trade", "max_loss_during_trade",
                      "entry_time", "exit_time", "exit_reason"]])

    print("\n=== Aggregate Stats ===")
    print(f"Total P&L (per lot): {total_pnl:.2f}")
    print(f"Max single-day profit: {max_day_profit:.2f}")
    print(f"Max single-day loss: {max_day_loss:.2f}")
    print(f"Win rate: {win_rate:.1f}%")

    # Save to CSV
    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    summary_df.to_csv(OUT_PATH, index=False)
    print(f"\nSummary saved to {OUT_PATH}")


if __name__ == "__main__":
    main()
