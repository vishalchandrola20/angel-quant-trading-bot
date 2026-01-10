import argparse
import pandas as pd
from pathlib import Path
import logging
import yaml
from colorama import Fore, Style, init as colorama_init

# Initialize colorama
colorama_init(autoreset=True)

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(message)s')

def load_config(index_name):
    """Loads lot size configuration from yaml."""
    config_path = Path("config/itm_momentum.yaml")
    # Defaults
    lot_size = 65 if index_name == "NIFTY" else 20
    num_lots = 1
    
    if config_path.exists():
        try:
            with open(config_path, "r") as f:
                config = yaml.safe_load(f)
            idx_config = config.get(index_name, {})
            lot_size = idx_config.get("lot_size", lot_size)
            num_lots = idx_config.get("num_lots", num_lots)
        except Exception as e:
            log.error(f"Error loading config: {e}")
            
    return lot_size, num_lots

def analyze_results(directory, index_name):
    dir_path = Path(directory)
    if not dir_path.exists():
        log.error(f"Directory not found: {dir_path}")
        return

    # Pattern matches files like trades_NIFTY_2024-01-01.csv
    pattern = f"trades_{index_name}_*.csv"
    files = list(dir_path.glob(pattern))
    
    if not files:
        log.warning(f"No files found matching '{pattern}' in {dir_path}")
        return

    log.info(f"Found {len(files)} files for {index_name}. Processing...")

    all_trades = []
    for f in files:
        try:
            df = pd.read_csv(f)
            if not df.empty:
                # Ensure date column is datetime for sorting
                if 'date' in df.columns:
                    df['date'] = pd.to_datetime(df['date'])
                all_trades.append(df)
        except Exception as e:
            log.error(f"Error reading {f}: {e}")

    if not all_trades:
        log.info("No trades found in files.")
        return

    full_df = pd.concat(all_trades, ignore_index=True)
    if 'date' in full_df.columns:
        full_df = full_df.sort_values('date')

    # Load lot size info for PnL calculation
    lot_size, num_lots = load_config(index_name)
    quantity = lot_size * num_lots

    # --- Calculate Metrics ---
    daily_stats = []
    
    # Group by date
    if 'date' in full_df.columns:
        grouped = full_df.groupby(full_df['date'].dt.date)
    else:
        # Fallback if date column missing (unlikely based on backtester code)
        log.error("Date column missing in CSV data.")
        return

    for date_val, group in grouped:
        pnl_points = group['pnl'].sum()
        trades = len(group)
        wins = len(group[group['pnl'] > 0])
        losses = len(group[group['pnl'] <= 0])
        
        gross_pnl = pnl_points * quantity
        brokerage = trades * 40 # Approx 20 buy + 20 sell
        net_pnl = gross_pnl - brokerage
        
        avg_max_pot_profit = group['max_potential_profit'].mean() if 'max_potential_profit' in group else 0.0
        avg_max_pot_loss = group['max_potential_loss'].mean() if 'max_potential_loss' in group else 0.0

        daily_stats.append({
            "Date": date_val,
            "PnL(Pts)": pnl_points,
            "Gross PnL": gross_pnl,
            "Brokerage": brokerage,
            "Net PnL": net_pnl,
            "Trades": trades,
            "Wins": wins,
            "Losses": losses,
            "AvgMaxPotProfit": avg_max_pot_profit,
            "AvgMaxPotLoss": avg_max_pot_loss
        })

    summary_df = pd.DataFrame(daily_stats)

    print("\n" + "="*100)
    print(f"BACKTEST ANALYSIS REPORT: {index_name}")
    print(f"Source: {dir_path}")
    print("="*100)
    print(summary_df.to_string(index=False, float_format="%.2f"))

    # --- Hourly Analysis ---
    print("\n" + "="*100)
    print("HOURLY PERFORMANCE BREAKDOWN (9:15 Start)")
    print("="*100)

    if 'entry_time' in full_df.columns:
        # Convert entry_time string to datetime objects to perform time arithmetic
        # (Date defaults to today/1900, which is fine as we only need relative time)
        full_df['temp_ts'] = pd.to_datetime(full_df['entry_time'], format='%H:%M:%S')
        
        # Shift time back by 15 minutes so that:
        # 09:15 -> 09:00 (Hour 9)
        # 10:14 -> 09:59 (Hour 9)
        # 10:15 -> 10:00 (Hour 10)
        full_df['shifted_ts'] = full_df['temp_ts'] - pd.Timedelta(minutes=15)
        full_df['hour_bucket'] = full_df['shifted_ts'].dt.hour
        
        hourly_stats = []
        # Group by the hour bucket (automatically sorts by hour)
        hourly_grouped = full_df.groupby('hour_bucket')
        
        for hour, group in hourly_grouped:
            # Create readable label: e.g., Hour 9 -> "09:15 - 10:15"
            start_str = f"{hour:02d}:15"
            end_str = f"{hour+1:02d}:15"
            label = f"{start_str} - {end_str}"
            
            pnl_points = group['pnl'].sum()
            trades = len(group)
            wins = len(group[group['pnl'] > 0])
            losses = len(group[group['pnl'] <= 0])
            win_rate = (wins / trades * 100) if trades > 0 else 0
            
            gross_pnl = pnl_points * quantity
            brokerage = trades * 40
            net_pnl = gross_pnl - brokerage
            
            hourly_stats.append({
                "Time Slot": label,
                "PnL(Pts)": pnl_points,
                "Gross PnL": gross_pnl,
                "Net PnL": net_pnl,
                "Trades": trades,
                "Win Rate": f"{win_rate:.1f}%",
                "Wins": wins,
                "Losses": losses
            })
            
        hourly_df = pd.DataFrame(hourly_stats)
        if not hourly_df.empty:
            print(hourly_df.to_string(index=False, float_format="%.2f"))
        else:
            print("No hourly data available.")
    else:
        print("entry_time column missing, skipping hourly analysis.")

    # Overall Totals
    total_pnl_points = summary_df['PnL(Pts)'].sum()
    total_gross = summary_df['Gross PnL'].sum()
    total_brokerage = summary_df['Brokerage'].sum()
    total_net = summary_df['Net PnL'].sum()
    
    total_trades = summary_df['Trades'].sum()
    total_wins = summary_df['Wins'].sum()
    total_losses = summary_df['Losses'].sum()
    win_rate = (total_wins / total_trades * 100) if total_trades > 0 else 0

    print("-" * 100)
    print(f"OVERALL TOTALS")
    print(f"Total PnL (Pts): {total_pnl_points:.2f}")
    print(f"Gross PnL:       {Fore.GREEN if total_gross > 0 else Fore.RED}{total_gross:.2f}{Style.RESET_ALL}")
    print(f"Total Brokerage: {total_brokerage:.2f}")
    print(f"Net PnL:         {Fore.GREEN if total_net > 0 else Fore.RED}{total_net:.2f}{Style.RESET_ALL}")
    print(f"Total Trades:    {total_trades}")
    print(f"Win Rate:        {win_rate:.1f}% ({total_wins}W / {total_losses}L)")
    print("="*100)

def main():
    parser = argparse.ArgumentParser(description="Analyze existing backtest CSV results.")
    parser.add_argument("--dir", default="data/backtest/momentum_buying", help="Directory containing trade CSVs")
    parser.add_argument("--index", default="NIFTY", help="Index name (NIFTY/SENSEX) to filter files and determine lot size")
    
    args = parser.parse_args()
    analyze_results(args.dir, args.index.upper())

if __name__ == "__main__":
    main()