"""
Backtester for ITM Option Momentum Breakdown Strategy.

Logic:
 - Time Window: 9:15 to 10:00
 - Put Setup: 3 consecutive Green candles -> Entry if Spot breaks 1st candle Low & ITM PE breaks 1st candle High.
 - Call Setup: 3 consecutive Red candles -> Entry if Spot breaks 1st candle High & ITM CE breaks 1st candle High.
 - Risk: Target +15 pts, SL -15 pts.

Usage:
 python -m src.backtest.itm_momentum_bt --start 2024-01-01 --end 2024-01-10 --index NIFTY
"""

import argparse
import logging
import pandas as pd
import time
from datetime import datetime, date, timedelta, time as dt_time
from colorama import Fore, Style, init as colorama_init

from src.api.smartapi_client import AngelAPI
from src.market.contracts import find_option, get_next_expiry

# Initialize colorama
colorama_init(autoreset=True)

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(message)s', datefmt='%H:%M:%S')

class ITMMomentumBacktest:
    INDEX_CONFIG = {
        "NIFTY": {"token": "99926000", "exchange": "NSE", "options_exchange": "NFO", "strike_step": 50},
        "SENSEX": {"token": "99919000", "exchange": "BSE", "options_exchange": "BFO", "strike_step": 100}
    }

    def __init__(self, index_name="NIFTY"):
        self.index_name = index_name.upper()
        if self.index_name not in self.INDEX_CONFIG:
            raise ValueError(f"Invalid index '{self.index_name}'")
        
        config = self.INDEX_CONFIG[self.index_name]
        self.index_token = config["token"]
        self.index_exchange = config["exchange"]
        self.strike_step = config["strike_step"]
        
        self.api = AngelAPI()
        self.api.login()
        time.sleep(1)
        
        self.results = []

    def _get_itm_strike(self, spot: float, option_type: str) -> int:
        atm = round(spot / self.strike_step) * self.strike_step
        if option_type == "CE":
            return atm - self.strike_step
        else:
            return atm + self.strike_step

    def _fetch_candles(self, token, exchange, date_obj, start_time_str="09:15", end_time_str="15:30"):
        time.sleep(1)
        from_time = f"{date_obj.strftime('%Y-%m-%d')} {start_time_str}"
        to_time = f"{date_obj.strftime('%Y-%m-%d')} {end_time_str}"
        
        try:
            data = self.api.connection.getCandleData({
                "exchange": exchange, "symboltoken": token, "interval": "ONE_MINUTE",
                "fromdate": from_time, "todate": to_time
            })
            candles = data.get("data", [])
            if not candles: return pd.DataFrame()
            
            df = pd.DataFrame(candles, columns=["ts", "open", "high", "low", "close", "volume"])
            df["ts"] = pd.to_datetime(df["ts"]).dt.tz_localize(None)
            df[["open", "high", "low", "close"]] = df[["open", "high", "low", "close"]].astype(float)
            df.set_index("ts", inplace=True)
            return df
        except Exception as e:
            log.error(f"Error fetching candles: {e}")
            return pd.DataFrame()

    def run(self, start_date: date, end_date: date):
        current_date = start_date
        while current_date <= end_date:
            if current_date.weekday() < 5: # Skip weekends
                log.info(f"{Fore.CYAN}Processing {current_date}...{Style.RESET_ALL}")
                self._process_day(current_date)
            current_date += timedelta(days=1)
        
        self._save_results()

    def _process_day(self, trading_date: date):
        # 1. Fetch Spot Data
        spot_df = self._fetch_candles(self.index_token, self.index_exchange, trading_date, "09:00", "15:30")
        if spot_df.empty:
            log.warning(f"No spot data for {trading_date}")
            return

        try:
            expiry = get_next_expiry(self.index_name, trading_date)
        except Exception as e:
            log.warning(f"Skipping {trading_date}: {e}")
            return

        active_pe_setup = None
        active_ce_setup = None
        last_exit_time = None
        
        # Iterate minutes from 09:18 to 10:00 for setup detection
        # We need at least 3 previous candles (15, 16, 17) to check at 18
        start_time = datetime.combine(trading_date, dt_time(9, 18))
        end_scan_time = datetime.combine(trading_date, dt_time(10, 0))
        
        # Get all timestamps in the scan window
        scan_times = [t for t in spot_df.index if start_time <= t <= end_scan_time]
        
        for current_ts in scan_times:
            # Skip scanning if we are currently in a trade
            if last_exit_time and current_ts <= last_exit_time:
                log.info(f"Skipping {current_ts.time()} (Trade active/just finished until {last_exit_time.time()})")
                continue

            # --- 1. Check for Pattern (using previous 3 candles) ---
            # Candles at t-3, t-2, t-1 (relative to current minute start)
            # Actually, if current_ts is 09:18, it represents the candle STARTING at 09:18? 
            # Angel API returns timestamp as start of candle.
            # So at 09:18, we have completed candles for 09:15, 09:16, 09:17.
            
            c3_ts = current_ts - timedelta(minutes=1)
            c2_ts = current_ts - timedelta(minutes=2)
            c1_ts = current_ts - timedelta(minutes=3)
            c0_ts = current_ts - timedelta(minutes=4)

            if active_pe_setup:
                log.info(f"Scan {current_ts.time()} | Active PE Setup pending trigger...")
            if active_ce_setup:
                log.info(f"Scan {current_ts.time()} | Active CE Setup pending trigger...")

            # Always scan for patterns (Independent scanning for CE/PE)
            if c1_ts in spot_df.index and c2_ts in spot_df.index and c3_ts in spot_df.index:
                c1 = spot_df.loc[c1_ts]
                c2 = spot_df.loc[c2_ts]
                c3 = spot_df.loc[c3_ts]
                c0 = spot_df.loc[c0_ts] if c0_ts in spot_df.index else None

                def get_type(c): return 'G' if c.close > c.open else 'R'
                def fmt_c(c): return f"[{get_type(c)} O:{c.open:.2f} C:{c.close:.2f}]"

                if last_exit_time and c1_ts <= last_exit_time:
                    log.info(f"Scan {current_ts.time()} | Ignored (Overlap with prev trade exit {last_exit_time.time()}) | Candles: {c1_ts.time()}...")
                    continue

                log.info(f"Scan {current_ts.time()} | Candles: {c1_ts.time()}{fmt_c(c1)} {c2_ts.time()}{fmt_c(c2)} {c3_ts.time()}{fmt_c(c3)}")

                # Put Setup: 3 Green
                if c1.close > c1.open and c2.close > c2.open and c3.close > c3.open:
                    is_continuation = False
                    if active_pe_setup:
                        # If previous candle was also Green, it's a continuation. Keep original setup.
                        if c0 is not None and c0.close > c0.open:
                            is_continuation = True
                            log.info(f"  Continuation of Green leg. Ignoring update.")

                    if not is_continuation:
                        strike = self._get_itm_strike(c3.close, "PE")
                        try:
                            contract = find_option(self.index_name, strike, "PE", expiry, trading_date)
                            # Fetch Option Candle for Ref Time (C1)
                            opt_df = self._fetch_candles(contract.token, contract.exchange, trading_date,
                                                         c1_ts.strftime("%H:%M"), (c1_ts + timedelta(minutes=1)).strftime("%H:%M"))
                            if not opt_df.empty:
                                opt_high = opt_df.iloc[0]['high']
                                active_pe_setup = {
                                    "type": "PE", "contract": contract,
                                    "spot_trigger": c1.low, "opt_trigger": opt_high,
                                    "setup_time": current_ts
                                }
                                log.info(f"  [PE Setup] {current_ts.time()} | Spot < {c1.low} | Opt {contract.symbol} > {opt_high}")
                        except Exception as e:
                            log.warning(f"  Could not setup PE: {e}")

                # Call Setup: 3 Red
                elif c1.close < c1.open and c2.close < c2.open and c3.close < c3.open:
                    is_continuation = False
                    if active_ce_setup:
                        # If previous candle was also Red, it's a continuation. Keep original setup.
                        if c0 is not None and c0.close < c0.open:
                            is_continuation = True
                            log.info(f"  Continuation of Red leg. Ignoring update.")

                    if not is_continuation:
                        strike = self._get_itm_strike(c3.close, "CE")
                        try:
                            contract = find_option(self.index_name, strike, "CE", expiry, trading_date)
                            opt_df = self._fetch_candles(contract.token, contract.exchange, trading_date,
                                                         c1_ts.strftime("%H:%M"), (c1_ts + timedelta(minutes=1)).strftime("%H:%M"))
                            if not opt_df.empty:
                                opt_high = opt_df.iloc[0]['high']
                                active_ce_setup = {
                                    "type": "CE", "contract": contract,
                                    "spot_trigger": c1.high, "opt_trigger": opt_high,
                                    "setup_time": current_ts
                                }
                                log.info(f"  [CE Setup] {current_ts.time()} | Spot > {c1.high} | Opt {contract.symbol} > {opt_high}")
                        except Exception as e:
                            log.warning(f"  Could not setup CE: {e}")

            # --- 2. Check Triggers (Check both independently) ---
            setups_to_check = []
            if active_pe_setup: setups_to_check.append(active_pe_setup)
            if active_ce_setup: setups_to_check.append(active_ce_setup)

            for setup in setups_to_check:
                # Check current minute candle for trigger
                spot_candle = spot_df.loc[current_ts]
                
                spot_triggered = False
                if setup['type'] == "PE":
                    if spot_candle.low < setup['spot_trigger']: spot_triggered = True
                else:
                    if spot_candle.high > setup['spot_trigger']: spot_triggered = True
                
                if spot_triggered:
                    # Fetch Option Candle for current minute to confirm
                    contract = setup['contract']
                    opt_candle_df = self._fetch_candles(contract.token, contract.exchange, trading_date, 
                                                        current_ts.strftime("%H:%M"), (current_ts + timedelta(minutes=1)).strftime("%H:%M"))
                    
                    if not opt_candle_df.empty:
                        opt_candle = opt_candle_df.iloc[0]
                        if opt_candle.high > setup['opt_trigger']:
                            # ENTRY CONFIRMED
                            entry_price = max(opt_candle.open, setup['opt_trigger'])
                            log.info(f"{Fore.GREEN}  >>> ENTRY {setup['type']} {contract.symbol} @ {entry_price} (Time: {current_ts.time()}){Style.RESET_ALL}")
                            
                            exit_time = self._simulate_trade(contract, entry_price, current_ts, trading_date)
                            last_exit_time = exit_time
                            
                            # Reset ALL setups on entry
                            active_pe_setup = None
                            active_ce_setup = None
                            log.info(f"  Trade Exited at {exit_time.time()}. Resetting setup.")
                            break # Stop checking other setups for this minute
                        else:
                            log.info(f"  Spot triggered but Opt High {opt_candle.high} <= Trigger {setup['opt_trigger']}")
                else:
                    log.info(f"  Spot Trigger Fail: {spot_candle.low if setup['type']=='PE' else spot_candle.high} vs {setup['spot_trigger']}")

    def _simulate_trade(self, contract, entry_price, entry_time, trading_date):
        sl = entry_price - 10
        target = entry_price + 15
        
        # Fetch remaining data for the day for this option
        full_opt_df = self._fetch_candles(contract.token, contract.exchange, trading_date, 
                                          entry_time.strftime("%H:%M"), "15:30")
        
        # Filter for candles strictly after entry time
        rest_of_day_df = full_opt_df[full_opt_df.index > entry_time]

        # Calculate Max Potential Loss (EOD)
        if not rest_of_day_df.empty:
            max_potential_loss = rest_of_day_df['low'].min() - entry_price
        else:
            max_potential_loss = 0.0

        exit_price = 0.0
        exit_reason = "EOD"
        exit_time = None
        
        highest_price_during_trade = entry_price
        max_potential_profit = 0.0
        
        # Iterate subsequent candles
        for ts, row in rest_of_day_df.iterrows():
            # Update highest price seen so far (for SL/EOD cases)
            if row.high > highest_price_during_trade:
                highest_price_during_trade = row.high

            # 1. Check Exit against SL (Conservative: Check Low first)
            if row.low <= sl:
                exit_price = sl
                exit_reason = "SL"
                exit_time = ts
                max_potential_profit = highest_price_during_trade - entry_price
                break
            
            # 2. Check Target
            if row.high >= target:
                exit_price = target
                exit_reason = "TARGET"
                exit_time = ts
                
                # Calculate Max Potential Profit (Run-up after target before retrace)
                peak_price = row.high
                
                # If this candle closed below target, we assume it retraced immediately
                if row.close <= target:
                    max_potential_profit = peak_price - entry_price
                else:
                    # Look ahead for peak before price drops back to target
                    future_candles = rest_of_day_df[rest_of_day_df.index > ts]
                    for _, f_row in future_candles.iterrows():
                        if f_row.high > peak_price:
                            peak_price = f_row.high
                        
                        if f_row.low <= target:
                            # Price dropped back to target level
                            break
                    max_potential_profit = peak_price - entry_price
                break
        
        if exit_time is None:
            # EOD Exit
            if not rest_of_day_df.empty:
                exit_price = rest_of_day_df.iloc[-1].close
                exit_time = rest_of_day_df.index[-1]
            else:
                exit_price = entry_price
                exit_time = entry_time
            max_potential_profit = highest_price_during_trade - entry_price

        pnl = (exit_price - entry_price)
        color = Fore.GREEN if pnl > 0 else Fore.RED
        log.info(f"{color}  <<< EXIT {contract.symbol} @ {exit_price} ({exit_reason}) | PnL: {pnl:.2f} | MaxPotProfit: {max_potential_profit:.2f} | MaxPotLoss: {max_potential_loss:.2f}{Style.RESET_ALL}")
        
        self.results.append({
            "date": trading_date,
            "type": contract.option_type,
            "symbol": contract.symbol,
            "entry_time": entry_time.time(),
            "exit_time": exit_time.time(),
            "entry_price": entry_price,
            "exit_price": exit_price,
            "pnl": pnl,
            "max_potential_profit": max_potential_profit,
            "max_potential_loss": max_potential_loss,
            "reason": exit_reason
        })
        return exit_time

    def _save_results(self):
        if not self.results:
            log.info("No trades generated.")
            return
            
        df = pd.DataFrame(self.results)
        print("\n" + "="*50)
        print("BACKTEST RESULTS")
        print("="*50)
        print(df)
        
        total_pnl = df['pnl'].sum()
        wins = len(df[df['pnl'] > 0])
        losses = len(df[df['pnl'] <= 0])
        win_rate = (wins / len(df)) * 100 if len(df) > 0 else 0
        
        print("-" * 30)
        print(f"Total Trades: {len(df)}")
        print(f"Total PnL:    {total_pnl:.2f}")
        print(f"Win Rate:     {win_rate:.1f}% ({wins}W / {losses}L)")
        if not df.empty:
            print(f"Avg Max Pot Profit: {df['max_potential_profit'].mean():.2f}")
            print(f"Avg Max Pot Loss:   {df['max_potential_loss'].mean():.2f}")
        print("="*50)
        
        filename = f"backtest_itm_{self.index_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        df.to_csv(filename, index=False)
        log.info(f"Results saved to {filename}")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--index", default="NIFTY", help="NIFTY or SENSEX")
    parser.add_argument("--start", required=True, help="Start Date YYYY-MM-DD")
    parser.add_argument("--end", required=True, help="End Date YYYY-MM-DD")
    args = parser.parse_args()

    try:
        start_dt = datetime.strptime(args.start, "%Y-%m-%d").date()
        end_dt = datetime.strptime(args.end, "%Y-%m-%d").date()
    except ValueError:
        print("Invalid date format. Use YYYY-MM-DD")
        return

    bt = ITMMomentumBacktest(index_name=args.index)
    bt.run(start_dt, end_dt)

if __name__ == "__main__":
    main()
