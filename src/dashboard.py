import streamlit as st
import pandas as pd
import plotly.express as px
from pathlib import Path

# Page Config
st.set_page_config(page_title="Angel Quant Dashboard", layout="wide", page_icon="📈")

def color_pnl(val):
    try:
        val = float(val)
        color = 'green' if val > 0 else 'red' if val < 0 else ''
        return f'color: {color}'
    except:
        return ''

def calculate_charges(row):
    """Calculates detailed charges for Options Trading (NSE/BSE)."""
    try:
        qty = float(row.get('Quantity', 0))
        buy_price = float(row.get('Entry Price', 0))
        sell_price = float(row.get('Exit Price', 0))
        
        if qty == 0 and buy_price > 0 and sell_price > 0 and row.get('PnL', 0) != 0:
            # Infer quantity for old data
            qty = abs(float(row['PnL']) / (sell_price - buy_price))
        
        if qty == 0: return 0.0

        buy_turnover = buy_price * qty
        sell_turnover = sell_price * qty
        total_turnover = buy_turnover + sell_turnover

        brokerage = 40.0 # Flat 20 buy + 20 sell
        stt = 0.1 / 100 * sell_turnover # 0.1% on Sell
        txn_charges = 0.05 / 100 * total_turnover # Approx 0.05% NSE/BSE
        stamp_duty = 0.003 / 100 * buy_turnover # 0.003% on Buy
        sebi_fees = 0.0001 / 100 * total_turnover # 10 per crore
        gst = 18 / 100 * (brokerage + txn_charges + sebi_fees) # 18% GST

        return brokerage + stt + txn_charges + stamp_duty + sebi_fees + gst
    except:
        return 0.0

def configure_plotly_timeline(fig):
    """Configures x-axis to hide weekends and non-trading hours (15:15 to 9:15)."""
    fig.update_xaxes(
        rangebreaks=[
            dict(bounds=["sat", "mon"]), # Hide weekends
            dict(bounds=[15.25, 9.25], pattern="hour") # Hide 15:15 to 9:15
        ]
    )

st.title("⚡ Angel Quant Trading Dashboard")

# Sidebar
st.sidebar.header("Configuration")
mode = st.sidebar.radio("Select Mode", ["Backtest Analysis", "Live Trading Monitor"])

def load_backtest_data(strategy_name, index_name):
    if strategy_name == "ITM Momentum":
        path = Path("data/backtest/momentum_buying")
        pattern = f"trades_{index_name}_*.csv"
        files = list(path.glob(pattern))
        
        if not files:
            return pd.DataFrame()
        
        dfs = []
        for f in files:
            try:
                df = pd.read_csv(f)
                dfs.append(df)
            except:
                pass
        
        if not dfs:
            return pd.DataFrame()
            
        full_df = pd.concat(dfs, ignore_index=True)
        
        # Ensure pnl is numeric
        if 'pnl' in full_df.columns:
            full_df['pnl'] = pd.to_numeric(full_df['pnl'], errors='coerce').fillna(0.0)
            
        if 'date' in full_df.columns:
            full_df['date'] = pd.to_datetime(full_df['date'])
            full_df = full_df[full_df['date'].dt.dayofweek < 5] # Remove weekends
            if 'entry_time' in full_df.columns:
                # Combine Date and Time for strict chronological sorting
                full_df['timestamp'] = pd.to_datetime(full_df['date'].dt.strftime('%Y-%m-%d') + ' ' + full_df['entry_time'].astype(str))
                full_df = full_df.sort_values(by='timestamp')
            else:
                full_df = full_df.sort_values(by='date')
        
        # Normalize columns for charge calc
        full_df.rename(columns={'entry_price': 'Entry Price', 'exit_price': 'Exit Price', 'pnl': 'PnL', 'quantity': 'Quantity'}, inplace=True)
        
        # Ensure Quantity column exists for backward compatibility
        if 'Quantity' not in full_df.columns:
            full_df['Quantity'] = 65 if index_name == "NIFTY" else 20
            
        return full_df
    return pd.DataFrame()

def load_live_data(sim_mode=False):
    filename = "itm_trades_sim.csv" if sim_mode else "itm_trades.csv"
    path = Path(f"data/live/{filename}")
    if path.exists():
        try:
            return pd.read_csv(path)
        except Exception as e:
            st.error(f"Error reading live data: {e}")
    return pd.DataFrame()

if mode == "Backtest Analysis":
    strategy = st.sidebar.selectbox("Strategy", ["ITM Momentum"])
    index = st.sidebar.selectbox("Index", ["NIFTY", "SENSEX"])
    
    st.header(f"{strategy} - {index} Backtest Results")
    
    df = load_backtest_data(strategy, index)
    
    if not df.empty:
        # Metrics
        # Backtest PnL is in points, convert to Value (Gross PnL)
        df['Quantity'] = pd.to_numeric(df['Quantity'], errors='coerce').fillna(0)
        df['PnL'] = pd.to_numeric(df['PnL'], errors='coerce').fillna(0.0)
        df['Gross PnL'] = df['PnL'] * df['Quantity']
        
        df['Charges'] = df.apply(calculate_charges, axis=1)
        
        total_gross_pnl = df['Gross PnL'].sum()
        total_trades = len(df)
        total_charges = df['Charges'].sum()
        net_pnl = total_gross_pnl - total_charges
        
        wins = len(df[df['Gross PnL'] > 0])
        win_rate = (wins / total_trades * 100) if total_trades > 0 else 0
        
        col1, col2, col3, col4, col5 = st.columns(5)
        col1.metric("Gross PnL", f"{total_gross_pnl:.2f}")
        col2.metric("Net PnL", f"{net_pnl:.2f}", delta=f"-{total_charges:.2f}")
        col3.metric("Total Trades", total_trades)
        col4.metric("Win Rate", f"{win_rate:.1f}%")
        col5.metric("Avg Net PnL", f"{net_pnl/total_trades:.2f}")
        
        # Profit Reach Analysis
        if 'max_potential_profit' in df.columns:
            st.subheader("Profit Reach Analysis")
            r1, r2, r3, r4 = st.columns(4)
            r1.metric("Reached 10+ Pts", int((df['max_potential_profit'] >= 10).sum()))
            r2.metric("Reached 15+ Pts", int((df['max_potential_profit'] >= 15).sum()))
            r3.metric("Reached 20+ Pts", int((df['max_potential_profit'] >= 20).sum()))
            r4.metric("Reached 25+ Pts", int((df['max_potential_profit'] >= 25).sum()))

        # Charts
        st.subheader("Performance Curve")
        df['cumulative_pnl'] = df['Gross PnL'].cumsum()
        x_col = 'timestamp' if 'timestamp' in df.columns else 'date'
        fig = px.line(df, x=x_col, y='cumulative_pnl', title='Cumulative Net PnL Over Time (Gross)', markers=True)
        fig.update_layout(xaxis_title="Date", yaxis_title="Cumulative PnL (Currency)")
        configure_plotly_timeline(fig)
        st.plotly_chart(fig, use_container_width=True)
        
        # --- Analysis Charts ---
        st.markdown("---")
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("Hourly Performance")
            if 'timestamp' in df.columns:
                # Shift back by 15 mins so 9:15-10:15 becomes bucket 9
                df['hour_bucket'] = (df['timestamp'] - pd.Timedelta(minutes=15)).dt.hour
                hourly_pnl = df.groupby('hour_bucket')['Gross PnL'].sum().reset_index()
                
                # Create labels
                hourly_pnl['label'] = hourly_pnl['hour_bucket'].apply(lambda h: f"{h:02d}:15 - {h+1:02d}:15")
                hourly_pnl['color'] = hourly_pnl['Gross PnL'] >= 0
                
                fig_hourly = px.bar(hourly_pnl, x='label', y='Gross PnL', 
                                    title='PnL by Hour (9:15 Start)',
                                    color='color', 
                                    color_discrete_map={True: 'green', False: 'red'})
                fig_hourly.update_layout(showlegend=False, xaxis_title="Time Slot", yaxis_title="Net PnL")
                st.plotly_chart(fig_hourly, use_container_width=True)
            else:
                st.warning("Hourly data not available.")

        with col2:
            st.subheader("Day of Week Performance")
            df['day_name'] = df['date'].dt.day_name()
            days_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday']
            
            daily_pnl = df.groupby('day_name')['Gross PnL'].sum().reset_index()
            daily_pnl['day_name'] = pd.Categorical(daily_pnl['day_name'], categories=days_order, ordered=True)
            daily_pnl = daily_pnl.sort_values('day_name')
            daily_pnl['color'] = daily_pnl['Gross PnL'] >= 0
            
            fig_daily = px.bar(daily_pnl, x='day_name', y='Gross PnL', 
                               title='PnL by Day of Week',
                               color='color', 
                               color_discrete_map={True: 'green', False: 'red'})
            fig_daily.update_layout(showlegend=False, xaxis_title="Day", yaxis_title="Net PnL")
            st.plotly_chart(fig_daily, use_container_width=True)

        # Data
        st.subheader("Trade Log")
        
        # Format for display
        display_df = df.copy()
        column_mapping = {
            'date': 'Date', 'type': 'Type', 'symbol': 'Symbol',
            'setup_time': 'Setup Time', 'entry_time': 'Entry Time',
            'exit_time': 'Exit Time', 
            'reason': 'Reason',
            'max_potential_profit': 'Max Pot Profit',
            'max_potential_loss': 'Max Pot Loss'
        }
        display_df.rename(columns=column_mapping, inplace=True)
        
        # Sort by Date and Entry Time descending
        sort_cols = [c for c in ['Date', 'Setup Time', 'Entry Time'] if c in display_df.columns]
        if sort_cols:
            display_df = display_df.sort_values(sort_cols, ascending=False)
            
        st.dataframe(display_df.style.applymap(color_pnl, subset=['PnL', 'Gross PnL']).format("{:.2f}", subset=['PnL', 'Gross PnL']), use_container_width=True)
    else:
        st.warning("No backtest data found. Run the backtester first.")

elif mode == "Live Trading Monitor":
    st.header("Live Trading Monitor")
    
    data_source = st.radio("Data Source", ["Real Money (Live)", "Paper Trading (Sim)"], horizontal=True)
    sim_mode = (data_source == "Paper Trading (Sim)")
    
    if st.button("Refresh Data"):
        st.rerun()

    df = load_live_data(sim_mode)
    
    if not df.empty:
        # Ensure numeric columns
        cols_to_numeric = ['PnL', 'Entry Price', 'Exit Price']
        for col in cols_to_numeric:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0.0)
        
        # Filter weekends
        if 'Date' in df.columns:
            temp_date = pd.to_datetime(df['Date'])
            df = df[temp_date.dt.dayofweek < 5]

        # Calculate Charges
        df['Charges'] = df.apply(calculate_charges, axis=1)

        # Filters
        index_filter = st.sidebar.multiselect("Filter Index", df['Index'].unique(), default=df['Index'].unique())
        if index_filter:
            df = df[df['Index'].isin(index_filter)]
            
        # Metrics
        total_pnl = df['PnL'].sum()
        total_trades = len(df)
        total_charges = df['Charges'].sum()
        net_pnl = total_pnl - total_charges
        
        wins = len(df[df['PnL'] > 0])
        win_rate = (wins / total_trades * 100) if total_trades > 0 else 0.0
        total_points = (df['Exit Price'] - df['Entry Price']).sum()
        
        today = pd.Timestamp.now().strftime("%Y-%m-%d")
        
        # Ensure Date column is string for comparison
        df['Date'] = df['Date'].astype(str)
        df_today = df[df['Date'] == today]
        today_pnl = df_today['PnL'].sum() if not df_today.empty else 0.0
        
        col1, col2, col3, col4, col5, col6 = st.columns(6)
        col1.metric("Total Live PnL", f"{total_pnl:.2f}")
        col2.metric("Net PnL (w/ Charges)", f"{net_pnl:.2f}", delta=f"-{total_charges}")
        col3.metric("Today's PnL", f"{today_pnl:.2f}", delta=f"{today_pnl:.2f}")
        col4.metric("Total Trades", total_trades)
        col5.metric("Win Rate", f"{win_rate:.1f}%")
        col6.metric("Total Points", f"{total_points:.2f}")
        
        st.subheader("Live Performance Curve")
        try:
            plot_df = df.copy()
            plot_df['Net PnL'] = plot_df['PnL'] - plot_df['Charges']
            # Date is already converted to string above
            plot_df['timestamp'] = pd.to_datetime(plot_df['Date'] + ' ' + plot_df['Exit Time'].astype(str))
            plot_df = plot_df.sort_values('timestamp')
            plot_df['Cumulative Net PnL'] = plot_df['Net PnL'].cumsum()
            
            fig = px.line(plot_df, x='timestamp', y='Cumulative Net PnL', title='Cumulative Net PnL Over Time', markers=True)
            fig.update_layout(xaxis_title="Time", yaxis_title="Net PnL (Currency)")
            configure_plotly_timeline(fig)
            st.plotly_chart(fig, use_container_width=True)
        except Exception as e:
            st.warning(f"Could not generate performance chart: {e}")

        st.subheader("Hourly Performance")
        try:
            hourly_df = df.copy()
            hourly_df['Net PnL'] = hourly_df['PnL'] - hourly_df['Charges']
            # Convert Exit Time to datetime to extract hour
            hourly_df['exit_dt'] = pd.to_datetime(hourly_df['Date'] + ' ' + hourly_df['Exit Time'].astype(str))
            # Shift back by 15 mins so 9:15-10:15 becomes bucket 9
            hourly_df['hour_bucket'] = (hourly_df['exit_dt'] - pd.Timedelta(minutes=15)).dt.hour
            
            hourly_pnl = hourly_df.groupby('hour_bucket')['Net PnL'].sum().reset_index()
            hourly_pnl['label'] = hourly_pnl['hour_bucket'].apply(lambda h: f"{h:02d}:15 - {h+1:02d}:15")
            hourly_pnl['color'] = hourly_pnl['Net PnL'] >= 0
            
            fig_hourly = px.bar(hourly_pnl, x='label', y='Net PnL', title='Net PnL by Hour', color='color', color_discrete_map={True: 'green', False: 'red'})
            st.plotly_chart(fig_hourly, use_container_width=True)
        except Exception as e:
            st.warning(f"Could not generate hourly chart: {e}")

        if 'Max Points' in df.columns:
            # Ensure numeric
            df['Max Points'] = pd.to_numeric(df['Max Points'], errors='coerce').fillna(0.0)
            
            st.subheader("Profit Reach Analysis")
            r1, r2, r3, r4 = st.columns(4)
            r1.metric("Reached 10+ Pts", int((df['Max Points'] >= 10).sum()))
            r2.metric("Reached 15+ Pts", int((df['Max Points'] >= 15).sum()))
            r3.metric("Reached 20+ Pts", int((df['Max Points'] >= 20).sum()))
            r4.metric("Reached 25+ Pts", int((df['Max Points'] >= 25).sum()))

        st.subheader("Recent Trades")
        st.dataframe(df.sort_values(['Date', 'Exit Time'], ascending=False).style.applymap(color_pnl, subset=['PnL']).format("{:.2f}", subset=['PnL']), use_container_width=True)
    else:
        st.info("No live trades recorded yet.")
        st.write("Trades will appear here once the strategy executes an exit.")