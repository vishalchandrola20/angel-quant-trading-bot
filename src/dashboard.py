import streamlit as st
import pandas as pd
import plotly.express as px
from pathlib import Path

# Page Config
st.set_page_config(page_title="Angel Quant Dashboard", layout="wide", page_icon="📈")

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
            if 'entry_time' in full_df.columns:
                # Combine Date and Time for strict chronological sorting
                full_df['timestamp'] = pd.to_datetime(full_df['date'].dt.strftime('%Y-%m-%d') + ' ' + full_df['entry_time'].astype(str))
                full_df = full_df.sort_values(by='timestamp')
            else:
                full_df = full_df.sort_values(by='date')
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
        total_pnl = df['pnl'].sum()
        total_trades = len(df)
        wins = len(df[df['pnl'] > 0])
        losses = len(df[df['pnl'] <= 0])
        win_rate = (wins / total_trades * 100) if total_trades > 0 else 0
        
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Total PnL (Points)", f"{total_pnl:.2f}", delta_color="normal")
        col2.metric("Total Trades", total_trades)
        col3.metric("Win Rate", f"{win_rate:.1f}%")
        col4.metric("Avg PnL per Trade", f"{total_pnl/total_trades:.2f}")
        
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
        df['cumulative_pnl'] = df['pnl'].cumsum()
        x_col = 'timestamp' if 'timestamp' in df.columns else 'date'
        fig = px.line(df, x=x_col, y='cumulative_pnl', title='Cumulative PnL Over Time', markers=True)
        fig.update_layout(xaxis_title="Date", yaxis_title="Cumulative PnL (Points)")
        st.plotly_chart(fig, use_container_width=True)
        
        # --- Analysis Charts ---
        st.markdown("---")
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("Hourly Performance")
            if 'timestamp' in df.columns:
                # Shift back by 15 mins so 9:15-10:15 becomes bucket 9
                df['hour_bucket'] = (df['timestamp'] - pd.Timedelta(minutes=15)).dt.hour
                hourly_pnl = df.groupby('hour_bucket')['pnl'].sum().reset_index()
                
                # Create labels
                hourly_pnl['label'] = hourly_pnl['hour_bucket'].apply(lambda h: f"{h:02d}:15 - {h+1:02d}:15")
                hourly_pnl['color'] = hourly_pnl['pnl'] >= 0
                
                fig_hourly = px.bar(hourly_pnl, x='label', y='pnl', 
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
            
            daily_pnl = df.groupby('day_name')['pnl'].sum().reset_index()
            daily_pnl['day_name'] = pd.Categorical(daily_pnl['day_name'], categories=days_order, ordered=True)
            daily_pnl = daily_pnl.sort_values('day_name')
            daily_pnl['color'] = daily_pnl['pnl'] >= 0
            
            fig_daily = px.bar(daily_pnl, x='day_name', y='pnl', 
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
            'exit_time': 'Exit Time', 'entry_price': 'Entry Price',
            'exit_price': 'Exit Price', 'pnl': 'PnL',
            'reason': 'Reason',
            'max_potential_profit': 'Max Pot Profit',
            'max_potential_loss': 'Max Pot Loss'
        }
        display_df.rename(columns=column_mapping, inplace=True)
        
        # Sort by Date and Entry Time descending
        sort_cols = [c for c in ['Date', 'Setup Time', 'Entry Time'] if c in display_df.columns]
        if sort_cols:
            display_df = display_df.sort_values(sort_cols, ascending=False)
            
        st.dataframe(display_df, use_container_width=True)
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
        # Filters
        index_filter = st.sidebar.multiselect("Filter Index", df['Index'].unique(), default=df['Index'].unique())
        if index_filter:
            df = df[df['Index'].isin(index_filter)]
            
        # Metrics
        total_pnl = df['PnL'].sum()
        today = pd.Timestamp.now().strftime("%Y-%m-%d")
        
        # Ensure Date column is string for comparison
        df['Date'] = df['Date'].astype(str)
        df_today = df[df['Date'] == today]
        today_pnl = df_today['PnL'].sum() if not df_today.empty else 0.0
        
        col1, col2 = st.columns(2)
        col1.metric("Total Live PnL", f"{total_pnl:.2f}")
        col2.metric("Today's PnL", f"{today_pnl:.2f}", delta=f"{today_pnl:.2f}")
        
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
        st.dataframe(df.sort_values(['Date', 'Exit Time'], ascending=False), use_container_width=True)
    else:
        st.info("No live trades recorded yet.")
        st.write("Trades will appear here once the strategy executes an exit.")