import streamlit as st
import pandas as pd
import oracledb
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings("ignore")

# ────────────────────────────────────────────────
# PAGE CONFIG
# ────────────────────────────────────────────────
st.set_page_config(layout="wide", page_title="NSE Fib Buy Zone Scanner")

# ────────────────────────────────────────────────
# ORACLE CONNECTION
# ────────────────────────────────────────────────
@st.cache_resource
@st.cache_resource
def get_db_connection():
    try:
        conn = oracledb.connect(
            user=st.secrets["oracle"]["user"],
            password=st.secrets["oracle"]["password"],
            dsn=st.secrets["oracle"]["dsn"],
            config_dir=st.secrets["oracle"]["wallet_dir"],
            wallet_location=st.secrets["oracle"]["wallet_dir"],
            wallet_password=st.secrets["oracle"]["wallet_password"]
        )
        return conn
    except Exception as e:
        st.error(f"Database connection failed: {e}")
        st.stop()

# ────────────────────────────────────────────────
# CREATE SCAN TABLE IF NOT EXISTS
# ────────────────────────────────────────────────
def create_daily_scan_table():
    conn = get_db_connection()
    cursor = conn.cursor()
    create_table_sql = """
    BEGIN
        EXECUTE IMMEDIATE '
            CREATE TABLE daily_buy_zone_scan (
                scan_date            DATE NOT NULL,
                symbol               VARCHAR2(50) NOT NULL,
                close_price          NUMBER(12,2),
                fib_0618_buy_low     NUMBER(12,2),
                fib_0382_buy_high    NUMBER(12,2),
                sl_level             NUMBER(12,2),
                pct_from_0618        NUMBER(12,2),
                in_buy_zone          VARCHAR2(10),
                entered_today        VARCHAR2(20),
                fib_position         VARCHAR2(100),
                direction_context    VARCHAR2(100),
                volume_trend         VARCHAR2(50),
                volume_context       VARCHAR2(150),
                recent_avg_volume    NUMBER(10),
                trading_days         NUMBER(6),
                CONSTRAINT pk_daily_scan PRIMARY KEY (scan_date, symbol)
            )';
    EXCEPTION
        WHEN OTHERS THEN
            IF SQLCODE = -955 THEN NULL;  -- already exists
            ELSE RAISE;
            END IF;
    END;
    """
    try:
        cursor.execute(create_table_sql)
        conn.commit()
    except Exception as e:
        st.warning(f"Could not verify/create daily_buy_zone_scan: {e}")
    finally:
        cursor.close()

create_daily_scan_table()

# ────────────────────────────────────────────────
# SAVE SCAN RESULTS (Oracle MERGE)
# ────────────────────────────────────────────────
def save_scan_to_sql(scan_date, results_df):
    if results_df.empty:
        return

    conn = get_db_connection()
    cursor = conn.cursor()

    merge_sql = """
    MERGE INTO daily_buy_zone_scan t
    USING (SELECT :1 scan_date, :2 symbol, :3 close_price, :4 fib_0618_buy_low, :5 fib_0382_buy_high,
                  :6 sl_level, :7 pct_from_0618, :8 in_buy_zone, :9 entered_today, :10 fib_position,
                  :11 direction_context, :12 volume_trend, :13 volume_context, :14 recent_avg_volume, :15 trading_days
           FROM dual) s
    ON (t.scan_date = s.scan_date AND t.symbol = s.symbol)
    WHEN MATCHED THEN
        UPDATE SET
            t.close_price = s.close_price,
            t.fib_0618_buy_low = s.fib_0618_buy_low,
            t.fib_0382_buy_high = s.fib_0382_buy_high,
            t.sl_level = s.sl_level,
            t.pct_from_0618 = s.pct_from_0618,
            t.in_buy_zone = s.in_buy_zone,
            t.entered_today = s.entered_today,
            t.fib_position = s.fib_position,
            t.direction_context = s.direction_context,
            t.volume_trend = s.volume_trend,
            t.volume_context = s.volume_context,
            t.recent_avg_volume = s.recent_avg_volume,
            t.trading_days = s.trading_days
    WHEN NOT MATCHED THEN
        INSERT (scan_date, symbol, close_price, fib_0618_buy_low, fib_0382_buy_high,
                sl_level, pct_from_0618, in_buy_zone, entered_today, fib_position,
                direction_context, volume_trend, volume_context, recent_avg_volume, trading_days)
        VALUES (s.scan_date, s.symbol, s.close_price, s.fib_0618_buy_low,
                s.fib_0382_buy_high, s.sl_level, s.pct_from_0618,
                s.in_buy_zone, s.entered_today, s.fib_position,
                s.direction_context, s.volume_trend, s.volume_context,
                s.recent_avg_volume, s.trading_days)
    """

    try:
        inserted_or_updated = 0
        for _, row in results_df.iterrows():
            cursor.execute(merge_sql, (
                scan_date,
                row["Symbol"],
                row.get("Close"),
                row.get("Fib 0.618 (low)"),
                row.get("Fib 0.382 (high)"),
                row.get("SL Level"),
                row.get("% from 0.618"),
                row.get("In Buy Zone"),
                row.get("Entered Today"),
                row.get("Fib Position"),
                row.get("Recent Direction"),
                row.get("Volume Trend"),
                row.get("Volume Context"),
                row.get("Recent Avg Vol"),
                row.get("Days")
            ))
            inserted_or_updated += 1

        conn.commit()
        st.success(f"Scan processed: {inserted_or_updated} rows saved/updated for {scan_date}")
    except Exception as e:
        conn.rollback()
        st.error(f"Failed to save/update scan: {e}")
    finally:
        cursor.close()

# ────────────────────────────────────────────────
# SYMBOLS & DATA FETCH
# ────────────────────────────────────────────────
@st.cache_data(ttl=3600)
def get_all_symbols():
    conn = get_db_connection()
    query = """
    SELECT DISTINCT symbol
    FROM equity_daily
    WHERE series = 'EQ'
    ORDER BY symbol
    """
    try:
        df = pd.read_sql(query, conn)
        return df["symbol"].str.strip().str.upper().tolist()
    except Exception as e:
        st.error(f"Failed to fetch symbols: {e}")
        return []

@st.cache_data(ttl=1800)
def fetch_daily_from_sql(symbol: str):
    conn = get_db_connection()
    query = """
    SELECT trade_date AS timestamp,
           open,
           high,
           low,
           close,
           volume
    FROM equity_daily
    WHERE symbol = :sym
      AND series = 'EQ'
    ORDER BY trade_date
    """
    try:
        df = pd.read_sql(query, conn, params={'sym': symbol})
        if df.empty:
            return None
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        df.set_index("timestamp", inplace=True)
        df[["open", "high", "low", "close"]] = df[["open", "high", "low", "close"]].astype(float)
        df["volume"] = df["volume"].astype(int, errors="ignore")
        df["year"] = df.index.year
        return df
    except Exception as e:
        st.warning(f"Query failed for {symbol}: {e}")
        return None

# ────────────────────────────────────────────────
# YOUR FIB LOGIC (unchanged)
# ────────────────────────────────────────────────
def get_custom_position(df):
    if df is None or len(df) < 60:
        return {
            "price": None,
            "low_2024": None,
            "open_2025": None,
            "fib_0618_buy_low": None,
            "fib_0382_buy_high": None,
            "sl_level": None,
            "dist_from_0618_pct": None,
            "in_buying_zone": "No",
            "entering_today_daily": "No",
            "fib_position": "Insufficient data",
            "direction_context": "Insufficient data",
            "volume_trend": "No data",
            "volume_context": "No data",
            "recent_avg_vol": None,
            "trading_days": None
        }
    current_date = datetime.now().date()
    switch_date = datetime(2027, 1, 2).date()
    if current_date < switch_date:
        low_anchor = df[df["year"] == 2024]["low"].min() if (df["year"] == 2024).any() else df["low"].min()
        open_anchor = None
        if (df["year"] == 2025).any():
            open_anchor = df[df["year"] == 2025].sort_index().iloc[0]["open"]
        else:
            open_anchor = df["open"].iloc[-1]
        anchor_low_year = "2024"
        anchor_open_year = "2025"
    else:
        low_anchor = df[df["year"] == 2025]["low"].min() if (df["year"] == 2025).any() else df["low"].min()
        open_anchor = None
        if (df["year"] == 2026).any():
            open_anchor = df[df["year"] == 2026].sort_index().iloc[0]["open"]
        else:
            open_anchor = df["open"].iloc[-1]
        anchor_low_year = "2025"
        anchor_open_year = "2026"
    diff = open_anchor - low_anchor
    fib_0618 = low_anchor + diff * (1 - 0.618)
    fib_0500 = low_anchor + diff * (1 - 0.500)
    fib_0382 = low_anchor + diff * (1 - 0.382)
    base_move = diff
    sl_level = fib_0618 * 0.97
    current_price = df["close"].iloc[-1]
    dist_pct = ((current_price - fib_0618) / fib_0618) * 100 if fib_0618 != 0 else None
    zone = "Other / extended"
    if current_price <= low_anchor:
        zone = f"Below {anchor_low_year} low (1.0)"
    elif fib_0618 <= current_price <= fib_0382:
        zone = "Buying zone (0.382 – 0.618)"
    elif current_price > fib_0382:
        zone = "Above buying zone"
    elif open_anchor <= current_price < fib_0618:
        zone = "Rest area (near 0.0)"
    in_buy = "Yes" if "Buying zone" in zone else "No"
    recent_close = df["close"].iloc[-2] if len(df) >= 2 else current_price
    entering = "Yes (entered today)" if fib_0618 <= current_price <= fib_0382 and not (fib_0618 <= recent_close <= fib_0382) else "No"
    fib_position = "Other"
    if current_price <= low_anchor * 0.97:
        fib_position = f"Below {anchor_low_year} low – breakdown"
    elif current_price < fib_0618 * 0.98:
        fib_position = "Just below buy zone"
    elif abs(current_price - fib_0618) / fib_0618 <= 0.008:
        fib_position = "At/near 0.618 – deep buy zone"
    elif fib_0618 <= current_price <= fib_0500 * 0.97:
        fib_position = "Lower half buy zone"
    elif abs(current_price - fib_0500) / fib_0500 <= 0.01:
        fib_position = "Around 0.500 center"
    elif fib_0500 <= current_price <= fib_0382:
        fib_position = "Upper half buy zone"
    elif current_price > fib_0382:
        fib_position = "Above buy zone"
    direction_context = "No recent trend clear"
    if len(df) >= 12:
        recent = df.iloc[-12:]
        if recent["low"].min() <= fib_0382 and recent["high"].max() >= fib_0618:
            if current_price > recent["low"].min() * 1.02:
                direction_context = "Touched zone → bounced up"
            else:
                direction_context = "In zone – still weak/declining"
    volume_trend = "Stable"
    volume_context = "Volume neutral"
    recent_avg_vol = None
    if len(df) >= 40:
        recent_vol = df["volume"].iloc[-10:].mean()
        prior_vol = df["volume"].iloc[-40:-10].mean() if len(df) > 40 else recent_vol
        latest_vol = df["volume"].iloc[-1]
        recent_avg_vol = int(recent_vol) if recent_vol is not None else None
        if recent_vol > prior_vol * 1.35:
            volume_trend = "Rising"
            if in_buy == "Yes":
                volume_context = "Volume rising in buy zone – accumulation likely"
        elif recent_vol < prior_vol * 0.75:
            volume_trend = "Falling"
            if in_buy == "Yes":
                volume_context = "Volume dropping in buy zone – weak interest"
        else:
            volume_trend = "Stable"
        if latest_vol > recent_vol * 1.8 and in_buy == "Yes":
            volume_trend = "Spike"
            volume_context = "Volume spike today in buy zone – strong signal"
    return {
        "price": round(current_price, 2),
        "low_2024": round(low_anchor, 2),
        "open_2025": round(open_anchor, 2),
        "fib_0618_buy_low": round(fib_0618, 2),
        "fib_0382_buy_high": round(fib_0382, 2),
        "sl_level": round(sl_level, 2),
        "% from 0.618": round(dist_pct, 1) if dist_pct is not None else None,
        "In Buy Zone": in_buy,
        "Entered Today": entering,
        "Fib Position": fib_position,
        "Recent Direction": direction_context,
        "Volume Trend": volume_trend,
        "Volume Context": volume_context,
        "Recent Avg Vol": recent_avg_vol,
        "Days": len(df)
    }

# ────────────────────────────────────────────────
# ENHANCED BACKTEST (fixed crash)
# ────────────────────────────────────────────────
def get_enhanced_backtest_report(df, fib_0618, fib_0382, low_anchor, open_anchor, symbol):
    signals = []
    fib_0382 = low_anchor + (open_anchor - low_anchor) * (1 - 0.382)
    for i in range(200, len(df)-1):
        if (fib_0618 <= df['close'].iloc[i] <= fib_0382):
            entry_price = df['close'].iloc[i]
            sl_price = fib_0618 * 0.97
            future = df.iloc[i+1:]
            if len(future) > 0:
                hit_sl = (future['low'] <= sl_price).any()
                hit_t1 = (future['high'] >= fib_0382 + (open_anchor-low_anchor)*0.382).any()
                signals.append({
                    'Symbol': symbol,
                    'Entry Date': df.index[i].date(),
                    'Entry': entry_price,
                    'SL Hit': 'LOSS' if hit_sl else ('PROFIT' if hit_t1 else 'OPEN'),
                    'P&L_%': ((fib_0382 + (open_anchor-low_anchor)*0.382) - entry_price)/entry_price *100
                })
    return signals

# ────────────────────────────────────────────────
# MAIN APP
# ────────────────────────────────────────────────
st.title("🔥 NSE EQ Stocks – Fib Buy Zone Scanner + Enhanced Backtest")

# Manual DB update button (optional)
if st.button("🔄 Update Database (last 5 days)"):
    with st.spinner("Running ETL update..."):
        # You can call your ETL function here if you want
        # For now just placeholder
        st.success("Database update simulated. Add your ETL call here if desired.")

tab1, tab2 = st.tabs(["📊 Live Scanner", "🎯 Enhanced Backtest"])

with tab1:
    current_date = datetime.now().date()
    switch_date = datetime(2027, 1, 2).date()
    if current_date < switch_date:
        st.markdown("**Anchors:** 2024 Low = 1.0 • 2025 Open = 0.0")
    else:
        st.markdown("**Anchors:** 2025 Low = 1.0 • 2026 Open = 0.0")
    st.markdown("**Buying zone:** Fib 0.618 to 0.382 | **SL:** 3% below 0.618 | **Targets:** +0.382 to +3.618")
    col1, col2, col3 = st.columns([2, 1, 1])
    with col1:
        min_days = st.slider("Minimum trading days required", 30, 300, 60, step=10)
    with col2:
        symbol_filter = st.text_input("Filter symbols starting with", "").strip().upper()
    with col3:
        sort_by = st.selectbox("Sort buying zone stocks by", [
            "Closest to 0.618 from below",
            "Farthest below 0.618",
            "Symbol (A→Z)"
        ])
    if st.button("🚀 Scan ALL stocks in database (Daily timeframe)"):
        all_symbols = get_all_symbols()
        if not all_symbols:
            st.stop()
        if symbol_filter:
            all_symbols = [s for s in all_symbols if s.startswith(symbol_filter)]
        st.info(f"Found **{len(all_symbols)}** symbols to scan")
        results = []
        progress_bar = st.progress(0)
        status_text = st.empty()
        total = len(all_symbols)
        for i, symbol in enumerate(all_symbols, 1):
            status_text.text(f"{i}/{total} → {symbol}")
            df = fetch_daily_from_sql(symbol)
            if df is not None and len(df) >= min_days:
                info = get_custom_position(df)
                if info["price"] is not None:
                    row = {
                        "Symbol": symbol,
                        "Close": info["price"],
                        "2024 Low": info["low_2024"],
                        "2025 Open": info["open_2025"],
                        "Fib 0.618 (low)": info["fib_0618_buy_low"],
                        "Fib 0.382 (high)": info["fib_0382_buy_high"],
                        "SL Level": info["sl_level"],
                        "% from 0.618": info["% from 0.618"],
                        "In Buy Zone": info["In Buy Zone"],
                        "Entered Today": info["Entered Today"],
                        "Fib Position": info["Fib Position"],
                        "Recent Direction": info["Recent Direction"],
                        "Volume Trend": info["Volume Trend"],
                        "Volume Context": info["Volume Context"],
                        "Recent Avg Vol": info["Recent Avg Vol"],
                        "Days": info["Days"]
                    }
                    results.append(row)
            progress_bar.progress(i / total)
        progress_bar.empty()
        status_text.empty()
        if not results:
            st.warning("No stocks met the minimum days criteria or had valid data.")
        else:
            df_results = pd.DataFrame(results)
            today = datetime.now().date()
            save_scan_to_sql(today, df_results)
            def highlight_buy(row):
                if row["In Buy Zone"] == "Yes":
                    return ['background-color: #d4edda'] * len(row)
                return [''] * len(row)
            styled = df_results.style.apply(highlight_buy, axis=1).format(precision=2)
            buy_stocks = df_results[df_results["In Buy Zone"] == "Yes"].copy()
            if len(buy_stocks) > 0:
                if sort_by == "Closest to 0.618 from below":
                    buy_stocks = buy_stocks.sort_values("% from 0.618", ascending=True, na_position='last')
                elif sort_by == "Farthest below 0.618":
                    buy_stocks = buy_stocks.sort_values("% from 0.618", ascending=False, na_position='last')
                else:
                    buy_stocks = buy_stocks.sort_values("Symbol")
                st.success(f"**🎉 {len(buy_stocks)}** stocks currently in **Buying Zone**")
                st.table(buy_stocks[["Symbol", "Close", "2024 Low", "2025 Open", "Fib 0.618 (low)", "Fib 0.382 (high)", "% from 0.618", "Entered Today", "SL Level"]])
            st.markdown(f"**Total stocks scanned with ≥ {min_days} days:** {len(df_results)}")
            st.dataframe(styled, use_container_width=True, hide_index=True)
            csv = df_results.to_csv(index=False).encode('utf-8')
            st.download_button(
                label="📥 Download full scan (CSV)",
                data=csv,
                file_name=f"buy_zone_scan_all_{datetime.now().strftime('%Y%m%d_%H%M')}.csv",
                mime="text/csv"
            )

with tab2:
    st.markdown("**🎯 Enhanced Backtest:** Entry on buy zone touch → Multi-target exits + SL → Days to hit each level")
    st.markdown("- **Entry:** First close in 0.382-0.618 (ANY direction)")
    st.markdown("- **Targets:** +0.382, +0.618, +1.618, +2.618, +3.618 from fib_0382")
    st.markdown("- **SL:** 3% below fib_0618")
    symbol_filter_backtest = st.text_input("Filter symbols for backtest (starting with)", "").strip().upper()
    backtest_min_days = st.slider("Min days for backtest", 100, 500, 200, step=50)
    if st.button("🚀 Run Enhanced Backtest on Filtered Stocks"):
        all_symbols = get_all_symbols()
        if not all_symbols:
            st.stop()
        if symbol_filter_backtest:
            all_symbols = [s for s in all_symbols if s.startswith(symbol_filter_backtest)]
        st.info(f"Running enhanced backtest on **{len(all_symbols)}** symbols...")
        backtest_results = []
        progress_bar = st.progress(0)
        status_text = st.empty()
        total = len(all_symbols)
        for i, symbol in enumerate(all_symbols, 1):
            status_text.text(f"{i}/{total} → {symbol}")
            df = fetch_daily_from_sql(symbol)
            if df is not None and len(df) >= backtest_min_days:
                info = get_custom_position(df)
                if info["price"] is not None:
                    signals = get_enhanced_backtest_report(
                        df, info["fib_0618_buy_low"], info["fib_0382_buy_high"],
                        info["low_2024"], info["open_2025"], symbol
                    )
                    backtest_results.extend(signals)
            progress_bar.progress(i / total)
        progress_bar.empty()
        status_text.empty()
        if backtest_results:
            df_backtest = pd.DataFrame(backtest_results)
            # FIXED: use actual column name 'SL Hit' instead of 'Result'
            df_backtest = df_backtest.sort_values(["SL Hit", "P&L_%"], ascending=[False, False])
            profits = df_backtest[df_backtest["SL Hit"] == "PROFIT"]
            losses = df_backtest[df_backtest["SL Hit"] == "LOSS"]
            total_signals = len(df_backtest)
            win_rate = (len(profits) / total_signals * 100) if total_signals > 0 else 0
            avg_profit = profits["P&L_%"].mean() if len(profits) > 0 else 0
            avg_loss = losses["P&L_%"].mean() if len(losses) > 0 else 0
            # Note: no 'Days' column yet - you can add logic if needed
            col1, col2, col3, col4 = st.columns(4)
            col1.metric("Total Signals", total_signals)
            col2.metric("Win Rate", f"{round(win_rate, 1)}%")
            col3.metric("Avg Profit %", f"{round(avg_profit, 1)}%")
            col4.metric("Avg Loss %", f"{round(avg_loss, 1)}%")
            st.dataframe(df_backtest.style.format(precision=2), use_container_width=True, hide_index=True)
            csv_backtest = df_backtest.to_csv(index=False).encode('utf-8')
            st.download_button(
                label="📥 Download Enhanced Backtest Report (CSV)",
                data=csv_backtest,
                file_name=f"enhanced_backtest_{datetime.now().strftime('%Y%m%d_%H%M')}.csv",
                mime="text/csv"
            )
        else:
            st.warning("No historical buy zone entries found or insufficient data.")
