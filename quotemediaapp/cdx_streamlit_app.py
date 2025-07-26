import streamlit as st
import pandas as pd
import requests
import json
from datetime import datetime, timedelta

# --- QuoteMedia Class ---
class QuoteMediaExchangeHistory:
    def __init__(self, wm_id, username, password):
        self.wm_id = wm_id
        self.username = username
        self.password = password
        self.sid = None
        self.last_auth = None
        self.authenticate()

    def authenticate(self):
        url = "https://app.quotemedia.com/auth/p/authenticate/v0/"
        payload = {
            "wmId": self.wm_id,
            "username": self.username,
            "password": self.password
        }
        try:
            response = requests.post(url, headers={"Content-Type": "application/json"}, data=json.dumps(payload))
            data = response.json()
            if data.get("code", {}).get("value") == 0:
                self.sid = data["sid"]
                self.last_auth = datetime.now()
                st.success(f"✅ Authenticated. SID: {self.sid}")
            else:
                st.error(f"❌ Auth failed: {data.get('code', {}).get('name')}")
        except Exception as e:
            st.error(f"❌ Auth error: {str(e)}")

    def refresh_session(self):
        if not self.sid or (datetime.now() - self.last_auth) > timedelta(minutes=25):
            st.info("🔄 Refreshing session...")
            self.authenticate()

    def fetch_exchange_history(self, excode, date):
        self.refresh_session()
        url = "https://app.quotemedia.com/data/getExchangeHistory.json"
        params = {
            "webmasterId": self.wm_id,
            "sid": self.sid,
            "excode": excode,
            "date": date
        }
        try:
            response = requests.get(url, params=params)
            try:
                data = response.json()
            except Exception as e:
                st.error(f"❌ API request failed: {str(e)}\nRaw response: {response.text}")
                return pd.DataFrame()
            history = data.get("results", {}).get("history", [])
            records = []
            for item in history:
                for quote in item.get("eoddata", []):
                    quote["symbol"] = item.get("symbolstring", item.get("symbol", "UNKNOWN"))
                    quote["exchange"] = item.get("key", {}).get("exchange", "UNKNOWN")
                    records.append(quote)
            return pd.DataFrame(records)
        except Exception as e:
            st.error(f"❌ API request failed: {str(e)}")
            return pd.DataFrame()

def fetch_nethouse_summary(symbol, webmaster_id, sid, date):
    url = "https://app.quotemedia.com/data/getNethouseBySymbol.json"
    params = {
        "webmasterId": webmaster_id,
        "sid": sid,
        "symbol": symbol,
        "start": date,
        "end": date
    }

    try:
        response = requests.get(url, params=params)
        if response.status_code != 200:
            st.warning(f"⚠️ Unexpected status code {response.status_code} for {symbol}")
            return pd.DataFrame()
        try:
            data = response.json()
        except Exception as e:
            st.error(f"❌ Error fetching nethouse summary for {symbol}: {e}\nRaw response: {response.text}")
            return pd.DataFrame()
        participants = data.get("results", {}).get("nethouse", {}).get("summary", {}).get("participant", [])
        rows = []
        for p in participants:
            buy_volume = p.get("buy", {}).get("volume", 0)
            sell_volume = p.get("sell", {}).get("volume", 0)
            total_volume = p.get("volume", 0)
            buy_pct = p.get("buy", {}).get("volpct", 0)

            # Skip brokers with no activity
            if buy_volume == 0 and sell_volume == 0:
                continue

            rows.append({
                "broker": p.get("pname"),
                "buy_volume": buy_volume,
                "sell_volume": sell_volume,
                "total_volume": total_volume,
                "buy_pct": buy_pct,
                "sell_pct": p.get("sell", {}).get("volpct", 0),
                "net_volume": p.get("netvol", 0),
                "net_value": p.get("netval", 0)
            })

        return pd.DataFrame(rows)

    except Exception as e:
        st.error(f"❌ Error fetching nethouse summary for {symbol}: {e}")
        return pd.DataFrame()
    
# === STREAMLIT UI ===
st.set_page_config(page_title="CDX Broker Volume Analyzer", layout="wide")
st.title("📈 CDX Broker Volume Spike Analyzer")

# --- Credentials ---

# --- Use session state for credentials and filters ---
if 'WM_ID' not in st.session_state:
    st.session_state['WM_ID'] = ''
if 'USERNAME' not in st.session_state:
    st.session_state['USERNAME'] = ''
if 'PASSWORD' not in st.session_state:
    st.session_state['PASSWORD'] = ''

WM_ID = st.text_input("Webmaster ID", value=st.session_state['WM_ID'], key="wmid_input")
USERNAME = st.text_input("Username", value=st.session_state['USERNAME'], key="username_input")
PASSWORD = st.text_input("Password", type="password", value=st.session_state['PASSWORD'], key="password_input")

st.session_state['WM_ID'] = WM_ID



# --- Ensure persistent QuoteMedia object and SID ---
def ensure_qm_object():
    creds = (st.session_state['WM_ID'], st.session_state['USERNAME'], st.session_state['PASSWORD'])
    if 'qm_obj' not in st.session_state:
        st.session_state['qm_obj'] = QuoteMediaExchangeHistory(*creds)
    else:
        qm = st.session_state['qm_obj']
        if (qm.wm_id, qm.username, qm.password) != creds:
            st.session_state['qm_obj'] = QuoteMediaExchangeHistory(*creds)
    st.session_state['sid'] = st.session_state['qm_obj'].sid

# ...existing code for UI and variable definitions...

# --- Place the Run Analysis button and logic at the very end, after all variables are defined ---
if st.button("\U0001F680 Run Analysis"):
    ensure_qm_object()
    qm = st.session_state['qm_obj']
    # 1. Fetch EOD data for all days in range
    all_data = []
    if single_day_selected and LOOKBACK_DAYS:
        # Fetch lookback days before the selected day (excluding the selected day)
        lookback_dates = pd.date_range(
            end=start_date - timedelta(days=1), periods=LOOKBACK_DAYS, freq='B'  # business days
        ).to_pydatetime().tolist()
        fetch_dates = lookback_dates + [start_date]
        st.info(f"\U0001F4C6 Fetching CDX EOD data for {LOOKBACK_DAYS} lookback days and selected day {start_date.strftime('%d-%b-%Y')}")
    else:
        fetch_dates = date_range
        st.info(f"\U0001F4C6 Fetching CDX EOD data from {start_date.strftime('%d-%b-%Y')} to {end_date.strftime('%d-%b-%Y')}")
    progress = st.progress(0)
    for i, date_obj in enumerate(fetch_dates):
        date_str = date_obj.strftime("%Y-%m-%d")
        daily_df = qm.fetch_exchange_history(excode, date_str)
        if not daily_df.empty:
            daily_df["date"] = date_str
            all_data.append(daily_df)
        progress.progress((i + 1) / len(fetch_dates))

    if not all_data:
        st.session_state['analysis_warning'] = "\U0001F4ED No data was returned for the selected date range."
        st.session_state['spikes_df'] = None
        st.session_state['full_df'] = None
    else:
        full_df = pd.concat(all_data, ignore_index=True)
        st.session_state['analysis_success'] = f"\u2705 Fetched {len(full_df)} rows of EOD data."
        st.session_state['full_df_head'] = full_df.head(50)
        st.session_state['full_df'] = full_df

        # 2. Find outlier volume days (spikes) for each symbol
        grouped = full_df.groupby("symbol")
        spike_rows = []
        if single_day_selected and LOOKBACK_DAYS:
            # For each symbol, calculate avg over lookback (excluding selected day), compare selected day
            selected_day_str = start_date.strftime("%Y-%m-%d")
            for symbol, group in grouped:
                group = group.sort_values("date")
                # Exclude selected day for avg
                lookback_group = group[group['date'] != selected_day_str]
                if lookback_group.empty:
                    continue
                avg_vol = lookback_group["sharevolume"].astype(float).mean()
                if avg_vol == 0:
                    continue
                # Get row for selected day
                selected_row = group[group['date'] == selected_day_str]
                if selected_row.empty:
                    continue
                row = selected_row.iloc[0].copy()
                this_vol = float(row["sharevolume"])
                vol_percent = ((this_vol - avg_vol) / avg_vol) * 100 if avg_vol > 0 else 0
                price = float(row.get("close", 0))
                price_ok = MIN_PRICE <= price <= MAX_PRICE
                min_vol = avg_vol * (1 + MIN_PERCENT / 100)
                max_vol_limit = avg_vol * (1 + MAX_PERCENT / 100)
                if (
                    this_vol >= min_vol
                    and this_vol <= max_vol_limit
                    and price_ok
                ):
                    row["avg_volume"] = round(avg_vol, 2)
                    row["vol_percent"] = round(vol_percent, 2)
                    spike_rows.append(row)
        else:
            # ...existing multi-day logic...
            for symbol, group in grouped:
                group = group.sort_values("date")
                avg_vol = group["sharevolume"].astype(float).mean()
                if avg_vol == 0:
                    continue
                max_vol = group["sharevolume"].astype(float).max()
                for _, row in group.iterrows():
                    this_vol = float(row["sharevolume"])
                    price = float(row.get("close", 0))
                    price_ok = MIN_PRICE <= price <= MAX_PRICE
                    vol_percent = ((this_vol - avg_vol) / avg_vol) * 100 if avg_vol > 0 else 0
                    min_vol = avg_vol * (1 + MIN_PERCENT / 100)
                    max_vol_limit = avg_vol * (1 + MAX_PERCENT / 100)
                    if (
                        this_vol == max_vol
                        and this_vol >= min_vol
                        and this_vol <= max_vol_limit
                        and price_ok
                    ):
                        row = row.copy()
                        row["avg_volume"] = round(avg_vol, 2)
                        row["vol_percent"] = round(vol_percent, 2)
                        spike_rows.append(row)

        if not spike_rows:
            st.session_state['analysis_warning'] = "\U0001F9D8 No CDX stocks matched the volume percentage criteria."
            st.session_state['spikes_df'] = None
        else:
            spikes_df = pd.DataFrame(spike_rows).sort_values(["symbol", "date", "vol_percent"], ascending=[True, True, False])
            st.session_state['spikes_df'] = spikes_df
            st.session_state['analysis_warning'] = None
            st.session_state['analysis_success'] = None
            st.session_state['outlier_table'] = spikes_df[["symbol", "date", "sharevolume", "avg_volume", "vol_percent", "close"]].reset_index(drop=True)
        if single_day_selected and LOOKBACK_DAYS:
            # For each symbol, calculate avg over lookback (excluding selected day), compare selected day
            selected_day_str = start_date.strftime("%Y-%m-%d")
            for symbol, group in grouped:
                group = group.sort_values("date")
                # Exclude selected day for avg
                lookback_group = group[group['date'] != selected_day_str]
                if lookback_group.empty:
                    continue
                avg_vol = lookback_group["sharevolume"].astype(float).mean()
                if avg_vol == 0:
                    continue
                # Get row for selected day
                selected_row = group[group['date'] == selected_day_str]
                if selected_row.empty:
                    continue
                row = selected_row.iloc[0].copy()
                this_vol = float(row["sharevolume"])
                vol_percent = ((this_vol - avg_vol) / avg_vol) * 100 if avg_vol > 0 else 0
                price = float(row.get("close", 0))
                price_ok = MIN_PRICE <= price <= MAX_PRICE
                min_vol = avg_vol * (1 + MIN_PERCENT / 100)
                max_vol_limit = avg_vol * (1 + MAX_PERCENT / 100)
                if (
                    this_vol >= min_vol
                    and this_vol <= max_vol_limit
                    and price_ok
                ):
                    row["avg_volume"] = round(avg_vol, 2)
                    row["vol_percent"] = round(vol_percent, 2)
                    spike_rows.append(row)
        else:
            # ...existing multi-day logic...
            for symbol, group in grouped:
                group = group.sort_values("date")
                avg_vol = group["sharevolume"].astype(float).mean()
                if avg_vol == 0:
                    continue
                max_vol = group["sharevolume"].astype(float).max()
                for _, row in group.iterrows():
                    this_vol = float(row["sharevolume"])
                    price = float(row.get("close", 0))
                    price_ok = MIN_PRICE <= price <= MAX_PRICE
                    vol_percent = ((this_vol - avg_vol) / avg_vol) * 100 if avg_vol > 0 else 0
                    min_vol = avg_vol * (1 + MIN_PERCENT / 100)
                    max_vol_limit = avg_vol * (1 + MAX_PERCENT / 100)
                    if (
                        this_vol == max_vol
                        and this_vol >= min_vol
                        and this_vol <= max_vol_limit
                        and price_ok
                    ):
                        row = row.copy()
                        row["avg_volume"] = round(avg_vol, 2)
                        row["vol_percent"] = round(vol_percent, 2)
                        spike_rows.append(row)

        if not spike_rows:
            st.session_state['analysis_warning'] = "🧘 No CDX stocks matched the volume percentage criteria."
            st.session_state['spikes_df'] = None
        else:
            spikes_df = pd.DataFrame(spike_rows).sort_values(["symbol", "date", "vol_percent"], ascending=[True, True, False])
            st.session_state['spikes_df'] = spikes_df
            st.session_state['analysis_warning'] = None
            st.session_state['analysis_success'] = None
            st.session_state['outlier_table'] = spikes_df[["symbol", "date", "sharevolume", "avg_volume", "vol_percent", "close"]].reset_index(drop=True)

# --- Show analysis results if available ---
if 'spikes_df' in st.session_state and st.session_state['spikes_df'] is not None:
    spikes_df = st.session_state['spikes_df']
    st.subheader("🎯 Outlier Volume Stocks & Days (Full Table)")
    outlier_table = st.session_state['outlier_table']
    st.dataframe(outlier_table)

    # --- Search/select for broker summary ---
    st.subheader("🔎 Broker Activity for Outlier Stocks (Aggregated Over Date Range)")
    outlier_symbols = spikes_df["symbol"].unique().tolist()
    if outlier_symbols:
        selected_symbol = st.selectbox(
            "Select a stock to view broker summary:",
            options=outlier_symbols,
            index=0,
            key="broker_symbol_select"
        )
        st.markdown(f"### {selected_symbol} Broker Summary ({start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')})")
        # Aggregate broker data for lookback days (single day mode) or all days in range (multi-day)
        broker_frames = []
        if single_day_selected and LOOKBACK_DAYS:
            lookback_dates = pd.date_range(
                end=start_date - timedelta(days=1), periods=LOOKBACK_DAYS, freq='B'
            ).to_pydatetime().tolist()
            summary_dates = lookback_dates
        else:
            summary_dates = date_range
        for date_obj in summary_dates:
            date_str = date_obj.strftime("%Y-%m-%d")
            nethouse_df = fetch_nethouse_summary(selected_symbol, WM_ID, st.session_state.get('sid', ''), date_str)
            if not nethouse_df.empty:
                nethouse_df["date"] = date_str
                broker_frames.append(nethouse_df)
        if not broker_frames:
            st.info("No broker data for this stock in the selected summary period.")
        else:
            all_brokers = pd.concat(broker_frames, ignore_index=True)
            for col in ["buy_volume", "sell_volume", "total_volume", "net_volume", "net_value"]:
                all_brokers[col] = pd.to_numeric(all_brokers[col], errors='coerce').fillna(0)
            broker_summary = all_brokers.groupby("broker").agg({
                "buy_volume": "sum",
                "sell_volume": "sum",
                "total_volume": "sum",
                "net_volume": "sum",
                "net_value": "sum"
            }).reset_index()
            # Use EOD total volume for the symbol as denominator
            full_df = st.session_state.get('full_df', None)
            if full_df is None:
                st.warning("Full EOD data not available for percent calculation.")
                total_symbol_eod_volume = None
            else:
                if single_day_selected and LOOKBACK_DAYS:
                    # Use only lookback days for EOD volume
                    symbol_df = full_df[(full_df['symbol'] == selected_symbol) & (full_df['date'].isin([d.strftime('%Y-%m-%d') for d in lookback_dates]))]
                else:
                    symbol_df = full_df[full_df['symbol'] == selected_symbol]
                total_symbol_eod_volume = symbol_df['sharevolume'].astype(float).sum()
            if total_symbol_eod_volume and total_symbol_eod_volume > 0:
                broker_summary["pct_of_symbol_volume"] = (broker_summary["buy_volume"] / total_symbol_eod_volume) * 100
            else:
                broker_summary["pct_of_symbol_volume"] = 0

            # Also calculate pct using only EOD volume for days where broker had activity
            def pct_active_days(row):
                broker = row['broker']
                active_dates = all_brokers[all_brokers['broker'] == broker]['date'].unique()
                if full_df is not None:
                    if single_day_selected and LOOKBACK_DAYS:
                        symbol_df = full_df[(full_df['symbol'] == selected_symbol) & (full_df['date'].isin([d.strftime('%Y-%m-%d') for d in lookback_dates]))]
                    else:
                        symbol_df = full_df[full_df['symbol'] == selected_symbol]
                    eod_on_active = symbol_df[symbol_df['date'].isin(active_dates)]['sharevolume'].astype(float).sum()
                    if eod_on_active > 0:
                        return (row['buy_volume'] / eod_on_active) * 100
                return 0
            broker_summary['pct_of_symbol_volume_active_days'] = broker_summary.apply(pct_active_days, axis=1)

            # Filter brokers by MIN_BROKER_PERCENT on either percent column
            min_broker_percent = st.session_state.get('MIN_BROKER_PERCENT', 0.0)
            broker_summary = broker_summary[(broker_summary['pct_of_symbol_volume'] >= min_broker_percent) |
                                            (broker_summary['pct_of_symbol_volume_active_days'] >= min_broker_percent)]

            broker_summary = broker_summary.sort_values("pct_of_symbol_volume", ascending=False)
            st.dataframe(broker_summary.reset_index(drop=True))
elif 'analysis_warning' in st.session_state and st.session_state['analysis_warning']:
    st.warning(st.session_state['analysis_warning'])
elif 'analysis_success' in st.session_state and st.session_state['analysis_success']:
    st.success(st.session_state['analysis_success'])
    st.dataframe(st.session_state['full_df_head'])
