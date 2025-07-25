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
            data = response.json()
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

        data = response.json()
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
WM_ID = st.text_input("Webmaster ID")
USERNAME = st.text_input("Username")
PASSWORD = st.text_input("Password", type="password")

# --- Date Picker ---
start_date, end_date = st.date_input(
    "📅 Select date range",
    value=[datetime.today() - timedelta(days=7), datetime.today()]
)

# Auto-correct if dates are reversed
if start_date > end_date:
    st.warning("⚠️ Start date is after end date. Swapping them.")
    start_date, end_date = end_date, start_date

# Generate list of dates
date_range = pd.date_range(start=start_date, end=end_date).to_pydatetime().tolist()
st.write(f"📆 Analyzing {len(date_range)} days of data from {start_date.strftime('%d-%b-%Y')} to {end_date.strftime('%d-%b-%Y')}")

excode = st.text_input("Exchange Code", value="CDX")

# --- Filters ---
st.subheader("💵 Price Filter")
MIN_PRICE = st.number_input("Minimum closing price to include", min_value=0.0, value=0.0)
MAX_PRICE = st.number_input("Maximum closing price to include", min_value=0.0, value=100.0)

st.subheader("📊 Volume Spike Filter")
MIN_PERCENT = st.slider("Minimum % increase over average volume", 0, 500, 80)
MAX_PERCENT = st.slider("Maximum % increase over average volume", MIN_PERCENT, 1000, 200)

st.subheader("🔍 Broker Buy Filter")
MIN_BROKER_PERCENT = st.slider("Minimum % of total volume bought by broker", 0.0, 100.0, 10.0)

# --- Run Button ---
if st.button("🚀 Run Analysis"):
    qm = QuoteMediaExchangeHistory(WM_ID, USERNAME, PASSWORD)

    # 1. Fetch EOD data for all days in range
    all_data = []
    st.info(f"📆 Fetching CDX EOD data from {start_date.strftime('%d-%b-%Y')} to {end_date.strftime('%d-%b-%Y')}...")
    progress = st.progress(0)
    for i, date_obj in enumerate(date_range):
        date_str = date_obj.strftime("%Y-%m-%d")
        daily_df = qm.fetch_exchange_history(excode, date_str)
        if not daily_df.empty:
            daily_df["date"] = date_str
            all_data.append(daily_df)
        progress.progress((i + 1) / len(date_range))

    if not all_data:
        st.warning("📭 No data was returned for the selected date range.")
    else:
        full_df = pd.concat(all_data, ignore_index=True)
        st.success(f"✅ Fetched {len(full_df)} rows of EOD data.")
        st.dataframe(full_df.head(50))

        # 2. Find outlier volume days (spikes) for each symbol over the whole range
        grouped = full_df.groupby("symbol")
        spike_rows = []
        for symbol, group in grouped:
            group = group.sort_values("date")
            if len(group) < 2:
                continue
            avg_vol = group["sharevolume"].astype(float).mean()
            if avg_vol == 0:
                continue
            # Find the day(s) with the highest volume for this symbol
            max_vol = group["sharevolume"].astype(float).max()
            # Optionally, you could use a threshold, e.g., only flag if max_vol is at least X% above avg
            for _, row in group.iterrows():
                this_vol = float(row["sharevolume"])
                vol_percent = (this_vol / avg_vol) * 100
                price = float(row.get("close", 0))
                price_ok = MIN_PRICE <= price <= MAX_PRICE
                # Only flag the max volume day(s) and if it meets the percent threshold
                if this_vol == max_vol and MIN_PERCENT <= vol_percent <= MAX_PERCENT and price_ok:
                    row = row.copy()
                    row["avg_volume"] = round(avg_vol, 2)
                    row["vol_percent"] = round(vol_percent, 2)
                    spike_rows.append(row)

        if not spike_rows:
            st.warning("🧘 No CDX stocks matched the volume percentage criteria.")
        else:
            spikes_df = pd.DataFrame(spike_rows).sort_values(["symbol", "date", "vol_percent"], ascending=[True, True, False])
            st.subheader("🎯 Outlier Volume Stocks & Days (Full Table)")
            outlier_table = spikes_df[["symbol", "date", "sharevolume", "avg_volume", "vol_percent", "close"]].reset_index(drop=True)
            st.dataframe(outlier_table)
            csv = outlier_table.to_csv(index=False).encode('utf-8')
            st.download_button("Download Outlier Stocks Table as CSV", csv, "outlier_stocks.csv", "text/csv")

            # For each outlier stock, aggregate broker data for the whole date range
            st.subheader("� Broker Activity for Outlier Stocks (Aggregated Over Date Range)")
            for symbol in spikes_df["symbol"].unique():
                st.markdown(f"### {symbol} Broker Summary ({start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')})")
                # Aggregate broker data for all days in range for this symbol
                broker_frames = []
                for date_obj in date_range:
                    date_str = date_obj.strftime("%Y-%m-%d")
                    nethouse_df = fetch_nethouse_summary(symbol, WM_ID, qm.sid, date_str)
                    if not nethouse_df.empty:
                        nethouse_df["date"] = date_str
                        broker_frames.append(nethouse_df)
                if not broker_frames:
                    st.info("No broker data for this stock in the date range.")
                    continue
                all_brokers = pd.concat(broker_frames, ignore_index=True)
                # Ensure numeric columns for aggregation
                for col in ["buy_volume", "sell_volume", "total_volume", "net_volume", "net_value"]:
                    all_brokers[col] = pd.to_numeric(all_brokers[col], errors='coerce').fillna(0)
                # Aggregate by broker
                broker_summary = all_brokers.groupby("broker").agg({
                    "buy_volume": "sum",
                    "sell_volume": "sum",
                    "total_volume": "sum",
                    "net_volume": "sum",
                    "net_value": "sum"
                }).reset_index()
                # Calculate % of total symbol volume for each broker
                total_symbol_volume = broker_summary["buy_volume"].sum() + broker_summary["sell_volume"].sum()
                if total_symbol_volume == 0:
                    broker_summary["pct_of_symbol_volume"] = 0
                else:
                    broker_summary["pct_of_symbol_volume"] = (broker_summary["buy_volume"] / total_symbol_volume) * 100
                broker_summary = broker_summary.sort_values("pct_of_symbol_volume", ascending=False)
                st.dataframe(broker_summary.reset_index(drop=True))
                csv_broker = broker_summary.to_csv(index=False).encode('utf-8')
                st.download_button(f"Download {symbol} Broker Summary as CSV", csv_broker, f"{symbol}_broker_summary.csv", "text/csv")
