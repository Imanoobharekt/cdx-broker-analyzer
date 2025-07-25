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

        # 2. Find outlier volume days (spikes) for each symbol
        grouped = full_df.groupby("symbol")
        spike_rows = []
        for symbol, group in grouped:
            group = group.sort_values("date")
            if len(group) < 5:
                continue
            # For each row, compare to average of all other days for that symbol
            for idx, row in group.iterrows():
                other = group.drop(idx)
                if other.empty:
                    continue
                avg_vol = other["sharevolume"].astype(float).mean()
                if avg_vol == 0:
                    continue
                this_vol = float(row["sharevolume"])
                vol_percent = (this_vol / avg_vol) * 100
                price = float(row.get("close", 0))
                price_ok = MIN_PRICE <= price <= MAX_PRICE
                if MIN_PERCENT <= vol_percent <= MAX_PERCENT and price_ok:
                    row = row.copy()
                    row["avg_volume"] = round(avg_vol, 2)
                    row["vol_percent"] = round(vol_percent, 2)
                    spike_rows.append(row)

        if not spike_rows:
            st.warning("🧘 No CDX stocks matched the volume percentage criteria.")
        else:
            spikes_df = pd.DataFrame(spike_rows).sort_values(["symbol", "date", "vol_percent"], ascending=[True, True, False])
            st.subheader("🎯 Outlier Volume Stocks & Days")
            # List unique outlier stocks
            outlier_symbols = spikes_df["symbol"].unique().tolist()
            st.write(f"Found {len(outlier_symbols)} outlier stocks:")
            st.write(", ".join(outlier_symbols))
            st.dataframe(spikes_df[["symbol", "date", "sharevolume", "avg_volume", "vol_percent", "close"]].reset_index(drop=True))

            # For each outlier day, show broker breakdown
            st.subheader("🔎 Broker Breakdown for Outlier Days")
            for _, row in spikes_df.iterrows():
                symbol = row["symbol"]
                date_str = row["date"]
                st.markdown(f"**{symbol}** on **{date_str}** (Volume: {row['sharevolume']}, Outlier %: {row['vol_percent']})")
                nethouse_df = fetch_nethouse_summary(symbol, WM_ID, qm.sid, date_str)
                if nethouse_df.empty:
                    st.info("No broker data for this day.")
                    continue
                # Calculate % of total volume for each broker
                total_day_volume = nethouse_df["buy_volume"].sum() + nethouse_df["sell_volume"].sum()
                if total_day_volume == 0:
                    nethouse_df["pct_of_day_volume"] = 0
                else:
                    nethouse_df["pct_of_day_volume"] = (nethouse_df["buy_volume"] / total_day_volume) * 100
                nethouse_df = nethouse_df.sort_values("pct_of_day_volume", ascending=False)
                st.dataframe(nethouse_df[["broker", "buy_volume", "sell_volume", "total_volume", "buy_pct", "sell_pct", "pct_of_day_volume"]].reset_index(drop=True))
