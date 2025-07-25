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
        rows = [{
            "broker": p.get("pname"),
            "buy_volume": p.get("buy", {}).get("volume", 0),
            "sell_volume": p.get("sell", {}).get("volume", 0),
            "net_volume": p.get("netvol", 0),
            "buy_pct": p.get("buy", {}).get("volpct", 0),
            "sell_pct": p.get("sell", {}).get("volpct", 0)
        } for p in participants]
        return pd.DataFrame(rows)
    except Exception as e:
        st.error(f"⚠️ Failed to fetch Net House for {symbol}: {e}")
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

    all_data = []
    st.info(f"📆 Fetching CDX EOD data from {start_date.strftime('%d-%b-%Y')} to {end_date.strftime('%d-%b-%Y')}...")
    progress = st.progress(0)

    for i, date_obj in enumerate(date_range):
        date = date_obj.strftime("%Y-%m-%d")
        df = qm.fetch_exchange_history(excode, date)
        if not df.empty:
            all_data.append(df)
        progress.progress((i + 1) / len(date_range))

    if all_data:
        full_df = pd.concat(all_data, ignore_index=True)
        st.success(f"✅ Fetched {len(full_df)} rows of EOD data.")

        # --- Volume Spike Detection ---
        grouped = full_df.groupby("symbol")
        spike_rows = []

        for symbol, group in grouped:
            group = group.sort_values("date")
            if len(group) < 5:
                continue

            historical = group.iloc[:-1]
            latest = group.iloc[-1].copy()

            volumes = historical["sharevolume"].astype(float)
            avg_vol = volumes.mean()
            if avg_vol == 0:
                continue

            latest_vol = float(latest["sharevolume"])
            vol_percent = (latest_vol / avg_vol) * 100
            price = float(latest.get("close", 0))
            price_ok = MIN_PRICE <= price <= MAX_PRICE

            if MIN_PERCENT <= vol_percent <= MAX_PERCENT and price_ok:
                latest["avg_volume"] = round(avg_vol, 2)
                latest["vol_percent"] = round(vol_percent, 2)
                spike_rows.append(latest)

        spikes_df = pd.DataFrame(spike_rows).sort_values("vol_percent", ascending=False)
        st.subheader("🎯 Volume Spike Matches")
        st.dataframe(spikes_df[["symbol", "date", "sharevolume", "avg_volume", "vol_percent", "close"]].reset_index(drop=True))

        # --- Net House Broker Summary ---
        st.subheader("📁 Net House Broker Summary")
        nethouse_all = []
        for row in spike_rows:
            symbol = row["symbol"]
            date = row["date"]
            nethouse_df = fetch_nethouse_summary(symbol, WM_ID, qm.sid, date)
            if not nethouse_df.empty:
                nethouse_df.insert(0, "symbol", symbol)
                nethouse_df.insert(1, "date", date)
                nethouse_all.append(nethouse_df)

        if nethouse_all:
            final_df = pd.concat(nethouse_all, ignore_index=True)
            st.dataframe(final_df.reset_index(drop=True))

            # --- Broker Buy Filter ---
            broker_summary = []
            for df in nethouse_all:
                for _, row in df.iterrows():
                buy_pct = row.get("buy_pct", 0)
                if buy_pct >= MIN_BROKER_PERCENT:
                    broker_summary.append({
                        "broker": row["broker"],
                        "symbol": row["symbol"],
                        "date": row["date"],
                        "buy_volume": row["buy_volume"],
                        "quoted_buy_pct": buy_pct
                    })

            if broker_summary:
                broker_df = pd.DataFrame(broker_summary).sort_values(["broker", "buy_volume"], ascending=[True, False])
                st.dataframe(broker_df.reset_index(drop=True))
            else:
                st.info("📭 No broker buy data matched the minimum % filter.")
        else:
            st.info("📭 No broker data to display.")
    else:
        st.warning("🧘 No CDX stocks matched the volume percentage criteria.")
