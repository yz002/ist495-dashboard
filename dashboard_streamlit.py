# dashboard_streamlit.py
# Run: streamlit run dashboard_streamlit.py
# Requires: pip install streamlit pymongo pandas

import streamlit as st
import pandas as pd
from pymongo import MongoClient
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

ET = ZoneInfo("America/New_York")
UTC = ZoneInfo("UTC")

st.set_page_config(page_title="Stocktwits Dashboard", layout="wide")

# -----------------------
# Helpers
# -----------------------
def et_to_utc_iso(dt_et: datetime) -> str:
    return dt_et.astimezone(UTC).isoformat().replace("+00:00", "Z")

@st.cache_data(ttl=10)
def load_finviz_csv(path: str) -> pd.DataFrame:
    fin = pd.read_csv(path, encoding="utf-8-sig")
    fin["Ticker"] = fin["Ticker"].astype(str).str.strip().str.upper()
    return fin

@st.cache_data(ttl=10)
def query_window(mongo_uri, mongo_db, mongo_collection, start_z, end_z):
    client = MongoClient(mongo_uri)
    col = client[mongo_db][mongo_collection]

    q = {"created_at": {"$gte": start_z, "$lt": end_z}}
    cursor = col.find(q, {"stream_symbol": 1, "sentiment": 1, "created_at": 1, "post": 1, "link": 1, "_id": 0})
    rows = list(cursor)
    client.close()

    df = pd.DataFrame(rows)
    if df.empty:
        return pd.DataFrame(columns=["stream_symbol", "sentiment", "created_at", "post", "link"])
    df["stream_symbol"] = df["stream_symbol"].astype(str).str.strip().str.upper()
    df["sentiment"] = df["sentiment"].fillna("null").astype(str)
    return df

def agg_social(st_df: pd.DataFrame) -> pd.DataFrame:
    g = st_df.groupby("stream_symbol")["sentiment"]
    agg = pd.DataFrame({
        "social_total_posts": g.size(),
        "social_bullish": g.apply(lambda s: (s == "Bullish").sum()),
        "social_bearish": g.apply(lambda s: (s == "Bearish").sum()),
        "social_unlabeled": g.apply(lambda s: (s == "null").sum()),
    }).reset_index().rename(columns={"stream_symbol": "Ticker"})

    labeled = (agg["social_bullish"] + agg["social_bearish"]).replace(0, pd.NA)
    agg["social_sentiment_score"] = (agg["social_bullish"] - agg["social_bearish"]) / labeled
    agg["social_sentiment_score"] = agg["social_sentiment_score"].fillna(0.0)

    # density in posts/hour
    return agg

# -----------------------
# Sidebar: controls
# -----------------------
st.sidebar.title("Controls")

mongo_uri = st.sidebar.text_input("Mongo URI", "mongodb://localhost:27017")
mongo_db = st.sidebar.text_input("Mongo DB", "stocktwits")
mongo_collection = st.sidebar.text_input("Mongo Collection", "messages")

finviz_path = st.sidebar.text_input("Finviz CSV path", r"C:\path\to\finviz.csv")

mode = st.sidebar.radio("Time window mode", ["Last N minutes", "Custom ET range"])

if mode == "Last N minutes":
    minutes = st.sidebar.selectbox("Minutes", [5, 15, 30, 60, 120, 240], index=2)
    end_et = datetime.now(ET)
    start_et = end_et - timedelta(minutes=int(minutes))
else:
    c1, c2 = st.sidebar.columns(2)
    start_date = c1.date_input("Start date (ET)", datetime.now(ET).date())
    start_time = c2.time_input("Start time (ET)", datetime.now(ET).time().replace(second=0, microsecond=0))
    c3, c4 = st.sidebar.columns(2)
    end_date = c3.date_input("End date (ET)", datetime.now(ET).date())
    end_time = c4.time_input("End time (ET)", datetime.now(ET).time().replace(second=0, microsecond=0))

    start_et = datetime.combine(start_date, start_time).replace(tzinfo=ET)
    end_et = datetime.combine(end_date, end_time).replace(tzinfo=ET)

min_posts = st.sidebar.number_input("Min total posts per ticker", min_value=0, value=20, step=1)

auto_refresh = st.sidebar.checkbox("Auto-refresh (every 10s)", value=True)
if auto_refresh:
    st.caption("Auto-refresh enabled (10s)")
    st.cache_data.clear()

start_z = et_to_utc_iso(start_et)
end_z = et_to_utc_iso(end_et)

st.title("Real-time Stocktwits Ticker Dashboard")
st.write(f"**ET window:** {start_et.strftime('%Y-%m-%d %H:%M')} → {end_et.strftime('%Y-%m-%d %H:%M')}")
st.write(f"**UTC window:** `{start_z}` → `{end_z}`")

# -----------------------
# Load data
# -----------------------
if not finviz_path or not finviz_path.lower().endswith(".csv"):
    st.warning("Enter a valid Finviz CSV path in the sidebar.")
    st.stop()

fin = load_finviz_csv(finviz_path)
st_df = query_window(mongo_uri, mongo_db, mongo_collection, start_z, end_z)

if st_df.empty:
    st.warning("No Stocktwits messages found in this window.")
    st.stop()

agg = agg_social(st_df)

# merge
merged = fin.merge(agg, on="Ticker", how="left").fillna(0)
merged = merged[merged["social_total_posts"] >= min_posts].copy()

# density
window_hours = (end_et.astimezone(UTC) - start_et.astimezone(UTC)).total_seconds() / 3600.0
merged["message_density"] = merged["social_total_posts"] / max(window_hours, 1e-9)
merged["weighted_density"] = merged["message_density"] * merged["social_sentiment_score"]

# -----------------------
# Main table (sortable)
# -----------------------
st.subheader("Tickers (sortable)")
st.dataframe(
    merged.sort_values("weighted_density", ascending=False),
    use_container_width=True,
    hide_index=True
)

# -----------------------
# Drilldown ticker selector (page 2 “lite” inside same app for now)
# -----------------------
st.subheader("Ticker drilldown")
ticker = st.selectbox("Pick a ticker", sorted(merged["Ticker"].unique().tolist()))

t_df = st_df[st_df["stream_symbol"] == ticker].copy()
t_df = t_df.sort_values("created_at", ascending=False)

st.write(f"Showing **{len(t_df)}** messages for **{ticker}** (most recent first)")
st.dataframe(t_df[["created_at", "sentiment", "post", "link"]].head(200), use_container_width=True, hide_index=True)