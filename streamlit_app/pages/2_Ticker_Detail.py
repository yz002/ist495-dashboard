import streamlit as st
import plotly.express as px
import pandas as pd
from zoneinfo import ZoneInfo

from mongo_rt import (
    MongoCfg,
    parse_window,
    agg_ticker_summary,
    agg_time_buckets_for_ticker,
    ticker_summary,
    get_latest_messages,
)

ET = ZoneInfo("America/New_York")

try:
    from streamlit_autorefresh import st_autorefresh
except Exception:
    st_autorefresh = None

st.set_page_config(page_title="Ticker Detail", layout="wide")

st.markdown(
    """
    <style>
    .stApp {
        background: linear-gradient(180deg, #0b1020 0%, #111827 45%, #1e1b4b 100%);
    }
    [data-testid="stSidebar"] {
        background: linear-gradient(180deg, #111827 0%, #1f2937 100%);
        border-right: 1px solid rgba(255,255,255,0.08);
    }
    .block-container {
        padding-top: 1.2rem;
        padding-bottom: 2rem;
    }
    .hero-card {
        background: linear-gradient(135deg, #7c3aed 0%, #2563eb 100%);
        padding: 1.2rem 1.5rem;
        border-radius: 18px;
        color: white;
        margin-bottom: 1rem;
        box-shadow: 0 10px 30px rgba(0,0,0,0.22);
    }
    .hero-title {
        font-size: 1.85rem;
        font-weight: 800;
        margin: 0;
    }
    .hero-subtitle {
        font-size: 0.96rem;
        opacity: 0.92;
        margin-top: 0.3rem;
    }
    .section-title {
        font-size: 1.12rem;
        font-weight: 700;
        margin-top: 0.2rem;
        margin-bottom: 0.55rem;
        color: #f8fafc;
    }
    .mini-card {
        border-radius: 16px;
        padding: 0.8rem 1rem;
        color: white;
        margin-bottom: 0.6rem;
        box-shadow: 0 8px 22px rgba(0,0,0,0.18);
    }
    .mini-blue { background: linear-gradient(135deg, #2563eb, #06b6d4); }
    .mini-green { background: linear-gradient(135deg, #059669, #22c55e); }
    .mini-orange { background: linear-gradient(135deg, #ea580c, #f59e0b); }
    .mini-red { background: linear-gradient(135deg, #dc2626, #ef4444); }
    .mini-purple { background: linear-gradient(135deg, #7c3aed, #a855f7); }
    .mini-label {
        font-size: 0.82rem;
        opacity: 0.92;
        margin-bottom: 0.2rem;
    }
    .mini-value {
        font-size: 1.35rem;
        font-weight: 800;
        line-height: 1.1;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

st.markdown(
    """
    <div class="hero-card">
        <div class="hero-title">📌 Ticker Detail Dashboard</div>
        <div class="hero-subtitle">
            Drill down into sentiment, rumor intensity, source mix, and message-level evidence
        </div>
    </div>
    """,
    unsafe_allow_html=True,
)

cfg = MongoCfg()

st.sidebar.header("Ticker Controls")

window_mode = st.sidebar.radio("Window", ["Last N", "Custom", "All Time"], index=0)

preset = st.sidebar.selectbox(
    "Preset (Last N)",
    ["30 minutes", "1 hour", "6 hours", "24 hours", "7 days"],
    index=0,
    disabled=(window_mode != "Last N"),
)

preset_to_minutes = {
    "30 minutes": 30,
    "1 hour": 60,
    "6 hours": 360,
    "24 hours": 1440,
    "7 days": 10080,
}

minutes = st.sidebar.number_input(
    "Last N minutes",
    min_value=1,
    max_value=200_000,
    value=int(preset_to_minutes.get(preset, 30)),
    step=1,
    disabled=(window_mode != "Last N"),
)

custom_start = st.sidebar.text_input(
    'Custom start (ET) "YYYY-MM-DD HH:MM"',
    value="",
    disabled=(window_mode != "Custom"),
)

custom_end = st.sidebar.text_input(
    'Custom end (ET) "YYYY-MM-DD HH:MM"',
    value="",
    disabled=(window_mode != "Custom"),
)

bucket_minutes = st.sidebar.selectbox("Bucket size", [1, 5, 10, 15, 30, 60], index=1)
max_messages = st.sidebar.slider("Messages to show", 50, 500, 200, 25)

st.sidebar.markdown("---")
auto_refresh = st.sidebar.checkbox("Enable auto-refresh", value=False)
refresh_seconds = st.sidebar.slider("Refresh every (seconds)", 3, 120, 10, 1)

if window_mode == "Last N":
    start_utc, end_utc = parse_window("last_n", last_n=int(minutes), unit="minutes")
elif window_mode == "Custom":
    if not custom_start.strip() or not custom_end.strip():
        st.warning('Enter Custom start/end in ET (YYYY-MM-DD HH:MM).')
        st.stop()
    start_utc, end_utc = parse_window("custom_et", start_et=custom_start.strip(), end_et=custom_end.strip())
else:
    start_utc, end_utc = parse_window("all_time")

st.markdown('<div class="section-title">Choose a Ticker</div>', unsafe_allow_html=True)

candidates = st.session_state.get("last_live_tickers", [])

if not candidates:
    tmp = agg_ticker_summary(cfg, start_utc, end_utc)
    if not tmp.empty and "stream_symbol" in tmp.columns:
        candidates = (
            tmp["stream_symbol"]
            .dropna()
            .astype(str)
            .str.upper()
            .sort_values()
            .unique()
            .tolist()
        )

manual = st.text_input("Or type a ticker", value="").strip().upper()

if candidates:
    current = (st.session_state.get("ticker") or "").strip().upper()
    default_index = candidates.index(current) if current in candidates else 0
    picked = st.selectbox("Select ticker", candidates, index=default_index)
else:
    picked = ""

ticker = manual if manual else picked
ticker = ticker.strip().upper()

if not ticker:
    st.info("Select a ticker first.")
    st.stop()

st.session_state["ticker"] = ticker

st.markdown(f"### Ticker: **{ticker}**")

nav_a, nav_b = st.columns(2)
if nav_a.button("⬅️ Back to Live"):
    st.switch_page("pages/1_Live_Dashboard.py")
if nav_b.button("🔁 Refresh now"):
    st.rerun()

if auto_refresh:
    if st_autorefresh is None:
        st.sidebar.warning("Install streamlit-autorefresh")
    else:
        st_autorefresh(interval=int(refresh_seconds) * 1000, key=f"refresh_{ticker}")

summary = ticker_summary(cfg, ticker, start_utc, end_utc)

m1, m2, m3, m4 = st.columns(4)
with m1:
    st.markdown(f'<div class="mini-card mini-blue"><div class="mini-label">Total Posts</div><div class="mini-value">{int(summary.get("total_posts", 0))}</div></div>', unsafe_allow_html=True)
with m2:
    st.markdown(f'<div class="mini-card mini-green"><div class="mini-label">Bullish</div><div class="mini-value">{int(summary.get("bullish", 0))}</div></div>', unsafe_allow_html=True)
with m3:
    st.markdown(f'<div class="mini-card mini-red"><div class="mini-label">Bearish</div><div class="mini-value">{int(summary.get("bearish", 0))}</div></div>', unsafe_allow_html=True)
with m4:
    st.markdown(f'<div class="mini-card mini-purple"><div class="mini-label">Sentiment Score</div><div class="mini-value">{float(summary.get("sentiment_score", 0)):.4f}</div></div>', unsafe_allow_html=True)

m5, m6, m7, m8 = st.columns(4)
with m5:
    st.markdown(f'<div class="mini-card mini-green"><div class="mini-label">Traditional Posts</div><div class="mini-value">{int(summary.get("traditional_posts", 0))}</div></div>', unsafe_allow_html=True)
with m6:
    st.markdown(f'<div class="mini-card mini-orange"><div class="mini-label">Social Posts</div><div class="mini-value">{int(summary.get("social_posts", 0))}</div></div>', unsafe_allow_html=True)
with m7:
    st.markdown(f'<div class="mini-card mini-red"><div class="mini-label">Rumor Posts</div><div class="mini-value">{int(summary.get("rumor_posts", 0))}</div></div>', unsafe_allow_html=True)
with m8:
    st.markdown(f'<div class="mini-card mini-blue"><div class="mini-label">Density / Min</div><div class="mini-value">{float(summary.get("density_per_min", 0)):.4f}</div></div>', unsafe_allow_html=True)

st.caption(
    f"Window: {start_utc.astimezone(ET).strftime('%Y-%m-%d %I:%M %p')} → "
    f"{end_utc.astimezone(ET).strftime('%Y-%m-%d %I:%M %p')} ET"
)

st.markdown("---")
st.markdown('<div class="section-title">🧭 Rumor vs Traditional Driver View</div>', unsafe_allow_html=True)

driver_left, driver_right = st.columns([1, 1.2])

with driver_left:
    driver_df = pd.DataFrame(
        {
            "Category": ["Traditional", "Social / Rumor-Social", "Rumor-Flagged"],
            "Posts": [
                int(summary.get("traditional_posts", 0)),
                int(summary.get("social_posts", 0)),
                int(summary.get("rumor_posts", 0)),
            ],
        }
    )

    fig_driver = px.bar(
        driver_df,
        x="Category",
        y="Posts",
        title=f"{ticker}: Source Driver Comparison",
        text="Posts",
        color="Category",
        color_discrete_sequence=["#22c55e", "#f59e0b", "#ef4444"],
    )
    fig_driver.update_layout(xaxis_title="", yaxis_title="Post Count", showlegend=False)
    st.plotly_chart(fig_driver, use_container_width=True)

with driver_right:
    total_posts = max(int(summary.get("total_posts", 0)), 1)
    traditional_ratio = int(summary.get("traditional_posts", 0)) / total_posts
    social_ratio = int(summary.get("social_posts", 0)) / total_posts
    rumor_ratio = int(summary.get("rumor_posts", 0)) / total_posts

    ratio_df = pd.DataFrame(
        {
            "Metric": ["Traditional Share", "Social Share", "Rumor Share"],
            "Ratio": [traditional_ratio, social_ratio, rumor_ratio],
        }
    )

    fig_ratio = px.bar(
        ratio_df,
        x="Metric",
        y="Ratio",
        title=f"{ticker}: Driver Share of Conversation",
        text="Ratio",
        color="Metric",
        color_discrete_sequence=["#22c55e", "#f59e0b", "#ef4444"],
    )
    fig_ratio.update_traces(texttemplate="%{text:.2%}")
    fig_ratio.update_layout(xaxis_title="", yaxis_title="Share of Posts", yaxis_tickformat=".0%", showlegend=False)
    st.plotly_chart(fig_ratio, use_container_width=True)

st.markdown("---")
st.markdown('<div class="section-title">Time-Series Charts</div>', unsafe_allow_html=True)

bucket_df = agg_time_buckets_for_ticker(
    cfg,
    ticker,
    start_utc,
    end_utc,
    bucket_minutes=int(bucket_minutes),
)

if not bucket_df.empty:
    bucket_df["bucket_start_et"] = pd.to_datetime(bucket_df["bucket_start_et"])

    left, right = st.columns(2)

    with left:
        st.markdown("### 📊 Message Volume")
        fig = px.line(
            bucket_df,
            x="bucket_start_et",
            y="total_posts",
            markers=True,
            color_discrete_sequence=["#38bdf8"],
        )
        fig.update_layout(xaxis_title="Time (ET)", yaxis_title="Posts", paper_bgcolor="rgba(0,0,0,0)")
        st.plotly_chart(fig, use_container_width=True)

    with right:
        st.markdown("### 📈 Sentiment")
        fig = px.line(
            bucket_df,
            x="bucket_start_et",
            y="sentiment_score",
            markers=True,
            color_discrete_sequence=["#a855f7"],
        )
        fig.update_layout(xaxis_title="Time (ET)", yaxis_title="Sentiment Score", paper_bgcolor="rgba(0,0,0,0)")
        st.plotly_chart(fig, use_container_width=True)
else:
    st.info("No bucket data in this window.")

st.markdown("---")
st.markdown('<div class="section-title">Latest Messages</div>', unsafe_allow_html=True)

msgs = get_latest_messages(cfg, ticker, start_utc, end_utc, limit=int(max_messages))

if msgs.empty:
    st.info("No messages in this window.")
else:
    msgs = msgs.copy()

    msg_display_map = {
        "created_at_et": "Time (ET)",
        "author": "Author",
        "sentiment": "Sentiment",
        "source_type": "Source Type",
        "rumor_flag": "Rumor Flag",
        "rumor_reason": "Rumor Reason",
        "post": "Post",
        "link": "Link",
    }

    tab1, tab2, tab3 = st.tabs(["All Clean Messages", "Traditional Only", "Rumor / Social Only"])

    with tab1:
        show_cols = ["created_at_et", "author", "sentiment", "source_type", "rumor_flag", "rumor_reason", "post", "link"]
        show_cols = [c for c in show_cols if c in msgs.columns]
        st.dataframe(msgs[show_cols].rename(columns=msg_display_map), use_container_width=True, hide_index=True)

    with tab2:
        trad_df = msgs.copy()
        if "source_type" in trad_df.columns:
            trad_df = trad_df[trad_df["source_type"] == "Traditional"]
        show_cols = ["created_at_et", "author", "sentiment", "source_type", "post", "link"]
        show_cols = [c for c in show_cols if c in trad_df.columns]
        if trad_df.empty:
            st.info("No traditional-source messages in this window.")
        else:
            st.dataframe(trad_df[show_cols].rename(columns=msg_display_map), use_container_width=True, hide_index=True)

    with tab3:
        rumor_df = msgs.copy()
        if "source_type" in rumor_df.columns:
            rumor_df = rumor_df[
                (rumor_df["source_type"] == "Rumor/Social")
                | (rumor_df.get("rumor_flag", False) == True)
            ]
        show_cols = ["created_at_et", "author", "sentiment", "source_type", "rumor_flag", "rumor_reason", "post", "link"]
        show_cols = [c for c in show_cols if c in rumor_df.columns]
        if rumor_df.empty:
            st.info("No rumor/social messages in this window.")
        else:
            st.dataframe(rumor_df[show_cols].rename(columns=msg_display_map), use_container_width=True, hide_index=True)

st.caption("Tip: Use 30–60 minute windows for real-time analysis.")