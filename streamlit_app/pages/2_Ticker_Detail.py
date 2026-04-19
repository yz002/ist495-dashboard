import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from zoneinfo import ZoneInfo
from datetime import datetime

from mongo_rt import (
    MongoCfg,
    parse_window,
    agg_ticker_summary,
    agg_time_buckets_for_ticker,
    ticker_summary,
    get_latest_messages,
    get_active_rumor_for_ticker,
)

ET = ZoneInfo("America/New_York")

try:
    from streamlit_autorefresh import st_autorefresh
except Exception:
    st_autorefresh = None


def today_6am_et_str() -> str:
    now_et = datetime.now(ET)
    start_et = now_et.replace(hour=6, minute=0, second=0, microsecond=0)
    return start_et.strftime("%Y-%m-%d %H:%M")


def now_et_str() -> str:
    return datetime.now(ET).strftime("%Y-%m-%d %H:%M")


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
    .rumor-banner {
        border-radius: 16px;
        padding: 1rem 1.15rem;
        color: white;
        margin-bottom: 1rem;
        box-shadow: 0 8px 20px rgba(0,0,0,0.2);
    }
    .rumor-buy {
        background: linear-gradient(135deg, #16a34a, #22c55e);
    }
    .rumor-leave {
        background: linear-gradient(135deg, #dc2626, #ef4444);
    }
    .rumor-neutral {
        background: linear-gradient(135deg, #334155, #475569);
    }
    .rumor-title {
        font-size: 1.02rem;
        font-weight: 800;
        margin-bottom: 0.3rem;
    }
    .rumor-meta {
        font-size: 0.92rem;
        opacity: 0.95;
        margin-bottom: 0.35rem;
    }
    .rumor-text {
        font-size: 0.96rem;
        line-height: 1.45;
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
            Drill down into sentiment, active rumor direction, source mix, and message-level evidence
        </div>
    </div>
    """,
    unsafe_allow_html=True,
)

cfg = MongoCfg()

# -----------------------------
# Persistent ticker detail filters
# -----------------------------
DETAIL_DEFAULTS = dict(
    window_mode="Today",
    preset="30 minutes",
    minutes=30,
    custom_start=today_6am_et_str(),
    custom_end=now_et_str(),
    bucket_minutes=10,
    max_messages=200,
    auto_refresh=False,
    refresh_seconds=10,
)

if "ticker_detail_filters" not in st.session_state:
    st.session_state["ticker_detail_filters"] = DETAIL_DEFAULTS.copy()

detail_filters = st.session_state["ticker_detail_filters"]

st.sidebar.header("Ticker Controls")

with st.sidebar.form("ticker_detail_controls", clear_on_submit=False):
    window_mode = st.radio(
        "Window",
        ["Today", "Last N", "Custom", "All Time"],
        index=["Today", "Last N", "Custom", "All Time"].index(detail_filters["window_mode"]),
    )

    preset = st.selectbox(
        "Preset (Last N)",
        ["30 minutes", "1 hour", "6 hours", "24 hours", "7 days"],
        index=["30 minutes", "1 hour", "6 hours", "24 hours", "7 days"].index(detail_filters["preset"]),
        disabled=(window_mode != "Last N"),
    )

    preset_to_minutes = {
        "30 minutes": 30,
        "1 hour": 60,
        "6 hours": 360,
        "24 hours": 1440,
        "7 days": 10080,
    }

    minutes = st.number_input(
        "Last N minutes",
        min_value=1,
        max_value=200_000,
        value=int(detail_filters["minutes"]),
        step=1,
        disabled=(window_mode != "Last N"),
    )

    custom_start = st.text_input(
        'Custom start (ET) "YYYY-MM-DD HH:MM"',
        value=detail_filters["custom_start"],
        disabled=(window_mode != "Custom"),
    )

    custom_end = st.text_input(
        'Custom end (ET) "YYYY-MM-DD HH:MM"',
        value=detail_filters["custom_end"],
        disabled=(window_mode != "Custom"),
    )

    bucket_minutes = st.selectbox(
        "Bucket size",
        [1, 5, 10, 15, 30, 60],
        index=[1, 5, 10, 15, 30, 60].index(detail_filters["bucket_minutes"]),
    )

    max_messages = st.slider(
        "Messages to show",
        50,
        500,
        int(detail_filters["max_messages"]),
        25,
    )

    st.markdown("---")
    auto_refresh = st.checkbox("Enable auto-refresh", value=bool(detail_filters["auto_refresh"]))
    refresh_seconds = st.slider("Refresh every (seconds)", 3, 120, int(detail_filters["refresh_seconds"]), 1)

    c1, c2 = st.columns(2)
    apply_clicked = c1.form_submit_button("✅ Apply")
    reset_clicked = c2.form_submit_button("↩️ Reset")

if apply_clicked:
    st.session_state["ticker_detail_filters"] = dict(
        window_mode=window_mode,
        preset=preset,
        minutes=int(minutes),
        custom_start=custom_start.strip(),
        custom_end=custom_end.strip(),
        bucket_minutes=int(bucket_minutes),
        max_messages=int(max_messages),
        auto_refresh=bool(auto_refresh),
        refresh_seconds=int(refresh_seconds),
    )
    st.rerun()

if reset_clicked:
    st.session_state["ticker_detail_filters"] = DETAIL_DEFAULTS.copy()
    st.rerun()

detail_filters = st.session_state["ticker_detail_filters"]

# -----------------------------
# Parse window robustly
# -----------------------------
try:
    if detail_filters["window_mode"] == "Today":
        start_utc, end_utc = parse_window(
            "custom_et",
            start_et=today_6am_et_str(),
            end_et=now_et_str(),
        )
    elif detail_filters["window_mode"] == "Last N":
        start_utc, end_utc = parse_window(
            "last_n",
            last_n=int(detail_filters["minutes"]),
            unit="minutes",
        )
    elif detail_filters["window_mode"] == "Custom":
        if not detail_filters["custom_start"] or not detail_filters["custom_end"]:
            st.warning('Enter Custom start/end in ET (YYYY-MM-DD HH:MM), then click Apply.')
            st.stop()
        start_utc, end_utc = parse_window(
            "custom_et",
            start_et=detail_filters["custom_start"],
            end_et=detail_filters["custom_end"],
        )
    else:
        start_utc, end_utc = parse_window("all_time")
except Exception as e:
    st.error(f"Window error: {e}")
    st.stop()

# -----------------------------
# Ticker chooser
# -----------------------------
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
    st.stop()
if nav_b.button("🔁 Refresh now"):
    st.rerun()

if detail_filters["auto_refresh"]:
    if st_autorefresh is None:
        st.sidebar.warning("Install streamlit-autorefresh")
    else:
        st_autorefresh(interval=int(detail_filters["refresh_seconds"]) * 1000, key=f"refresh_{ticker}")

# -----------------------------
# Summary + rumor
# -----------------------------
summary = ticker_summary(cfg, ticker, start_utc, end_utc)
active_rumor = get_active_rumor_for_ticker(cfg, ticker, start_utc, end_utc)

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
st.markdown('<div class="section-title">Today’s Active Rumor</div>', unsafe_allow_html=True)

rumor_direction = active_rumor.get("rumor_direction", "")
rumor_class = "rumor-buy" if rumor_direction == "Buy-In" else "rumor-leave" if rumor_direction == "Leave" else "rumor-neutral"
rumor_title = "🟢 Buy-In Rumor" if rumor_direction == "Buy-In" else "🔴 Leave Rumor" if rumor_direction == "Leave" else "ℹ️ No Actionable Rumor"

if active_rumor.get("active_rumor"):
    st.markdown(
        f"""
        <div class="rumor-banner {rumor_class}">
            <div class="rumor-title">{rumor_title}</div>
            <div class="rumor-meta">
                {active_rumor.get("rumor_time_label", "")} &nbsp;|&nbsp;
                {active_rumor.get("rumor_author", "") or "Unknown author"}
            </div>
            <div class="rumor-text">{active_rumor.get("active_rumor", "")}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )
else:
    st.markdown(
        """
        <div class="rumor-banner rumor-neutral">
            <div class="rumor-title">ℹ️ No Actionable Rumor</div>
            <div class="rumor-text">No current-day buy-in or leave rumor was found for this ticker.</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

# -----------------------------
# Driver view
# -----------------------------
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

# -----------------------------
# Time window chart
# -----------------------------
st.markdown("---")
st.markdown('<div class="section-title">Ticker Time Window</div>', unsafe_allow_html=True)

bucket_df = agg_time_buckets_for_ticker(
    cfg,
    ticker,
    start_utc,
    end_utc,
    bucket_minutes=int(detail_filters["bucket_minutes"]),
)

view_mode = st.radio(
    "Chart view",
    ["Sentiment", "Message Volume"],
    horizontal=True,
)

if not bucket_df.empty:
    bucket_df["bucket_start_et"] = pd.to_datetime(bucket_df["bucket_start_et"], errors="coerce")

    rumor_time = active_rumor.get("rumor_time_et", None)
    rumor_color = "#22c55e" if rumor_direction == "Buy-In" else "#ef4444"

    if view_mode == "Sentiment":
        fig = go.Figure()

        fig.add_trace(
            go.Scatter(
                x=bucket_df["bucket_start_et"],
                y=bucket_df["sentiment_score"],
                mode="lines+markers",
                name="Sentiment",
                line=dict(color="#a855f7", width=3),
                marker=dict(size=7),
            )
        )

        if rumor_time is not None and pd.notna(rumor_time):
            y_min = float(bucket_df["sentiment_score"].min())
            y_max = float(bucket_df["sentiment_score"].max())

            if y_min == y_max:
                y_min -= 0.1
                y_max += 0.1

            fig.add_trace(
                go.Scatter(
                    x=[rumor_time, rumor_time],
                    y=[y_min, y_max],
                    mode="lines",
                    name="Active Rumor",
                    line=dict(color=rumor_color, width=2, dash="dash"),
                    hovertext=[active_rumor.get("active_rumor", ""), active_rumor.get("active_rumor", "")],
                    hoverinfo="text",
                    showlegend=True,
                )
            )

        fig.update_layout(
            title=f"{ticker}: Sentiment Timeline",
            xaxis_title="Time (ET)",
            yaxis_title="Sentiment Score",
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            margin=dict(l=10, r=10, t=50, b=10),
        )
        st.plotly_chart(fig, use_container_width=True)

    else:
        fig = go.Figure()

        bar_colors = ["#22c55e" if val >= 0 else "#ef4444" for val in bucket_df["sentiment_score"]]

        fig.add_trace(
            go.Bar(
                x=bucket_df["bucket_start_et"],
                y=bucket_df["total_posts"],
                marker_color=bar_colors,
                name="Messages",
            )
        )

        if rumor_time is not None and pd.notna(rumor_time):
            y_max = float(bucket_df["total_posts"].max())
            if y_max <= 0:
                y_max = 1.0

            fig.add_trace(
                go.Scatter(
                    x=[rumor_time, rumor_time],
                    y=[0, y_max],
                    mode="lines",
                    name="Active Rumor",
                    line=dict(color=rumor_color, width=2, dash="dash"),
                    hovertext=[active_rumor.get("active_rumor", ""), active_rumor.get("active_rumor", "")],
                    hoverinfo="text",
                    showlegend=True,
                )
            )

        fig.update_layout(
            title=f"{ticker}: Message Volume Timeline",
            xaxis_title="Time (ET)",
            yaxis_title="Messages",
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            margin=dict(l=10, r=10, t=50, b=10),
        )
        st.plotly_chart(fig, use_container_width=True)

else:
    st.info("No bucket data in this window.")

# -----------------------------
# Latest messages
# -----------------------------
st.markdown("---")
st.markdown('<div class="section-title">Latest Messages</div>', unsafe_allow_html=True)

msgs = get_latest_messages(cfg, ticker, start_utc, end_utc, limit=int(detail_filters["max_messages"]))

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

st.caption("Tip: use Today, Last N, or Custom windows and click Apply in the sidebar for stable historical views.")