import streamlit as st
import plotly.express as px
import pandas as pd
from zoneinfo import ZoneInfo
from datetime import datetime

from mongo_rt import (
    MongoCfg,
    parse_window,
    agg_ticker_summary,
    load_latest_finviz,
    get_active_rumors_for_tickers,
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


st.set_page_config(page_title="Market Intelligence Engine", layout="wide")

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
        background: linear-gradient(135deg, #1d4ed8 0%, #7c3aed 100%);
        padding: 1.25rem 1.5rem;
        border-radius: 18px;
        color: white;
        margin-bottom: 1rem;
        box-shadow: 0 10px 30px rgba(0,0,0,0.25);
    }
    .hero-title {
        font-size: 2rem;
        font-weight: 800;
        margin: 0;
    }
    .hero-subtitle {
        font-size: 0.98rem;
        opacity: 0.92;
        margin-top: 0.35rem;
    }
    .section-title {
        font-size: 1.15rem;
        font-weight: 700;
        margin-top: 0.25rem;
        margin-bottom: 0.5rem;
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
        font-size: 1.5rem;
        font-weight: 800;
        line-height: 1.1;
    }
    .table-note {
        font-size: 0.9rem;
        color: #cbd5e1;
        margin-top: 0.25rem;
    }
    .status-banner {
        background: linear-gradient(135deg, #0ea5e9, #7c3aed);
        padding: 1rem 1.2rem;
        border-radius: 16px;
        color: white;
        margin-bottom: 1rem;
        box-shadow: 0 8px 24px rgba(0,0,0,0.20);
    }
    .status-title {
        font-size: 1.05rem;
        font-weight: 800;
        margin-bottom: 0.35rem;
    }
    .status-text {
        font-size: 0.95rem;
        opacity: 0.95;
    }
    .ticker-card {
        background: #18212f;
        border-radius: 14px;
        padding: 0.8rem 0.9rem;
        color: white;
        margin-bottom: 0.75rem;
        box-shadow: 0 8px 18px rgba(0,0,0,0.18);
    }
    .ticker-name {
        font-size: 1rem;
        font-weight: 800;
        margin-bottom: 0.35rem;
    }
    .ticker-meta {
        font-size: 0.86rem;
        color: #dbeafe;
        line-height: 1.45;
    }
    .insight-box {
        background: linear-gradient(135deg, #1f2937, #111827);
        border-left: 4px solid #38bdf8;
        border-radius: 12px;
        padding: 0.85rem 1rem;
        color: #f8fafc;
        margin-bottom: 0.55rem;
    }
    @keyframes fadeIn {
        from { opacity: 0; transform: translateY(6px); }
        to { opacity: 1; transform: translateY(0); }
    }
    .fade {
        animation: fadeIn 0.8s ease-in-out;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

st.markdown(
    """
    <div class="hero-card fade">
        <div class="hero-title">📈 Market Intelligence Engine</div>
        <div class="hero-subtitle">
            Real-time social sentiment, one active rumor per ticker, message density, and market context
        </div>
    </div>
    """,
    unsafe_allow_html=True,
)

cfg = MongoCfg()

# IMPORTANT:
# No auto-overwriting custom dates anymore.
DEFAULTS = dict(
    window_type="Custom",
    preset="30 minutes",
    minutes=30,
    custom_start="2026-02-06 06:00",
    custom_end=now_et_str(),
    min_posts=0,
    sort_by="total_posts",
    sort_dir="Descending",
    top_n=300,
    refresh_seconds=10,
    auto_refresh=False,
)

if "filters" not in st.session_state:
    st.session_state["filters"] = DEFAULTS.copy()

applied = st.session_state["filters"]

st.sidebar.header("Controls")

with st.sidebar.form("controls_form", clear_on_submit=False):
    st.markdown("### Thresholds")

    window_type = st.radio(
        "Window type",
        ["Last N", "Custom", "All Time"],
        horizontal=True,
        index=["Last N", "Custom", "All Time"].index(applied["window_type"]),
    )

    preset = st.selectbox(
        "Preset",
        ["5 minutes", "30 minutes", "1 hour", "6 hours", "24 hours", "7 days"],
        index=["5 minutes", "30 minutes", "1 hour", "6 hours", "24 hours", "7 days"].index(applied["preset"]),
        disabled=(window_type != "Last N"),
    )

    preset_to_minutes = {
        "5 minutes": 5,
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
        value=int(applied["minutes"]),
        step=1,
        disabled=(window_type != "Last N"),
    )

    custom_start = st.text_input(
        'Custom start (ET) "YYYY-MM-DD HH:MM"',
        value=applied["custom_start"],
        disabled=(window_type != "Custom"),
    )

    custom_end = st.text_input(
        'Custom end (ET) "YYYY-MM-DD HH:MM"',
        value=applied["custom_end"],
        disabled=(window_type != "Custom"),
    )

    min_posts = st.slider("Minimum posts per ticker", 0, 200, int(applied["min_posts"]), 1)

    st.markdown("---")
    st.markdown("### Sorting")

    sort_options = [
        "density_per_min",
        "sentiment_score",
        "total_posts",
        "bullish",
        "bearish",
        "unlabeled",
        "traditional_posts",
        "social_posts",
        "rumor_posts",
        "relative_volume",
        "price_change_num",
        "volume",
    ]

    prev_sort = applied["sort_by"] if applied["sort_by"] in sort_options else "density_per_min"
    sort_by = st.selectbox("Sort by", sort_options, index=sort_options.index(prev_sort))

    sort_dir = st.radio(
        "Direction",
        ["Descending", "Ascending"],
        horizontal=True,
        index=0 if applied["sort_dir"] == "Descending" else 1,
    )

    top_n = st.slider("Show top N", 10, 300, int(applied["top_n"]), 5)

    st.markdown("---")
    st.markdown("### Auto Refresh")

    refresh_seconds = st.slider("Refresh every (seconds)", 3, 60, int(applied["refresh_seconds"]), 1)
    auto_refresh = st.checkbox("Enable auto-refresh", value=bool(applied["auto_refresh"]))

    colA, colB = st.columns(2)
    apply_clicked = colA.form_submit_button("✅ Apply")
    reset_clicked = colB.form_submit_button("↩️ Reset")

if apply_clicked:
    chosen_minutes = int(minutes)
    if window_type == "Last N" and chosen_minutes <= 0:
        chosen_minutes = int(preset_to_minutes.get(preset, 30))

    st.session_state["filters"] = dict(
        window_type=window_type,
        preset=preset,
        minutes=chosen_minutes,
        custom_start=custom_start.strip(),
        custom_end=custom_end.strip(),
        min_posts=int(min_posts),
        sort_by=sort_by,
        sort_dir=sort_dir,
        top_n=int(top_n),
        refresh_seconds=int(refresh_seconds),
        auto_refresh=bool(auto_refresh),
    )
    st.rerun()

if reset_clicked:
    st.session_state["filters"] = DEFAULTS.copy()
    st.rerun()

applied = st.session_state["filters"]

if applied["auto_refresh"]:
    if st_autorefresh is None:
        st.sidebar.warning("Install: pip install streamlit-autorefresh")
    else:
        st_autorefresh(interval=int(applied["refresh_seconds"]) * 1000, key="live_refresh")

# Robust window parsing with visible error
try:
    if applied["window_type"] == "Last N":
        start_utc, end_utc = parse_window("last_n", last_n=int(applied["minutes"]), unit="minutes")
    elif applied["window_type"] == "Custom":
        if not applied["custom_start"] or not applied["custom_end"]:
            st.warning("Enter custom times, then click Apply.")
            st.stop()
        start_utc, end_utc = parse_window(
            "custom_et",
            start_et=applied["custom_start"],
            end_et=applied["custom_end"],
        )
    else:
        start_utc, end_utc = parse_window("all_time")
except Exception as e:
    st.error(f"Window error: {e}")
    st.stop()

df = agg_ticker_summary(cfg, start_utc, end_utc)
finviz = load_latest_finviz()

if not finviz.empty and not df.empty:
    df = df.merge(finviz, on="stream_symbol", how="left")

for c in ["relative_volume", "price_change_num", "volume"]:
    if c in df.columns:
        df[c] = pd.to_numeric(df[c], errors="coerce")

if not df.empty:
    df = df[df["total_posts"] >= applied["min_posts"]]

ascending = (applied["sort_dir"] == "Ascending")
if not df.empty and applied["sort_by"] in df.columns:
    df = df.sort_values(applied["sort_by"], ascending=ascending)

if not df.empty:
    df = df.head(applied["top_n"]).reset_index(drop=True)

if not df.empty:
    rumor_df = get_active_rumors_for_tickers(cfg, df["stream_symbol"].astype(str).tolist(), start_utc, end_utc)
    if not rumor_df.empty:
        df = df.merge(
            rumor_df[["stream_symbol", "active_rumor", "rumor_direction", "rumor_time_label"]],
            on="stream_symbol",
            how="left",
        )

if not df.empty:
    st.session_state["last_live_tickers"] = df["stream_symbol"].astype(str).tolist()
else:
    st.session_state["last_live_tickers"] = []

total_tickers = int(len(df)) if not df.empty else 0
total_posts = int(df["total_posts"].sum()) if (not df.empty and "total_posts" in df.columns) else 0
tickers_with_active_rumor = int(df["active_rumor"].fillna("").ne("").sum()) if (not df.empty and "active_rumor" in df.columns) else 0
total_traditional = int(df["traditional_posts"].sum()) if (not df.empty and "traditional_posts" in df.columns) else 0
avg_sentiment = float(df["sentiment_score"].mean()) if (not df.empty and "sentiment_score" in df.columns) else 0.0

market_sentiment = "Neutral"
if avg_sentiment > 0.2:
    market_sentiment = "Bullish"
elif avg_sentiment < -0.2:
    market_sentiment = "Bearish"

rumor_level = "Low"
if total_tickers > 0 and tickers_with_active_rumor > total_tickers * 0.4:
    rumor_level = "High"
elif total_tickers > 0 and tickers_with_active_rumor > total_tickers * 0.2:
    rumor_level = "Medium"

top_ticker = df.iloc[0]["stream_symbol"] if not df.empty else "N/A"

st.markdown(
    f"""
    <div class="status-banner fade">
        <div class="status-title">🧭 Market Status</div>
        <div class="status-text">
            Sentiment: <b>{market_sentiment}</b> &nbsp;|&nbsp;
            Active Rumor Level: <b>{rumor_level}</b> &nbsp;|&nbsp;
            Top Ticker: <b>{top_ticker}</b> &nbsp;|&nbsp;
            Window Start: <b>{start_utc.astimezone(ET).strftime('%m/%d %I:%M %p')}</b>
        </div>
    </div>
    """,
    unsafe_allow_html=True,
)

c1, c2, c3, c4, c5 = st.columns(5)

with c1:
    st.markdown(f'<div class="mini-card mini-blue fade"><div class="mini-label">Tickers in View</div><div class="mini-value">{total_tickers}</div></div>', unsafe_allow_html=True)
with c2:
    st.markdown(f'<div class="mini-card mini-purple fade"><div class="mini-label">Total Clean Posts</div><div class="mini-value">{total_posts}</div></div>', unsafe_allow_html=True)
with c3:
    st.markdown(f'<div class="mini-card mini-red fade"><div class="mini-label">Tickers With Active Rumor</div><div class="mini-value">{tickers_with_active_rumor}</div></div>', unsafe_allow_html=True)
with c4:
    st.markdown(f'<div class="mini-card mini-green fade"><div class="mini-label">Traditional Posts</div><div class="mini-value">{total_traditional}</div></div>', unsafe_allow_html=True)
with c5:
    st.markdown(f'<div class="mini-card mini-orange fade"><div class="mini-label">Average Sentiment</div><div class="mini-value">{avg_sentiment:.3f}</div></div>', unsafe_allow_html=True)

display_name_map = {
    "stream_symbol": "Ticker",
    "total_posts": "Total Posts",
    "bullish": "Bullish",
    "bearish": "Bearish",
    "unlabeled": "Unlabeled",
    "traditional_posts": "Traditional Posts",
    "social_posts": "Social Posts",
    "rumor_posts": "Rumor Posts",
    "sentiment_score": "Sentiment Score",
    "density_per_min": "Density / Min",
    "relative_volume": "Relative Volume",
    "price_change": "Price Change",
    "price_change_num": "Price Change (%)",
    "volume": "Volume",
    "active_rumor": "Active Rumor",
    "rumor_direction": "Rumor Direction",
    "rumor_time_label": "Rumor Time (ET)",
}

st.markdown('<div class="section-title">Featured Tickers</div>', unsafe_allow_html=True)

if df.empty:
    st.info("No featured tickers available.")
else:
    top_cards = df.head(10).copy()
    card_cols = st.columns(5)

    for i, (_, row) in enumerate(top_cards.iterrows()):
        col = card_cols[i % 5]
        sentiment_val = float(row.get("sentiment_score", 0))
        if sentiment_val > 0:
            border = "#22c55e"
        elif sentiment_val < 0:
            border = "#ef4444"
        else:
            border = "#38bdf8"

        col.markdown(
            f"""
            <div class="ticker-card fade" style="border-left: 4px solid {border};">
                <div class="ticker-name">{row.get('stream_symbol', 'N/A')}</div>
                <div class="ticker-meta">
                    Posts: <b>{int(row.get('total_posts', 0))}</b><br>
                    Sentiment: <b>{sentiment_val:.2f}</b><br>
                    Rumor Direction: <b>{row.get('rumor_direction', '') or '—'}</b><br>
                    Rumor Time: <b>{row.get('rumor_time_label', '') or '—'}</b>
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )
        if col.button(f"Open {row.get('stream_symbol', '')}", key=f"open_card_{row.get('stream_symbol', '')}"):
            st.session_state["ticker"] = str(row.get("stream_symbol", "")).strip().upper()
            st.switch_page("pages/2_Ticker_Detail.py")
            st.stop()

st.markdown('<div class="section-title">🧠 Insights Engine</div>', unsafe_allow_html=True)

insights = []

if avg_sentiment > 0.2:
    insights.append("Overall market sentiment is positive across the current clean-message window.")
elif avg_sentiment < -0.2:
    insights.append("Overall market sentiment is negative across the current clean-message window.")
else:
    insights.append("Overall market sentiment is relatively balanced in the current window.")

if tickers_with_active_rumor > 0:
    insights.append(f"There are {tickers_with_active_rumor} tickers with one surfaced active rumor signal right now.")

if not df.empty:
    insights.append(f"{df.iloc[0]['stream_symbol']} is currently leading overall activity in this view.")

if "traditional_posts" in df.columns and "social_posts" in df.columns and not df.empty:
    if df["traditional_posts"].sum() > df["social_posts"].sum():
        insights.append("Traditional-source coverage is stronger than social/rumor-social coverage right now.")
    else:
        insights.append("Social/rumor-social coverage is dominating the conversation right now.")

for insight in insights:
    st.markdown(f'<div class="insight-box fade">• {insight}</div>', unsafe_allow_html=True)

st.markdown('<div class="section-title">Signal Panels</div>', unsafe_allow_html=True)

top_a, top_b, top_c = st.columns(3)

with top_a:
    st.markdown("### 🚨 Active Rumor Tickers")
    if df.empty or "active_rumor" not in df.columns:
        st.info("No rumor data available")
    else:
        rumor_df = df[df["active_rumor"].fillna("") != ""].copy().head(10)
        if rumor_df.empty:
            st.info("No active rumor tickers available")
        else:
            show = ["stream_symbol", "rumor_direction", "rumor_time_label", "active_rumor"]
            st.dataframe(rumor_df[show].rename(columns=display_name_map), use_container_width=True, hide_index=True)

with top_b:
    st.markdown("### 📰 Top Traditional Coverage")
    if df.empty or "traditional_posts" not in df.columns:
        st.info("No traditional-source data available")
    else:
        trad_df = df.sort_values("traditional_posts", ascending=False).head(10).copy()
        trad_show = [c for c in ["stream_symbol", "traditional_posts", "social_posts", "sentiment_score"] if c in trad_df.columns]
        st.dataframe(trad_df[trad_show].rename(columns=display_name_map), use_container_width=True, hide_index=True)

with top_c:
    st.markdown("### 📣 Top Social / Rumor-Social")
    if df.empty or "social_posts" not in df.columns:
        st.info("No social-source data available")
    else:
        social_df = df.sort_values("social_posts", ascending=False).head(10).copy()
        social_show = [c for c in ["stream_symbol", "social_posts", "traditional_posts", "sentiment_score"] if c in social_df.columns]
        st.dataframe(social_df[social_show].rename(columns=display_name_map), use_container_width=True, hide_index=True)

st.markdown("---")

col_left, col_right = st.columns([1.45, 1])

with col_left:
    st.markdown('<div class="section-title">Ticker Table — Check One Row Then Open</div>', unsafe_allow_html=True)

    if df.empty:
        st.info("No tickers found for this window.")
    else:
        display_df = df.copy().rename(columns=display_name_map)
        preferred_order = [
            "Ticker", "Total Posts", "Bullish", "Bearish", "Unlabeled",
            "Traditional Posts", "Social Posts", "Sentiment Score", "Density / Min",
            "Rumor Direction", "Rumor Time (ET)", "Active Rumor",
            "Relative Volume", "Price Change", "Price Change (%)", "Volume",
        ]
        cols = [c for c in preferred_order if c in display_df.columns]

        selectable_df = display_df[cols].copy()
        selectable_df.insert(0, "Open", False)

        edited = st.data_editor(
            selectable_df,
            use_container_width=True,
            hide_index=True,
            key="ticker_open_editor",
            column_config={
                "Open": st.column_config.CheckboxColumn("Open", help="Select one ticker to open"),
            },
            disabled=[c for c in selectable_df.columns if c != "Open"],
        )

        picked = edited[edited["Open"] == True]

        if len(picked) > 0:
            chosen = str(picked.iloc[0]["Ticker"]).strip().upper()
            st.session_state["ticker"] = chosen
            st.switch_page("pages/2_Ticker_Detail.py")
            st.stop()

        st.markdown(
            '<div class="table-note">The row-click version was unreliable here, so this uses a stable one-click Open checkbox.</div>',
            unsafe_allow_html=True
        )

with col_right:
    st.markdown('<div class="section-title">🎯 Market Intelligence Space</div>', unsafe_allow_html=True)
    st.caption("Visualizing sentiment, activity, and market momentum in one view.")

    if df.empty:
        st.info("No data to plot.")
    else:
        required = ["relative_volume", "price_change_num", "volume"]
        has_market_data = all(c in df.columns for c in required)

        if has_market_data:
            plot_df = df.copy()
            for c in required + ["sentiment_score", "density_per_min"]:
                plot_df[c] = pd.to_numeric(plot_df[c], errors="coerce")
            plot_df = plot_df.dropna(subset=required)

            fig = px.scatter_3d(
                plot_df,
                x="sentiment_score",
                y="density_per_min",
                z="relative_volume",
                size="volume",
                color="price_change_num",
                hover_name="stream_symbol",
                title="Sentiment × Density × Relative Volume",
                color_continuous_scale="RdYlGn",
            )
        else:
            fig = px.scatter_3d(
                df,
                x="sentiment_score",
                y="density_per_min",
                z="total_posts",
                hover_name="stream_symbol",
                title="Sentiment × Density × Posts",
                color="sentiment_score",
                color_continuous_scale="Turbo",
            )

        fig.update_layout(
            paper_bgcolor="rgba(0,0,0,0)",
            margin=dict(l=0, r=0, t=45, b=0),
        )
        st.plotly_chart(fig, use_container_width=True)

st.caption(
    f"Window: {start_utc.astimezone(ET).strftime('%Y-%m-%d %I:%M %p')} → "
    f"{end_utc.astimezone(ET).strftime('%Y-%m-%d %I:%M %p')} ET"
)