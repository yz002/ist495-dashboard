import sqlite3
import pandas as pd
import json
from datetime import datetime
from zoneinfo import ZoneInfo
from collections import Counter

DB_PATH = "stocktwits.db"
TICKERS = ["AMD", "NVDA", "AAPL", "TSLA", "APP", "SMX"]

ET = ZoneInfo("America/New_York")

ET = ZoneInfo("America/New_York")
WINDOW_DATE = datetime.now(ET).strftime("%Y-%m-%d")  # <-- replaces "2026-02-04"
START_ET_STR = "6:00"
END_ET_STR   = "17:00"

def et_window_to_utc_iso(date_str: str, start_hhmm: str, end_hhmm: str):
    y, m, d = map(int, date_str.split("-"))
    sh, sm = map(int, start_hhmm.split(":"))
    eh, em = map(int, end_hhmm.split(":"))

    start_et = datetime(y, m, d, sh, sm, tzinfo=ET)
    end_et   = datetime(y, m, d, eh, em, tzinfo=ET)

    start_utc = start_et.astimezone(ZoneInfo("UTC"))
    end_utc   = end_et.astimezone(ZoneInfo("UTC"))
    # SQLite compares TEXT lexicographically; ISO UTC strings work fine.
    return (
        start_et, end_et,
        start_utc.isoformat().replace("+00:00", "Z"),
        end_utc.isoformat().replace("+00:00", "Z")
    )

def load_rows(conn, start_utc_iso, end_utc_iso):
    placeholders = ",".join(["?"] * len(TICKERS))
    q = f"""
        SELECT stream_symbol, created_at, post, sentiment, notes, link,
               ticker_mentions_json, keywords_json
        FROM messages
        WHERE stream_symbol IN ({placeholders})
          AND created_at >= ?
          AND created_at < ?
    """
    params = TICKERS + [start_utc_iso, end_utc_iso]
    return pd.read_sql(q, conn, params=params)

def parse_json_list(x):
    if x is None:
        return []
    if isinstance(x, list):
        return x
    try:
        return json.loads(x)
    except:
        return []

def main():
    start_et, end_et, start_utc_iso, end_utc_iso = et_window_to_utc_iso(
        WINDOW_DATE, START_ET_STR, END_ET_STR
    )

    print(f"=== WINDOW (ET) === {start_et} -> {end_et}")
    print(f"=== WINDOW (UTC) === {start_utc_iso} -> {end_utc_iso}\n")

    conn = sqlite3.connect(DB_PATH)

    df = load_rows(conn, start_utc_iso, end_utc_iso)

    print(f"Total rows (selected tickers, window): {len(df)}\n")

    # -----------------
    # 1) Rows by symbol
    # -----------------
    by_symbol = (
        df.groupby("stream_symbol")
          .size()
          .reset_index(name="n")
          .sort_values("n", ascending=False)
    )
    print("=== ROWS BY SYMBOL ===")
    print(by_symbol.to_string(index=False), "\n")

    # -------------------------
    # 2) Sentiment by symbol
    # -------------------------
    # Normalize null-ish values
    df["sentiment_clean"] = df["sentiment"].fillna("null").astype(str)
    df["sentiment_clean"] = df["sentiment_clean"].replace({"": "null", "None": "null"})

    sentiment = (
        df.assign(
            bullish=(df["sentiment_clean"].str.lower() == "bullish").astype(int),
            bearish=(df["sentiment_clean"].str.lower() == "bearish").astype(int),
            unlabeled=(~df["sentiment_clean"].str.lower().isin(["bullish", "bearish"])).astype(int),
        )
        .groupby("stream_symbol")[["bullish", "bearish", "unlabeled"]]
        .sum()
        .reset_index()
    )
    sentiment["total"] = sentiment["bullish"] + sentiment["bearish"] + sentiment["unlabeled"]
    sentiment["net_sentiment"] = (sentiment["bullish"] - sentiment["bearish"]) / sentiment["total"]
    sentiment = sentiment.sort_values("total", ascending=False)

    print("=== SENTIMENT BY SYMBOL ===")
    print(sentiment.to_string(index=False), "\n")

    # -------------------------
    # 3) Hourly net sentiment (ET)
    # -------------------------
    # -------------------------
# 3) Hourly net sentiment (ET) — includes unlabeled counts too
# -------------------------
    df["created_at_dt"] = pd.to_datetime(df["created_at"], utc=True, errors="coerce")
    df = df.dropna(subset=["created_at_dt"]).copy()

    df["created_at_et"] = df["created_at_dt"].dt.tz_convert("America/New_York")
    df["hour_et"] = df["created_at_et"].dt.hour

    df["is_bullish"] = (df["sentiment_clean"].str.lower() == "bullish").astype(int)
    df["is_bearish"] = (df["sentiment_clean"].str.lower() == "bearish").astype(int)
    df["is_unlabeled"] = (~df["sentiment_clean"].str.lower().isin(["bullish", "bearish"])).astype(int)

    hourly = (
        df.groupby(["stream_symbol", "hour_et"])[["is_bullish", "is_bearish", "is_unlabeled"]]
        .sum()
        .reset_index()
)

    hourly = hourly.rename(columns={
        "is_bullish": "bullish",
        "is_bearish": "bearish",
        "is_unlabeled": "unlabeled"
    })

    hourly["total"] = hourly["bullish"] + hourly["bearish"] + hourly["unlabeled"]
    hourly["net_sentiment"] = (hourly["bullish"] - hourly["bearish"]) / hourly["total"].replace(0, pd.NA)

    print("=== HOURLY NET SENTIMENT (ET) ===")
    print(hourly.to_string(index=False), "\n")

    # -------------------------
    # 4) Sentiment momentum (Δ)
    # -------------------------
    hourly_sorted = hourly.sort_values(["stream_symbol", "hour_et"])
    hourly_sorted["sentiment_change"] = hourly_sorted.groupby("stream_symbol")["net_sentiment"].diff()

    print("=== SENTIMENT MOMENTUM (Δ over time, ET hours) ===")
    print(hourly_sorted.to_string(index=False), "\n")

    # -------------------------
    # 5) Sentiment volatility (std)
    # -------------------------
    vol = (
        hourly_sorted.groupby("stream_symbol")["net_sentiment"]
        .std()
        .reset_index(name="sentiment_std")
        .sort_values("sentiment_std", ascending=False)
    )
    print("=== SENTIMENT VOLATILITY (STD of hourly net sentiment) ===")
    print(vol.to_string(index=False), "\n")

    # -------------------------
    # 6) Top keywords per ticker
    # -------------------------
    print("=== TOP KEYWORDS (per ticker) ===\n")
    for sym in TICKERS:
        sub = df[df["stream_symbol"] == sym]
        all_kw = []
        for x in sub["keywords_json"].tolist():
            all_kw.extend(parse_json_list(x))
        c = Counter([k.upper() for k in all_kw if k])
        print(f"[{sym}]", c.most_common(12))
        print()

    # -------------------------
    # 7) Top ticker mentions per ticker
    # -------------------------
    print("\n=== TOP TICKER MENTIONS (per ticker) ===\n")
    for sym in TICKERS:
        sub = df[df["stream_symbol"] == sym]
        all_tm = []
        for x in sub["ticker_mentions_json"].tolist():
            all_tm.extend(parse_json_list(x))
        c = Counter([t.upper() for t in all_tm if t])
        print(f"[{sym}]", c.most_common(12))
        print()

    # -------------------------
    # 8) Notes/theme flags per ticker
    # -------------------------
    print("\n=== NOTES/THEME FLAGS (per ticker) ===\n")
    for sym in TICKERS:
        sub = df[df["stream_symbol"] == sym]
        flags = []
        for s in sub["notes"].fillna("").astype(str):
            if s.strip():
                flags.extend([f for f in s.split(",") if f])
        c = Counter(flags)
        print(f"[{sym}]", c.most_common())
        print()

    # -------------------------
    # 9) Notable posts sample (same style as your 2/3)
    # -------------------------
    print("\n=== NOTABLE POSTS (sample, per ticker) ===\n")
    for sym in TICKERS:
        sub = df[df["stream_symbol"] == sym].copy()
        if sub.empty:
            continue

        # prioritize posts that have flags
        sub["flag_count"] = sub["notes"].fillna("").apply(lambda x: len([f for f in str(x).split(",") if f]))
        sub = sub.sort_values(["flag_count"], ascending=False).head(5)

        print(f"[{sym}]")
        for _, r in sub.iterrows():
            flags = r["notes"] if r["notes"] else "no_flags"
            print(f"- {flags} | {r['link']} | {str(r['post'])[:200]}")
        print()

    conn.close()

if __name__ == "__main__":
    main()