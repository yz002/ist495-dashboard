import argparse
import sqlite3
import pandas as pd
import json
from collections import Counter
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
import os

ET = ZoneInfo("America/New_York")

BASE_TICKERS_DEFAULT = ["AMD", "NVDA", "AAPL", "TSLA", "SMX", "SMCI", "OPENAI"]

# -----------------------------
# Helpers
# -----------------------------
def parse_json_list(x):
    if x is None:
        return []
    if isinstance(x, list):
        return x
    try:
        return json.loads(x)
    except Exception:
        return []

def to_utc(dt_et: datetime) -> datetime:
    return dt_et.astimezone(timezone.utc)

def day_window_et(date_str: str):
    # date_str: YYYY-MM-DD
    d = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=ET)
    start_et = d.replace(hour=0, minute=0, second=0, microsecond=0)
    end_et = start_et + timedelta(days=1)
    return start_et, end_et

def normalize_sentiment(s):
    s = str(s) if s is not None else "null"
    s = s.strip().lower()
    if s == "bullish":
        return "bullish"
    if s == "bearish":
        return "bearish"
    return "unlabeled"

def top_k_from_lists(series, k=12, upper=True):
    c = Counter()
    for v in series:
        for item in parse_json_list(v):
            s = str(item)
            c[s.upper() if upper else s] += 1
    return c.most_common(k)

def flags_count(notes_series):
    flags = Counter()
    for x in notes_series.fillna("").astype(str):
        for f in [t.strip() for t in x.split(",") if t.strip()]:
            flags[f] += 1
    return flags

# -----------------------------
# DB load
# -----------------------------
def load_msgs(conn, start_utc, end_utc, ticker: str):
    q = """
    SELECT stream_symbol, created_at, sentiment, keywords_json, ticker_mentions_json,
           notes, post, link
    FROM messages
    WHERE datetime(created_at) >= datetime(?)
      AND datetime(created_at) <  datetime(?)
      AND stream_symbol = ?
    """
    params = [
        start_utc.isoformat().replace("+00:00", "Z"),
        end_utc.isoformat().replace("+00:00", "Z"),
        ticker
    ]
    return pd.read_sql(q, conn, params=params)

# -----------------------------
# Hourly compute (per ticker)
# -----------------------------
def compute_hourly(df):
    if df.empty:
        return pd.DataFrame()

    ts = pd.to_datetime(df["created_at"], utc=True, errors="coerce")
    df = df.copy()
    df["created_at_utc"] = ts
    df = df.dropna(subset=["created_at_utc"])
    df["created_at_et"] = df["created_at_utc"].dt.tz_convert(ET)
    df["hour_et"] = df["created_at_et"].dt.strftime("%H").astype(int)

    df["sent_norm"] = df["sentiment"].apply(normalize_sentiment)

    hourly = (
        df.groupby(["stream_symbol", "hour_et"])
          .agg(
              bullish=("sent_norm", lambda x: (x == "bullish").sum()),
              bearish=("sent_norm", lambda x: (x == "bearish").sum()),
              unlabeled=("sent_norm", lambda x: (x == "unlabeled").sum()),
              total=("sent_norm", "size"),
          )
          .reset_index()
          .sort_values(["stream_symbol", "hour_et"])
    )
    hourly["net_sentiment"] = (hourly["bullish"] - hourly["bearish"]) / hourly["total"]
    hourly["sentiment_change"] = hourly.groupby("stream_symbol")["net_sentiment"].diff()
    return hourly

# -----------------------------
# Metrics CSV load + matching
# -----------------------------
def load_metrics_csv(metrics_csv_path):
    if not metrics_csv_path or not os.path.exists(metrics_csv_path):
        return None
    try:
        df = pd.read_csv(metrics_csv_path)
        # Standardize 'date' if needed
        if "date" in df.columns:
            df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.strftime("%Y-%m-%d")
        return df
    except Exception:
        return None

def metrics_row(metrics_df, date_str, ticker):
    if metrics_df is None or metrics_df.empty:
        return None
    if not {"date", "ticker"}.issubset(set(metrics_df.columns)):
        return None
    out = metrics_df[(metrics_df["date"] == date_str) & (metrics_df["ticker"].astype(str).str.upper() == ticker)]
    if out.empty:
        return None
    return out

# -----------------------------
# Write per-ticker report
# -----------------------------
def write_ticker_report(out_path, date_str, ticker, start_et, end_et, df, hourly, metrics_snapshot=None):
    lines = []
    lines.append("=" * 76)
    lines.append("DAILY SOCIAL FINANCIAL SENTIMENT REPORT (Stocktwits)")
    lines.append(f"Ticker: {ticker}")
    lines.append(f"Analysis Date (ET): {date_str}")
    lines.append(f"Window (ET): {start_et.isoformat()} -> {end_et.isoformat()}")
    lines.append(
        f"Window (UTC): {to_utc(start_et).isoformat().replace('+00:00','Z')} -> "
        f"{to_utc(end_et).isoformat().replace('+00:00','Z')}"
    )
    lines.append("=" * 76)
    lines.append("")

    if df.empty:
        lines.append("No data found for this ticker in this all-day window.")
        with open(out_path, "w", encoding="utf-8") as f:
            f.write("\n".join(lines))
        return

    df = df.copy()
    df["sent_norm"] = df["sentiment"].apply(normalize_sentiment)

    total = len(df)
    bullish = (df["sent_norm"] == "bullish").sum()
    bearish = (df["sent_norm"] == "bearish").sum()
    unlabeled = (df["sent_norm"] == "unlabeled").sum()
    net = (bullish - bearish) / total if total else 0

    lines.append("=== SUMMARY ===")
    lines.append(f"- Total posts: {total}")
    lines.append(f"- Bullish: {bullish} | Bearish: {bearish} | Unlabeled: {unlabeled}")
    lines.append(f"- Net sentiment: {net:.6f}")
    lines.append("")

    lines.append("=== HOURLY NET SENTIMENT (ET) ===")
    if hourly is not None and not hourly.empty:
        g = hourly.sort_values("hour_et")
        for _, r in g.iterrows():
            delta = ""
            if not pd.isna(r["sentiment_change"]):
                delta = f"{r['sentiment_change']:.6f}"
            lines.append(
                f"- hour={int(r['hour_et']):02d} "
                f"bull={int(r['bullish'])} bear={int(r['bearish'])} unl={int(r['unlabeled'])} "
                f"total={int(r['total'])} net={r['net_sentiment']:.6f} Δ={delta}"
            )
    else:
        lines.append("(No hourly breakdown available.)")
    lines.append("")

    lines.append("=== SENTIMENT VOLATILITY (STD of hourly net sentiment) ===")
    if hourly is not None and not hourly.empty:
        v = hourly["net_sentiment"].std()
        lines.append(f"- volatility_std: {0.0 if pd.isna(v) else float(v):.6f}")
    else:
        lines.append("(No volatility computed.)")
    lines.append("")

    lines.append("=== TOP KEYWORDS ===")
    top_kw = top_k_from_lists(df["keywords_json"], k=12, upper=True)
    lines.append(", ".join([f"{w}({c})" for w, c in top_kw]) if top_kw else "(none)")
    lines.append("")

    lines.append("=== TOP TICKER MENTIONS (from $TICKER mentions) ===")
    c = Counter()
    for v in df["ticker_mentions_json"]:
        for m in parse_json_list(v):
            c[str(m).upper()] += 1
    top_mentions = c.most_common(20)
    if top_mentions:
        for t, n in top_mentions:
            lines.append(f"- {t}: {n}")
    else:
        lines.append("(none)")
    lines.append("")

    lines.append("=== NOTES / THEME FLAGS ===")
    flags = flags_count(df["notes"])
    if flags:
        lines.append(", ".join([f"{k}({v})" for k, v in flags.most_common(12)]))
    else:
        lines.append("(none)")
    lines.append("")

    lines.append("=== NOTABLE POSTS (flagged) ===")
    notable = df[df["notes"].fillna("").astype(str).str.strip() != ""].head(8)
    if notable.empty:
        lines.append("(none flagged)")
    else:
        for _, r in notable.iterrows():
            snippet = (r["post"] or "").replace("\n", " ").strip()
            if len(snippet) > 240:
                snippet = snippet[:240] + "..."
            lines.append(f"- {str(r['notes']).strip()} | {str(r['link']).strip()} | {snippet}")
    lines.append("")

    if metrics_snapshot is not None and not metrics_snapshot.empty:
        lines.append("=== DAILY_METRICS.CSV ROW (if available) ===")
        lines.append(metrics_snapshot.to_string(index=False))
        lines.append("")

    lines.append("=== YOUR NOTES (fill in) ===")
    lines.append("- Major narrative drivers today:")
    lines.append("- Any unusual spikes / anomalies:")
    lines.append("- What to watch next session:")
    lines.append("")

    with open(out_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))

# -----------------------------
# Main
# -----------------------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", required=True, help="Path to stocktwits.db")
    ap.add_argument("--out_dir", required=True, help="Output folder for txt reports")
    ap.add_argument("--dates", nargs="+", required=True, help="Dates in YYYY-MM-DD")
    ap.add_argument("--tickers", nargs="*", default=BASE_TICKERS_DEFAULT, help="Tickers to generate reports for")
    ap.add_argument("--metrics_csv", default="", help="Optional path to daily_sentiment_metrics.csv")
    args = ap.parse_args()

    os.makedirs(args.out_dir, exist_ok=True)

    conn = sqlite3.connect(args.db)
    metrics_df = load_metrics_csv(args.metrics_csv)

    tickers = [t.upper() for t in args.tickers]

    for date_str in args.dates:
        start_et, end_et = day_window_et(date_str)
        start_utc, end_utc = to_utc(start_et), to_utc(end_et)

        for ticker in tickers:
            df = load_msgs(conn, start_utc, end_utc, ticker)
            hourly = compute_hourly(df)
            snap = metrics_row(metrics_df, date_str, ticker)
            out_path = os.path.join(args.out_dir, f"{date_str}_{ticker}_report.txt")
            write_ticker_report(out_path, date_str, ticker, start_et, end_et, df, hourly, snap)
            print(f"Wrote: {out_path} (rows={len(df)})")

    conn.close()

if __name__ == "__main__":
    main()
