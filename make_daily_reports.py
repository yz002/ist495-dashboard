import argparse
import sqlite3
import pandas as pd
import json
from collections import Counter
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
import os

ET = ZoneInfo("America/New_York")

BASE_TICKERS_DEFAULT = ["AMD", "NVDA", "AAPL", "TSLA", "SMX", "APP"]

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

def load_msgs(conn, start_utc, end_utc, tickers):
    q = f"""
    SELECT stream_symbol, created_at, sentiment, keywords_json, ticker_mentions_json,
           notes, post, link
    FROM messages
    WHERE created_at >= ?
      AND created_at < ?
      AND stream_symbol IN ({",".join(["?"] * len(tickers))})
    """
    params = [start_utc.isoformat().replace("+00:00", "Z"),
              end_utc.isoformat().replace("+00:00", "Z")] + tickers
    return pd.read_sql(q, conn, params=params)

def normalize_sentiment(s):
    s = str(s) if s is not None else "null"
    s = s.strip().lower()
    # DB sometimes stores "Bullish"/"Bearish"
    if s == "bullish":
        return "bullish"
    if s == "bearish":
        return "bearish"
    return "unlabeled"

def compute_hourly(df):
    if df.empty:
        return pd.DataFrame()

    # created_at looks like "2026-02-04T19:45:15Z"
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

def spillover_matrix(df, selected):
    # counts of posts in base stream that mention target ticker
    mat = pd.DataFrame(0, index=selected, columns=selected, dtype=int)
    if df.empty:
        return mat, mat.astype(float)

    for base in selected:
        base_rows = df[df["stream_symbol"] == base]
        for target in selected:
            if base == target:
                mat.loc[base, target] = 0
                continue
            cnt = 0
            for v in base_rows["ticker_mentions_json"]:
                mentions = parse_json_list(v)
                if target in [m.upper() for m in mentions]:
                    cnt += 1
            mat.loc[base, target] = cnt

    pct = mat.div(mat.sum(axis=1).replace(0, 1), axis=0)
    return mat, pct

def load_metrics_csv(metrics_csv_path):
    if not metrics_csv_path or not os.path.exists(metrics_csv_path):
        return None
    try:
        return pd.read_csv(metrics_csv_path)
    except Exception:
        return None

def write_report(out_path, date_str, start_et, end_et, df, hourly, metrics_snapshot=None):
    lines = []
    lines.append("=" * 70)
    lines.append("DAILY SOCIAL FINANCIAL SENTIMENT REPORT (Stocktwits)")
    lines.append(f"Analysis Date (ET): {date_str}")
    lines.append(f"Window (ET): {start_et.isoformat()} -> {end_et.isoformat()}")
    lines.append(f"Window (UTC): {to_utc(start_et).isoformat().replace('+00:00','Z')} -> {to_utc(end_et).isoformat().replace('+00:00','Z')}")
    lines.append("=" * 70)
    lines.append("")

    total_rows = len(df)
    lines.append(f"Total rows (selected streams, all-day ET): {total_rows}")
    lines.append("")

    if df.empty:
        lines.append("No data found in this window. (Either not scraped that day or outside time range.)")
        with open(out_path, "w", encoding="utf-8") as f:
            f.write("\n".join(lines))
        return

    # sentiment normalize
    df = df.copy()
    df["sent_norm"] = df["sentiment"].apply(normalize_sentiment)

    # volume by symbol
    by_symbol = df.groupby("stream_symbol").size().sort_values(ascending=False)
    lines.append("=== ATTENTION / VOLUME ===")
    for sym, n in by_symbol.items():
        share = n / total_rows if total_rows else 0
        lines.append(f"- {sym}: {n} posts ({share:.1%})")
    lines.append("")

    # sentiment by symbol
    lines.append("=== SENTIMENT SUMMARY ===")
    for sym, g in df.groupby("stream_symbol"):
        bullish = (g["sent_norm"] == "bullish").sum()
        bearish = (g["sent_norm"] == "bearish").sum()
        unlabeled = (g["sent_norm"] == "unlabeled").sum()
        total = len(g)
        net = (bullish - bearish) / total if total else 0
        lines.append(f"- {sym}: bullish={bullish}, bearish={bearish}, unlabeled={unlabeled}, total={total}, net={net:.6f}")
    lines.append("")

    # hourly table
    lines.append("=== HOURLY NET SENTIMENT (ET) ===")
    if hourly is not None and not hourly.empty:
        for sym, g in hourly.groupby("stream_symbol"):
            lines.append(f"[{sym}]")
            for _, r in g.iterrows():
                lines.append(
                    f"  hour={int(r['hour_et']):02d} "
                    f"bull={int(r['bullish'])} bear={int(r['bearish'])} unl={int(r['unlabeled'])} "
                    f"total={int(r['total'])} net={r['net_sentiment']:.6f} "
                    f"Δ={'' if pd.isna(r['sentiment_change']) else f'{r['sentiment_change']:.6f}'}"
                )
            lines.append("")
    else:
        lines.append("(No hourly breakdown available.)")
        lines.append("")

    # volatility
    lines.append("=== SENTIMENT VOLATILITY (STD of hourly net sentiment) ===")
    if hourly is not None and not hourly.empty:
        vol = hourly.groupby("stream_symbol")["net_sentiment"].std().sort_values(ascending=False)
        for sym, v in vol.items():
            lines.append(f"- {sym}: {v:.6f}")
    else:
        lines.append("(No volatility computed.)")
    lines.append("")

    # top keywords
    lines.append("=== TOP KEYWORDS (per ticker) ===")
    for sym, g in df.groupby("stream_symbol"):
        top_kw = top_k_from_lists(g["keywords_json"], k=12, upper=True)
        lines.append(f"- {sym}: " + ", ".join([f"{w}({c})" for w, c in top_kw]))
    lines.append("")

    # theme flags
    lines.append("=== NOTES / THEME FLAGS (per ticker) ===")
    for sym, g in df.groupby("stream_symbol"):
        flags = flags_count(g["notes"])
        if flags:
            lines.append(f"- {sym}: " + ", ".join([f"{k}({v})" for k, v in flags.most_common(10)]))
        else:
            lines.append(f"- {sym}: (none)")
    lines.append("")

    # Top mentioned tickers overall + per stream
    lines.append("=== TOP MENTIONED TICKERS (overall, from $TICKER mentions) ===")
    overall_mentions = Counter()
    for v in df["ticker_mentions_json"]:
        for m in parse_json_list(v):
            overall_mentions[str(m).upper()] += 1
    # don’t suppress base tickers; seeing cross-refs is useful
    for t, c in overall_mentions.most_common(20):
        lines.append(f"- {t}: {c}")
    lines.append("")

    lines.append("=== TOP MENTIONED TICKERS (per stream) ===")
    for sym, g in df.groupby("stream_symbol"):
        c = Counter()
        for v in g["ticker_mentions_json"]:
            for m in parse_json_list(v):
                c[str(m).upper()] += 1
        top = c.most_common(12)
        lines.append(f"- {sym}: " + ", ".join([f"{t}({n})" for t, n in top]))
    lines.append("")

    # spillover among base streams present that day
    selected = sorted(df["stream_symbol"].unique().tolist())
    # keep only known “main” streams if present
    preferred_order = [t for t in ["AMD","NVDA","AAPL","TSLA","SMX","SMCI"] if t in selected]
    selected = preferred_order if preferred_order else selected

    mat, pct = spillover_matrix(df, selected)
    lines.append("=== CROSS-TICKER SPILLOVER MATRIX (count of posts in stream mentioning target) ===")
    lines.append(mat.to_string())
    lines.append("")
    lines.append("=== NORMALIZED SPILLOVER (row-normalized %) ===")
    lines.append((pct * 100).round(1).to_string())
    lines.append("")

    # notable posts
    lines.append("=== NOTABLE POSTS (sample, per ticker) ===")
    for sym, g in df.groupby("stream_symbol"):
        notable = g[g["notes"].fillna("").astype(str).str.strip() != ""].head(6)
        lines.append(f"[{sym}]")
        if notable.empty:
            lines.append("  (none flagged)")
        else:
            for _, r in notable.iterrows():
                snippet = (r["post"] or "").replace("\n", " ").strip()
                if len(snippet) > 220:
                    snippet = snippet[:220] + "..."
                notes = str(r["notes"] or "").strip()
                link = str(r["link"] or "").strip()
                lines.append(f"  - {notes} | {link} | {snippet}")
        lines.append("")
    lines.append("")

    # optional metrics snapshot from CSV
    if metrics_snapshot is not None and not metrics_snapshot.empty:
        lines.append("=== DAILY_METRICS.CSV SNAPSHOT (if available) ===")
        lines.append(metrics_snapshot.to_string(index=False))
        lines.append("")

    # short interpretation (research-friendly)
    lines.append("=== INTERPRETATION (auto) ===")
    # strongest net sentiment
    per_sym_net = []
    for sym, g in df.groupby("stream_symbol"):
        bullish = (g["sent_norm"] == "bullish").sum()
        bearish = (g["sent_norm"] == "bearish").sum()
        total = len(g)
        net = (bullish - bearish) / total if total else 0
        per_sym_net.append((sym, net, total))
    per_sym_net.sort(key=lambda x: x[1], reverse=True)
    best = per_sym_net[0]
    worst = per_sym_net[-1]
    lines.append(f"- Most bullish stream (net): {best[0]} (net={best[1]:.3f}, n={best[2]})")
    lines.append(f"- Least bullish stream (net): {worst[0]} (net={worst[1]:.3f}, n={worst[2]})")
    # concentration
    top_sym, top_n = by_symbol.index[0], int(by_symbol.iloc[0])
    lines.append(f"- Attention concentration: top stream {top_sym} captured {top_n}/{total_rows} ({top_n/total_rows:.1%}) of posts.")
    # btc mention flag
    if "BTC" in overall_mentions:
        lines.append(f"- BTC appeared in mentions: {overall_mentions['BTC']} times (indicator of crypto spillover into equity streams).")
    lines.append("")
    lines.append("=== YOUR NOTES (fill in) ===")
    lines.append("- Major narrative drivers today:")
    lines.append("- Any unusual spikes / anomalies:")
    lines.append("- What to watch next session:")
    lines.append("")

    with open(out_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", required=True, help="Path to stocktwits.db")
    ap.add_argument("--out_dir", required=True, help="Output folder for txt reports")
    ap.add_argument("--dates", nargs="+", required=True, help="Dates in YYYY-MM-DD")
    ap.add_argument("--tickers", nargs="*", default=BASE_TICKERS_DEFAULT, help="Stream tickers to include")
    ap.add_argument("--metrics_csv", default="", help="Optional path to daily_sentiment_metrics.csv")
    args = ap.parse_args()

    os.makedirs(args.out_dir, exist_ok=True)

    conn = sqlite3.connect(args.db)
    metrics_df = load_metrics_csv(args.metrics_csv)

    tickers = [t.upper() for t in args.tickers]

    for date_str in args.dates:
        start_et, end_et = day_window_et(date_str)
        start_utc, end_utc = to_utc(start_et), to_utc(end_et)

        df = load_msgs(conn, start_utc, end_utc, tickers)
        hourly = compute_hourly(df)

        snapshot = None
        if metrics_df is not None and not metrics_df.empty:
            # metrics CSV has date as mixed formats; match both YYYY-MM-DD and M/D/YYYY style
            # We’ll match loosely by converting to a common day string.
            target_mdY = f"{start_et.month}/{start_et.day}/{start_et.year}"
            snapshot = metrics_df[metrics_df["date"].astype(str).isin([date_str, target_mdY])]

        out_path = os.path.join(args.out_dir, f"{date_str}_report.txt")
        write_report(out_path, date_str, start_et, end_et, df, hourly, snapshot)
        print(f"Wrote: {out_path} (rows={len(df)})")

    conn.close()

if __name__ == "__main__":
    main()
