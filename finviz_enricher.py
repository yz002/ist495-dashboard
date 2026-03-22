# finviz_enricher.py
import argparse
import sqlite3
import pandas as pd
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

ET = ZoneInfo("America/New_York")

def parse_args():
    ap = argparse.ArgumentParser(description="Enrich Finviz screener CSV with Stocktwits social sentiment + message density.")
    ap.add_argument("--db", required=True, help="Path to stocktwits.db")
    ap.add_argument("--finviz_csv", required=True, help="Path to Finviz CSV (must contain 'Ticker' column)")
    ap.add_argument("--out_csv", required=True, help="Output enriched CSV path")

    # Window selection
    ap.add_argument("--window_minutes", type=int, default=60, help="Lookback window in minutes (e.g., 5, 60, 2880 for 2 days)")

    # Threshold filters (optional)
    ap.add_argument("--min_sentiment", type=float, default=None, help="Minimum net sentiment threshold (e.g., 0.1)")
    ap.add_argument("--max_sentiment", type=float, default=None, help="Maximum net sentiment threshold (e.g., 0.8)")
    ap.add_argument("--min_density", type=float, default=None, help="Minimum message density threshold (posts per minute)")
    ap.add_argument("--max_density", type=float, default=None, help="Maximum message density threshold (posts per minute)")

    # Sorting
    ap.add_argument("--sort_by", choices=["sentiment", "density", "weighted_density", "total_posts"], default=None)
    ap.add_argument("--desc", action="store_true", help="Sort descending (default asc)")

    # Optional: choose whether density should be scaled per 5 minutes
    ap.add_argument("--density_unit", choices=["per_minute", "per_5min"], default="per_minute")

    return ap.parse_args()

def normalize_sentiment(s):
    if s is None:
        return "unlabeled"
    s = str(s).strip().lower()
    if s == "bullish":
        return "bullish"
    if s == "bearish":
        return "bearish"
    return "unlabeled"

def iso_z(dt_utc: datetime) -> str:
    # DB stores created_at like 2026-02-09T23:58:52Z
    return dt_utc.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")

def fetch_metrics_for_ticker(conn: sqlite3.Connection, ticker: str, start_utc: datetime, end_utc: datetime):
    q = """
    SELECT sentiment
    FROM messages
    WHERE stream_symbol = ?
      AND created_at >= ?
      AND created_at < ?
    """
    df = pd.read_sql(q, conn, params=(ticker, iso_z(start_utc), iso_z(end_utc)))

    total = len(df)
    if total == 0:
        return {
            "total_posts": 0,
            "bullish": 0,
            "bearish": 0,
            "unlabeled": 0,
            "net_sentiment": 0.0,
            "net_sentiment_labeled_only": 0.0,
        }

    sent_norm = df["sentiment"].apply(normalize_sentiment)
    bullish = int((sent_norm == "bullish").sum())
    bearish = int((sent_norm == "bearish").sum())
    unlabeled = int((sent_norm == "unlabeled").sum())

    net = (bullish - bearish) / total if total else 0.0
    labeled = bullish + bearish
    net_labeled = (bullish - bearish) / labeled if labeled else 0.0

    return {
        "total_posts": total,
        "bullish": bullish,
        "bearish": bearish,
        "unlabeled": unlabeled,
        "net_sentiment": float(net),
        "net_sentiment_labeled_only": float(net_labeled),
    }

def main():
    args = parse_args()

    finviz = pd.read_csv(args.finviz_csv)
    if "Ticker" not in finviz.columns:
        raise ValueError("Finviz CSV must contain a 'Ticker' column.")

    finviz["Ticker"] = finviz["Ticker"].astype(str).str.upper().str.strip()
    tickers = finviz["Ticker"].dropna().unique().tolist()

    # Window: "now" in ET -> convert to UTC
    end_et = datetime.now(ET)
    end_utc = end_et.astimezone(timezone.utc)
    start_utc = end_utc - timedelta(minutes=args.window_minutes)

    # compute per ticker
    conn = sqlite3.connect(args.db)
    rows = []
    for t in tickers:
        m = fetch_metrics_for_ticker(conn, t, start_utc, end_utc)

        # density
        if args.density_unit == "per_minute":
            density = m["total_posts"] / args.window_minutes if args.window_minutes > 0 else 0.0
        else:
            # posts per 5 minutes
            blocks = (args.window_minutes / 5) if args.window_minutes > 0 else 1
            density = m["total_posts"] / blocks if blocks else 0.0

        weighted_density = density * abs(m["net_sentiment"])

        rows.append({
            "Ticker": t,
            "social_window_minutes": args.window_minutes,
            "social_window_end_et": end_et.strftime("%Y-%m-%d %H:%M:%S %Z"),
            "social_total_posts": m["total_posts"],
            "social_bullish": m["bullish"],
            "social_bearish": m["bearish"],
            "social_unlabeled": m["unlabeled"],
            "social_sentiment_score": m["net_sentiment"],
            "social_sentiment_labeled_only": m["net_sentiment_labeled_only"],
            "message_density": density,
            "weighted_density": weighted_density,
        })

    conn.close()

    metrics = pd.DataFrame(rows)

    # merge back into finviz
    out = finviz.merge(metrics, on="Ticker", how="left")

    # apply thresholds
    if args.min_sentiment is not None:
        out = out[out["social_sentiment_score"] >= args.min_sentiment]
    if args.max_sentiment is not None:
        out = out[out["social_sentiment_score"] <= args.max_sentiment]
    if args.min_density is not None:
        out = out[out["message_density"] >= args.min_density]
    if args.max_density is not None:
        out = out[out["message_density"] <= args.max_density]

    # sorting
    if args.sort_by:
        col = {
            "sentiment": "social_sentiment_score",
            "density": "message_density",
            "weighted_density": "weighted_density",
            "total_posts": "social_total_posts",
        }[args.sort_by]
        out = out.sort_values(col, ascending=not args.desc)

    out.to_csv(args.out_csv, index=False, encoding="utf-8")
    print(f"Saved enriched screener: {args.out_csv}")
    print(f"Window ET: {end_et.strftime('%Y-%m-%d %H:%M %Z')}  | Lookback minutes: {args.window_minutes}")
    print(f"Tickers processed: {len(tickers)}  | Rows out: {len(out)}")

if __name__ == "__main__":
    main()