import argparse
import os
import json
import sqlite3
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from collections import Counter, defaultdict
import csv
import math

ET = ZoneInfo("America/New_York")

def parse_iso_utc(ts: str):
    if not ts:
        return None
    try:
        return datetime.fromisoformat(ts.replace("Z", "+00:00")).astimezone(timezone.utc)
    except Exception:
        return None

def utc_to_et(dt_utc: datetime) -> datetime:
    return dt_utc.astimezone(ET)

def net_sentiment(bullish: int, bearish: int, total: int) -> float:
    if total <= 0:
        return 0.0
    return (bullish - bearish) / total

def safe_std(vals):
    vals = [v for v in vals if v is not None]
    if len(vals) <= 1:
        return 0.0
    mu = sum(vals) / len(vals)
    var = sum((v - mu) ** 2 for v in vals) / (len(vals) - 1)
    return math.sqrt(var)

def ensure_parent_dir(path: str):
    d = os.path.dirname(os.path.abspath(path))
    if d and not os.path.exists(d):
        os.makedirs(d, exist_ok=True)

def main():
    ap = argparse.ArgumentParser(description="Export per-ticker metrics for a given ET time window from stocktwits.db -> append to CSV.")
    ap.add_argument("--db", required=True, help="Path to stocktwits.db")
    ap.add_argument("--out_csv", required=True, help="Output CSV path (appends)")
    ap.add_argument("--start_et", required=True, help='Window start in ET: "2026-02-04 12:00"')
    ap.add_argument("--end_et", required=True, help='Window end in ET: "2026-02-04 17:00"')
    ap.add_argument("--tickers", nargs="*", default=["AMD","NVDA","AAPL","TSLA", "APP", "SMX"], help="Tickers to include")
    args = ap.parse_args()

    tickers = [t.upper() for t in args.tickers]
    # Parse ET window -> UTC bounds
    start_et = datetime.strptime(args.start_et, "%Y-%m-%d %H:%M").replace(tzinfo=ET)
    end_et = datetime.strptime(args.end_et, "%Y-%m-%d %H:%M").replace(tzinfo=ET)
    start_utc = start_et.astimezone(timezone.utc)
    end_utc = end_et.astimezone(timezone.utc)

    # Pull rows from DB
    conn = sqlite3.connect(args.db)
    cur = conn.cursor()

    # We stored created_at as string UTC iso (Z). We'll filter in Python robustly.
    q = f"""
    SELECT stream_symbol, created_at, sentiment, notes, keywords_json, ticker_mentions_json
    FROM messages
    WHERE stream_symbol IN ({",".join(["?"]*len(tickers))})
    """
    cur.execute(q, tickers)
    rows = cur.fetchall()
    conn.close()

    # Aggregate per ticker within window
    per = {t: {
        "total": 0,
        "bullish": 0,
        "bearish": 0,
        "unlabeled": 0,
        "hour_bins": defaultdict(lambda: {"bullish":0,"bearish":0,"total":0}),
        "theme_counts": Counter(),
        "keyword_counts": Counter(),
        "mentions_counts": Counter(),   # counts of $mentions across posts (by ticker mention)
        "spillover_targets": Counter(), # only mentions of OTHER tickers from within a ticker's stream
    } for t in tickers}

    for sym, created_at, sentiment, notes, keywords_json, ticker_mentions_json in rows:
        sym = (sym or "").upper()
        dt_utc = parse_iso_utc(created_at)
        if dt_utc is None:
            continue
        if not (start_utc <= dt_utc < end_utc):
            continue

        bucket = per[sym]
        bucket["total"] += 1

        s = (sentiment or "null").lower()
        if s == "bullish":
            bucket["bullish"] += 1
        elif s == "bearish":
            bucket["bearish"] += 1
        else:
            bucket["unlabeled"] += 1

        # Themes from notes (comma-separated flags)
        if notes:
            for flag in notes.split(","):
                flag = flag.strip()
                if flag:
                    bucket["theme_counts"][flag] += 1

        # Keywords
        if keywords_json:
            try:
                kws = json.loads(keywords_json)
                for k in kws:
                    bucket["keyword_counts"][str(k).upper()] += 1
            except Exception:
                pass

        # Ticker mentions
        mentions = []
        if ticker_mentions_json:
            try:
                mentions = [m.upper() for m in json.loads(ticker_mentions_json)]
            except Exception:
                mentions = []

        # Count all mentions
        for m in mentions:
            bucket["mentions_counts"][m] += 1

        # Spillover = mentions of other tracked tickers (excluding itself)
        for other in tickers:
            if other != sym and other in mentions:
                bucket["spillover_targets"][other] += 1

        # Hourly net sentiment in ET
        dt_et = utc_to_et(dt_utc)
        hour_et = dt_et.strftime("%H")  # "14", "15", etc.
        hb = bucket["hour_bins"][hour_et]
        hb["total"] += 1
        if s == "bullish":
            hb["bullish"] += 1
        elif s == "bearish":
            hb["bearish"] += 1

    # Prepare rows to append
    out_rows = []
    date_str = start_et.strftime("%Y-%m-%d")

    for t in tickers:
        b = per[t]
        total = b["total"]
        bull = b["bullish"]
        bear = b["bearish"]
        unl = b["unlabeled"]

        ns = net_sentiment(bull, bear, total)

        # Hourly net sentiment series (ET hours inside window)
        hourly = []
        hours_sorted = sorted(b["hour_bins"].keys())
        for h in hours_sorted:
            hb = b["hour_bins"][h]
            hourly.append(net_sentiment(hb["bullish"], hb["bearish"], hb["total"]))

        vol = safe_std(hourly)
        max_h = max(hourly) if hourly else 0.0
        min_h = min(hourly) if hourly else 0.0
        rng = max_h - min_h

        # momentum: largest absolute change hour-to-hour
        largest_change = 0.0
        if len(hourly) >= 2:
            diffs = [hourly[i] - hourly[i-1] for i in range(1, len(hourly))]
            largest_change = max((abs(d) for d in diffs), default=0.0)

        # spillover summary
        spill_total = sum(b["spillover_targets"].values())
        top_target, top_count = ("", 0)
        if b["spillover_targets"]:
            top_target, top_count = b["spillover_targets"].most_common(1)[0]
        top_pct = (top_count / spill_total) if spill_total > 0 else 0.0

        # Top keywords (store as compact string)
        top_keywords = [k for k, _ in b["keyword_counts"].most_common(12)]
        top_keywords_str = "|".join(top_keywords)

        # Theme flags
        theme = b["theme_counts"]
        mentions_earnings = theme.get("mentions_earnings", 0)
        mentions_options = theme.get("mentions_options", 0)
        has_link = theme.get("has_link", 0)
        mentions_chart = theme.get("mentions_chart", 0)

        out_rows.append({
            "date": date_str,
            "window_start_et": start_et.strftime("%Y-%m-%d %H:%M"),
            "window_end_et": end_et.strftime("%Y-%m-%d %H:%M"),
            "ticker": t,

            "total_posts": total,
            "bullish": bull,
            "bearish": bear,
            "unlabeled": unl,
            "bullish_pct": (bull / total) if total else 0.0,
            "bearish_pct": (bear / total) if total else 0.0,
            "net_sentiment": ns,

            "sentiment_volatility": vol,
            "max_hourly_sentiment": max_h,
            "min_hourly_sentiment": min_h,
            "sentiment_range": rng,
            "largest_hourly_change": largest_change,

            "spillover_mentions_total": spill_total,
            "spillover_top_target": top_target,
            "spillover_top_target_pct": top_pct,

            "mentions_earnings_count": mentions_earnings,
            "mentions_options_count": mentions_options,
            "has_link_count": has_link,
            "mentions_chart_count": mentions_chart,

            "top_keywords_12": top_keywords_str
        })

    # Append to CSV (write header only if file doesn't exist)
    ensure_parent_dir(args.out_csv)
    file_exists = os.path.exists(args.out_csv)

    fieldnames = list(out_rows[0].keys()) if out_rows else []
    with open(args.out_csv, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        if not file_exists:
            w.writeheader()
        for r in out_rows:
            w.writerow(r)

    print(f"✅ Appended {len(out_rows)} rows to: {args.out_csv}")
    print(f"Window ET: {args.start_et} -> {args.end_et} | Ticketers: {', '.join(tickers)}")

if __name__ == "__main__":
    main()