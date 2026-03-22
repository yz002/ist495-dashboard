print("RUNNING FILE:", __file__)
print("RUN MARKER: 2026-02-12-FILTERED-V2")

import argparse
import sqlite3
import pandas as pd
import numpy as np
import re
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

ET = ZoneInfo("America/New_York")

def pct_to_float(x):
    # "135.19%" -> 135.19
    if pd.isna(x):
        return np.nan
    s = str(x).strip()
    if s.endswith("%"):
        s = s[:-1]
    try:
        return float(s)
    except:
        return np.nan

def infer_day_from_filename(path: str) -> str | None:
    """
    Tries to infer YYYY_MM_DD from finviz_YYYY_MM_DD.csv in the filename.
    Returns 'YYYY-MM-DD' or None.
    """
    m = re.search(r"finviz_(\d{4})_(\d{2})_(\d{2})", path.replace("\\", "/"))
    if not m:
        return None
    y, mo, d = m.group(1), m.group(2), m.group(3)
    return f"{y}-{mo}-{d}"

def et_window_to_utc(day_str: str, start_hour: int):
    """
    day_str = 'YYYY-MM-DD' interpreted in ET.
    Window: [day start_hour ET, next day start_hour ET)
    """
    day = datetime.strptime(day_str, "%Y-%m-%d").replace(tzinfo=ET)
    start_et = day.replace(hour=start_hour, minute=0, second=0, microsecond=0)
    end_et = start_et + timedelta(days=1)
    start_utc = start_et.astimezone(ZoneInfo("UTC"))
    end_utc = end_et.astimezone(ZoneInfo("UTC"))
    return start_utc, end_utc

def main():
    ap = argparse.ArgumentParser(description="Day-1 baseline: Finviz Change vs Stocktwits density/sentiment.")
    ap.add_argument("--db", required=True, help="Path to SQLite stocktwits db")
    ap.add_argument("--finviz_csv", required=True, help="Path to Finviz daily CSV (finviz_YYYY_MM_DD.csv)")
    ap.add_argument("--out_csv", default="", help="Optional output merged CSV path")
    ap.add_argument("--min_total_posts", type=int, default=5, help="Filter: minimum Stocktwits posts per ticker")
    ap.add_argument("--day", default="", help='Optional explicit day "YYYY-MM-DD" (overrides filename inference)')
    ap.add_argument("--start_hour_et", type=int, default=6, help="ET hour for daily window start (default 6)")
    ap.add_argument("--density_mode", choices=["fixed_window", "active_span"], default="fixed_window",
                    help="fixed_window = posts / 24h window; active_span = posts / (max-min) per ticker")
    args = ap.parse_args()

    # --- Load Finviz ---
    fin = pd.read_csv(args.finviz_csv, encoding="utf-8-sig")
    if "Ticker" not in fin.columns or "Change" not in fin.columns:
        raise ValueError(f"Finviz CSV must have columns Ticker and Change. Found: {fin.columns.tolist()}")

    fin["Ticker"] = fin["Ticker"].astype(str).str.strip().str.upper()
    fin["Change_num"] = fin["Change"].apply(pct_to_float)

    # --- Determine day window ---
    day_str = args.day.strip()
    if not day_str:
        day_str = infer_day_from_filename(args.finviz_csv) or ""
    if not day_str:
        raise ValueError('Could not infer day from filename. Provide --day "YYYY-MM-DD".')

    start_utc, end_utc = et_window_to_utc(day_str, args.start_hour_et)

    # --- Load Stocktwits messages (ONLY within window) ---
    conn = sqlite3.connect(args.db)

    q = """
    SELECT stream_symbol, sentiment, created_at
    FROM messages
    WHERE created_at >= ? AND created_at < ?
    """
    st = pd.read_sql_query(q, conn, params=(start_utc.isoformat().replace("+00:00", "Z"),
                                           end_utc.isoformat().replace("+00:00", "Z")))
    conn.close()

    st["stream_symbol"] = st["stream_symbol"].astype(str).str.strip().str.upper()
    st["sentiment"] = st["sentiment"].fillna("null").astype(str)

    # --- Aggregate Stocktwits per ticker (for that day window) ---
    g = st.groupby("stream_symbol")["sentiment"]

    agg = pd.DataFrame({
        "social_total_posts": g.size(),
        "social_bullish": g.apply(lambda s: (s == "Bullish").sum()),
        "social_bearish": g.apply(lambda s: (s == "Bearish").sum()),
        "social_unlabeled": g.apply(lambda s: (s == "null").sum()),
    }).reset_index().rename(columns={"stream_symbol": "Ticker"})

    labeled = (agg["social_bullish"] + agg["social_bearish"]).replace(0, np.nan)
    agg["social_sentiment_score"] = (agg["social_bullish"] - agg["social_bearish"]) / labeled
    agg["social_sentiment_score"] = agg["social_sentiment_score"].fillna(0.0)

    # --- Density ---
    if args.density_mode == "fixed_window":
        window_hours = (end_utc - start_utc).total_seconds() / 3600.0
        agg["message_density"] = agg["social_total_posts"] / window_hours
    else:
        # active_span density using per-ticker min/max inside the window
        st["created_at_dt"] = pd.to_datetime(st["created_at"], utc=True, errors="coerce")
        t = st.groupby("stream_symbol").agg(
            min_created=("created_at_dt", "min"),
            max_created=("created_at_dt", "max"),
            cnt=("created_at_dt", "size"),
        ).reset_index().rename(columns={"stream_symbol": "Ticker"})
        duration_hours = (t["max_created"] - t["min_created"]).dt.total_seconds() / 3600.0
        duration_hours = duration_hours.replace(0, np.nan)
        t["message_density"] = (t["cnt"] / duration_hours).fillna(0.0)
        agg = agg.merge(t[["Ticker", "message_density"]], on="Ticker", how="left")
        agg["message_density"] = agg["message_density"].fillna(0.0)

    agg["weighted_density"] = agg["message_density"] * agg["social_sentiment_score"]

    # --- Merge Finviz + Stocktwits ---
    merged = fin.merge(agg, on="Ticker", how="left")

    for c in ["social_total_posts","social_bullish","social_bearish","social_unlabeled",
              "social_sentiment_score","message_density","weighted_density"]:
        merged[c] = merged[c].fillna(0)

    # Filter AFTER merge
    merged_f = merged[merged["social_total_posts"] >= args.min_total_posts].copy()
    print("\nMin/Max social_total_posts in merged_f:",
      merged_f["social_total_posts"].min(),
      merged_f["social_total_posts"].max())

    # --- Correlations (filtered set) ---
    def corr(a, b):
        if len(a) < 3:
            return np.nan
        return float(pd.Series(a).corr(pd.Series(b)))

    d = corr(merged_f["message_density"], merged_f["Change_num"])
    s = corr(merged_f["social_sentiment_score"], merged_f["Change_num"])
    w = corr(merged_f["weighted_density"], merged_f["Change_num"])

    print("\n=== BASELINE (Same-day) ===")
    print(f"Finviz day (ET window): {day_str} {args.start_hour_et:02d}:00 -> next day {args.start_hour_et:02d}:00")
    print(f"DB rows in window: {len(st)}")
    print(f"Rows finviz: {len(fin)}")
    print(f"Rows used (min_total_posts={args.min_total_posts}): {len(merged_f)}")
    print(f"Corr density vs change:   {d}")
    print(f"Corr sentiment vs change: {s}")
    print(f"Corr weighted vs change:  {w}")

    print("\nDEBUG merged_f min social_total_posts:", merged_f["social_total_posts"].min())
    print("DEBUG does merged_f contain GLL?:", (merged_f["Ticker"] == "GLL").any())

    print("\nTop 15 by weighted_density (FILTERED):")
    show_cols = ["Ticker","Change","Change_num","social_total_posts","social_bullish","social_bearish",
                 "social_sentiment_score","message_density","weighted_density"]
    print(merged_f.sort_values("weighted_density", ascending=False)[show_cols].head(15).to_string(index=False))

    if args.out_csv.strip():
        merged.to_csv(args.out_csv, index=False)
        print(f"\nSaved merged baseline CSV: {args.out_csv}")

if __name__ == "__main__":
    main()