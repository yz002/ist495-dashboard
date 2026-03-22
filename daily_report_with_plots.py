# daily_report_with_plots_mongo.py
import argparse
import os
import re
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from pathlib import Path

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

from pymongo import MongoClient

ET = ZoneInfo("America/New_York")
UTC = ZoneInfo("UTC")


def pct_to_float(x):
    if pd.isna(x):
        return np.nan
    s = str(x).strip()
    if s.endswith("%"):
        s = s[:-1]
    try:
        return float(s)
    except Exception:
        return np.nan


def infer_day_from_filename(path: str) -> str | None:
    m = re.search(r"finviz_(\d{4})_(\d{2})_(\d{2})", path.replace("\\", "/"))
    if not m:
        return None
    y, mo, d = m.group(1), m.group(2), m.group(3)
    return f"{y}-{mo}-{d}"


def et_window_to_utc(day_str: str, start_hour: int):
    day = datetime.strptime(day_str, "%Y-%m-%d").replace(tzinfo=ET)
    start_et = day.replace(hour=start_hour, minute=0, second=0, microsecond=0)
    end_et = start_et + timedelta(days=1)
    return start_et.astimezone(UTC), end_et.astimezone(UTC)


def iso_z(dt: datetime) -> str:
    return dt.astimezone(UTC).isoformat().replace("+00:00", "Z")


def corr(a, b):
    if len(a) < 3:
        return np.nan
    return float(pd.Series(a).corr(pd.Series(b)))


def save_excel_reports(
    out_dir: str,
    day_tag: str,
    fin: pd.DataFrame,
    agg: pd.DataFrame,
    merged: pd.DataFrame,
    merged_f: pd.DataFrame,
    summary_row: dict,
    top_weighted: pd.DataFrame,
):
    out_path = Path(out_dir)
    out_path.mkdir(parents=True, exist_ok=True)

    # --- Daily workbook (one file per day) ---
    daily_xlsx = out_path / f"daily_report_{day_tag}.xlsx"
    with pd.ExcelWriter(daily_xlsx, engine="openpyxl") as writer:
        pd.DataFrame([summary_row]).to_excel(writer, sheet_name="summary", index=False)
        top_weighted.to_excel(writer, sheet_name="top_weighted", index=False)
        merged_f.to_excel(writer, sheet_name="merged_filtered", index=False)
        merged.to_excel(writer, sheet_name="merged_all", index=False)
        fin.to_excel(writer, sheet_name="finviz_raw", index=False)
        agg.to_excel(writer, sheet_name="social_agg", index=False)

    # --- Master workbook (keeps growing) ---
    master_xlsx = out_path / "MASTER_daily_summaries.xlsx"
    summary_df = pd.DataFrame([summary_row])

    if master_xlsx.exists():
        old = pd.read_excel(master_xlsx, sheet_name="Summary")
        new = pd.concat([old, summary_df], ignore_index=True)

        # Replace Summary + replace/add day sheet
        with pd.ExcelWriter(master_xlsx, engine="openpyxl", mode="a", if_sheet_exists="replace") as writer:
            new.to_excel(writer, sheet_name="Summary", index=False)
            merged_f.to_excel(writer, sheet_name=day_tag, index=False)
    else:
        with pd.ExcelWriter(master_xlsx, engine="openpyxl") as writer:
            summary_df.to_excel(writer, sheet_name="Summary", index=False)
            merged_f.to_excel(writer, sheet_name=day_tag, index=False)

    print("\nSaved daily Excel:", daily_xlsx.resolve())
    print("Updated master Excel:", master_xlsx.resolve())


def main():
    ap = argparse.ArgumentParser(description="Daily stats + baseline merge + plots + Excel export (MongoDB).")
    ap.add_argument("--mongo_uri", default="mongodb://localhost:27017")
    ap.add_argument("--mongo_db", default="stocktwits")
    ap.add_argument("--mongo_collection", default="messages")
    ap.add_argument("--finviz_csv", required=True, help="Path to finviz_YYYY_MM_DD.csv")
    ap.add_argument("--day", default="", help='Optional day "YYYY-MM-DD" (overrides filename inference)')
    ap.add_argument("--start_hour_et", type=int, default=6, help="Daily window start hour ET (default 6)")
    ap.add_argument("--min_total_posts", type=int, default=20, help="Filter for baseline/plots")
    ap.add_argument(
        "--density_mode",
        choices=["fixed_window", "active_span"],
        default="fixed_window",
        help="fixed_window = posts/24h; active_span = posts/(max-min) per ticker",
    )
    ap.add_argument("--out_dir", default=".", help="Where to save plots")
    ap.add_argument("--show", action="store_true", help="Also display plots interactively")
    args = ap.parse_args()

    # ---- Day inference ----
    day = args.day.strip() or (infer_day_from_filename(args.finviz_csv) or "")
    if not day:
        raise ValueError('Could not infer day from filename. Provide --day "YYYY-MM-DD".')
    day_tag = day

    # ---- Dedicated reports folder next to script ----
    script_dir = Path(__file__).resolve().parent
    reports_dir = script_dir / "weekly_reports"
    reports_dir.mkdir(parents=True, exist_ok=True)
    print("\nREPORTS DIR:", reports_dir.resolve())

    start_utc, end_utc = et_window_to_utc(day, args.start_hour_et)
    start_z, end_z = iso_z(start_utc), iso_z(end_utc)

    # ---- Load Finviz ----
    fin = pd.read_csv(args.finviz_csv, encoding="utf-8-sig")
    if "Ticker" not in fin.columns or "Change" not in fin.columns:
        raise ValueError(f"Finviz CSV must have columns Ticker and Change. Found: {fin.columns.tolist()}")

    fin["Ticker"] = fin["Ticker"].astype(str).str.strip().str.upper()
    fin["Change_num"] = fin["Change"].apply(pct_to_float)

    # ---- MongoDB load ----
    client = MongoClient(args.mongo_uri)
    col = client[args.mongo_db][args.mongo_collection]

    # All-time stats
    total_rows_all = col.count_documents({})
    unique_all = len(col.distinct("stream_symbol"))

    # Min/Max created_at (created_at is ISO string "....Z" so lexicographic sort works)
    min_doc = col.find({}, {"created_at": 1}).sort("created_at", 1).limit(1)
    max_doc = col.find({}, {"created_at": 1}).sort("created_at", -1).limit(1)
    min_created = next(min_doc, {}).get("created_at", None)
    max_created = next(max_doc, {}).get("created_at", None)
    date_range = (min_created, max_created)

    # Window query
    q = {"created_at": {"$gte": start_z, "$lt": end_z}}
    window_count = col.count_documents(q)

    cursor = col.find(q, {"stream_symbol": 1, "sentiment": 1, "created_at": 1, "_id": 0})
    st = pd.DataFrame(list(cursor))

    # Normalize columns even if empty
    if st.empty:
        st = pd.DataFrame(columns=["stream_symbol", "sentiment", "created_at"])

    st["stream_symbol"] = st["stream_symbol"].astype(str).str.strip().str.upper()
    st["sentiment"] = st["sentiment"].fillna("null").astype(str)

    # ---- Daily stats output ----
    print(f"\nDAY (ET window): {day} {args.start_hour_et:02d}:00 -> next day {args.start_hour_et:02d}:00")
    print(f"UTC window: [{start_z}, {end_z})")

    print("\n--- TOTAL ROWS (ALL TIME) ---")
    print(total_rows_all)

    print("\n--- UNIQUE TICKERS (ALL TIME) ---")
    print(unique_all)

    print("\n--- DATE RANGE (ALL TIME) ---")
    print(date_range)

    print(f"\n--- DB ROWS IN WINDOW ({day}) ---")
    print(window_count)

    print(f"\n--- UNIQUE TICKERS IN WINDOW ({day}) ---")
    print(int(st["stream_symbol"].nunique()) if not st.empty else 0)

    if not st.empty:
        top = st.groupby("stream_symbol").size().sort_values(ascending=False).head(10)
        print(f"\n--- TOP 10 TICKERS BY COUNT (WINDOW {day}) ---")
        for sym, cnt in top.items():
            print((sym, int(cnt)))

        lt20 = st.groupby("stream_symbol").size()
        lt20 = lt20[lt20 < 20].sort_values()
        print(f"\n--- TICKERS WITH <20 POSTS (WINDOW {day}) ---")
        for sym, cnt in lt20.items():
            print((sym, int(cnt)))

        dist = st["sentiment"].value_counts(dropna=False)
        total = dist.sum() if len(dist) else 0
        print(f"\n--- SENTIMENT DISTRIBUTION (WINDOW {day}) ---")
        for k, v in dist.items():
            pct = (100.0 * v / total) if total else 0.0
            print(f"{k} {int(v)} ({pct:.2f}%)")
    else:
        print("\n(No messages found in this window.)")

    # ---- Baseline aggregation ----
    g = st.groupby("stream_symbol")["sentiment"] if not st.empty else None

    if st.empty:
        agg = pd.DataFrame(columns=["Ticker","social_total_posts","social_bullish","social_bearish","social_unlabeled",
                                    "social_sentiment_score","message_density","weighted_density"])
    else:
        agg = pd.DataFrame(
            {
                "social_total_posts": g.size(),
                "social_bullish": g.apply(lambda s: (s == "Bullish").sum()),
                "social_bearish": g.apply(lambda s: (s == "Bearish").sum()),
                "social_unlabeled": g.apply(lambda s: (s == "null").sum()),
            }
        ).reset_index().rename(columns={"stream_symbol": "Ticker"})

        labeled = (agg["social_bullish"] + agg["social_bearish"]).replace(0, np.nan)
        agg["social_sentiment_score"] = (agg["social_bullish"] - agg["social_bearish"]) / labeled

        if args.density_mode == "fixed_window":
            window_hours = (end_utc - start_utc).total_seconds() / 3600.0
            agg["message_density"] = agg["social_total_posts"] / window_hours
        else:
            st2 = st.copy()
            st2["created_at_dt"] = pd.to_datetime(st2["created_at"], utc=True, errors="coerce")
            t = (
                st2.groupby("stream_symbol")
                .agg(min_created=("created_at_dt", "min"),
                     max_created=("created_at_dt", "max"),
                     cnt=("created_at_dt", "size"))
                .reset_index()
                .rename(columns={"stream_symbol": "Ticker"})
            )
            duration_hours = (t["max_created"] - t["min_created"]).dt.total_seconds() / 3600.0
            duration_hours = duration_hours.replace(0, np.nan)
            t["message_density"] = (t["cnt"] / duration_hours).fillna(0.0)
            agg = agg.merge(t[["Ticker", "message_density"]], on="Ticker", how="left")
            agg["message_density"] = agg["message_density"].fillna(0.0)

        agg["weighted_density"] = agg["message_density"] * agg["social_sentiment_score"]

    merged = fin.merge(agg, on="Ticker", how="left")

    for c in [
        "social_total_posts",
        "social_bullish",
        "social_bearish",
        "social_unlabeled",
        "social_sentiment_score",
        "message_density",
        "weighted_density",
    ]:
        if c in merged.columns:
            merged[c] = merged[c].fillna(0)
        else:
            merged[c] = 0

    merged_f = merged[merged["social_total_posts"] >= args.min_total_posts].copy()

    d = corr(merged_f["message_density"], merged_f["Change_num"])
    s = corr(merged_f["social_sentiment_score"], merged_f["Change_num"])
    w = corr(merged_f["weighted_density"], merged_f["Change_num"])

    # Outlier robustness
    top_ticker = ""
    d2 = s2 = w2 = np.nan
    print("\n=== OUTLIER ROBUSTNESS (drop highest density ticker) ===")
    if len(merged_f) >= 4:
        top_idx = merged_f["message_density"].idxmax()
        top_ticker = str(merged_f.loc[top_idx, "Ticker"])
        merged_no = merged_f.drop(index=top_idx)
        d2 = corr(merged_no["message_density"], merged_no["Change_num"])
        s2 = corr(merged_no["social_sentiment_score"], merged_no["Change_num"])
        w2 = corr(merged_no["weighted_density"], merged_no["Change_num"])

        print("Dropping highest-density ticker:", top_ticker)
        print(f"Rows used (no outlier): {len(merged_no)}")
        print(f"Corr density vs change (no outlier):   {d2}")
        print(f"Corr sentiment vs change (no outlier): {s2}")
        print(f"Corr weighted vs change (no outlier):  {w2}")
    else:
        print("Not enough rows for outlier test.")

    print("\n=== BASELINE (Same-day) ===")
    print(f"Rows finviz: {len(merged)}")
    print(f"Rows used (min_total_posts={args.min_total_posts}): {len(merged_f)}")
    print(f"Corr density vs change:   {d}")
    print(f"Corr sentiment vs change: {s}")
    print(f"Corr weighted vs change:  {w}")

    show_cols = [
        "Ticker",
        "Change",
        "Change_num",
        "social_total_posts",
        "social_bullish",
        "social_bearish",
        "social_sentiment_score",
        "message_density",
        "weighted_density",
    ]
    print("\nTop 15 by weighted_density (FILTERED):")
    print(merged_f.sort_values("weighted_density", ascending=False)[show_cols].head(15).to_string(index=False))

    top_weighted = merged_f.sort_values("weighted_density", ascending=False)[show_cols].head(15)

    summary_row = {
        "day": day_tag,
        "window_start_et": f"{day_tag} {args.start_hour_et:02d}:00",
        "window_end_et": f"{(datetime.strptime(day_tag, '%Y-%m-%d') + timedelta(days=1)).strftime('%Y-%m-%d')} {args.start_hour_et:02d}:00",
        "window_start_utc": start_z,
        "window_end_utc": end_z,
        "posts_in_window": int(window_count),
        "unique_tickers_in_window": int(st["stream_symbol"].nunique()) if not st.empty else 0,
        "pct_bullish": float((st["sentiment"] == "Bullish").mean() * 100.0) if not st.empty else 0.0,
        "pct_bearish": float((st["sentiment"] == "Bearish").mean() * 100.0) if not st.empty else 0.0,
        "pct_null": float((st["sentiment"] == "null").mean() * 100.0) if not st.empty else 0.0,
        "finviz_rows": int(len(merged)),
        "rows_used_min_posts": int(len(merged_f)),
        "min_total_posts": int(args.min_total_posts),
        "corr_density": float(d) if pd.notna(d) else np.nan,
        "corr_sentiment": float(s) if pd.notna(s) else np.nan,
        "corr_weighted": float(w) if pd.notna(w) else np.nan,
        "outlier_ticker": top_ticker,
        "corr_density_no_outlier": float(d2) if pd.notna(d2) else np.nan,
        "corr_sentiment_no_outlier": float(s2) if pd.notna(s2) else np.nan,
        "corr_weighted_no_outlier": float(w2) if pd.notna(w2) else np.nan,
        "mongo_db": args.mongo_db,
        "mongo_collection": args.mongo_collection,
    }

    # ---- Save Excel reports to weekly_reports ----
    save_excel_reports(
        out_dir=str(reports_dir),
        day_tag=day_tag,
        fin=fin,
        agg=agg,
        merged=merged,
        merged_f=merged_f,
        summary_row=summary_row,
        top_weighted=top_weighted,
    )

    # ---- PLOTS (saved to --out_dir) ----
    os.makedirs(args.out_dir, exist_ok=True)

    def scatter_plot(x, y, xlabel, ylabel, title, out_path):
        plt.figure()
        plt.scatter(x, y)
        plt.xlabel(xlabel)
        plt.ylabel(ylabel)
        plt.title(title)
        plt.tight_layout()
        plt.savefig(out_path, dpi=200)
        if args.show:
            plt.show()
        plt.close()

    n_used = len(merged_f)

    scatter_plot(
        merged_f["message_density"],
        merged_f["Change_num"],
        "Message Density (posts/hour)",
        "Price Change (%)",
        f"Density vs Price Change ({day_tag})  n={n_used}",
        os.path.join(args.out_dir, f"Density_vs_Change_{day_tag}.png"),
    )

    scatter_plot(
        merged_f["social_sentiment_score"],
        merged_f["Change_num"],
        "Sentiment Score (bull-bear)/(bull+bear)",
        "Price Change (%)",
        f"Sentiment vs Price Change ({day_tag})  n={n_used}",
        os.path.join(args.out_dir, f"Sentiment_vs_Change_{day_tag}.png"),
    )

    scatter_plot(
        merged_f["weighted_density"],
        merged_f["Change_num"],
        "Weighted Density (density * sentiment)",
        "Price Change (%)",
        f"Weighted Density vs Price Change ({day_tag})  n={n_used}",
        os.path.join(args.out_dir, f"Weighted_Density_vs_Change_{day_tag}.png"),
    )

    print("\nSaved plots to:", os.path.abspath(args.out_dir))


if __name__ == "__main__":
    main()