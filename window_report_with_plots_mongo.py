# window_report_with_plots_mongo.py
# - Window-based report (MongoDB) for real-time / custom time thresholds
# - Supports:
#   1) last_n_minutes window (real-time)
#   2) custom_et range window
#   3) bucketing (5min, 15min, 30min, 1h, etc.) for threshold-style summaries
#   4) optional finviz merge (if finviz_csv provided)
#   5) sortable tables via --sort_by and --sort_dir
# - Saves Excel + plots into a guaranteed folder next to this script

import argparse
import os
import re
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from pathlib import Path

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from pymongo import MongoClient

ET = ZoneInfo("America/New_York")
UTC = timezone.utc


# -----------------------------
# Helpers
# -----------------------------
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


def iso_z(dt: datetime) -> str:
    """UTC ISO string with Z."""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC).isoformat().replace("+00:00", "Z")


def parse_et(dt_str: str) -> datetime:
    """Parse 'YYYY-MM-DD HH:MM' as ET-aware datetime."""
    return datetime.strptime(dt_str, "%Y-%m-%d %H:%M").replace(tzinfo=ET)


def safe_corr(a, b):
    if len(a) < 3:
        return np.nan
    return float(pd.Series(a).corr(pd.Series(b)))


def infer_day_from_filename(path: str) -> str | None:
    m = re.search(r"finviz_(\d{4})_(\d{2})_(\d{2})", path.replace("\\", "/"))
    if not m:
        return None
    y, mo, d = m.group(1), m.group(2), m.group(3)
    return f"{y}-{mo}-{d}"


def ensure_out_dirs():
    script_dir = Path(__file__).resolve().parent
    out_dir = script_dir / "weekly_reports"
    out_dir.mkdir(parents=True, exist_ok=True)
    return out_dir


def load_finviz_csv_optional(finviz_csv: str | None) -> pd.DataFrame | None:
    if not finviz_csv:
        return None
    fin = pd.read_csv(finviz_csv, encoding="utf-8-sig")
    if "Ticker" not in fin.columns:
        raise ValueError(f"Finviz CSV must include 'Ticker'. Found: {fin.columns.tolist()}")
    fin["Ticker"] = fin["Ticker"].astype(str).str.strip().str.upper()

    # Change is optional here (some finviz exports might differ)
    if "Change" in fin.columns:
        fin["Change_num"] = fin["Change"].apply(pct_to_float)
    else:
        fin["Change_num"] = np.nan
    return fin


def bucket_label_from_minutes(bucket_minutes: int) -> str:
    if bucket_minutes % 60 == 0:
        hours = bucket_minutes // 60
        return f"{hours}h"
    return f"{bucket_minutes}min"


def bucketize_messages(st: pd.DataFrame, bucket_minutes: int) -> pd.DataFrame:
    """
    Returns a time-bucketed dataframe with:
    - bucket_start_utc (timezone-naive so Excel can write it)
    - total_posts
    - bullish, bearish, null
    - sentiment_score
    """
    if st.empty:
        return pd.DataFrame(
            columns=[
                "bucket_start_utc",
                "total_posts",
                "bullish",
                "bearish",
                "null",
                "sentiment_score",
            ]
        )

    st2 = st.copy()
    st2["created_at_dt"] = pd.to_datetime(st2["created_at"], utc=True, errors="coerce")
    st2 = st2.dropna(subset=["created_at_dt"])

    if st2.empty:
        return pd.DataFrame(
            columns=[
                "bucket_start_utc",
                "total_posts",
                "bullish",
                "bearish",
                "null",
                "sentiment_score",
            ]
        )

    freq = f"{int(bucket_minutes)}min"
    st2["bucket_start_utc"] = st2["created_at_dt"].dt.floor(freq)

    def _agg_bucket(df):
        total_posts = len(df)
        bullish = int((df["sentiment"] == "Bullish").sum())
        bearish = int((df["sentiment"] == "Bearish").sum())
        null = int((df["sentiment"] == "null").sum())
        denom = bullish + bearish
        score = (bullish - bearish) / denom if denom else np.nan
        return pd.Series(
            {
                "total_posts": total_posts,
                "bullish": bullish,
                "bearish": bearish,
                "null": null,
                "sentiment_score": score,
            }
        )

    # group_keys=False prevents future behavior changes from biting you
    out = (
    st2.groupby("bucket_start_utc", group_keys=False)
      .apply(_agg_bucket)
      .reset_index()
    )


    if "bucket_start_utc" not in out.columns:
        out = out.reset_index()

    out["bucket_start_utc"] = pd.to_datetime(out["bucket_start_utc"], utc=True, errors="coerce")
    out = out.sort_values("bucket_start_utc")

    # ✅ Excel cannot write timezone-aware datetimes
    out["bucket_start_utc"] = out["bucket_start_utc"].dt.tz_localize(None)

    return out


def aggregate_per_ticker(st: pd.DataFrame, window_hours: float) -> pd.DataFrame:
    """
    Per-ticker aggregation:
    - social_total_posts
    - social_bullish / bearish / unlabeled
    - social_sentiment_score
    - message_density (posts/hour)
    - weighted_density
    """
    if st.empty:
        return pd.DataFrame(
            columns=[
                "Ticker",
                "social_total_posts",
                "social_bullish",
                "social_bearish",
                "social_unlabeled",
                "social_sentiment_score",
                "message_density",
                "weighted_density",
            ]
        )

    g = st.groupby("stream_symbol")["sentiment"]
    agg = pd.DataFrame(
        {
            "social_total_posts": g.size(),
            "social_bullish": g.apply(lambda s: int((s == "Bullish").sum())),
            "social_bearish": g.apply(lambda s: int((s == "Bearish").sum())),
            "social_unlabeled": g.apply(lambda s: int((s == "null").sum())),
        }
    ).reset_index().rename(columns={"stream_symbol": "Ticker"})

    labeled = (agg["social_bullish"] + agg["social_bearish"]).replace(0, np.nan)
    agg["social_sentiment_score"] = (agg["social_bullish"] - agg["social_bearish"]) / labeled
    agg["message_density"] = agg["social_total_posts"] / float(window_hours if window_hours > 0 else 1.0)
    agg["weighted_density"] = agg["message_density"] * agg["social_sentiment_score"]
    return agg


def save_excel_window_report(
    out_dir: Path,
    tag: str,
    summary_row: dict,
    merged: pd.DataFrame,
    merged_f: pd.DataFrame,
    bucket_df: pd.DataFrame,
    raw_st: pd.DataFrame,
):
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"window_report_{tag}.xlsx"

    with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
        pd.DataFrame([summary_row]).to_excel(writer, sheet_name="summary", index=False)
        merged_f.to_excel(writer, sheet_name="merged_filtered", index=False)
        merged.to_excel(writer, sheet_name="merged_all", index=False)
        bucket_df.to_excel(writer, sheet_name="bucketed_window", index=False)
        raw_st.to_excel(writer, sheet_name="raw_messages_window", index=False)

    print("\nSaved window Excel:", out_path.resolve())


def save_plots(out_dir: Path, tag: str, merged_f: pd.DataFrame, bucket_df: pd.DataFrame, show: bool):
    out_dir.mkdir(parents=True, exist_ok=True)

    # Scatter: density vs weighted_density (quick signal sanity check)
    if not merged_f.empty:
        plt.figure()
        plt.scatter(merged_f["message_density"], merged_f["weighted_density"])
        plt.xlabel("Message Density (posts/hour)")
        plt.ylabel("Weighted Density (density * sentiment)")
        plt.title(f"Density vs Weighted Density ({tag})  n={len(merged_f)}")
        plt.tight_layout()
        plt.savefig(out_dir / f"scatter_density_vs_weighted_{tag}.png", dpi=200)
        if show:
            plt.show()
        plt.close()

    # Time series: bucketed total posts
    if not bucket_df.empty:
        plt.figure()
        plt.plot(bucket_df["bucket_start_utc"], bucket_df["total_posts"])
        plt.xlabel("Bucket Start (UTC)")
        plt.ylabel("Total Posts")
        plt.title(f"Total Posts Over Time ({tag})")
        plt.xticks(rotation=30, ha="right")
        plt.tight_layout()
        plt.savefig(out_dir / f"timeseries_total_posts_{tag}.png", dpi=200)
        if show:
            plt.show()
        plt.close()

        # Time series: bucketed sentiment_score
        plt.figure()
        plt.plot(bucket_df["bucket_start_utc"], bucket_df["sentiment_score"])
        plt.xlabel("Bucket Start (UTC)")
        plt.ylabel("Sentiment Score")
        plt.title(f"Sentiment Score Over Time ({tag})")
        plt.xticks(rotation=30, ha="right")
        plt.tight_layout()
        plt.savefig(out_dir / f"timeseries_sentiment_score_{tag}.png", dpi=200)
        if show:
            plt.show()
        plt.close()

    print("Saved plots to:", out_dir.resolve())


# -----------------------------
# Main
# -----------------------------
def main():
    ap = argparse.ArgumentParser(description="Window report (MongoDB) with bucketing + optional Finviz merge.")
    ap.add_argument("--mongo_uri", default="mongodb://localhost:27017")
    ap.add_argument("--mongo_db", default="stocktwits")
    ap.add_argument("--mongo_collection", default="messages")

    # Window selection
    ap.add_argument("--window_mode", choices=["last_n_minutes", "custom_et"], default="last_n_minutes")
    ap.add_argument("--minutes", type=int, default=30, help="Used when window_mode=last_n_minutes")
    ap.add_argument("--start_et", default="", help='Used when window_mode=custom_et: "YYYY-MM-DD HH:MM" (ET)')
    ap.add_argument("--end_et", default="", help='Used when window_mode=custom_et: "YYYY-MM-DD HH:MM" (ET)')

    # Threshold/bucketing
    ap.add_argument("--bucket_minutes", type=int, default=30, help="Bucket size in minutes (e.g., 5, 15, 30, 60)")

    # Filtering/sorting
    ap.add_argument("--min_total_posts", type=int, default=20, help="Filter tickers by minimum posts in the window")
    ap.add_argument(
        "--sort_by",
        default="weighted_density",
        help="Column to sort the filtered table by (e.g., weighted_density, message_density, social_total_posts, social_sentiment_score)",
    )
    ap.add_argument("--sort_dir", choices=["asc", "desc"], default="desc")

    # Optional Finviz merge
    ap.add_argument("--finviz_csv", default="", help="Optional finviz CSV to merge (must contain Ticker; Change optional)")

    # Output
    ap.add_argument("--out_dir", default="", help="Optional plot output dir (default: weekly_reports next to script)")
    ap.add_argument("--show", action="store_true", help="Show plots interactively")
    args = ap.parse_args()

    reports_dir = ensure_out_dirs()

    # Determine window
    if args.window_mode == "last_n_minutes":
        end_utc = datetime.now(UTC)
        start_utc = end_utc - timedelta(minutes=max(1, int(args.minutes)))
        window_tag = f"last{int(args.minutes)}min_{datetime.now(ET).strftime('%Y%m%d_%H%M')}"
    else:
        if not args.start_et.strip() or not args.end_et.strip():
            raise ValueError('custom_et requires --start_et and --end_et, format "YYYY-MM-DD HH:MM"')
        start_et_dt = parse_et(args.start_et.strip())
        end_et_dt = parse_et(args.end_et.strip())
        if end_et_dt <= start_et_dt:
            raise ValueError("end_et must be after start_et")
        start_utc = start_et_dt.astimezone(UTC)
        end_utc = end_et_dt.astimezone(UTC)
        window_tag = f"custom_{start_et_dt.strftime('%Y%m%d_%H%M')}_to_{end_et_dt.strftime('%Y%m%d_%H%M')}"

    start_z, end_z = iso_z(start_utc), iso_z(end_utc)
    window_hours = (end_utc - start_utc).total_seconds() / 3600.0

    print("\n=== WINDOW REPORT ===")
    print("ET window:", start_utc.astimezone(ET).strftime("%Y-%m-%d %H:%M"), "->", end_utc.astimezone(ET).strftime("%Y-%m-%d %H:%M"))
    print("UTC window:", start_z, "->", end_z)
    print("Bucket:", bucket_label_from_minutes(int(args.bucket_minutes)))
    print("Min posts per ticker:", int(args.min_total_posts))

    # Mongo load
    client = MongoClient(args.mongo_uri)
    col = client[args.mongo_db][args.mongo_collection]

    q = {"created_at": {"$gte": start_z, "$lt": end_z}}
    cursor = col.find(q, {"stream_symbol": 1, "sentiment": 1, "created_at": 1, "_id": 0})

    st = pd.DataFrame(list(cursor))
    if st.empty:
        st = pd.DataFrame(columns=["stream_symbol", "sentiment", "created_at"])

    st["stream_symbol"] = st["stream_symbol"].astype(str).str.strip().str.upper()
    st["sentiment"] = st["sentiment"].fillna("null").astype(str)

    # Optional finviz
    fin = load_finviz_csv_optional(args.finviz_csv.strip() or None)

    # Per-ticker aggregate
    agg = aggregate_per_ticker(st, window_hours=window_hours)

    # Merge
    if fin is not None:
        merged = fin.merge(agg, on="Ticker", how="left")
    else:
        merged = agg.copy()

    # Fill numeric columns safely
    for c in ["social_total_posts", "social_bullish", "social_bearish", "social_unlabeled", "message_density", "weighted_density"]:
        if c in merged.columns:
            merged[c] = merged[c].fillna(0)

    if "social_sentiment_score" in merged.columns:
        # sentiment score can be NaN (no labeled posts); keep NaN then fill to 0 for sorting
        merged["social_sentiment_score"] = merged["social_sentiment_score"].fillna(0)

    # Filter
    if "social_total_posts" in merged.columns:
        merged_f = merged[merged["social_total_posts"] >= int(args.min_total_posts)].copy()
    else:
        merged_f = merged.copy()

    # Sort
    sort_col = args.sort_by
    if sort_col not in merged_f.columns:
        print(f"[WARN] sort_by='{sort_col}' not found. Falling back to 'social_total_posts' if available.")
        sort_col = "social_total_posts" if "social_total_posts" in merged_f.columns else merged_f.columns[0]

    merged_f = merged_f.sort_values(sort_col, ascending=(args.sort_dir == "asc"))

    # Bucketed window time series (threshold-style)
    bucket_df = bucketize_messages(st, bucket_minutes=int(args.bucket_minutes))

    # Summary
    total_posts = int(len(st))
    unique_tickers = int(st["stream_symbol"].nunique()) if not st.empty else 0
    pct_bull = float((st["sentiment"] == "Bullish").mean() * 100.0) if total_posts else 0.0
    pct_bear = float((st["sentiment"] == "Bearish").mean() * 100.0) if total_posts else 0.0
    pct_null = float((st["sentiment"] == "null").mean() * 100.0) if total_posts else 0.0

    # Correlations (only if finviz has Change_num)
    corr_density = corr_sentiment = corr_weighted = np.nan
    if fin is not None and "Change_num" in merged_f.columns and len(merged_f) >= 3:
        # Ensure numeric
        merged_f["Change_num"] = pd.to_numeric(merged_f["Change_num"], errors="coerce")
        corr_density = safe_corr(merged_f["message_density"], merged_f["Change_num"]) if "message_density" in merged_f.columns else np.nan
        corr_sentiment = safe_corr(merged_f["social_sentiment_score"], merged_f["Change_num"]) if "social_sentiment_score" in merged_f.columns else np.nan
        corr_weighted = safe_corr(merged_f["weighted_density"], merged_f["Change_num"]) if "weighted_density" in merged_f.columns else np.nan

    summary_row = {
        "window_mode": args.window_mode,
        "window_start_et": start_utc.astimezone(ET).strftime("%Y-%m-%d %H:%M"),
        "window_end_et": end_utc.astimezone(ET).strftime("%Y-%m-%d %H:%M"),
        "window_start_utc": start_z,
        "window_end_utc": end_z,
        "window_minutes": float((end_utc - start_utc).total_seconds() / 60.0),
        "bucket_minutes": int(args.bucket_minutes),
        "posts_in_window": total_posts,
        "unique_tickers_in_window": unique_tickers,
        "pct_bullish": pct_bull,
        "pct_bearish": pct_bear,
        "pct_null": pct_null,
        "min_total_posts": int(args.min_total_posts),
        "rows_after_filter": int(len(merged_f)),
        "sort_by": sort_col,
        "sort_dir": args.sort_dir,
        "corr_density": float(corr_density) if pd.notna(corr_density) else np.nan,
        "corr_sentiment": float(corr_sentiment) if pd.notna(corr_sentiment) else np.nan,
        "corr_weighted": float(corr_weighted) if pd.notna(corr_weighted) else np.nan,
        "mongo_db": args.mongo_db,
        "mongo_collection": args.mongo_collection,
    }

    # Console preview (top 15)
    print("\n=== TOP 15 (Filtered + Sorted) ===")
    preview_cols = [c for c in [
        "Ticker",
        "social_total_posts",
        "message_density",
        "social_sentiment_score",
        "weighted_density",
        "Change",
        "Change_num",
    ] if c in merged_f.columns]
    print(merged_f[preview_cols].head(15).to_string(index=False))

    # Save Excel + plots
    out_excel_dir = reports_dir
    plots_dir = Path(args.out_dir).resolve() if args.out_dir.strip() else reports_dir

    save_excel_window_report(
        out_dir=out_excel_dir,
        tag=window_tag,
        summary_row=summary_row,
        merged=merged,
        merged_f=merged_f,
        bucket_df=bucket_df,
        raw_st=st,
    )

    save_plots(
        out_dir=plots_dir,
        tag=window_tag,
        merged_f=merged_f,
        bucket_df=bucket_df,
        show=args.show,
    )

    client.close()


if __name__ == "__main__":
    main()
