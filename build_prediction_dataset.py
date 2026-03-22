import argparse
import glob
import os
import re
import sqlite3
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import pandas as pd

ET = ZoneInfo("America/New_York")


# -----------------------
# Helpers
# -----------------------
def parse_date_from_filename(path: str) -> str:
    """
    Expect filenames like finviz_YYYY-MM-DD.csv somewhere in the name.
    Returns YYYY-MM-DD string.
    """
    m = re.search(r"(\d{4}-\d{2}-\d{2})", os.path.basename(path))
    if not m:
        raise ValueError(f"Could not parse YYYY-MM-DD from filename: {path}")
    return m.group(1)


def change_to_float(x) -> float:
    if pd.isna(x):
        return float("nan")
    s = str(x).strip()
    s = s.replace("%", "").replace(",", "")
    try:
        return float(s)
    except:
        return float("nan")


def et_day_window_utc(date_str: str):
    d = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=ET)
    start_et = d.replace(hour=0, minute=0, second=0, microsecond=0)
    end_et = start_et + timedelta(days=1)
    return start_et.astimezone(timezone.utc), end_et.astimezone(timezone.utc)


def normalize_sentiment(s) -> str:
    s = str(s) if s is not None else "null"
    s = s.strip().lower()
    if s == "bullish":
        return "bullish"
    if s == "bearish":
        return "bearish"
    return "unlabeled"


# -----------------------
# Stocktwits daily features from SQLite
# -----------------------
def stocktwits_daily_features(conn: sqlite3.Connection, date_str: str) -> pd.DataFrame:
    """
    Returns per-ticker daily aggregates from SQLite:
    - social_total_posts
    - social_bullish, social_bearish, social_unlabeled
    - social_sentiment_score: (bull - bear) / total
    - message_density_per_hour: total / 24 (simple daily density)
    """
    start_utc, end_utc = et_day_window_utc(date_str)

    q = """
    SELECT stream_symbol, created_at, sentiment
    FROM messages
    WHERE created_at >= ?
      AND created_at < ?
    """

    start_iso = start_utc.isoformat().replace("+00:00", "Z")
    end_iso = end_utc.isoformat().replace("+00:00", "Z")

    df = pd.read_sql(q, conn, params=[start_iso, end_iso])
    if df.empty:
        return pd.DataFrame(columns=[
            "date", "Ticker",
            "social_total_posts", "social_bullish", "social_bearish", "social_unlabeled",
            "social_sentiment_score", "message_density_per_hour"
        ])

    df["Ticker"] = df["stream_symbol"].astype(str).str.upper()
    df["sent_norm"] = df["sentiment"].apply(normalize_sentiment)

    g = df.groupby("Ticker")["sent_norm"]

    out = pd.DataFrame({
        "social_bullish": g.apply(lambda x: (x == "bullish").sum()),
        "social_bearish": g.apply(lambda x: (x == "bearish").sum()),
        "social_unlabeled": g.apply(lambda x: (x == "unlabeled").sum()),
        "social_total_posts": g.size(),
    }).reset_index()

    out["social_sentiment_score"] = (out["social_bullish"] - out["social_bearish"]) / out["social_total_posts"]
    out["message_density_per_hour"] = out["social_total_posts"] / 24.0
    out["date"] = date_str

    return out


# -----------------------
# Finviz daily features from CSV
# -----------------------
def finviz_daily_features(csv_path: str) -> pd.DataFrame:
    date_str = parse_date_from_filename(csv_path)
    df = pd.read_csv(csv_path, encoding="utf-8-sig")

    if "Ticker" not in df.columns:
        raise ValueError(f"{csv_path} missing 'Ticker' column. Columns={df.columns.tolist()}")
    if "Change" not in df.columns:
        raise ValueError(f"{csv_path} missing 'Change' column. Columns={df.columns.tolist()}")

    out = df.copy()
    out["Ticker"] = out["Ticker"].astype(str).str.upper().str.strip()
    out["Change_num"] = out["Change"].apply(change_to_float)
    out["date"] = date_str

    # keep only what we need (you can add more Finviz columns later)
    keep_cols = ["date", "Ticker", "Change", "Change_num"]
    return out[keep_cols]


# -----------------------
# Build dataset with next-day label
# -----------------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", required=True, help="Path to SQLite stocktwits db")
    ap.add_argument("--finviz_glob", required=True, help=r'Glob for daily finviz exports, e.g. "finviz_daily/finviz_*.csv"')
    ap.add_argument("--out_csv", required=True, help="Output dataset csv")
    args = ap.parse_args()

    finviz_paths = sorted(glob.glob(args.finviz_glob))
    if len(finviz_paths) < 2:
        raise SystemExit("Need at least 2 daily finviz CSVs to create next-day labels.")

    # Build finviz daily frame
    finviz_all = pd.concat([finviz_daily_features(p) for p in finviz_paths], ignore_index=True)
    finviz_all = finviz_all.dropna(subset=["Ticker", "date"])

    # Load stocktwits daily features for each date we have finviz
    dates = sorted(finviz_all["date"].unique().tolist())

    conn = sqlite3.connect(args.db)
    st_all = pd.concat([stocktwits_daily_features(conn, d) for d in dates], ignore_index=True)
    conn.close()

    # Merge (left join on finviz to keep the screener universe)
    merged = finviz_all.merge(st_all, on=["date", "Ticker"], how="left")

    # Fill missing Stocktwits stats (no posts that day)
    for col in ["social_total_posts","social_bullish","social_bearish","social_unlabeled"]:
        merged[col] = merged[col].fillna(0).astype(int)
    merged["social_sentiment_score"] = merged["social_sentiment_score"].fillna(0.0)
    merged["message_density_per_hour"] = merged["message_density_per_hour"].fillna(0.0)

    # Weighted feature (simple): density * sentiment
    merged["weighted_density"] = merged["message_density_per_hour"] * merged["social_sentiment_score"]

    # Create NEXT DAY label: shift Change_num by ticker
    merged = merged.sort_values(["Ticker", "date"]).reset_index(drop=True)
    merged["next_change_num"] = merged.groupby("Ticker")["Change_num"].shift(-1)

    # Classification target: next day up/down
    merged["y_next_up"] = (merged["next_change_num"] > 0).astype(int)

    # Drop last day per ticker (no next-day available)
    merged = merged.dropna(subset=["next_change_num"]).reset_index(drop=True)

    merged.to_csv(args.out_csv, index=False)
    print(f"Saved dataset: {args.out_csv}")
    print("Rows:", len(merged))
    print("Dates:", merged["date"].min(), "->", merged["date"].max())
    print("Tickers:", merged["Ticker"].nunique())


if __name__ == "__main__":
    main()