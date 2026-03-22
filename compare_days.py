import pandas as pd

CSV_PATH = r"C:\Users\yosef\OneDrive\Desktop\Research Internship IST495\daily_sentiment_metrics.csv"

df = pd.read_csv(CSV_PATH)

print("\n=== AVAILABLE DATES (from CSV) ===")
print(sorted(df["date"].dropna().astype(str).unique()))

# Parse date column safely
df["date_parsed"] = pd.to_datetime(df["date"], errors="coerce")

# Get latest *date* (ignore time)
latest = df["date_parsed"].max()

if pd.isna(latest):
    raise ValueError("No valid dates found in CSV after parsing.")

latest_date_str = latest.strftime("%Y-%m-%d")

print(f"\n=== LATEST DATE IN CSV: {latest_date_str} ===")

latest_rows = df[df["date"] == latest_date_str].copy()

if latest_rows.empty:
    # Fallback: match on parsed date, just in case formatting differs
    latest_rows = df[df["date_parsed"].dt.strftime("%Y-%m-%d") == latest_date_str].copy()

if latest_rows.empty:
    raise ValueError(f"Could not find rows for latest date {latest_date_str} in CSV.")

cols = [
    "date", "ticker", "total_posts", "bullish", "bearish", "unlabeled",
    "net_sentiment", "sentiment_volatility", "largest_hourly_change",
    "spillover_top_target", "spillover_top_target_pct"
]
print(latest_rows[cols].to_string(index=False))