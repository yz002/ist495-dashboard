import sqlite3
import pandas as pd
import json
from collections import Counter

DB_PATH = "stocktwits.db"
ANALYSIS_DATE = "2026-02-03"  # YYYY-MM-DD

TICKERS = ["AMD", "NVDA", "AAPL", "TSLA", "SMCI"]

def load_msgs(conn):
    q = """
    SELECT stream_symbol, created_at, sentiment, keywords_json, ticker_mentions_json, notes, post, link
    FROM messages
    WHERE DATE(created_at) = ?
      AND stream_symbol IN ({})
    """.format(",".join(["?"] * len(TICKERS)))
    params = [ANALYSIS_DATE] + TICKERS
    return pd.read_sql(q, conn, params=params)

def parse_json_list(x):
    if x is None:
        return []
    if isinstance(x, list):
        return x
    try:
        return json.loads(x)
    except Exception:
        return []

def top_k_from_lists(series, k=10):
    c = Counter()
    for v in series:
        for item in parse_json_list(v):
            c[str(item).upper()] += 1
    return c.most_common(k)

def main():
    conn = sqlite3.connect(DB_PATH)

    df = load_msgs(conn)
    print(f"\n=== ANALYSIS DATE: {ANALYSIS_DATE} ===")
    print(f"Total rows (selected tickers): {len(df)}")

    if df.empty:
        print("No rows found for this date/tickers. Check DATE(created_at) and symbols.")
        conn.close()
        return

    # Normalize sentiment
    df["sentiment_norm"] = df["sentiment"].fillna("null").astype(str).str.lower().str.strip()

    # 1) Counts per ticker
    by_symbol = df.groupby("stream_symbol").size().reset_index(name="n").sort_values("n", ascending=False)

    # 2) Sentiment breakdown + net sentiment
    def count_where(s, val): return (s == val).sum()

    out = []
    for sym, g in df.groupby("stream_symbol"):
        bullish = count_where(g["sentiment_norm"], "bullish")
        bearish = count_where(g["sentiment_norm"], "bearish")
        unlabeled = len(g) - bullish - bearish
        total = len(g)
        net = (bullish - bearish) / total if total else 0
        out.append([sym, bullish, bearish, unlabeled, total, net])

    sentiment_tbl = pd.DataFrame(out, columns=["stream_symbol","bullish","bearish","unlabeled","total","net_sentiment"]) \
        .sort_values("total", ascending=False)

    print("\n=== ROWS BY SYMBOL ===")
    print(by_symbol.to_string(index=False))

    print("\n=== SENTIMENT BY SYMBOL ===")
    print(sentiment_tbl.to_string(index=False))

    # 3) Top keywords per ticker (from keywords_json)
    print("\n=== TOP KEYWORDS (per ticker) ===")
    for sym, g in df.groupby("stream_symbol"):
        top_kw = top_k_from_lists(g["keywords_json"], k=12)
        print(f"\n[{sym}] {top_kw}")

    # 4) Top mentioned tickers inside posts (from ticker_mentions_json)
    print("\n=== TOP TICKER MENTIONS (per ticker) ===")
    for sym, g in df.groupby("stream_symbol"):
        top_tm = top_k_from_lists(g["ticker_mentions_json"], k=12)
        print(f"\n[{sym}] {top_tm}")

    # 5) Notes flags aggregation (themes proxy)
    print("\n=== NOTES/THEME FLAGS (per ticker) ===")
    for sym, g in df.groupby("stream_symbol"):
        flags = Counter()
        for x in g["notes"].fillna("").astype(str):
            for f in [t.strip() for t in x.split(",") if t.strip()]:
                flags[f] += 1
        print(f"\n[{sym}] {flags.most_common(10)}")

    # 6) Notable links/posts (simple: highest “signal” = has_link OR mentions_chart/options/earnings)
    print("\n=== NOTABLE POSTS (sample, per ticker) ===")
    for sym, g in df.groupby("stream_symbol"):
        notable = g[g["notes"].fillna("").astype(str) != ""].head(5)
        print(f"\n[{sym}]")
        for _, r in notable.iterrows():
            snippet = (r["post"] or "").replace("\n"," ").strip()
            if len(snippet) > 160:
                snippet = snippet[:160] + "..."
            print(f"- {r['notes']} | {r['link']} | {snippet}")
    

    # ===============================
    # CROSS-TICKER SPILLOVER MATRIX
    # ===============================

    print("\n=== CROSS-TICKER MENTIONS MATRIX ===")

    selected = ["AMD", "NVDA", "AAPL", "TSLA", "SMCI"]

    spillover = {}

    for base in selected:
        spillover[base] = {}
        for target in selected:
            if base == target:
                spillover[base][target] = 0
                continue

            query = """
            SELECT ticker_mentions_json
            FROM messages
            WHERE DATE(created_at) = ?
            AND stream_symbol = ?
            """
            rows = pd.read_sql(query, conn, params=(ANALYSIS_DATE, base))

            count = 0
            for _, r in rows.iterrows():
                mentions = json.loads(r["ticker_mentions_json"] or "[]")
                if target in mentions:
                    count += 1

            spillover[base][target] = count

    # Convert to DataFrame
    matrix_df = pd.DataFrame(spillover).T
    print(matrix_df)
    print("\n=== NORMALIZED SPILLOVER (%) ===")

    matrix_df_pct = matrix_df.div(matrix_df.sum(axis=1).replace(0,1), axis=0)
    print(matrix_df_pct.round(3))
    print("\n=== HOURLY NET SENTIMENT ===")

    hourly = pd.read_sql("""
    SELECT 
        stream_symbol,
        strftime('%H', created_at) AS hour_utc,
        SUM(CASE WHEN sentiment='Bullish' THEN 1 ELSE 0 END) AS bullish,
        SUM(CASE WHEN sentiment='Bearish' THEN 1 ELSE 0 END) AS bearish,
        COUNT(*) AS total
    FROM messages
    WHERE DATE(created_at) = ?
    GROUP BY stream_symbol, hour_utc
    ORDER BY stream_symbol, hour_utc
    """, conn, params=(ANALYSIS_DATE,))

    hourly["net_sentiment"] = (hourly["bullish"] - hourly["bearish"]) / hourly["total"]

    print(hourly)

    print("\n=== SENTIMENT MOMENTUM (Δ over time) ===")

    hourly_sorted = hourly.sort_values(["stream_symbol", "hour_utc"]).copy()

    hourly_sorted["sentiment_change"] = (
        hourly_sorted.groupby("stream_symbol")["net_sentiment"]
        .diff()
    )

    print(hourly_sorted)

    print("\n=== SENTIMENT VOLATILITY (STD of hourly net sentiment) ===")

    vol = (
        hourly_sorted.groupby("stream_symbol")["net_sentiment"]
        .std()
        .reset_index(name="sentiment_std")
    )

    print(vol)

    print("\n=== VOLATILITY RANKING ===")
    print(vol.sort_values("sentiment_std", ascending=False))

    
    conn.close()

if __name__ == "__main__":
    main()