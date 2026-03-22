import sqlite3

db = r"C:\Users\yosef\OneDrive\Desktop\Research Internship IST495\stocktwits.db"
conn = sqlite3.connect(db)
cur = conn.cursor()

print("\n--- TOTAL ROWS ---")
cur.execute("SELECT COUNT(*) FROM messages")
print(cur.fetchone()[0])

print("\n--- POSTS FROM 2026-02-11 ---")
cur.execute("""
SELECT COUNT(*) FROM messages
WHERE created_at LIKE '2026-02-11%'
""")
print(cur.fetchone()[0])

conn.close()



import sqlite3

db = r"C:\Users\yosef\OneDrive\Desktop\Research Internship IST495\stocktwits_2026_02_10_snapshot.db"
conn = sqlite3.connect(db)
cur = conn.cursor()

print("\n--- OVERALL SENTIMENT DISTRIBUTION ---")

cur.execute("""
SELECT sentiment, COUNT(*)
FROM messages
GROUP BY sentiment
""")

rows = cur.fetchall()
total = sum(r[1] for r in rows)

for sentiment, count in rows:
    print(sentiment, count, f"({round(100*count/total,2)}%)")

print("\nTotal:", total)

conn.close()


import sqlite3

db = r"C:\Users\yosef\OneDrive\Desktop\Research Internship IST495\stocktwits_2026_02_10_snapshot.db"
conn = sqlite3.connect(db)
cur = conn.cursor()

print("\n--- TOP 15 TICKERS BY BULLISH RATIO (min 20 labeled posts) ---")

cur.execute("""
SELECT
    stream_symbol,
    SUM(CASE WHEN sentiment='Bullish' THEN 1 ELSE 0 END) as bulls,
    SUM(CASE WHEN sentiment='Bearish' THEN 1 ELSE 0 END) as bears,
    COUNT(*) as total
FROM messages
GROUP BY stream_symbol
HAVING (bulls + bears) >= 20
ORDER BY (CAST(bulls AS FLOAT) / (bulls + bears)) DESC
LIMIT 15
""")

for row in cur.fetchall():
    print(row)

conn.close()

import sqlite3
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

db = r"C:\Users\yosef\OneDrive\Desktop\Research Internship IST495\stocktwits_2026_02_10_snapshot.db"
conn = sqlite3.connect(db)
cur = conn.cursor()

ET = ZoneInfo("America/New_York")
now_et = datetime.now(ET)
start_et = now_et.replace(hour=6, minute=0, second=0, microsecond=0)
hours = (now_et - start_et).total_seconds() / 3600

print("\n--- TICKERS WITH DENSITY + SENTIMENT ---")

cur.execute("""
SELECT
    stream_symbol,
    SUM(CASE WHEN sentiment='Bullish' THEN 1 ELSE 0 END) as bulls,
    SUM(CASE WHEN sentiment='Bearish' THEN 1 ELSE 0 END) as bears,
    COUNT(*) as total
FROM messages
GROUP BY stream_symbol
HAVING total >= 50
ORDER BY total DESC
""")

rows = cur.fetchall()

for r in rows:
    symbol, bulls, bears, total = r
    labeled = bulls + bears
    ratio = bulls / labeled if labeled > 0 else 0
    density = total / hours
    print(f"{symbol}: total={total}, bull_ratio={ratio:.2f}, density/hr={density:.2f}")

conn.close()

import pandas as pd

# Load your enriched screener file
df = pd.read_csv(r"C:\Users\yosef\OneDrive\Desktop\Research Internship IST495\finviz_enriched_120_sorted.csv")

# Keep relevant columns
df = df[['Ticker','Change','social_total_posts','message_density','social_sentiment_score','weighted_density']]

# Convert Change from "34.56%" to float
df['Change_pct'] = df['Change'].str.replace('%','').astype(float)

print(df.sort_values('weighted_density', ascending=False).head(15))

print("\nCorrelation with price change:")
print("Density vs Change:", df['message_density'].corr(df['Change_pct']))
print("Sentiment vs Change:", df['social_sentiment_score'].corr(df['Change_pct']))
print("Weighted vs Change:", df['weighted_density'].corr(df['Change_pct']))

import matplotlib.pyplot as plt

plt.scatter(df['message_density'], df['Change_pct'])
plt.xlabel("Message Density")
plt.ylabel("Price % Change")
plt.title("Density vs Price Change")
plt.show()

import pandas as pd

df = pd.read_csv(r"C:\Users\yosef\OneDrive\Desktop\Research Internship IST495\finviz_daily\finviz_2026_02_10.csv")
print(df.columns)


import pandas as pd

df = pd.read_csv(r"C:\Users\yosef\OneDrive\Desktop\Research Internship IST495\finviz_enriched_live.csv")

print(df.dtypes)
print("\nSample Change values:")
print(df["Change"].head())

import pandas as pd

df = pd.read_csv(r"C:\Users\yosef\OneDrive\Desktop\Research Internship IST495\finviz_enriched_live.csv")

# Remove % and convert to float
df["Change_num"] = (
    df["Change"]
    .str.replace("%", "", regex=False)
    .astype(float)
)

print(df[["Ticker", "Change", "Change_num"]].head())

print(df[["message_density",
          "social_sentiment_score",
          "weighted_density"]].dtypes)

print("Density vs Change:",
      df["message_density"].corr(df["Change_num"]))

print("Sentiment vs Change:",
      df["social_sentiment_score"].corr(df["Change_num"]))

print("Weighted vs Change:",
      df["weighted_density"].corr(df["Change_num"]))


import sqlite3

db = r"C:\Users\yosef\OneDrive\Desktop\Research Internship IST495\stocktwits.db"
conn = sqlite3.connect(db)
cur = conn.cursor()

print("\n--- TOTAL ROWS ---")
cur.execute("SELECT COUNT(*) FROM messages")
print(cur.fetchone()[0])

print("\n--- UNIQUE TICKERS ---")
cur.execute("SELECT COUNT(DISTINCT stream_symbol) FROM messages")
print(cur.fetchone()[0])

print("\n--- DATE RANGE ---")
cur.execute("SELECT MIN(created_at), MAX(created_at) FROM messages")
print(cur.fetchone())

print("\n--- TOP 10 TICKERS BY COUNT ---")
cur.execute("""
SELECT stream_symbol, COUNT(*) as c
FROM messages
GROUP BY stream_symbol
ORDER BY c DESC
LIMIT 10
""")
for row in cur.fetchall():
    print(row)

conn.close()



import sqlite3

db = r"C:\Users\yosef\OneDrive\Desktop\Research Internship IST495\stocktwits.db"
conn = sqlite3.connect(db)
cur = conn.cursor()

print("\n--- POSTS FROM 2026-02-11 ---")
cur.execute("""
SELECT COUNT(*) 
FROM messages
WHERE created_at LIKE '2026-02-11%'
""")
print(cur.fetchone()[0])

conn.close()

import sqlite3

DB_PATH = r"C:\Users\yosef\OneDrive\Desktop\Research Internship IST495\stocktwits.db"

conn = sqlite3.connect(DB_PATH)
cur = conn.cursor()

# Total rows in database
cur.execute("SELECT COUNT(*) FROM messages")
print("TOTAL ROWS:", cur.fetchone()[0])

# Posts from 2026-02-12 only
cur.execute("""
SELECT COUNT(*) 
FROM messages
WHERE date(created_at) = '2026-02-12'
""")
print("POSTS FROM 2026-02-12:", cur.fetchone()[0])

# Unique tickers for 2/12
cur.execute("""
SELECT COUNT(DISTINCT stream_symbol)
FROM messages
WHERE date(created_at) = '2026-02-12'
""")
print("UNIQUE TICKERS 2026-02-12:", cur.fetchone()[0])

# Top 10 tickers for 2/12
cur.execute("""
SELECT stream_symbol, COUNT(*) as cnt
FROM messages
WHERE date(created_at) = '2026-02-12'
GROUP BY stream_symbol
ORDER BY cnt DESC
LIMIT 10
""")

print("\nTOP 10 TICKERS 2026-02-12:")
for row in cur.fetchall():
    print(row)

conn.close()


import sqlite3

DB_PATH = r"C:\Users\yosef\OneDrive\Desktop\Research Internship IST495\stocktwits.db"

conn = sqlite3.connect(DB_PATH)
cur = conn.cursor()

cur.execute("""
SELECT stream_symbol, COUNT(*)
FROM messages
WHERE date(created_at) = '2026-02-12'
GROUP BY stream_symbol
HAVING COUNT(*) < 20
ORDER BY COUNT(*) ASC
""")

print("Tickers with <20 posts:")
for row in cur.fetchall():
    print(row)

conn.close()



import sqlite3
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

DB_PATH = r"C:\Users\yosef\OneDrive\Desktop\Research Internship IST495\stocktwits.db"
DAY = "2026-02-13"
START_HOUR_ET = 6

ET = ZoneInfo("America/New_York")
UTC = ZoneInfo("UTC")

# Build ET window -> UTC window
day_dt = datetime.strptime(DAY, "%Y-%m-%d").replace(tzinfo=ET)
start_et = day_dt.replace(hour=START_HOUR_ET, minute=0, second=0, microsecond=0)
end_et = start_et + timedelta(days=1)

start_utc = start_et.astimezone(UTC).isoformat().replace("+00:00", "Z")
end_utc = end_et.astimezone(UTC).isoformat().replace("+00:00", "Z")

conn = sqlite3.connect(DB_PATH)
cur = conn.cursor()

print("DAY (ET window):", f"{DAY} {START_HOUR_ET:02d}:00 -> next day {START_HOUR_ET:02d}:00")
print("UTC window:", f"[{start_utc}, {end_utc})")

print("\n--- TOTAL ROWS (ALL TIME) ---")
cur.execute("SELECT COUNT(*) FROM messages")
print(cur.fetchone()[0])

print("\n--- UNIQUE TICKERS (ALL TIME) ---")
cur.execute("SELECT COUNT(DISTINCT stream_symbol) FROM messages")
print(cur.fetchone()[0])

print("\n--- DATE RANGE (ALL TIME) ---")
cur.execute("SELECT MIN(created_at), MAX(created_at) FROM messages")
print(cur.fetchone())

print(f"\n--- DB ROWS IN WINDOW ({DAY}) ---")
cur.execute("""
SELECT COUNT(*)
FROM messages
WHERE created_at >= ? AND created_at < ?
""", (start_utc, end_utc))
print(cur.fetchone()[0])

print(f"\n--- UNIQUE TICKERS IN WINDOW ({DAY}) ---")
cur.execute("""
SELECT COUNT(DISTINCT stream_symbol)
FROM messages
WHERE created_at >= ? AND created_at < ?
""", (start_utc, end_utc))
print(cur.fetchone()[0])

print(f"\n--- TOP 10 TICKERS BY COUNT (WINDOW {DAY}) ---")
cur.execute("""
SELECT stream_symbol, COUNT(*) as cnt
FROM messages
WHERE created_at >= ? AND created_at < ?
GROUP BY stream_symbol
ORDER BY cnt DESC
LIMIT 10
""", (start_utc, end_utc))
for row in cur.fetchall():
    print(row)

print(f"\n--- SENTIMENT DISTRIBUTION (WINDOW {DAY}) ---")
cur.execute("""
SELECT COALESCE(sentiment,'null') as sentiment, COUNT(*) as cnt
FROM messages
WHERE created_at >= ? AND created_at < ?
GROUP BY COALESCE(sentiment,'null')
ORDER BY cnt DESC
""", (start_utc, end_utc))

rows = cur.fetchall()
total = sum(r[1] for r in rows)
for s, c in rows:
    pct = (100*c/total) if total else 0
    print(f"{s} {c} ({pct:.2f}%)")

conn.close()