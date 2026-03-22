import argparse
import sqlite3

def run(db_path: str, day: str):
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    print(f"\nDB: {db_path}")
    print(f"DAY: {day}")

    # ---- overall ----
    cur.execute("SELECT COUNT(*) FROM messages")
    total_rows = cur.fetchone()[0]
    print("\n--- TOTAL ROWS (ALL TIME) ---")
    print(total_rows)

    cur.execute("SELECT COUNT(DISTINCT stream_symbol) FROM messages")
    print("\n--- UNIQUE TICKERS (ALL TIME) ---")
    print(cur.fetchone()[0])

    cur.execute("SELECT MIN(created_at), MAX(created_at) FROM messages")
    print("\n--- DATE RANGE (ALL TIME) ---")
    print(cur.fetchone())

    # ---- day-specific ----
    cur.execute("SELECT COUNT(*) FROM messages WHERE date(created_at) = ?", (day,))
    day_rows = cur.fetchone()[0]
    print(f"\n--- POSTS FROM {day} ---")
    print(day_rows)

    cur.execute("SELECT COUNT(DISTINCT stream_symbol) FROM messages WHERE date(created_at) = ?", (day,))
    print(f"\n--- UNIQUE TICKERS {day} ---")
    print(cur.fetchone()[0])

    print(f"\n--- TOP 10 TICKERS BY COUNT ({day}) ---")
    cur.execute("""
        SELECT stream_symbol, COUNT(*) as cnt
        FROM messages
        WHERE date(created_at) = ?
        GROUP BY stream_symbol
        ORDER BY cnt DESC
        LIMIT 10
    """, (day,))
    for row in cur.fetchall():
        print(row)

    print(f"\n--- TICKERS WITH <20 POSTS ({day}) ---")
    cur.execute("""
        SELECT stream_symbol, COUNT(*) as cnt
        FROM messages
        WHERE date(created_at) = ?
        GROUP BY stream_symbol
        HAVING COUNT(*) < 20
        ORDER BY cnt ASC
    """, (day,))
    rows = cur.fetchall()
    if not rows:
        print("(none)")
    else:
        for row in rows[:50]:
            print(row)
        if len(rows) > 50:
            print(f"... ({len(rows)-50} more)")

    # ---- sentiment distribution (day) ----
    print(f"\n--- SENTIMENT DISTRIBUTION ({day}) ---")
    cur.execute("""
        SELECT COALESCE(sentiment,'null') as sentiment, COUNT(*)
        FROM messages
        WHERE date(created_at) = ?
        GROUP BY COALESCE(sentiment,'null')
        ORDER BY COUNT(*) DESC
    """, (day,))
    dist = cur.fetchall()
    total = sum(c for _, c in dist) or 1
    for s, c in dist:
        print(s, c, f"({round(100*c/total,2)}%)")

    conn.close()

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", required=True, help="Path to stocktwits.db")
    ap.add_argument("--day", required=True, help="YYYY-MM-DD (uses date(created_at))")
    args = ap.parse_args()
    run(args.db, args.day)