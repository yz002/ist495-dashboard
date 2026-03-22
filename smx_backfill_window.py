import argparse
import json
import os
import random
import re
import sqlite3
import time
from collections import Counter
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import cloudscraper

# -----------------------------
# Time / parsing
# -----------------------------
ET = ZoneInfo("America/New_York")
TICKER_RE = re.compile(r"\$([A-Za-z]{1,10})")

STOPWORDS = {
    "the","a","an","and","or","but","if","then","so","to","of","in","on","for","with","as","at","by",
    "is","are","was","were","be","been","it","this","that","these","those","i","you","we","they",
    "my","your","our","their","me","him","her","them","us","from","will","just","not","do","does",
    "can","could","should","would","about","into","over","under","up","down","more","most","very",
    "im","it's","its","rt","lol","yeah","gonna","today","tomorrow","week","day","one","two","new"
}

def parse_et_to_utc(dt_str: str) -> datetime:
    """dt_str format: 'YYYY-MM-DD HH:MM' interpreted as ET."""
    dt_et = datetime.strptime(dt_str, "%Y-%m-%d %H:%M").replace(tzinfo=ET)
    return dt_et.astimezone(timezone.utc)

def parse_stocktwits_time(created_at: str) -> datetime | None:
    if not created_at:
        return None
    try:
        return datetime.fromisoformat(created_at.replace("Z", "+00:00")).astimezone(timezone.utc)
    except Exception:
        return None

def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

# -----------------------------
# Text helpers
# -----------------------------
def extract_ticker_mentions(text: str) -> list[str]:
    if not text:
        return []
    return sorted({m.group(1).upper() for m in TICKER_RE.finditer(text)})

def extract_keywords(text: str, top_n: int = 8) -> list[str]:
    if not text:
        return []
    scrubbed = TICKER_RE.sub(" ", text)
    tokens = re.findall(r"[A-Za-z0-9]{3,}", scrubbed.lower())
    tokens = [t for t in tokens if t not in STOPWORDS and not t.isdigit()]
    if not tokens:
        return []
    counts = Counter(tokens)
    return [w for w, _ in counts.most_common(top_n)]

def reason_for_label(sentiment_val: str, text: str) -> str:
    base = (sentiment_val or "null").lower()
    if base in ("bullish", "bearish"):
        return f"Stocktwits tag: {base}"

    bullish_words = ("buy", "bull", "calls", "breakout", "moon", "pump", "long", "support", "rebound")
    bearish_words = ("sell", "bear", "puts", "breakdown", "dump", "short", "resistance", "crash")

    t = (text or "").lower()
    b = sum(w in t for w in bullish_words)
    r = sum(w in t for w in bearish_words)

    if b > r and b > 0:
        return "No Stocktwits sentiment tag; text contains bullish language"
    if r > b and r > 0:
        return "No Stocktwits sentiment tag; text contains bearish language"
    return "No Stocktwits sentiment tag; insufficient sentiment cues"

def auto_notes(text: str) -> str:
    if not text:
        return ""
    t = text.lower()
    flags = []
    if "http://" in t or "https://" in t:
        flags.append("has_link")
    if "chart" in t or "tradingview" in t:
        flags.append("mentions_chart")
    if "call" in t or "puts" in t or "option" in t:
        flags.append("mentions_options")
    if "earnings" in t or " er " in f" {t} ":
        flags.append("mentions_earnings")
    return ",".join(flags)

# -----------------------------
# Fetch (pagination via max)
# -----------------------------
def get_symbol_stream(scraper, symbol: str, max_id=None, retries: int = 3):
    url = f"https://api.stocktwits.com/api/2/streams/symbol/{symbol}.json"
    params = {"limit": 50}
    if max_id is not None:
        params["max"] = max_id

    for attempt in range(1, retries + 1):
        print(f"Fetching {symbol} (max={max_id})... attempt {attempt}/{retries}")
        try:
            resp = scraper.get(url, params=params, timeout=20)

            if resp.status_code == 200:
                return resp.json()

            if resp.status_code == 429:
                retry_after = resp.headers.get("Retry-After")
                wait = int(retry_after) if retry_after and retry_after.isdigit() else 60
                wait = min(wait, 600)
                print(f"Rate limited (429). Waiting {wait}s...")
                time.sleep(wait)
                return None

            if resp.status_code in (401, 403, 406, 409, 418, 500, 502, 503, 504):
                wait = 10 * attempt
                print(f"{symbol}: HTTP {resp.status_code}. Waiting {wait}s then retry...")
                time.sleep(wait)
                continue

            print(f"{symbol}: HTTP {resp.status_code} (non-retry).")
            return None

        except Exception as e:
            wait = 10 * attempt
            print(f"{symbol}: fetch error: {e}. Waiting {wait}s then retry...")
            time.sleep(wait)
            try:
                scraper = cloudscraper.create_scraper()
            except Exception:
                pass

    print(f"{symbol}: failed after {retries} retries.")
    return None

# -----------------------------
# SQLite layer
# -----------------------------
SCHEMA_SQL = """
PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;

CREATE TABLE IF NOT EXISTS messages (
  id                INTEGER PRIMARY KEY,
  stream_symbol      TEXT NOT NULL,
  author            TEXT,
  created_at        TEXT,
  scraped_at_utc     TEXT NOT NULL,
  post              TEXT,
  sentiment          TEXT,
  reason_for_label   TEXT,
  keywords_json      TEXT,
  ticker_mentions_json TEXT,
  notes             TEXT,
  link              TEXT,
  raw_json           TEXT
);

CREATE INDEX IF NOT EXISTS idx_messages_symbol_created
ON messages(stream_symbol, created_at);

CREATE INDEX IF NOT EXISTS idx_messages_created
ON messages(created_at);
"""

def db_connect(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys=ON;")
    conn.executescript(SCHEMA_SQL)
    return conn

def db_insert_messages_window(conn: sqlite3.Connection, symbol: str, msgs: list[dict],
                              start_utc: datetime, end_utc: datetime) -> int:
    """Insert msgs whose created_at_utc is in [start_utc, end_utc). Dedup on id."""
    if not msgs:
        return 0

    inserted = 0
    filtered_outside = 0
    parse_failed = 0

    now = utc_now_iso()
    cur = conn.cursor()

    for msg in msgs:
        mid = msg.get("id")
        if not mid:
            continue

        user = msg.get("user", {}).get("username", "Unknown")
        body = msg.get("body", "")
        created_at = msg.get("created_at", "")

        created_at_utc = parse_stocktwits_time(created_at)
        if created_at_utc is None:
            parse_failed += 1
            continue

        # keep only in the window
        if created_at_utc < start_utc or created_at_utc >= end_utc:
            filtered_outside += 1
            continue

        sentiment = msg.get("entities", {}).get("sentiment", {})
        sentiment_val = sentiment.get("basic", "null") if sentiment else "null"

        ticker_mentions = extract_ticker_mentions(body)
        kw = extract_keywords(body, top_n=8)
        reason = reason_for_label(sentiment_val, body)
        notes = auto_notes(body)
        link = f"https://stocktwits.com/{user}/message/{mid}"

        cur.execute(
            """
            INSERT OR IGNORE INTO messages
            (id, stream_symbol, author, created_at, scraped_at_utc, post, sentiment,
             reason_for_label, keywords_json, ticker_mentions_json, notes, link, raw_json)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                int(mid),
                symbol,
                user,
                created_at,
                now,
                body,
                sentiment_val,
                reason,
                json.dumps(kw, ensure_ascii=False),
                json.dumps(ticker_mentions, ensure_ascii=False),
                notes,
                link,
                json.dumps(msg, ensure_ascii=False),
            )
        )
        if cur.rowcount == 1:
            inserted += 1

    conn.commit()
    print(f"[{symbol}] inserted={inserted} filtered_outside={filtered_outside} parse_failed={parse_failed} api_msgs={len(msgs)}")
    return inserted

# -----------------------------
# Backfill driver
# -----------------------------
def backfill_symbol_window(conn, scraper, symbol: str, start_utc: datetime, end_utc: datetime,
                           pause_min: float, pause_max: float, max_pages: int = 500):
    """
    Page backwards using max=<id> until oldest message in page < start_utc.
    """
    max_id = None
    total_inserted = 0
    pages = 0

    print(f"\n=== BACKFILL {symbol} ===")
    print(f"WINDOW ET : {start_utc.astimezone(ET)} -> {end_utc.astimezone(ET)}")
    print(f"WINDOW UTC: {start_utc} -> {end_utc}\n")

    while pages < max_pages:
        data = get_symbol_stream(scraper, symbol, max_id=max_id)
        if not data:
            print(f"[{symbol}] No data returned; stopping.")
            break

        msgs = data.get("messages", [])
        if not msgs:
            print(f"[{symbol}] No messages in response; stopping.")
            break

        pages += 1

        parsed_times = []
        for m in msgs:
            t = parse_stocktwits_time(m.get("created_at", ""))
            if t:
                parsed_times.append(t)

        if not parsed_times:
            print(f"[{symbol}] Could not parse message times; stopping.")
            break

        newest_t = max(parsed_times)
        oldest_t = min(parsed_times)

        print(f"[{symbol}] Page {pages}: newest={newest_t} oldest={oldest_t} max_id={max_id}")

        # insert only in-window
        total_inserted += db_insert_messages_window(conn, symbol, msgs, start_utc, end_utc)

        # stop once we've paged earlier than start_utc
        if oldest_t < start_utc:
            print(f"[{symbol}] Oldest message is older than start window. Done.")
            break

        # advance max_id backward (msgs are newest->oldest, so last msg is oldest)
        oldest_id = msgs[-1].get("id")
        if not oldest_id:
            print(f"[{symbol}] Could not read oldest id; stopping.")
            break

        # prevent infinite loop
        if max_id is not None and int(oldest_id) >= int(max_id):
            print(f"[{symbol}] max_id did not move backward (oldest_id={oldest_id}, max_id={max_id}). Stopping.")
            break

        max_id = int(oldest_id) - 1
        time.sleep(random.uniform(pause_min, pause_max))

    print(f"\n[{symbol}] Backfill complete. pages={pages}, total_inserted={total_inserted}\n")

# -----------------------------
# Main
# -----------------------------
def main():
    parser = argparse.ArgumentParser(description="Stocktwits backfill for a single symbol within an ET window.")
    parser.add_argument("--symbol", default="SMX", help="Symbol to backfill (default: SMX).")
    parser.add_argument("--db", default="stocktwits.db", help="SQLite DB path.")
    parser.add_argument("--start_et", required=True, help="Window start ET: 'YYYY-MM-DD HH:MM'")
    parser.add_argument("--end_et", required=True, help="Window end ET: 'YYYY-MM-DD HH:MM'")
    parser.add_argument("--pause_min", type=float, default=0.7, help="Min pause between API calls.")
    parser.add_argument("--pause_max", type=float, default=1.5, help="Max pause between API calls.")
    parser.add_argument("--max_pages", type=int, default=500, help="Safety limit on number of pages.")
    args = parser.parse_args()

    symbol = args.symbol.upper()
    start_utc = parse_et_to_utc(args.start_et)
    end_utc = parse_et_to_utc(args.end_et)

    if end_utc <= start_utc:
        raise SystemExit("ERROR: end_et must be after start_et.")

    conn = db_connect(args.db)
    scraper = cloudscraper.create_scraper()

    try:
        backfill_symbol_window(
            conn=conn,
            scraper=scraper,
            symbol=symbol,
            start_utc=start_utc,
            end_utc=end_utc,
            pause_min=args.pause_min,
            pause_max=args.pause_max,
            max_pages=args.max_pages,
        )
    finally:
        conn.close()

if __name__ == "__main__":
    main()