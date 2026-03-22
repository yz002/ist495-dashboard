import argparse
import json
import os
import random
import re
import shutil
import sqlite3
import subprocess
import time
import urllib.parse
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import cloudscraper

# -----------------------------
# Time window filter (ET)
# -----------------------------
ET = ZoneInfo("America/New_York")

def build_start_utc() -> datetime:
    """
    Start at TODAY 12:00 PM Eastern (relative to when you run the script).
    """
    now_et = datetime.now(ET)
    start_et = now_et.replace(hour=6, minute=0, second=0, microsecond=0)
    return start_et.astimezone(timezone.utc)

START_UTC = build_start_utc()

def parse_stocktwits_time(created_at: str) -> datetime | None:
    if not created_at:
        return None
    try:
        return datetime.fromisoformat(created_at.replace("Z", "+00:00")).astimezone(timezone.utc)
    except Exception:
        return None

# -----------------------------
# Text helpers
# -----------------------------
STOPWORDS = {
    "the","a","an","and","or","but","if","then","so","to","of","in","on","for","with","as","at","by",
    "is","are","was","were","be","been","it","this","that","these","those","i","you","we","they",
    "my","your","our","their","me","him","her","them","us","from","will","just","not","do","does",
    "can","could","should","would","about","into","over","under","up","down","more","most","very",
    "im","it's","its","rt","lol","yeah","gonna","today","tomorrow","week","day","one","two","new"
}
TICKER_RE = re.compile(r"\$([A-Za-z]{1,10})")

def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

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
# Fetch
# -----------------------------
def get_symbol_stream(scraper, symbol: str, max_id=None, since_id=None, retries: int = 3):
    url = f"https://api.stocktwits.com/api/2/streams/symbol/{symbol}.json"
    params = {"limit": 50}
    if max_id:
        params["max"] = max_id
    if since_id:
        params["since"] = since_id

    for attempt in range(1, retries + 1):
        print(f"Fetching {symbol} (max={max_id}, since={since_id})... attempt {attempt}/{retries}")
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
            except:
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

def db_get_min_max_ids(conn: sqlite3.Connection, symbol: str) -> tuple[int | None, int | None, int]:
    cur = conn.cursor()
    cur.execute("SELECT MIN(id), MAX(id), COUNT(*) FROM messages WHERE stream_symbol = ?", (symbol,))
    mn, mx, cnt = cur.fetchone()
    return mn, mx, cnt

def db_insert_messages(conn: sqlite3.Connection, symbol: str, msgs: list[dict]) -> int:
    """
    Insert messages, dedupe on PRIMARY KEY id.
    Filters to ONLY include created_at >= START_UTC (today 12:00 PM ET).
    """
    if not msgs:
        return 0

    inserted = 0
    filtered_before_cutoff = 0
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

        # ✅ DEBUG BEFORE FILTER so you always see it during testing
        # print(f"[{symbol}] DEBUG raw={created_at} parsed_utc={created_at_utc} cutoff_utc={START_UTC}")

        if created_at_utc < START_UTC:
            filtered_before_cutoff += 1
            continue

        sentiment = msg.get("entities", {}).get("sentiment", {})
        sentiment_val = sentiment.get("basic", "null") if sentiment else "null"

        ticker_mentions = extract_ticker_mentions(body)
        kw = extract_keywords(body, top_n=8)
        reason = reason_for_label(sentiment_val, body)
        notes = auto_notes(body)
        link = f"https://stocktwits.com/{user}/message/{mid}"

        try:
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
        except Exception as e:
            print(f"[{symbol}] DB insert error for id={mid}: {e}")

    conn.commit()

    # Helpful summary per call
    print(f"[{symbol}] DEBUG: inserted={inserted}, filtered_before_cutoff={filtered_before_cutoff}, parse_failed={parse_failed}, api_msgs={len(msgs)}")
    if msgs:
        print(f"[{symbol}] DEBUG: newest_api_created_at={msgs[0].get('created_at')} oldest_api_created_at={msgs[-1].get('created_at')}")

    return inserted

# -----------------------------
# Per-symbol state
# -----------------------------
@dataclass
class SymbolState:
    symbol: str
    min_id: int | None = None
    max_id: int | None = None
    backfill_active: bool = True

    def refresh_from_db(self, conn: sqlite3.Connection):
        mn, mx, cnt = db_get_min_max_ids(conn, self.symbol)
        self.min_id, self.max_id = mn, mx
        print(f"[{self.symbol}] DB has {cnt} msgs. Range: {self.min_id} (old) <-> {self.max_id} (new)")

# -----------------------------
# Main
# -----------------------------
def main():
    parser = argparse.ArgumentParser(description="Stocktwits multi-symbol scraper -> SQLite (today-mode).")
    parser.add_argument("symbols", nargs="*", help="Symbols. Defaults: AMD NVDA AAPL TSLA APP.")
    parser.add_argument("--db", type=str, default="stocktwits.db", help="SQLite DB file path.")
    parser.add_argument("--sleep_min", type=int, default=300, help="Min sleep seconds between full cycles.")
    parser.add_argument("--sleep_max", type=int, default=600, help="Max sleep seconds between full cycles.")
    parser.add_argument("--per_symbol_pause_min", type=float, default=0.7, help="Min pause seconds between calls.")
    parser.add_argument("--per_symbol_pause_max", type=float, default=1.5, help="Max pause seconds between calls.")
    args = parser.parse_args()

    symbols = [s.upper() for s in (args.symbols if args.symbols else ["AMD", "NVDA", "AAPL", "TSLA", "APP"])]

    print(f"START_ET cutoff: {START_UTC.astimezone(ET).strftime('%Y-%m-%d %I:%M %p %Z')}")
    print(f"START_UTC cutoff: {START_UTC.isoformat()}")

    conn = db_connect(args.db)
    scraper = cloudscraper.create_scraper()

    states: list[SymbolState] = []
    for sym in symbols:
        st = SymbolState(symbol=sym)
        st.refresh_from_db(conn)
        states.append(st)

    print(f"Starting SQLite Multi-Symbol Scraper: {', '.join(symbols)}")
    print(f"DB: {os.path.abspath(args.db)}")

    try:
        while True:
            any_inserted = False

            for st in states:
                st.refresh_from_db(conn)

                # ✅ TODAY-MODE LIVE: do NOT use since_id
                data_live = get_symbol_stream(scraper, st.symbol, since_id=None)

                if data_live is None:
                    print(f"[{st.symbol}] Live: No response / fetch failed.")
                else:
                    msgs_live = data_live.get("messages", [])
                    print(f"[{st.symbol}] DEBUG: live messages returned={len(msgs_live)}")
                    ins = db_insert_messages(conn, st.symbol, msgs_live)
                    if ins > 0:
                        print(f"[{st.symbol}] Live: Inserted {ins} new messages.")
                        any_inserted = True
                    else:
                        print(f"[{st.symbol}] Live: Inserted 0 (duplicates or all before cutoff).")

                time.sleep(random.uniform(args.per_symbol_pause_min, args.per_symbol_pause_max))

            wait = random.uniform(args.sleep_min, args.sleep_max)
            print(f"Cycle complete. Inserted_any={any_inserted}. Sleeping {wait/60:.2f} minutes...\n")
            time.sleep(wait)

    except KeyboardInterrupt:
        print("\nStopped.")
    finally:
        conn.close()

if __name__ == "__main__":
    main()