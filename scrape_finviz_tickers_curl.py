# finviz_stocktwits_scraper_curl_dynamic.py
# - Dynamic ticker universe: reload finviz CSV every N seconds, detect add/remove
# - Scrape only active tickers
# - Keep per-ticker since_id state, so returning tickers continue smoothly
# - Blacklist 404 symbols
# - Daily cutoff auto-updates each cycle (default: today 6:00 AM ET)
# - Optional override: --start_et "YYYY-MM-DD HH:MM" (ET)
# - Uses curl-impersonate via curl_cffi

import argparse
import csv
import json
import os
import random
import re
import sqlite3
import time
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from curl_cffi import requests

ET = ZoneInfo("America/New_York")

# -----------------------------
# Time helpers
# -----------------------------
def build_start_utc_today_6am() -> datetime:
    now_et = datetime.now(ET)
    start_et = now_et.replace(hour=6, minute=0, second=0, microsecond=0)
    return start_et.astimezone(timezone.utc)

def parse_et_to_utc(dt_str: str) -> datetime:
    dt_et = datetime.strptime(dt_str, "%Y-%m-%d %H:%M").replace(tzinfo=ET)
    return dt_et.astimezone(timezone.utc)

def parse_stocktwits_time(created_at: str):
    if not created_at:
        return None
    try:
        return datetime.fromisoformat(created_at.replace("Z", "+00:00")).astimezone(timezone.utc)
    except Exception:
        return None

def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

def now_utc_ts() -> float:
    return datetime.now(timezone.utc).timestamp()

def file_mtime(path: str) -> float:
    try:
        return os.path.getmtime(path)
    except Exception:
        return 0.0

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

def extract_ticker_mentions(text: str):
    if not text:
        return []
    return sorted({m.group(1).upper() for m in TICKER_RE.finditer(text)})

def extract_keywords(text: str, top_n: int = 8):
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
# Finviz CSV reader
# -----------------------------
def read_finviz_tickers(csv_path: str):
    tickers = []
    with open(csv_path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames or "Ticker" not in reader.fieldnames:
            raise ValueError(f"Finviz CSV must contain a 'Ticker' column. Found: {reader.fieldnames}")
        for row in reader:
            t = (row.get("Ticker") or "").strip().upper()
            if t:
                tickers.append(t)

    # unique preserve order
    seen = set()
    out = []
    for t in tickers:
        if t not in seen:
            seen.add(t)
            out.append(t)
    return out

def safe_read_universe(csv_path: str, prev_active: list[str]) -> list[str]:
    """
    Read finviz tickers safely.
    If the file is temporarily unreadable OR returns 0 tickers, keep previous active list.
    """
    try:
        new_list = read_finviz_tickers(csv_path)
        if not new_list:
            print("[UNIVERSE] WARNING: read returned 0 tickers; keeping previous universe.")
            return prev_active
        return new_list
    except Exception as e:
        print(f"[UNIVERSE] WARNING: failed reading universe ({e}); keeping previous universe.")
        return prev_active

# -----------------------------
# Fetch (curl-impersonate)
# -----------------------------
def get_symbol_stream(symbol: str, max_id=None, since_id=None, retries: int = 3):
    url = f"https://api.stocktwits.com/api/2/streams/symbol/{symbol}.json"
    params = {"limit": 50}
    if max_id:
        params["max"] = max_id
    if since_id:
        params["since"] = since_id

    for attempt in range(1, retries + 1):
        print(f"Fetching {symbol} (max={max_id}, since={since_id})... attempt {attempt}/{retries}")
        try:
            resp = requests.get(
                url,
                params=params,
                impersonate="chrome",
                timeout=20,
                headers={
                    "accept": "application/json, text/plain, */*",
                    "accept-language": "en-US,en;q=0.9",
                    "origin": "https://stocktwits.com",
                    "referer": f"https://stocktwits.com/symbol/{symbol}",
                },
            )

            if resp.status_code == 200:
                return resp.json()

            if resp.status_code == 404:
                # Symbol not found → permanently skip
                return {"_status": 404}

            if resp.status_code == 429:
                retry_after = resp.headers.get("Retry-After")
                wait = int(retry_after) if retry_after and str(retry_after).isdigit() else 60
                wait = min(wait, 600)
                print(f"Rate limited (429). Waiting {wait}s...")
                time.sleep(wait)
                return None

            if resp.status_code in (401, 403, 406, 409, 418, 500, 502, 503, 504):
                wait = 10 * attempt
                print(f"{symbol}: HTTP {resp.status_code}. Waiting {wait}s then retry...")
                time.sleep(wait)
                continue

            print(f"{symbol}: HTTP {resp.status_code} (non-retry). Body snippet: {resp.text[:200]}")
            return None

        except Exception as e:
            wait = 10 * attempt
            print(f"{symbol}: fetch error: {e}. Waiting {wait}s then retry...")
            time.sleep(wait)

    print(f"{symbol}: failed after {retries} retries.")
    return None

# -----------------------------
# SQLite
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

def db_get_max_id(conn: sqlite3.Connection, symbol: str):
    cur = conn.cursor()
    cur.execute("SELECT MAX(id) FROM messages WHERE stream_symbol = ?", (symbol,))
    row = cur.fetchone()
    return row[0] if row and row[0] is not None else None

def db_insert_messages(conn: sqlite3.Connection, symbol: str, msgs: list[dict], start_utc: datetime) -> int:
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

        if created_at_utc < start_utc:
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
                ),
            )
            if cur.rowcount == 1:
                inserted += 1
        except Exception as e:
            print(f"[{symbol}] DB insert error for id={mid}: {e}")

    conn.commit()
    print(
        f"[{symbol}] inserted={inserted}, filtered_before_cutoff={filtered_before_cutoff}, "
        f"parse_failed={parse_failed}, api_msgs={len(msgs)}"
    )
    if msgs:
        print(f"[{symbol}] newest_api_created_at={msgs[0].get('created_at')} oldest_api_created_at={msgs[-1].get('created_at')}")
    return inserted

# -----------------------------
# State
# -----------------------------
@dataclass
class SymbolState:
    symbol: str
    last_max_id: int | None = None
    fail_404: bool = False
    last_seen_utc: float = 0.0

# -----------------------------
# Main
# -----------------------------
def main():
    ap = argparse.ArgumentParser(description="Dynamic Finviz → Stocktwits scraper (curl-impersonate) into SQLite.")
    ap.add_argument("--db", required=True, help="Path to stocktwits.db")
    ap.add_argument("--finviz_csv", required=True, help="Path to Finviz CSV export (must contain 'Ticker')")
    ap.add_argument("--limit_tickers", type=int, default=0, help="Optional cap on # tickers (0 = all)")
    ap.add_argument("--sleep_min", type=int, default=240, help="Min sleep seconds between scrape cycles")
    ap.add_argument("--sleep_max", type=int, default=480, help="Max sleep seconds between scrape cycles")
    ap.add_argument("--per_symbol_pause_min", type=float, default=0.8, help="Min pause seconds between API calls")
    ap.add_argument("--per_symbol_pause_max", type=float, default=1.8, help="Max pause seconds between API calls")
    ap.add_argument("--start_et", default="", help='Optional cutoff override in ET: "YYYY-MM-DD HH:MM" (default: today 06:00)')
    ap.add_argument("--full_refresh", action="store_true", help="Ignore since_id and always pull latest page")
    ap.add_argument("--universe_refresh_seconds", type=int, default=10, help="How often to reload finviz CSV")
    ap.add_argument("--stale_drop_seconds", type=int, default=60, help="Stop scraping tickers not seen in finviz for this long")
    args = ap.parse_args()

    conn = db_connect(args.db)

    states: dict[str, SymbolState] = {}
    active: list[str] = []
    active_set: set[str] = set()

    last_csv_mtime = 0.0
    last_universe_reload_ts = 0.0

    print(f"DB: {os.path.abspath(args.db)}")
    print(f"Universe refresh: {args.universe_refresh_seconds}s  | stale drop after: {args.stale_drop_seconds}s")

    try:
        while True:
            # cutoff recomputed each loop (overnight safe)
            if args.start_et.strip():
                start_utc = parse_et_to_utc(args.start_et.strip())
            else:
                start_utc = build_start_utc_today_6am()

            # Universe reload (only when file changes OR timer says reload anyway)
            ts = time.time()
            mtime = file_mtime(args.finviz_csv)

            if (ts - last_universe_reload_ts) >= args.universe_refresh_seconds:
                new_list = safe_read_universe(args.finviz_csv, active)
                if args.limit_tickers and args.limit_tickers > 0:
                    new_list = new_list[: args.limit_tickers]

                new_set = set(new_list)

                added = new_set - active_set
                removed = active_set - new_set

                print(f"\n[UNIVERSE] tickers={len(new_list)} added={len(added)} removed={len(removed)} mtime_changed=True")

                # add/seed states
                for t in added:
                    if t not in states:
                        states[t] = SymbolState(symbol=t, last_max_id=db_get_max_id(conn, t))
                    states[t].last_seen_utc = now_utc_ts()

                # update seen for existing tickers
                for t in (new_set & active_set):
                    states[t].last_seen_utc = now_utc_ts()

                active = new_list
                active_set = new_set
                last_csv_mtime = mtime
                last_universe_reload_ts = ts

            # drop stale tickers (not seen recently)
            cutoff_seen = now_utc_ts() - float(args.stale_drop_seconds)
            active_set = set(active)
            active = [t for t in active if states.get(t) and states[t].last_seen_utc >= cutoff_seen]

            print(
                f"\n=== CYCLE START === active={len(active)} cutoff_ET={start_utc.astimezone(ET).strftime('%Y-%m-%d %I:%M %p %Z')}  cutoff_UTC={start_utc.isoformat()}"
            )

            inserted_any = False

            for sym in active:
                st = states.get(sym)
                if st is None:
                    st = SymbolState(symbol=sym, last_max_id=db_get_max_id(conn, sym))
                    states[sym] = st

                if st.fail_404:
                    continue

                since_id = None
                if not args.full_refresh:
                    if st.last_max_id is None:
                        st.last_max_id = db_get_max_id(conn, sym)
                    since_id = st.last_max_id

                data = get_symbol_stream(sym, since_id=since_id)
                if isinstance(data, dict) and data.get("_status") == 404:
                    st.fail_404 = True
                    print(f"[{sym}] Symbol not found (404). Marking fail_404=True and skipping future.")
                    continue

                if data is None:
                    print(f"[{sym}] fetch failed / no data.")
                else:
                    msgs = data.get("messages") or []
                    ins = db_insert_messages(conn, sym, msgs, start_utc)
                    if ins > 0:
                        st.last_max_id = db_get_max_id(conn, sym)
                        inserted_any = True

                time.sleep(random.uniform(args.per_symbol_pause_min, args.per_symbol_pause_max))

            wait = random.uniform(args.sleep_min, args.sleep_max)
            print(f"=== CYCLE END === inserted_any={inserted_any}. Sleeping {wait/60:.2f} minutes...\n")
            time.sleep(wait)

    except KeyboardInterrupt:
        print("\nStopped.")
    finally:
        conn.close()

if __name__ == "__main__":
    main()
