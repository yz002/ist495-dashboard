# scrape_finviz_tickers_curl_mongo.py
# - Dynamic Finviz ticker universe
# - Live updates into MongoDB
# - Optional historical backfill into MongoDB
# - Per-ticker state stored in Mongo "state" collection
# - Daily cutoff auto-updates each cycle (default: today 6:00 AM ET)
# - Optional override: --start_et "YYYY-MM-DD HH:MM" (ET)
# - Uses curl-impersonate via curl_cffi
# - Added: unstructured-news enrichment fields for duplicate/spam/rumor/source tagging
# - Refactored with explicit normalize helper + live/backfill phases

import argparse
import csv
import hashlib
import os
import random
import re
import time
from collections import Counter
from datetime import datetime, timezone
from typing import Any, Optional
from urllib.parse import urlparse
from zoneinfo import ZoneInfo

from curl_cffi import requests
from pymongo import ASCENDING, DESCENDING, MongoClient
from pymongo.errors import BulkWriteError

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

def parse_stocktwits_time(created_at: str) -> Optional[datetime]:
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
URL_RE = re.compile(r"(https?://[^\s\]\)<>\"']+)", re.IGNORECASE)
WHITESPACE_RE = re.compile(r"\s+")

TRADITIONAL_DOMAINS = {
    "reuters.com",
    "bloomberg.com",
    "wsj.com",
    "ft.com",
    "cnbc.com",
    "marketwatch.com",
    "finance.yahoo.com",
    "seekingalpha.com",
    "investing.com",
    "sec.gov",
    "nasdaq.com",
    "nytimes.com",
    "apnews.com",
    "theverge.com",
    "techcrunch.com",
    "businesswire.com",
    "globenewswire.com",
    "prnewswire.com",
}

RUMOR_CUES = (
    "hearing",
    "rumor",
    "rumours",
    "unconfirmed",
    "supposedly",
    "people saying",
    "word is",
    "source says",
    "might be",
    "could be",
    "looks like",
    "apparently",
    "allegedly",
    "heard that",
)

VERIFIED_CUES = (
    "confirmed",
    "official",
    "press release",
    "sec filing",
    "earnings release",
    "8-k",
    "10-q",
    "10-k",
)

LOW_QUALITY_PATTERNS = (
    "to the moon",
    "mooning",
    "load up",
    "lfg",
    "🚀",
    "💎",
)

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

def extract_urls(text: str) -> list[str]:
    if not text:
        return []
    urls = URL_RE.findall(text)
    cleaned = []
    for u in urls:
        u = u.strip().rstrip(".,;:!?)\"]'")
        cleaned.append(u)
    out = []
    seen = set()
    for u in cleaned:
        if u not in seen:
            seen.add(u)
            out.append(u)
    return out

def domain_of(url: str) -> str:
    try:
        host = urlparse(url).netloc.lower()
        if host.startswith("www."):
            host = host[4:]
        return host
    except Exception:
        return ""

def classify_source_type(domains: list[str]) -> str:
    if not domains:
        return "No link"
    for dom in domains:
        for trusted in TRADITIONAL_DOMAINS:
            if dom == trusted or dom.endswith("." + trusted):
                return "Traditional"
    return "Rumor/Social"

def normalize_post(text: str) -> str:
    if not text:
        return ""
    t = text.lower().strip()
    t = URL_RE.sub(" ", t)
    t = TICKER_RE.sub(" ", t)
    t = WHITESPACE_RE.sub(" ", t)
    return t.strip()

def post_hash(normalized_text: str) -> str:
    if not normalized_text:
        return ""
    return hashlib.sha256(normalized_text.encode("utf-8")).hexdigest()

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

def rumor_flag_and_reason(text: str, source_type: str) -> tuple[bool, str]:
    t = (text or "").lower()

    if source_type == "Traditional":
        if any(cue in t for cue in VERIFIED_CUES):
            return False, "Traditional source and verification-style wording"
        return False, "Traditional source link present"

    if source_type == "No link":
        for cue in RUMOR_CUES:
            if cue in t:
                return True, f"No link; rumor-like wording: {cue}"
        return False, "No link but no explicit rumor cue"

    for cue in RUMOR_CUES:
        if cue in t:
            return True, f"Non-traditional source and rumor-like wording: {cue}"

    return True, "Non-traditional source link present"

def low_quality_flag_and_reason(text: str, normalized_text: str, ticker_mentions: list[str]) -> tuple[bool, str]:
    raw = text or ""
    t = raw.lower().strip()

    if not t:
        return True, "Empty post"

    if len(normalized_text) < 4:
        return True, "Very short normalized text"

    if len(raw.strip()) <= 3:
        return True, "Very short raw post"

    if len(ticker_mentions) > 0 and normalized_text == "":
        return True, "Ticker-only post"

    if raw.isupper() and len(raw) > 8:
        return True, "All-caps post"

    for pat in LOW_QUALITY_PATTERNS:
        if pat in t:
            return True, f"Low-quality hype wording: {pat}"

    if len(set(raw.strip())) == 1 and len(raw.strip()) > 3:
        return True, "Repeated single character pattern"

    return False, ""

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
# Normalization helper
# -----------------------------
def normalize_stocktwits_message(msg: dict[str, Any], symbol: str, scraped_at_iso: str) -> Optional[dict[str, Any]]:
    """
    Convert a Stocktwits API message into the enriched MongoDB document shape.
    Returns None if the message cannot be normalized.
    """
    mid = msg.get("id")
    if mid is None:
        return None

    user = msg.get("user", {}).get("username", "Unknown")
    body = msg.get("body", "")
    created_at = msg.get("created_at", "")
    created_at_utc = parse_stocktwits_time(created_at)
    if created_at_utc is None:
        return None

    sentiment = msg.get("entities", {}).get("sentiment", {})
    sentiment_val = sentiment.get("basic", "null") if sentiment else "null"

    ticker_mentions = extract_ticker_mentions(body)
    keywords = extract_keywords(body, top_n=8)
    label_reason = reason_for_label(sentiment_val, body)
    notes = auto_notes(body)

    stocktwits_link = f"https://stocktwits.com/{user}/message/{mid}"

    urls = extract_urls(body)
    domains = [domain_of(u) for u in urls if u]
    domains = [d for d in domains if d]

    normalized = normalize_post(body)
    body_hash = post_hash(normalized)
    source_type = classify_source_type(domains)
    rumor_flag, rumor_reason = rumor_flag_and_reason(body, source_type)
    is_low_quality, low_quality_reason = low_quality_flag_and_reason(body, normalized, ticker_mentions)

    return {
        "id": int(mid),
        "stream_symbol": symbol,
        "author": user,

        "created_at": created_at,
        "created_at_dt": created_at_utc,

        "scraped_at_utc": scraped_at_iso,
        "post": body,
        "normalized_post": normalized,
        "post_hash": body_hash,

        "sentiment": sentiment_val,
        "reason_for_label": label_reason,

        "keywords": keywords,
        "ticker_mentions": ticker_mentions,
        "notes": notes,

        "link": stocktwits_link,

        "urls": urls,
        "link_domains": domains,
        "has_link": len(urls) > 0,
        "source_type": source_type,

        "rumor_flag": rumor_flag,
        "rumor_reason": rumor_reason,

        "is_low_quality": is_low_quality,
        "low_quality_reason": low_quality_reason,

        # placeholders for later downstream processing
        "is_duplicate_exact": False,
        "is_duplicate_near": False,
        "duplicate_reason": "",
        "is_spam": False,
        "spam_reason": "",

        "raw_json": msg,
    }

# -----------------------------
# Finviz CSV reader
# -----------------------------
def read_finviz_tickers(csv_path: str) -> list[str]:
    tickers = []
    with open(csv_path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames or "Ticker" not in reader.fieldnames:
            raise ValueError(f"Finviz CSV must contain a 'Ticker' column. Found: {reader.fieldnames}")
        for row in reader:
            t = (row.get("Ticker") or "").strip().upper()
            if t:
                tickers.append(t)

    seen = set()
    out = []
    for t in tickers:
        if t not in seen:
            seen.add(t)
            out.append(t)
    return out

def safe_read_universe(csv_path: str, prev_active: list[str]) -> list[str]:
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
def get_symbol_stream(
    symbol: str,
    max_id: Optional[int] = None,
    since_id: Optional[int] = None,
    impersonate: str = "chrome",
    timeout: int = 20,
    retries: int = 3,
) -> Optional[dict[str, Any]]:
    """
    Fetch messages for a symbol using curl_cffi browser impersonation.
    since_id: fetch newer messages
    max_id: fetch older messages (backfill)
    """
    url = f"https://api.stocktwits.com/api/2/streams/symbol/{symbol}.json"
    params = {"limit": 50}

    if max_id is not None:
        params["max"] = max_id
    if since_id is not None:
        params["since"] = since_id

    for attempt in range(1, retries + 1):
        print(f"Fetching {symbol} (max={max_id}, since={since_id})... attempt {attempt}/{retries}")
        try:
            resp = requests.get(
                url,
                params=params,
                impersonate=impersonate,
                timeout=timeout,
                headers={
                    "Accept": "application/json, text/plain, */*",
                    "Referer": f"https://stocktwits.com/symbol/{symbol}",
                    "User-Agent": (
                        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/136.0.0.0 Safari/537.36"
                    ),
                },
            )

            if resp.status_code == 200:
                return resp.json()

            if resp.status_code == 404:
                return {"_status": 404}

            if resp.status_code == 429:
                retry_after = resp.headers.get("Retry-After")
                wait = int(retry_after) if retry_after and str(retry_after).isdigit() else 60
                wait = min(wait, 600)
                print(f"{symbol}: rate limited (429). Waiting {wait}s...")
                time.sleep(wait)
                return None

            if resp.status_code in (401, 403, 406, 409, 418, 500, 502, 503, 504):
                wait = 10 * attempt
                print(f"{symbol}: HTTP {resp.status_code}. Waiting {wait}s then retry...")
                time.sleep(wait)
                continue

            print(f"{symbol}: HTTP {resp.status_code} (non-retry). Body snippet: {resp.text[:300]}")
            return None

        except Exception as e:
            wait = 10 * attempt
            print(f"{symbol}: request error: {e}. Waiting {wait}s then retry...")
            time.sleep(wait)

    print(f"{symbol}: failed after {retries} retries.")
    return None

# -----------------------------
# Mongo helpers
# -----------------------------
def ensure_indexes(messages_col, state_col):
    existing = messages_col.index_information()

    def has_index_on(keys_tuple):
        for _, info in existing.items():
            if tuple(info.get("key", [])) == keys_tuple:
                return True
        return False

    if not has_index_on((("id", 1),)):
        messages_col.create_index([("id", ASCENDING)], unique=True, name="id_unique")

    if not has_index_on((("stream_symbol", 1), ("id", -1))):
        messages_col.create_index([("stream_symbol", ASCENDING), ("id", DESCENDING)], name="sym_id_desc")

    if not has_index_on((("created_at", 1),)):
        messages_col.create_index([("created_at", ASCENDING)], name="created_at_idx")

    if not has_index_on((("created_at_dt", 1),)):
        messages_col.create_index([("created_at_dt", ASCENDING)], name="created_at_dt_idx")

    if not has_index_on((("stream_symbol", 1), ("created_at_dt", -1))):
        messages_col.create_index([("stream_symbol", ASCENDING), ("created_at_dt", DESCENDING)], name="sym_created_dt_desc")

    if not has_index_on((("post_hash", 1),)):
        messages_col.create_index([("post_hash", ASCENDING)], name="post_hash_idx")

    if not has_index_on((("stream_symbol", 1), ("post_hash", 1))):
        messages_col.create_index([("stream_symbol", ASCENDING), ("post_hash", ASCENDING)], name="sym_post_hash_idx")

    if not has_index_on((("source_type", 1),)):
        messages_col.create_index([("source_type", ASCENDING)], name="source_type_idx")

    if not has_index_on((("rumor_flag", 1),)):
        messages_col.create_index([("rumor_flag", ASCENDING)], name="rumor_flag_idx")

    if not has_index_on((("is_low_quality", 1),)):
        messages_col.create_index([("is_low_quality", ASCENDING)], name="is_low_quality_idx")

    try:
        state_col.create_index([("_id", ASCENDING)], name="state_id_idx")
    except Exception:
        pass

def state_get(state_col, symbol: str) -> dict:
    doc = state_col.find_one({"_id": symbol})
    return doc or {
        "_id": symbol,
        "last_max_id": None,
        "oldest_min_id": None,
        "backfill_complete": False,
        "fail_404": False,
        "last_seen_utc": 0.0,
    }

def state_upsert(state_col, symbol: str, **fields):
    state_col.update_one({"_id": symbol}, {"$set": fields}, upsert=True)

def mongo_get_max_id(messages_col, symbol: str) -> Optional[int]:
    doc = messages_col.find_one({"stream_symbol": symbol}, sort=[("id", DESCENDING)], projection={"id": 1})
    return doc["id"] if doc and "id" in doc else None

def mongo_get_min_id(messages_col, symbol: str) -> Optional[int]:
    doc = messages_col.find_one({"stream_symbol": symbol}, sort=[("id", ASCENDING)], projection={"id": 1})
    return doc["id"] if doc and "id" in doc else None

def mongo_insert_messages(
    messages_col,
    symbol: str,
    msgs: list[dict[str, Any]],
    start_utc: datetime,
) -> tuple[int, Optional[int], Optional[int]]:
    """
    Insert normalized messages newer than the daily cutoff.
    Returns:
      (inserted_count, newest_inserted_id, oldest_inserted_id)
    """
    if not msgs:
        return 0, None, None

    inserted = 0
    filtered_before_cutoff = 0
    parse_failed = 0
    skipped_missing_id = 0

    now_iso = utc_now_iso()
    docs: list[dict[str, Any]] = []

    for msg in msgs:
        mid = msg.get("id")
        if mid is None:
            skipped_missing_id += 1
            continue

        created_at = msg.get("created_at", "")
        created_at_utc = parse_stocktwits_time(created_at)
        if created_at_utc is None:
            parse_failed += 1
            continue

        if created_at_utc < start_utc:
            filtered_before_cutoff += 1
            continue

        doc = normalize_stocktwits_message(msg, symbol=symbol, scraped_at_iso=now_iso)
        if doc is None:
            parse_failed += 1
            continue

        docs.append(doc)

    if not docs:
        print(
            f"[{symbol}] inserted=0, filtered_before_cutoff={filtered_before_cutoff}, "
            f"parse_failed={parse_failed}, skipped_missing_id={skipped_missing_id}, api_msgs={len(msgs)}"
        )
        return 0, None, None

    newest_inserted_id = max(d["id"] for d in docs) if docs else None
    oldest_inserted_id = min(d["id"] for d in docs) if docs else None

    try:
        res = messages_col.insert_many(docs, ordered=False)
        inserted = len(res.inserted_ids)
    except BulkWriteError as bwe:
        write_errors = bwe.details.get("writeErrors", [])
        dupes = sum(1 for e in write_errors if e.get("code") == 11000)
        inserted = bwe.details.get("nInserted", 0)
        other = len(write_errors) - dupes
        if other > 0:
            print(f"[{symbol}] BulkWriteError non-dup errors={other} (dupes={dupes}).")
    except Exception as e:
        print(f"[{symbol}] Mongo insert error: {e}")
        inserted = 0

    print(
        f"[{symbol}] inserted={inserted}, filtered_before_cutoff={filtered_before_cutoff}, "
        f"parse_failed={parse_failed}, skipped_missing_id={skipped_missing_id}, api_msgs={len(msgs)}"
    )
    if msgs:
        print(f"[{symbol}] newest_api_created_at={msgs[0].get('created_at')} oldest_api_created_at={msgs[-1].get('created_at')}")

    return inserted, newest_inserted_id, oldest_inserted_id

# -----------------------------
# Main
# -----------------------------
def main():
    ap = argparse.ArgumentParser(description="Dynamic Finviz → Stocktwits scraper (curl-impersonate) into MongoDB.")
    ap.add_argument("--finviz_csv", required=True, help="Path to Finviz CSV export (must contain 'Ticker')")
    ap.add_argument("--limit_tickers", type=int, default=0, help="Optional cap on # tickers (0 = all)")
    ap.add_argument("--sleep_min", type=int, default=240, help="Min sleep seconds between scrape cycles")
    ap.add_argument("--sleep_max", type=int, default=480, help="Max sleep seconds between scrape cycles")
    ap.add_argument("--per_symbol_pause_min", type=float, default=0.8, help="Min pause seconds between API calls")
    ap.add_argument("--per_symbol_pause_max", type=float, default=1.8, help="Max pause seconds between API calls")
    ap.add_argument("--start_et", default="", help='Optional cutoff override in ET: "YYYY-MM-DD HH:MM" (default: today 06:00)')
    ap.add_argument("--full_refresh", action="store_true", help="Ignore since_id and always pull latest page")
    ap.add_argument("--enable_backfill", action="store_true", help="Also backfill older messages using max_id")
    ap.add_argument("--backfill_pages_per_cycle", type=int, default=1, help="How many backfill pages per ticker per cycle")
    ap.add_argument("--impersonate", default="chrome", help='Browser fingerprint target, e.g. "chrome", "safari", "edge"')
    ap.add_argument("--timeout", type=int, default=20, help="HTTP timeout in seconds")
    ap.add_argument("--universe_refresh_seconds", type=int, default=10, help="How often to reload finviz CSV")
    ap.add_argument("--stale_drop_seconds", type=int, default=600, help="Stop scraping tickers not seen in finviz for this long")

    ap.add_argument("--mongo_uri", default="mongodb://localhost:27017", help="MongoDB URI")
    ap.add_argument("--mongo_db", default="stocktwits", help="Mongo database name")
    ap.add_argument("--mongo_collection", default="messages", help="Mongo collection for messages")
    ap.add_argument("--mongo_state_collection", default="state", help="Mongo collection for per-symbol state")
    args = ap.parse_args()

    client = MongoClient(args.mongo_uri)
    db = client[args.mongo_db]
    messages_col = db[args.mongo_collection]
    state_col = db[args.mongo_state_collection]

    ensure_indexes(messages_col, state_col)

    active: list[str] = []
    active_set: set[str] = set()

    last_csv_mtime = 0.0
    last_universe_reload_ts = 0.0

    print(f"Mongo: {args.mongo_uri}  DB={args.mongo_db}  messages={args.mongo_collection}  state={args.mongo_state_collection}")
    print(f"Universe refresh: {args.universe_refresh_seconds}s  | stale drop after: {args.stale_drop_seconds}s")
    print(f"Backfill enabled: {args.enable_backfill} | pages per cycle: {args.backfill_pages_per_cycle}")

    try:
        while True:
            if args.start_et.strip():
                start_utc = parse_et_to_utc(args.start_et.strip())
            else:
                start_utc = build_start_utc_today_6am()

            ts = time.time()
            mtime = file_mtime(args.finviz_csv)

            if (ts - last_universe_reload_ts) >= args.universe_refresh_seconds:
                new_list = safe_read_universe(args.finviz_csv, active)
                if args.limit_tickers and args.limit_tickers > 0:
                    new_list = new_list[: args.limit_tickers]

                new_set = set(new_list)
                added = new_set - active_set
                removed = active_set - new_set
                mtime_changed = (mtime != last_csv_mtime)

                print(f"\n[UNIVERSE] tickers={len(new_list)} added={len(added)} removed={len(removed)} mtime_changed={mtime_changed}")

                now_seen = now_utc_ts()
                for t in new_set:
                    st = state_get(state_col, t)
                    if st.get("last_max_id") is None:
                        st["last_max_id"] = mongo_get_max_id(messages_col, t)
                    if st.get("oldest_min_id") is None:
                        st["oldest_min_id"] = mongo_get_min_id(messages_col, t)

                    state_upsert(
                        state_col,
                        t,
                        last_max_id=st.get("last_max_id"),
                        oldest_min_id=st.get("oldest_min_id"),
                        backfill_complete=bool(st.get("backfill_complete", False)),
                        fail_404=bool(st.get("fail_404", False)),
                        last_seen_utc=now_seen,
                    )

                active = new_list
                active_set = new_set
                last_csv_mtime = mtime
                last_universe_reload_ts = ts

            cutoff_seen = now_utc_ts() - float(args.stale_drop_seconds)
            fresh = []
            for t in active:
                st = state_get(state_col, t)
                if float(st.get("last_seen_utc", 0.0)) >= cutoff_seen:
                    fresh.append(t)
            active = fresh
            active_set = set(active)

            print(
                f"\n=== CYCLE START === active={len(active)} "
                f"cutoff_ET={start_utc.astimezone(ET).strftime('%Y-%m-%d %I:%M %p %Z')} "
                f"cutoff_UTC={start_utc.isoformat()}"
            )

            inserted_any = False

            for sym in active:
                st_doc = state_get(state_col, sym)

                if st_doc.get("fail_404", False):
                    continue

                changed_state: dict[str, Any] = {}

                # -------------------------
                # Phase 1: Live update
                # -------------------------
                since_id = None
                if not args.full_refresh:
                    if st_doc.get("last_max_id") is None:
                        st_doc["last_max_id"] = mongo_get_max_id(messages_col, sym)
                    since_id = st_doc.get("last_max_id")

                data_live = get_symbol_stream(
                    sym,
                    since_id=since_id,
                    impersonate=args.impersonate,
                    timeout=args.timeout,
                )

                if isinstance(data_live, dict) and data_live.get("_status") == 404:
                    state_upsert(state_col, sym, fail_404=True)
                    print(f"[{sym}] Symbol not found (404). Marking fail_404=True and skipping future.")
                    continue

                if data_live is None:
                    print(f"[{sym}] live fetch failed / no data.")
                else:
                    msgs_live = data_live.get("messages") or []
                    ins_live, newest_live_id, oldest_live_id = mongo_insert_messages(messages_col, sym, msgs_live, start_utc)

                    if ins_live > 0:
                        changed_state["last_max_id"] = mongo_get_max_id(messages_col, sym)
                        # initialize oldest_min_id if empty
                        if st_doc.get("oldest_min_id") is None:
                            changed_state["oldest_min_id"] = mongo_get_min_id(messages_col, sym)
                        inserted_any = True
                        print(f"[{sym}] Live: added {ins_live} new messages.")

                time.sleep(random.uniform(args.per_symbol_pause_min, args.per_symbol_pause_max))

                # -------------------------
                # Phase 2: Optional backfill
                # -------------------------
                if args.enable_backfill and not bool(st_doc.get("backfill_complete", False)):
                    if st_doc.get("oldest_min_id") is None:
                        st_doc["oldest_min_id"] = mongo_get_min_id(messages_col, sym)

                    backfill_min_id = st_doc.get("oldest_min_id")
                    pages_done = 0

                    while backfill_min_id is not None and pages_done < int(args.backfill_pages_per_cycle):
                        data_hist = get_symbol_stream(
                            sym,
                            max_id=backfill_min_id,
                            impersonate=args.impersonate,
                            timeout=args.timeout,
                        )

                        if isinstance(data_hist, dict) and data_hist.get("_status") == 404:
                            changed_state["fail_404"] = True
                            break

                        if data_hist is None:
                            print(f"[{sym}] backfill fetch failed / no data.")
                            break

                        msgs_hist = data_hist.get("messages") or []
                        if not msgs_hist:
                            changed_state["backfill_complete"] = True
                            print(f"[{sym}] Backfill: end of history reached.")
                            break

                        ins_hist, newest_hist_id, oldest_hist_id = mongo_insert_messages(messages_col, sym, msgs_hist, start_utc)

                        if ins_hist > 0:
                            changed_state["oldest_min_id"] = mongo_get_min_id(messages_col, sym)
                            inserted_any = True
                            print(f"[{sym}] Backfill: added {ins_hist} older messages.")
                            backfill_min_id = changed_state["oldest_min_id"]
                        else:
                            # nothing inserted in this page, but advance guard using current DB min
                            current_min = mongo_get_min_id(messages_col, sym)
                            if current_min is None or current_min == backfill_min_id:
                                changed_state["backfill_complete"] = True
                                print(f"[{sym}] Backfill: no additional older messages inserted; marking complete.")
                                break
                            backfill_min_id = current_min
                            changed_state["oldest_min_id"] = current_min

                        pages_done += 1
                        time.sleep(random.uniform(args.per_symbol_pause_min, args.per_symbol_pause_max))

                if changed_state:
                    state_upsert(state_col, sym, **changed_state)

            wait = random.uniform(args.sleep_min, args.sleep_max)
            print(f"=== CYCLE END === inserted_any={inserted_any}. Sleeping {wait/60:.2f} minutes...\n")
            time.sleep(wait)

    except KeyboardInterrupt:
        print("\nStopped.")
    finally:
        client.close()

if __name__ == "__main__":
    main()