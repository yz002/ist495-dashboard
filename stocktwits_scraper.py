import cloudscraper
import json
import argparse
import os
import time
import random
from dataclasses import dataclass, field
import subprocess
import urllib.parse
import re
from collections import Counter
import shutil


STOPWORDS = {
    "the","a","an","and","or","but","if","then","so","to","of","in","on","for","with","as","at","by",
    "is","are","was","were","be","been","it","this","that","these","those","i","you","we","they",
    "my","your","our","their","me","him","her","them","us","from","will","just","not","do","does",
    "can","could","should","would","about","into","over","under","up","down","more","most","very"
}

def extract_keywords(text: str, top_n: int = 8) -> list[str]:
    """
    Simple keyword extractor:
    - lowercases
    - keeps words/nums/$
    - removes stopwords + very short tokens
    - returns top_n by frequency
    """
    if not text:
        return []
    tokens = re.findall(r"[A-Za-z0-9$]{3,}", text.lower())
    tokens = [t for t in tokens if t not in STOPWORDS and not t.isdigit()]
    if not tokens:
        return []
    counts = Counter(tokens)
    return [w for w, _ in counts.most_common(top_n)]

def reason_for_label(sentiment_val: str, text: str) -> str:
    """
    Simple rule-based explanation:
    - uses Stocktwits sentiment if present
    - otherwise infers neutral-ish and explains
    """
    base = sentiment_val.lower() if sentiment_val else "null"

    if base in ("bullish", "bearish"):
        return f"Stocktwits tag: {base}"
    # If no sentiment tag, add a lightweight heuristic:
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

def curl_impersonate_get_json(url: str, params: dict, impersonate: str = "chrome120"):
    """
    Fallback HTTP GET using curl-impersonate to mimic real browser TLS fingerprinting.
    Returns parsed JSON (dict) or raises on failure.
    """
    if shutil.which("curl-impersonate") is None:
        raise RuntimeError("curl-impersonate not found on PATH")
    qs = urllib.parse.urlencode(params)
    full_url = f"{url}?{qs}"

    cmd = [
        "curl-impersonate", impersonate,
        full_url,
        "-H", "accept: application/json",
        "-H", "user-agent: Mozilla/5.0",
        "--compressed",
        "--silent",
        "--show-error",
        "--fail",
    ]
    out = subprocess.check_output(cmd)
    return json.loads(out.decode("utf-8"))

def get_symbol_stream(symbol, max_id=None, since_id=None):
    """
    Fetches messages for a symbol.
    max_id: fetches messages older than this ID (backfill).
    since_id: fetches messages newer than this ID (live update).
    """
    scraper = cloudscraper.create_scraper()
    url = f"https://api.stocktwits.com/api/2/streams/symbol/{symbol}.json"
    params = {"limit": 50}
    if max_id:
        params["max"] = max_id
    if since_id:
        params["since"] = since_id

    print(f"Fetching {symbol} (max={max_id}, since={since_id})...")

    # ---- Attempt 1: cloudscraper (your current approach) ----
    try:
        response = scraper.get(url, params=params, timeout=20)
        if response.status_code == 200:
            return response.json()

        # Hard rate limit: keep your existing behavior
        if response.status_code == 429:
            print("Rate limited (429). Waiting 60s...")
            time.sleep(60)
            return None

        # Possible bot/tls blocks or transient issues -> try fallback
        if response.status_code in (401, 403, 406, 409, 418, 500, 502, 503, 504):
            print(f"Got {response.status_code}. Trying curl-impersonate fallback...")
            try:
                return curl_impersonate_get_json(url, params)
            except Exception as e2:
                print(f"curl-impersonate fallback failed: {e2}")
                return None

        print(f"Error {response.status_code} for {symbol}")
        return None

    except Exception as e:
        # Network/timeout/etc -> try fallback
        print(f"Error fetching {symbol} with cloudscraper: {e}")
        print("Trying curl-impersonate fallback...")
        try:
            return curl_impersonate_get_json(url, params)
        except Exception as e2:
            print(f"curl-impersonate fallback failed: {e2}")
            return None

@dataclass
class SymbolState:
    symbol: str
    output_filename: str
    existing_ids: set = field(default_factory=set)
    existing_data: list = field(default_factory=list)
    min_id: int | None = None   # backfill cursor (older)
    max_id: int | None = None   # live cursor (newer)
    backfill_active: bool = True

    def load_state(self):
        if os.path.exists(self.output_filename):
            try:
                with open(self.output_filename, "r", encoding="utf-8") as f:
                    self.existing_data = json.load(f) or []
                ids = [item.get("id") for item in self.existing_data if item.get("id")]
                self.existing_ids = set(ids)
                if ids:
                    self.min_id = min(ids)
                    self.max_id = max(ids)
                print(f"[{self.symbol}] Loaded {len(self.existing_data)} msgs. "
                      f"Range: {self.min_id} (old) <-> {self.max_id} (new)")
            except Exception as e:
                print(f"[{self.symbol}] Error loading file: {e}")

    def add_messages(self, msgs):
        added = 0
        for msg in msgs:
            mid = msg.get("id")
            if not mid or mid in self.existing_ids:
                continue
            user = msg.get("user", {}).get("username", "Unknown")
            body = msg.get("body", "")
            created_at = msg.get("created_at", "")
            sentiment = msg.get("entities", {}).get("sentiment", {})
            sentiment_val = sentiment.get("basic", "null") if sentiment else "null"

        # NOW safe to compute
            kw = extract_keywords(body, top_n=8)
            reason = reason_for_label(sentiment_val, body)
            notes = ""

            self.existing_data.append({
                "id": mid,
                "author": user,
                "time": created_at,
                "post": body,
                "sentiment": sentiment_val,
                "keywords": kw,
                "reason_for_label": reason,
                "notes": notes
            })
            self.existing_ids.add(mid)
            added += 1

        if self.existing_ids:
            self.max_id = max(self.existing_ids)
            self.min_id = min(self.existing_ids)

        return added

    def save(self):
        # Newest first (ID is a decent proxy)
        self.existing_data.sort(key=lambda x: x["id"], reverse=True)
        try:
            with open(self.output_filename, "w", encoding="utf-8") as f:
                json.dump(self.existing_data, f, indent=4)
            print(f"[{self.symbol}] Saved total {len(self.existing_data)} messages. "
                  f"(Newest: {self.max_id}, Oldest: {self.min_id})")
        except Exception as e:
            print(f"[{self.symbol}] Save error: {e}")

def main():
    parser = argparse.ArgumentParser(description="Scrape Stocktwits history + live updates (multi-symbol).")
    parser.add_argument(
        "symbols",
        nargs="*",
        help="Stock symbols (e.g., AAPL TSLA). If omitted, defaults to AMD NVDA AAPL TSLA."
    )
    parser.add_argument("--max_msgs", type=int, default=25, help="Stop after collecting this many messages")
    parser.add_argument("--outdir", type=str, default=".", help="Output folder for JSON files.")
    args = parser.parse_args()

    symbols = [s.upper() for s in (args.symbols if args.symbols else ["AMD", "NVDA", "AAPL", "TSLA"])]
    os.makedirs(args.outdir, exist_ok=True)

    # Build per-symbol states
    states = []
    for sym in symbols:
        out = os.path.join(args.outdir, f"{sym}_tweets.json")
        st = SymbolState(symbol=sym, output_filename=out)
        st.load_state()
        states.append(st)

    print(f"Starting Multi-Symbol Hybrid Scraper: {', '.join(symbols)}")

    try:
        while True:
            any_saved = False

            # Round-robin over symbols so the chart isn't insanely wide in time
            for st in states:
                # --- Phase 1: Live update ---
                data_live = get_symbol_stream(st.symbol, since_id=st.max_id)
                live_added = 0
                if data_live:
                    live_added = st.add_messages(data_live.get("messages", []))
                    if live_added:
                        print(f"[{st.symbol}] Live: Added {live_added} new messages.")
                        any_saved = True

                time.sleep(random.uniform(0.7, 1.5))
    
                # --- Phase 2: Backfill ---
                if st.backfill_active and st.min_id:
                    data_hist = get_symbol_stream(st.symbol, max_id=st.min_id)
                    hist_added = 0
                    if data_hist:
                        msgs = data_hist.get("messages", [])
                        if not msgs:
                            print(f"[{st.symbol}] Backfill: End of history reached.")
                            st.backfill_active = False
                        else:
                            hist_added = st.add_messages(msgs)
                            if hist_added:
                                print(f"[{st.symbol}] Backfill: Added {hist_added} older messages.")
                                any_saved = True

                time.sleep(random.uniform(0.7, 1.5))

            # Save once per full cycle if anything changed
            if any_saved:
                for st in states:
                    st.save()

            wait = random.uniform(300, 600)  # 5 to 10 minutes
            print(f"Cycle complete. Sleeping {wait/60:.2f} minutes...\n")
            time.sleep(wait)

    except KeyboardInterrupt:
        print("\nStopped.")

if __name__ == "__main__":
    main()

