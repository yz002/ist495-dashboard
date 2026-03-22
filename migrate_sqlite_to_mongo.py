import argparse
import sqlite3
import json
from datetime import datetime, timezone
from pymongo import MongoClient, UpdateOne

def parse_iso_z(s: str):
    # "2026-02-09T22:40:29Z"
    return datetime.fromisoformat(s.replace("Z", "+00:00")).astimezone(timezone.utc)

def json_list(x):
    if x is None:
        return []
    if isinstance(x, list):
        return x
    try:
        return json.loads(x)
    except Exception:
        return []

def notes_list(x):
    if not x:
        return []
    return [t.strip() for t in str(x).split(",") if t.strip()]

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--sqlite_db", required=True)
    ap.add_argument("--mongo_uri", default="mongodb://localhost:27017")
    ap.add_argument("--mongo_db", default="stocktwits")
    ap.add_argument("--collection", default="messages")
    ap.add_argument("--batch", type=int, default=2000)
    args = ap.parse_args()

    conn = sqlite3.connect(args.sqlite_db)
    cur = conn.cursor()

    client = MongoClient(args.mongo_uri)
    col = client[args.mongo_db][args.collection]

    cur.execute("""
        SELECT id, stream_symbol, author, created_at, scraped_at_utc, post, sentiment,
               reason_for_label, keywords_json, ticker_mentions_json, notes, link, raw_json
        FROM messages
        ORDER BY id ASC
    """)

    ops = []
    total = 0
    inserted = 0

    while True:
        rows = cur.fetchmany(args.batch)
        if not rows:
            break

        for r in rows:
            (mid, stream_symbol, author, created_at, scraped_at_utc, post, sentiment,
             reason_for_label, keywords_json, ticker_mentions_json, notes, link, raw_json) = r

            try:
                created_dt = parse_iso_z(created_at) if created_at else None
            except Exception:
                created_dt = None

            doc = {
                "_id": int(mid),
                "stream_symbol": stream_symbol,
                "author": author,
                "created_at_utc": created_dt,
                "created_at_raw": created_at,
                "scraped_at_utc": parse_iso_z(scraped_at_utc) if scraped_at_utc and "T" in scraped_at_utc else None,
                "post": post,
                "sentiment": sentiment,
                "reason_for_label": reason_for_label,
                "keywords": json_list(keywords_json),
                "ticker_mentions": json_list(ticker_mentions_json),
                "notes": notes_list(notes),
                "link": link,
                "raw": json.loads(raw_json) if raw_json else None
            }

            # upsert makes migration idempotent
            ops.append(UpdateOne({"_id": doc["_id"]}, {"$set": doc}, upsert=True))
            total += 1

        res = col.bulk_write(ops, ordered=False)
        inserted += (res.upserted_count or 0)
        ops = []
        print(f"Processed {total} rows... upserts this batch: {res.upserted_count}")

    print(f"Done. Total rows processed: {total}. Upserted new docs: {inserted}")

if __name__ == "__main__":
    main()