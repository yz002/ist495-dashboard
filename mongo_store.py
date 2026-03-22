import argparse
from datetime import datetime, timezone
from pymongo import MongoClient, UpdateOne

def parse_iso_z(s: str):
    if not s:
        return None
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00")).astimezone(timezone.utc)
    except Exception:
        return None

class MongoStore:
    def __init__(self, uri="mongodb://localhost:27017", db="stocktwits", collection="messages"):
        self.client = MongoClient(uri)
        self.col = self.client[db][collection]

    def upsert_messages(self, symbol: str, msgs: list[dict], enrich_func):
        if not msgs:
            return 0
        now = datetime.now(timezone.utc)

        ops = []
        for msg in msgs:
            mid = msg.get("id")
            if not mid:
                continue

            user = msg.get("user", {}).get("username", "Unknown")
            body = msg.get("body", "")
            created_raw = msg.get("created_at", "")
            created_dt = parse_iso_z(created_raw)

            enriched = enrich_func(msg, symbol=symbol, user=user, body=body)

            doc = {
                "_id": int(mid),
                "stream_symbol": symbol,
                "author": user,
                "created_at_utc": created_dt,
                "created_at_raw": created_raw,
                "scraped_at_utc": now,
                "post": body,
                "sentiment": enriched.get("sentiment", "null"),
                "reason_for_label": enriched.get("reason_for_label", ""),
                "keywords": enriched.get("keywords", []),
                "ticker_mentions": enriched.get("ticker_mentions", []),
                "notes": enriched.get("notes", []),
                "link": enriched.get("link", ""),
                "raw": msg,
            }

            # $setOnInsert prevents overwriting if you re-fetch the same id later
            ops.append(UpdateOne({"_id": doc["_id"]}, {"$setOnInsert": doc}, upsert=True))

        if not ops:
            return 0

        res = self.col.bulk_write(ops, ordered=False)
        return int(res.upserted_count or 0)