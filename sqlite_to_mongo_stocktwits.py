import argparse
import sqlite3
import hashlib
from dateutil import parser as dateparser
from pymongo import MongoClient, UpdateOne

def safe_parse_dt(s: str):
    if s is None:
        return None
    try:
        return dateparser.isoparse(s)
    except Exception:
        return None

def make_fallback_id(doc: dict) -> str:
    """
    Deterministic fallback _id if SQLite id is missing.
    Uses fields that should be stable for the same message.
    """
    parts = [
        str(doc.get("stream_symbol") or ""),
        str(doc.get("created_at") or ""),
        str(doc.get("link") or ""),
        str(doc.get("author") or ""),
        str(doc.get("post") or ""),
    ]
    raw = "||".join(parts).encode("utf-8", errors="ignore")
    return hashlib.sha1(raw).hexdigest()

def main():
    ap = argparse.ArgumentParser(description="Migrate SQLite stocktwits messages -> MongoDB")
    ap.add_argument("--sqlite_db", required=True, help="Path to stocktwits.db (SQLite)")
    ap.add_argument("--mongo_uri", default="mongodb://localhost:27017", help="MongoDB URI")
    ap.add_argument("--mongo_db", default="stocktwits", help="Mongo database name")
    ap.add_argument("--mongo_collection", default="messages", help="Mongo collection name")
    ap.add_argument("--batch_size", type=int, default=2000, help="Bulk write batch size")
    ap.add_argument("--limit", type=int, default=0, help="Optional limit for testing (0 = no limit)")
    ap.add_argument("--drop", action="store_true", help="Drop the target collection before importing")
    args = ap.parse_args()

    # --- Connect SQLite ---
    conn = sqlite3.connect(args.sqlite_db)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    cur.execute("PRAGMA table_info(messages)")
    cols = [r[1] for r in cur.fetchall()]
    print("SQLite columns:", cols)

    # --- Connect Mongo ---
    client = MongoClient(args.mongo_uri)
    db = client[args.mongo_db]
    collection = db[args.mongo_collection]

    if args.drop:
        print("Dropping Mongo collection:", f"{args.mongo_db}.{args.mongo_collection}")
        collection.drop()

    # Indexes for querying (NOT unique on 'id' to avoid null collisions)
    collection.create_index("stream_symbol")
    collection.create_index("created_at_dt")

    # --- Read SQLite rows ---
    sql = "SELECT * FROM messages"
    if args.limit and args.limit > 0:
        sql += f" LIMIT {int(args.limit)}"
    cur.execute(sql)

    ops = []
    total = 0

    def flush_ops():
        nonlocal ops
        if not ops:
            return
        collection.bulk_write(ops, ordered=False)
        ops = []

    for row in cur:
        doc = dict(row)

        # Normalize
        if "stream_symbol" in doc and doc["stream_symbol"] is not None:
            doc["stream_symbol"] = str(doc["stream_symbol"]).strip().upper()

        if "sentiment" in doc and doc["sentiment"] is None:
            doc["sentiment"] = "null"

        if "created_at" in doc:
            doc["created_at_dt"] = safe_parse_dt(doc["created_at"])

        # ---- IMPORTANT FIX ----
        # Use SQLite "id" as Mongo "_id" IF it exists and is not null.
        # Otherwise create a deterministic fallback _id.
        sqlite_id = doc.get("id", None)
        if sqlite_id is not None:
            doc["_id"] = sqlite_id
        else:
            doc["_id"] = make_fallback_id(doc)

        # Upsert by _id
        ops.append(UpdateOne({"_id": doc["_id"]}, {"$set": doc}, upsert=True))
        total += 1

        if len(ops) >= args.batch_size:
            flush_ops()
            if total % (args.batch_size * 5) == 0:
                print(f"Processed {total} rows...")

    flush_ops()

    print("\nDONE")
    print("SQLite rows processed:", total)
    print("Mongo total docs:", collection.count_documents({}))

    conn.close()
    client.close()

if __name__ == "__main__":
    main()