from pymongo import MongoClient, ASCENDING, DESCENDING

MONGO_URI = "mongodb://localhost:27017"
DB_NAME = "stocktwits"

client = MongoClient(MONGO_URI)
db = client[DB_NAME]
col = db["messages"]

# Core queries you do all the time:
# 1) per ticker over time windows
col.create_index([("stream_symbol", ASCENDING), ("created_at_utc", DESCENDING)])
# 2) time-only scans (debug / global)
col.create_index([("created_at_utc", DESCENDING)])

# Helpful extras (optional, but useful later)
col.create_index([("ticker_mentions", ASCENDING)])
col.create_index([("notes", ASCENDING)])

print("Indexes created.")