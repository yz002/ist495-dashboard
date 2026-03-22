from pymongo import MongoClient
from datetime import datetime

# Connect to local MongoDB
client = MongoClient("mongodb://localhost:27017/")

# Create / connect to database
db = client["stocktwits_db"]

# Create / connect to collection
collection = db["messages"]

# Insert test document
doc = {
    "stream_symbol": "TEST",
    "created_at": datetime.utcnow(),
    "post": "MongoDB test message",
}

result = collection.insert_one(doc)

print("Inserted ID:", result.inserted_id)

# Read back
print(list(collection.find({"stream_symbol": "TEST"})))

client.close()