import json
import pandas as pd
import os

files = ["AMD_tweets.json", "NVDA_tweets.json", "AAPL_tweets.json", "TSLA_tweets.json"]

all_rows = []

for file in files:
    if not os.path.exists(file):
        print(f"Missing file: {file}")
        continue

    symbol = file.replace("_tweets.json", "")

    with open(file, "r", encoding="utf-8") as f:
        data = json.load(f)

    for msg in data:
        all_rows.append({
            "Symbol": symbol,
            "Message_ID": msg.get("id"),
            "Author": msg.get("author"),
            "Time": msg.get("time"),
            "Post": msg.get("post"),
            "Sentiment": msg.get("sentiment")
        })

if not all_rows:
    print("\nNo messages loaded. Check filenames/location.")
    print("Try running: dir *tweets*.json")
    raise SystemExit(0)

df = pd.DataFrame(all_rows)

# sort only if the column exists
if "Message_ID" in df.columns:
    df = df.sort_values(by="Message_ID", ascending=False)

df.to_excel("stocktwits_messages.xlsx", index=False)

print("\nDONE! Saved: stocktwits_messages.xlsx")
