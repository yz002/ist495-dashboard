from curl_cffi import requests
import pandas as pd
from datetime import datetime, timezone
from pymongo import MongoClient


def fetch_finviz_elite_to_mongo():
    url = "https://elite.finviz.com/screener.ashx?v=152&f=sh_relvol_o5,ta_change_u3&ft=3&o=-change&ar=10&auth=d348e99b-3bfd-4c48-bba6-7fc5fab83343"

    print("Fetching Finviz elite data...")

    response = requests.get(
        url,
        impersonate="chrome",
        timeout=30,
        headers={
            "Referer": "https://elite.finviz.com/",
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/136.0.0.0 Safari/537.36"
            ),
        },
    )

    print(f"Status Code: {response.status_code}")

    if response.status_code != 200:
        print("Error fetching Finviz data")
        print(response.text[:500])
        return

    html = response.text

    if "screener_table" not in html or "Ticker" not in html:
        print("Could not find screener table in HTML.")
        return

    tables = pd.read_html(html)

    if not tables:
        print("No tables found in Finviz HTML.")
        return

    target_df = None
    for t in tables:
        cols = [str(c).strip().lower() for c in t.columns]
        if "ticker" in cols and "price" in cols and "change" in cols and "volume" in cols:
            target_df = t.copy()
            break

    if target_df is None:
        print("Could not find target screener table.")
        print("Tables found:")
        for i, t in enumerate(tables):
            print(f"Table {i}: {list(t.columns)}")
        return

    df = target_df.copy()
    df.columns = [str(c).strip().lower().replace(" ", "_") for c in df.columns]

    if "ticker" in df.columns:
        df = df[df["ticker"].astype(str).str.upper() != "TICKER"].copy()

    rename_map = {
        "ticker": "stream_symbol",
        "change": "price_change",
        "price": "price",
        "volume": "volume",
        "company": "company",
        "sector": "sector",
        "industry": "industry",
        "country": "country",
        "market_cap": "market_cap",
    }
    df = df.rename(columns=rename_map)

    preferred_cols = [
        "stream_symbol",
        "company",
        "sector",
        "industry",
        "country",
        "market_cap",
        "volume",
        "price",
        "price_change",
    ]
    keep_cols = [c for c in preferred_cols if c in df.columns]
    df = df[keep_cols].copy()

    if "stream_symbol" in df.columns:
        df["stream_symbol"] = df["stream_symbol"].astype(str).str.strip().str.upper()

    if "volume" in df.columns:
        df["volume"] = df["volume"].astype(str).str.replace(",", "", regex=False)
        df["volume"] = pd.to_numeric(df["volume"], errors="coerce")

    if "price" in df.columns:
        df["price"] = pd.to_numeric(df["price"], errors="coerce")

    if "price_change" in df.columns:
        df["price_change_num"] = (
            df["price_change"]
            .astype(str)
            .str.replace("%", "", regex=False)
            .str.replace("+", "", regex=False)
        )
        df["price_change_num"] = pd.to_numeric(df["price_change_num"], errors="coerce")

    df["fetched_at"] = datetime.now(timezone.utc)

    df = df.dropna(subset=["stream_symbol"]).drop_duplicates(subset=["stream_symbol"])

    print(f"Rows parsed: {len(df)}")
    print("Sample tickers:", df["stream_symbol"].head(10).tolist())

    client = MongoClient("mongodb://localhost:27017/")
    db = client["ist495"]
    collection = db["finviz_elite"]

    collection.delete_many({})
    if not df.empty:
        collection.insert_many(df.to_dict("records"))

    print(f"Inserted {len(df)} Finviz elite records into MongoDB")


if __name__ == "__main__":
    fetch_finviz_elite_to_mongo()