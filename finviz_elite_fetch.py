from datetime import datetime
from pathlib import Path

import pandas as pd
import requests
from pymongo import MongoClient


FINVIZ_URL = (
    "https://elite.finviz.com/screener.ashx"
    "?v=152&f=sh_relvol_o5,ta_change_u3&ft=3&o=-change&ar=10"
    "&auth=f512e941-7a35-45f3-b646-dbd458b46459"
)

BASE_DIR = Path(r"C:\Users\yosef\OneDrive\Desktop\Research Internship IST495")
FINVIZ_DIR = BASE_DIR / "finviz_daily"
LATEST_CSV = BASE_DIR / "export.csv"

MONGO_URI = "mongodb://localhost:27017/"
MONGO_DB = "ist495"
MONGO_COLLECTION = "finviz_elite"


def ensure_dirs() -> None:
    FINVIZ_DIR.mkdir(parents=True, exist_ok=True)


def clean_column_name(col: str) -> str:
    return (
        str(col)
        .strip()
        .lower()
        .replace(" ", "_")
        .replace("/", "_")
        .replace("-", "_")
        .replace("%", "pct")
    )


def parse_numeric_series(series: pd.Series) -> pd.Series:
    return pd.to_numeric(
        series.astype(str).str.replace(",", "", regex=False).str.strip(),
        errors="coerce",
    )


def fetch_finviz_csv_direct() -> bytes:
    """
    Uses the exact direct-request approach the user wanted:
        response = requests.get(url)
        open("export.csv", "wb").write(response.content)

    Added safety:
    - browser-like headers
    - timeout
    - validation that response is really CSV
    """
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/136.0.0.0 Safari/537.36"
        ),
        "Accept": "text/csv,application/octet-stream,text/plain,*/*",
        "Referer": "https://elite.finviz.com/",
    }

    response = requests.get(FINVIZ_URL, headers=headers, timeout=30)
    response.raise_for_status()

    # Save raw response exactly like requested
    with open(LATEST_CSV, "wb") as f:
        f.write(response.content)

    content_type = response.headers.get("Content-Type", "").lower()
    content = response.content

    # Check if response looks like CSV
    looks_like_csv = (
        "csv" in content_type
        or content.startswith(b"Ticker")
        or content.startswith(b"\xef\xbb\xbfTicker")
    )

    if not looks_like_csv:
        preview = content[:300].decode("utf-8", errors="ignore")
        raise ValueError(
            "Finviz returned HTML or non-CSV content instead of the export file.\n"
            f"Content-Type: {content_type}\n"
            f"Preview: {preview}"
        )

    return content


def save_dated_copy(csv_bytes: bytes) -> Path:
    now = datetime.now()
    dated_name = f"finviz_{now:%Y_%m_%d}.csv"
    dated_path = FINVIZ_DIR / dated_name

    with open(dated_path, "wb") as f:
        f.write(csv_bytes)

    return dated_path


def load_and_clean_csv(csv_path: Path) -> pd.DataFrame:
    df = pd.read_csv(csv_path, encoding="utf-8-sig")
    df.columns = [clean_column_name(c) for c in df.columns]

    rename_map = {
        "ticker": "stream_symbol",
        "rel_volume": "relative_volume",
        "change": "price_change",
    }
    df.rename(columns=rename_map, inplace=True)

    if "stream_symbol" in df.columns:
        df["stream_symbol"] = df["stream_symbol"].astype(str).str.strip().str.upper()

    if "price_change" in df.columns:
        df["price_change_num"] = (
            df["price_change"]
            .astype(str)
            .str.replace("%", "", regex=False)
            .str.replace(",", "", regex=False)
            .str.strip()
        )
        df["price_change_num"] = pd.to_numeric(df["price_change_num"], errors="coerce")

    if "relative_volume" in df.columns:
        df["relative_volume"] = parse_numeric_series(df["relative_volume"])

    if "volume" in df.columns:
        df["volume"] = parse_numeric_series(df["volume"])

    if "price" in df.columns:
        df["price"] = parse_numeric_series(df["price"])

    if "market_cap" in df.columns:
        df["market_cap_raw"] = df["market_cap"].astype(str)

    now_utc = datetime.utcnow()
    df["fetched_at_utc"] = now_utc
    df["fetch_date"] = now_utc.strftime("%Y-%m-%d")

    return df


def store_in_mongo(df: pd.DataFrame) -> int:
    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB]
    col = db[MONGO_COLLECTION]

    col.delete_many({})

    if df.empty:
        return 0

    records = df.where(pd.notnull(df), None).to_dict("records")
    col.insert_many(records)
    return len(records)


def main() -> None:
    try:
        ensure_dirs()

        print("Fetching Finviz Elite CSV...")
        csv_bytes = fetch_finviz_csv_direct()
        dated_path = save_dated_copy(csv_bytes)

        print(f"Saved latest CSV: {LATEST_CSV}")
        print(f"Saved dated CSV:  {dated_path}")

        df = load_and_clean_csv(LATEST_CSV)
        inserted = store_in_mongo(df)

        print(f"Rows loaded: {len(df)}")
        print(f"Rows inserted into MongoDB: {inserted}")

        preview_cols = [c for c in [
            "stream_symbol", "price", "price_change", "price_change_num",
            "relative_volume", "volume"
        ] if c in df.columns]

        if preview_cols:
            print("\nPreview:")
            print(df[preview_cols].head(10).to_string(index=False))

    except Exception as e:
        print(f"Finviz fetch failed: {e}")
        print(
            "\nPossible reasons:\n"
            "- Finviz returned an HTML page instead of CSV\n"
            "- auth token expired\n"
            "- Finviz requires browser cookies/session in addition to auth\n"
            "- access was blocked\n"
        )


if __name__ == "__main__":
    main()