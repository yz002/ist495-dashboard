import requests
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

FINVIZ_URL = (
    "https://elite.finviz.com/screener.ashx"
    "?v=150&f=sh_relvol_o5,ta_change_u&ft=5&o=-change&ar=10"
    "&auth=d348e99b-3bfd-4c48-bba6-7fc5fab83343"
)

SAVE_DIR = Path(r"C:\Users\yosef\OneDrive\Desktop\Research Internship IST495\finviz_daily")
ET = ZoneInfo("America/New_York")


def download_finviz_csv() -> Path:
    SAVE_DIR.mkdir(parents=True, exist_ok=True)

    day_tag = datetime.now(ET).strftime("%Y_%m_%d")
    out_path = SAVE_DIR / f"finviz_{day_tag}.csv"

    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "text/csv,application/octet-stream,*/*",
        "Referer": "https://elite.finviz.com/",
    }

    print("Downloading Finviz screener CSV...")
    response = requests.get(FINVIZ_URL, headers=headers, timeout=30)
    response.raise_for_status()

    out_path.write_bytes(response.content)
    print(f"Saved: {out_path}")

    return out_path


if __name__ == "__main__":
    download_finviz_csv()