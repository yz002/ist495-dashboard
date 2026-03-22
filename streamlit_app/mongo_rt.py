from __future__ import annotations

import re
from urllib.parse import urlparse
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from typing import Optional, Tuple
import glob
import os
import pandas as pd
from pymongo import MongoClient

ET = ZoneInfo("America/New_York")


@dataclass(frozen=True)
class MongoCfg:
    uri: str = "mongodb://localhost:27017"
    db: str = "stocktwits"
    messages_col: str = "messages"


def _client(cfg: MongoCfg) -> MongoClient:
    return MongoClient(cfg.uri)


def _parse_et_string(dt_str: str) -> datetime:
    """
    Parse 'YYYY-MM-DD HH:MM' as ET-aware datetime.
    """
    dt = datetime.strptime(dt_str.strip(), "%Y-%m-%d %H:%M")
    return dt.replace(tzinfo=ET)


def parse_window(
    mode: str,
    last_n: int = 30,
    unit: str = "minutes",
    start_et: Optional[str] = None,
    end_et: Optional[str] = None,
) -> Tuple[datetime, datetime]:
    """
    Returns (start_utc, end_utc).

    mode:
      - "last_n"      -> last_n minutes/hours ending now
      - "custom_et"   -> start_et/end_et strings in ET: "YYYY-MM-DD HH:MM"
      - "all_time"    -> 2000-01-01 UTC to now
    """
    now_utc = datetime.now(timezone.utc)
    mode = (mode or "").strip().lower()

    if mode == "last_n":
        if unit == "minutes":
            start_utc = now_utc - timedelta(minutes=int(last_n))
        elif unit == "hours":
            start_utc = now_utc - timedelta(hours=int(last_n))
        else:
            raise ValueError("unit must be minutes or hours")
        return start_utc, now_utc

    if mode == "custom_et":
        if not start_et or not end_et:
            raise ValueError('custom_et requires start_et and end_et strings like "YYYY-MM-DD HH:MM"')

        start_et_dt = _parse_et_string(start_et)
        end_et_dt = _parse_et_string(end_et)

        if end_et_dt <= start_et_dt:
            raise ValueError("custom_et: end_et must be after start_et")

        return start_et_dt.astimezone(timezone.utc), end_et_dt.astimezone(timezone.utc)

    if mode == "all_time":
        start_utc = datetime(2000, 1, 1, tzinfo=timezone.utc)
        return start_utc, now_utc

    raise ValueError("mode must be one of: last_n, custom_et, all_time")


def agg_ticker_summary(
    cfg: MongoCfg,
    start_utc: datetime,
    end_utc: datetime,
    limit: int = 50000
) -> pd.DataFrame:
    """
    Per-ticker aggregation for window [start_utc, end_utc).
    Uses created_at_dt (Mongo Date).

    Filters out low-quality/spam/exact-duplicate messages.

    Returns columns:
      stream_symbol, total_posts, bullish, bearish, unlabeled,
      traditional_posts, social_posts, rumor_posts,
      sentiment_score, density_per_min
    """
    col = _client(cfg)[cfg.db][cfg.messages_col]
    window_minutes = max(1e-9, (end_utc - start_utc).total_seconds() / 60.0)

    pipeline = [
        {"$match": {
            "created_at_dt": {"$gte": start_utc, "$lt": end_utc},
            "stream_symbol": {"$exists": True, "$ne": None},

            # quality filters
            "is_low_quality": {"$ne": True},
            "is_spam": {"$ne": True},
            "is_duplicate_exact": {"$ne": True},
        }},
        {"$group": {
            "_id": "$stream_symbol",

            "total_posts": {"$sum": 1},

            "bullish": {"$sum": {"$cond": [{"$eq": ["$sentiment", "Bullish"]}, 1, 0]}},
            "bearish": {"$sum": {"$cond": [{"$eq": ["$sentiment", "Bearish"]}, 1, 0]}},

            "unlabeled": {"$sum": {"$cond": [{
                "$or": [
                    {"$eq": ["$sentiment", None]},
                    {"$eq": ["$sentiment", "null"]},
                    {"$eq": ["$sentiment", ""]},
                    {"$eq": [{"$type": "$sentiment"}, "missing"]},
                ]
            }, 1, 0]}},

            "traditional_posts": {"$sum": {"$cond": [{"$eq": ["$source_type", "Traditional"]}, 1, 0]}},
            "social_posts": {"$sum": {"$cond": [{"$eq": ["$source_type", "Rumor/Social"]}, 1, 0]}},
            "rumor_posts": {"$sum": {"$cond": [{"$eq": ["$rumor_flag", True]}, 1, 0]}},
        }},
        {"$project": {
            "_id": 0,
            "stream_symbol": "$_id",

            "total_posts": 1,
            "bullish": 1,
            "bearish": 1,
            "unlabeled": 1,

            "traditional_posts": 1,
            "social_posts": 1,
            "rumor_posts": 1,

            "sentiment_score": {
                "$cond": [
                    {"$gt": [{"$add": ["$bullish", "$bearish"]}, 0]},
                    {"$divide": [
                        {"$subtract": ["$bullish", "$bearish"]},
                        {"$add": ["$bullish", "$bearish"]}
                    ]},
                    0
                ]
            },
        }},
        {"$limit": int(limit)}
    ]

    rows = list(col.aggregate(pipeline, allowDiskUse=True))
    df = pd.DataFrame(rows)

    if df.empty:
        return pd.DataFrame(columns=[
            "stream_symbol",
            "total_posts",
            "bullish",
            "bearish",
            "unlabeled",
            "traditional_posts",
            "social_posts",
            "rumor_posts",
            "sentiment_score",
            "density_per_min",
        ])

    df["density_per_min"] = df["total_posts"] / window_minutes
    return df


def agg_time_buckets_for_ticker(
    cfg: MongoCfg,
    ticker: str,
    start_utc: datetime,
    end_utc: datetime,
    bucket_minutes: int = 5,
) -> pd.DataFrame:
    """
    Bucketed time series for one ticker.
    Filters out low-quality/spam/exact-duplicate messages.

    Output:
      bucket_start_utc, bucket_start_et, total_posts, bullish, bearish, sentiment_score
    """
    col = _client(cfg)[cfg.db][cfg.messages_col]
    ticker = (ticker or "").strip().upper()
    bucket_ms = int(bucket_minutes * 60_000)

    pipeline = [
        {"$match": {
            "created_at_dt": {"$gte": start_utc, "$lt": end_utc},
            "stream_symbol": ticker,

            # quality filters
            "is_low_quality": {"$ne": True},
            "is_spam": {"$ne": True},
            "is_duplicate_exact": {"$ne": True},
        }},
        {"$group": {
            "_id": {
                "$toDate": {
                    "$subtract": [
                        {"$toLong": "$created_at_dt"},
                        {"$mod": [{"$toLong": "$created_at_dt"}, bucket_ms]}
                    ]
                }
            },
            "total_posts": {"$sum": 1},
            "bullish": {"$sum": {"$cond": [{"$eq": ["$sentiment", "Bullish"]}, 1, 0]}},
            "bearish": {"$sum": {"$cond": [{"$eq": ["$sentiment", "Bearish"]}, 1, 0]}},
        }},
        {"$project": {
            "_id": 0,
            "bucket_start_utc": "$_id",
            "total_posts": 1,
            "bullish": 1,
            "bearish": 1,
            "sentiment_score": {
                "$cond": [
                    {"$gt": [{"$add": ["$bullish", "$bearish"]}, 0]},
                    {"$divide": [
                        {"$subtract": ["$bullish", "$bearish"]},
                        {"$add": ["$bullish", "$bearish"]}
                    ]},
                    0
                ]
            },
        }},
        {"$sort": {"bucket_start_utc": 1}}
    ]

    rows = list(col.aggregate(pipeline, allowDiskUse=True))
    df = pd.DataFrame(rows)
    if df.empty:
        return pd.DataFrame(columns=[
            "bucket_start_utc",
            "bucket_start_et",
            "total_posts",
            "bullish",
            "bearish",
            "sentiment_score"
        ])

    df["bucket_start_utc"] = pd.to_datetime(df["bucket_start_utc"], utc=True)
    df["bucket_start_et"] = df["bucket_start_utc"].dt.tz_convert(ET)
    return df


def get_latest_messages(
    cfg: MongoCfg,
    ticker: str,
    start_utc: datetime,
    end_utc: datetime,
    limit: int = 200,
) -> pd.DataFrame:
    """
    Raw message feed for drilldown.
    Filters out low-quality/spam/exact-duplicate messages.
    """
    col = _client(cfg)[cfg.db][cfg.messages_col]
    ticker = (ticker or "").strip().upper()

    cur = col.find(
        {
            "created_at_dt": {"$gte": start_utc, "$lt": end_utc},
            "stream_symbol": ticker,

            # quality filters
            "is_low_quality": {"$ne": True},
            "is_spam": {"$ne": True},
            "is_duplicate_exact": {"$ne": True},
        },
        {
            "_id": 0,
            "created_at_dt": 1,
            "author": 1,
            "sentiment": 1,
            "post": 1,
            "link": 1,
            "source_type": 1,
            "rumor_flag": 1,
            "rumor_reason": 1,
        }
    ).sort("created_at_dt", -1).limit(int(limit))

    rows = list(cur)
    df = pd.DataFrame(rows)
    if df.empty:
        return pd.DataFrame(columns=[
            "created_at_et",
            "author",
            "sentiment",
            "source_type",
            "rumor_flag",
            "rumor_reason",
            "post",
            "link",
        ])

    df["created_at_dt"] = pd.to_datetime(df["created_at_dt"], utc=True, errors="coerce")
    df["created_at_et"] = df["created_at_dt"].dt.tz_convert(ET)

    keep_cols = [
        "created_at_et",
        "author",
        "sentiment",
        "source_type",
        "rumor_flag",
        "rumor_reason",
        "post",
        "link",
    ]
    keep_cols = [c for c in keep_cols if c in df.columns]
    return df[keep_cols]


def ticker_summary(cfg: MongoCfg, ticker: str, start_utc: datetime, end_utc: datetime) -> dict:
    col = _client(cfg)[cfg.db][cfg.messages_col]
    ticker = (ticker or "").strip().upper()

    pipeline = [
        {"$match": {
            "created_at_dt": {"$gte": start_utc, "$lt": end_utc},
            "stream_symbol": ticker,

            # quality filters
            "is_low_quality": {"$ne": True},
            "is_spam": {"$ne": True},
            "is_duplicate_exact": {"$ne": True},
        }},
        {"$group": {
            "_id": "$stream_symbol",
            "total_posts": {"$sum": 1},
            "bullish": {"$sum": {"$cond": [{"$eq": ["$sentiment", "Bullish"]}, 1, 0]}},
            "bearish": {"$sum": {"$cond": [{"$eq": ["$sentiment", "Bearish"]}, 1, 0]}},
            "unlabeled": {"$sum": {"$cond": [{
                "$or": [
                    {"$eq": ["$sentiment", None]},
                    {"$eq": ["$sentiment", "null"]},
                    {"$eq": ["$sentiment", ""]},
                    {"$eq": [{"$type": "$sentiment"}, "missing"]},
                ]
            }, 1, 0]}},
            "traditional_posts": {"$sum": {"$cond": [{"$eq": ["$source_type", "Traditional"]}, 1, 0]}},
            "social_posts": {"$sum": {"$cond": [{"$eq": ["$source_type", "Rumor/Social"]}, 1, 0]}},
            "rumor_posts": {"$sum": {"$cond": [{"$eq": ["$rumor_flag", True]}, 1, 0]}},
        }},
        {"$project": {
            "_id": 0,
            "ticker": "$_id",
            "total_posts": 1,
            "bullish": 1,
            "bearish": 1,
            "unlabeled": 1,
            "traditional_posts": 1,
            "social_posts": 1,
            "rumor_posts": 1,
            "sentiment_score": {
                "$cond": [
                    {"$gt": [{"$add": ["$bullish", "$bearish"]}, 0]},
                    {"$divide": [
                        {"$subtract": ["$bullish", "$bearish"]},
                        {"$add": ["$bullish", "$bearish"]}
                    ]},
                    0
                ]
            },
        }},
    ]

    rows = list(col.aggregate(pipeline, allowDiskUse=True))
    window_minutes = max(1e-9, (end_utc - start_utc).total_seconds() / 60.0)

    if not rows:
        return {
            "ticker": ticker,
            "total_posts": 0,
            "bullish": 0,
            "bearish": 0,
            "unlabeled": 0,
            "traditional_posts": 0,
            "social_posts": 0,
            "rumor_posts": 0,
            "sentiment_score": 0.0,
            "density_per_min": 0.0,
        }

    out = rows[0]
    out["density_per_min"] = out["total_posts"] / window_minutes
    return out


# --------------------------
# Link classification helpers
# --------------------------
URL_RE = re.compile(r"(https?://[^\s\]\)<>\"']+)", re.IGNORECASE)

TRADITIONAL_DOMAINS = {
    "reuters.com",
    "bloomberg.com",
    "wsj.com",
    "ft.com",
    "cnbc.com",
    "marketwatch.com",
    "finance.yahoo.com",
    "seekingalpha.com",
    "investing.com",
    "sec.gov",
    "nasdaq.com",
    "nytimes.com",
    "apnews.com",
    "theverge.com",
    "techcrunch.com",
}


def extract_urls(text: str) -> list[str]:
    if not text:
        return []
    urls = URL_RE.findall(text)
    cleaned = []
    for u in urls:
        u = u.strip().rstrip(".,;:!?)\"]'")
        cleaned.append(u)

    out = []
    seen = set()
    for u in cleaned:
        if u not in seen:
            seen.add(u)
            out.append(u)
    return out


def domain_of(url: str) -> str:
    try:
        host = urlparse(url).netloc.lower()
        if host.startswith("www."):
            host = host[4:]
        return host
    except Exception:
        return ""


def classify_domain(domain: str) -> str:
    if not domain:
        return "No link"
    for d in TRADITIONAL_DOMAINS:
        if domain == d or domain.endswith("." + d):
            return "Traditional"
    return "Rumor/Social"


# --------------------------
# Finviz loader
# --------------------------
FINVIZ_DIR = r"C:\Users\yosef\OneDrive\Desktop\Research Internship IST495\finviz_daily"


def load_latest_finviz():
    client = MongoClient("mongodb://localhost:27017/")
    db = client["ist495"]
    col = db["finviz_elite"]

    data = list(col.find({}, {"_id": 0}))

    if not data:
        return pd.DataFrame()

    return pd.DataFrame(data)