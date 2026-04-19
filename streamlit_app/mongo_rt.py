from __future__ import annotations

import re
from urllib.parse import urlparse
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from typing import Optional, Tuple
import pandas as pd
from pymongo import MongoClient

ET = ZoneInfo("America/New_York")

BUY_IN_CUES = (
    "buy", "loading", "load up", "adding", "added", "accumulate", "bullish",
    "breakout", "squeeze", "moon", "rip", "runner", "approval", "partnership",
    "contract", "deal", "acquisition", "merger", "news coming", "news soon",
    "upside", "bounce", "rebound", "calls", "covering"
)

LEAVE_CUES = (
    "sell", "selling", "exit", "get out", "leave", "dump", "rug", "rug pull",
    "offering", "dilution", "reverse split", "delist", "bankruptcy", "fraud",
    "bearish", "puts", "short", "collapse", "downside", "take profit",
    "profit taking", "bad news", "halt", "scam"
)


@dataclass(frozen=True)
class MongoCfg:
    uri: str = "mongodb://localhost:27017"
    db: str = "stocktwits"
    messages_col: str = "messages"


def _client(cfg: MongoCfg) -> MongoClient:
    return MongoClient(cfg.uri)


def _parse_et_string(dt_str: str) -> datetime:
    dt = datetime.strptime(dt_str.strip(), "%Y-%m-%d %H:%M")
    return dt.replace(tzinfo=ET)


def parse_window(
    mode: str,
    last_n: int = 30,
    unit: str = "minutes",
    start_et: Optional[str] = None,
    end_et: Optional[str] = None,
) -> Tuple[datetime, datetime]:
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


def _clean_message_match(start_utc: datetime, end_utc: datetime, ticker: Optional[str] = None) -> dict:
    """
    Mixed-history safe filter:
    - include older docs where flags do not exist yet
    - include newer docs where flags are explicitly False
    - exclude only docs where flags are explicitly True
    """
    match = {
        "created_at_dt": {"$gte": start_utc, "$lt": end_utc},
        "stream_symbol": {"$exists": True, "$ne": None},
        "$and": [
            {
                "$or": [
                    {"is_low_quality": {"$exists": False}},
                    {"is_low_quality": False},
                ]
            },
            {
                "$or": [
                    {"is_spam": {"$exists": False}},
                    {"is_spam": False},
                ]
            },
            {
                "$or": [
                    {"is_duplicate_exact": {"$exists": False}},
                    {"is_duplicate_exact": False},
                ]
            },
        ],
    }

    if ticker:
        match["stream_symbol"] = (ticker or "").strip().upper()

    return match


def _day_bounds_from_utc(end_utc: datetime) -> tuple[datetime, datetime]:
    end_et = end_utc.astimezone(ET)
    day_start_et = end_et.replace(hour=0, minute=0, second=0, microsecond=0)
    next_day_et = day_start_et + timedelta(days=1)
    return day_start_et.astimezone(timezone.utc), next_day_et.astimezone(timezone.utc)


def classify_rumor_direction(post: str, sentiment: Optional[str] = None) -> Optional[str]:
    text = (post or "").lower()
    buy_score = sum(cue in text for cue in BUY_IN_CUES)
    leave_score = sum(cue in text for cue in LEAVE_CUES)

    if (sentiment or "").lower() == "bullish":
        buy_score += 1
    elif (sentiment or "").lower() == "bearish":
        leave_score += 1

    if buy_score == 0 and leave_score == 0:
        return None
    if buy_score > leave_score:
        return "Buy-In"
    if leave_score > buy_score:
        return "Leave"
    return "Buy-In" if (sentiment or "").lower() == "bullish" else "Leave" if (sentiment or "").lower() == "bearish" else None


def get_active_rumor_for_ticker(
    cfg: MongoCfg,
    ticker: str,
    start_utc: datetime,
    end_utc: datetime,
) -> dict:
    """
    Return one active rumor for a ticker.
    Preference order:
    1) actionable rumor from current ET day
    2) actionable rumor from selected window
    """
    col = _client(cfg)[cfg.db][cfg.messages_col]
    ticker = (ticker or "").strip().upper()

    def _pick(match: dict) -> Optional[dict]:
        cur = col.find(
            match,
            {
                "_id": 0,
                "created_at_dt": 1,
                "author": 1,
                "sentiment": 1,
                "post": 1,
                "link": 1,
                "rumor_flag": 1,
                "rumor_reason": 1,
                "source_type": 1,
            },
        ).sort("created_at_dt", -1).limit(100)

        rows = list(cur)
        for row in rows:
            direction = classify_rumor_direction(row.get("post", ""), row.get("sentiment"))
            if direction is None:
                continue
            dt = pd.to_datetime(row.get("created_at_dt"), utc=True, errors="coerce")
            dt_et = dt.tz_convert(ET) if pd.notna(dt) else None
            return {
                "stream_symbol": ticker,
                "active_rumor": row.get("post", ""),
                "rumor_direction": direction,
                "rumor_time_et": dt_et,
                "rumor_time_label": dt_et.strftime("%b %d, %I:%M %p ET") if dt_et is not None else "",
                "rumor_author": row.get("author", ""),
                "rumor_link": row.get("link", ""),
                "rumor_reason": row.get("rumor_reason", ""),
            }
        return None

    today_start_utc, today_end_utc = _day_bounds_from_utc(end_utc)

    today_match = _clean_message_match(today_start_utc, today_end_utc, ticker)
    today_match["rumor_flag"] = True

    picked = _pick(today_match)
    if picked:
        return picked

    window_match = _clean_message_match(start_utc, end_utc, ticker)
    window_match["rumor_flag"] = True

    picked = _pick(window_match)
    if picked:
        return picked

    return {
        "stream_symbol": ticker,
        "active_rumor": "",
        "rumor_direction": "",
        "rumor_time_et": None,
        "rumor_time_label": "",
        "rumor_author": "",
        "rumor_link": "",
        "rumor_reason": "",
    }


def get_active_rumors_for_tickers(
    cfg: MongoCfg,
    tickers: list[str],
    start_utc: datetime,
    end_utc: datetime,
) -> pd.DataFrame:
    rows = []
    for ticker in tickers:
        rows.append(get_active_rumor_for_ticker(cfg, ticker, start_utc, end_utc))
    if not rows:
        return pd.DataFrame(columns=[
            "stream_symbol",
            "active_rumor",
            "rumor_direction",
            "rumor_time_et",
            "rumor_time_label",
            "rumor_author",
            "rumor_link",
            "rumor_reason",
        ])
    return pd.DataFrame(rows)


def agg_ticker_summary(
    cfg: MongoCfg,
    start_utc: datetime,
    end_utc: datetime,
    limit: int = 50000
) -> pd.DataFrame:
    col = _client(cfg)[cfg.db][cfg.messages_col]
    window_minutes = max(1e-9, (end_utc - start_utc).total_seconds() / 60.0)

    pipeline = [
        {"$match": _clean_message_match(start_utc, end_utc)},
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
    col = _client(cfg)[cfg.db][cfg.messages_col]
    ticker = (ticker or "").strip().upper()
    bucket_ms = int(bucket_minutes * 60_000)

    pipeline = [
        {"$match": _clean_message_match(start_utc, end_utc, ticker)},
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
    col = _client(cfg)[cfg.db][cfg.messages_col]
    ticker = (ticker or "").strip().upper()

    cur = col.find(
        _clean_message_match(start_utc, end_utc, ticker),
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
        {"$match": _clean_message_match(start_utc, end_utc, ticker)},
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


def load_latest_finviz():
    client = MongoClient("mongodb://localhost:27017/")
    db = client["ist495"]
    col = db["finviz_elite"]

    data = list(col.find({}, {"_id": 0}))
    if not data:
        return pd.DataFrame()

    return pd.DataFrame(data)