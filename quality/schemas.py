"""Data contracts (pandera) for the Silver and Gold layers.

A schema violation raises `pandera.errors.SchemaErrors`, failing the task before bad data
can reach the warehouse.
"""
from __future__ import annotations

import pandas as pd
import pandera.pandas as pa

from config.settings import ENGAGEMENT_TYPES, PLATFORMS

_ts = "datetime64[ns, UTC]"
_d = "datetime64[ns]"
_nonneg = pa.Check.ge(0)
_platform = pa.Check.isin(list(PLATFORMS))


def _strict(columns: dict, unique: list[str] | None = None) -> pa.DataFrameSchema:
    return pa.DataFrameSchema(columns, strict=True, coerce=False, unique=unique, report_duplicates="all")


SILVER_SCHEMAS: dict[str, pa.DataFrameSchema] = {
    "users": _strict(
        {
            "user_id": pa.Column(str), "username": pa.Column(str), "city": pa.Column(str),
            "niche": pa.Column(str), "joined_date": pa.Column(_d), "ingestion_date": pa.Column(str),
        },
        unique=["user_id"],
    ),
    "posts": _strict(
        {
            "post_id": pa.Column(str), "user_id": pa.Column(str), "platform": pa.Column(str, _platform),
            "content_type": pa.Column(str), "caption": pa.Column(str), "posted_at": pa.Column(_ts),
            "ingestion_date": pa.Column(str),
        },
        unique=["post_id"],
    ),
    "engagements": _strict(
        {
            "engagement_id": pa.Column(str), "post_id": pa.Column(str), "user_id": pa.Column(str),
            "engagement_type": pa.Column(str, pa.Check.isin(list(ENGAGEMENT_TYPES))),
            "platform": pa.Column(str, _platform), "timestamp": pa.Column(_ts),
            "ingestion_date": pa.Column(str),
        },
        unique=["engagement_id"],
    ),
    "followers": _strict(
        {
            "user_id": pa.Column(str), "follower_count": pa.Column("int64", _nonneg),
            "snapshot_date": pa.Column(_d), "ingestion_date": pa.Column(str),
        },
        unique=["user_id", "snapshot_date"],
    ),
    "hashtags": _strict(
        {"post_id": pa.Column(str), "hashtag": pa.Column(str, pa.Check.str_length(min_value=1)),
         "ingestion_date": pa.Column(str)},
        unique=["post_id", "hashtag"],
    ),
}

_int = "int64"
_num = {"nullable": False}
GOLD_SCHEMAS: dict[str, pa.DataFrameSchema] = {
    "post_performance": _strict(
        {
            "post_id": pa.Column(str), "user_id": pa.Column(str), "platform": pa.Column(str, _platform),
            "content_type": pa.Column(str), "caption": pa.Column(str), "posted_at": pa.Column(_ts),
            "posted_date": pa.Column(_d), "posted_hour_eat": pa.Column(_int, pa.Check.in_range(0, 23)),
            "posted_dow": pa.Column(_int, pa.Check.in_range(1, 7)), "posted_day_name": pa.Column(str),
            "likes": pa.Column(_int, _nonneg), "comments": pa.Column(_int, _nonneg),
            "shares": pa.Column(_int, _nonneg), "impressions": pa.Column(_int, _nonneg),
            "total_engagement": pa.Column(_int, _nonneg), "ingestion_date": pa.Column(str),
        },
        unique=["post_id"],
    ),
    "creator_performance": _strict(
        {
            "user_id": pa.Column(str), "username": pa.Column(str), "city": pa.Column(str),
            "niche": pa.Column(str), "snapshot_date": pa.Column(_d),
            "posts_count": pa.Column(_int, _nonneg), "likes": pa.Column(_int, _nonneg),
            "comments": pa.Column(_int, _nonneg), "shares": pa.Column(_int, _nonneg),
            "impressions": pa.Column(_int, _nonneg), "total_engagement": pa.Column(_int, _nonneg),
            "follower_count": pa.Column(_int, _nonneg),
            "engagement_rate_pct": pa.Column(float, _nonneg, nullable=True),
            "ingestion_date": pa.Column(str),
        },
        unique=["user_id"],
    ),
    "hashtag_performance": _strict(
        {
            "hashtag": pa.Column(str), "platform": pa.Column(str, _platform),
            "posts_used": pa.Column(_int, pa.Check.ge(1)), "likes": pa.Column(_int, _nonneg),
            "comments": pa.Column(_int, _nonneg), "shares": pa.Column(_int, _nonneg),
            "impressions": pa.Column(_int, _nonneg), "total_engagement": pa.Column(_int, _nonneg),
            "avg_engagement_per_post": pa.Column(float, _nonneg), "ingestion_date": pa.Column(str),
        },
        unique=["hashtag", "platform"],
    ),
    "audience_growth": _strict(
        {
            "user_id": pa.Column(str), "snapshot_date": pa.Column(_d),
            "follower_count": pa.Column(_int, _nonneg),
            "previous_follower_count": pa.Column("float64", _nonneg, nullable=True),
            "follower_change": pa.Column("float64", nullable=True),
            "growth_rate_pct": pa.Column("float64", nullable=True), "ingestion_date": pa.Column(str),
        },
        unique=["user_id"],
    ),
}


def validate_silver(entity: str, df: pd.DataFrame) -> pd.DataFrame:
    return SILVER_SCHEMAS[entity].validate(df, lazy=True)


def validate_gold(dataset: str, df: pd.DataFrame) -> pd.DataFrame:
    return GOLD_SCHEMAS[dataset].validate(df, lazy=True)
