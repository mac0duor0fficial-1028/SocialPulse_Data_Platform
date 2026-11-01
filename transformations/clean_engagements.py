"""Bronze -> Silver for engagements.

Rules (in order): required fields -> valid type/platform -> parse timestamp to UTC ->
de-duplicate engagement_id -> referential integrity (post_id in posts, user_id in users).
Orphans are quarantined to silver/_rejects/engagements/..., never silently dropped."""
from __future__ import annotations

from datetime import date

import pandas as pd

from config.settings import ENGAGEMENT_TYPES, PLATFORMS, Settings
from transformations.common import (
    CleanResult,
    Quarantine,
    StageStats,
    blank_to_na,
    normalize_platform,
    parse_to_utc,
    process_entity,
    read_silver_column,
    with_ingestion_date,
)

ENTITY = "engagements"
REQUIRED = ["engagement_id", "post_id", "user_id", "engagement_type", "platform", "timestamp"]
OUTPUT_COLUMNS = ["engagement_id", "post_id", "user_id", "engagement_type", "platform", "timestamp",
                  "ingestion_date"]


def clean_engagements(raw: pd.DataFrame, run_date: date, valid_post_ids: pd.Series,
                      valid_user_ids: pd.Series) -> CleanResult:
    raw = raw.reset_index(drop=True)
    q = Quarantine(raw)
    stats = StageStats(ENTITY, str(run_date), rows_in=len(raw))
    df = blank_to_na(raw)

    df = q.take(df, df[REQUIRED].isna().any(axis=1), "missing_required_field")
    df = df.assign(engagement_type=df["engagement_type"].str.lower(),
                   platform=normalize_platform(df["platform"]))
    df = q.take(df, ~df["engagement_type"].isin(ENGAGEMENT_TYPES), "invalid_engagement_type")
    df = q.take(df, ~df["platform"].isin(PLATFORMS), "invalid_platform")
    df = df.assign(timestamp=parse_to_utc(df["timestamp"]))
    df = q.take(df, df["timestamp"].isna(), "invalid_timestamp")
    df = q.take(df, df.duplicated("engagement_id", keep="first"), "duplicate_engagement_id")
    df = q.take(df, ~df["post_id"].isin(valid_post_ids), "orphan_post_id")
    df = q.take(df, ~df["user_id"].isin(valid_user_ids), "orphan_user_id")

    df = df.assign(platform=df["platform"].astype(object))
    df = with_ingestion_date(df, run_date)[OUTPUT_COLUMNS].reset_index(drop=True)
    rejects = q.frame()
    stats.rows_out, stats.rows_rejected, stats.reject_reasons = len(df), len(rejects), q.reasons()
    return CleanResult(df, rejects, stats)


def run(client, settings: Settings, run_date) -> StageStats:
    posts = read_silver_column(client, settings.r2_bucket, "posts", "post_id")
    users = read_silver_column(client, settings.r2_bucket, "users", "user_id")
    return process_entity(client, settings, ENTITY, run_date, clean_engagements,
                          valid_post_ids=posts, valid_user_ids=users)
