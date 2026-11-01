"""Bronze -> Silver for posts (validates user_id referential integrity)."""
from __future__ import annotations

from datetime import date

import pandas as pd

from config.settings import PLATFORMS, SILVER, Settings
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

ENTITY = "posts"
REQUIRED = ["post_id", "user_id", "platform", "content_type", "posted_at"]
OUTPUT_COLUMNS = ["post_id", "user_id", "platform", "content_type", "caption", "posted_at", "ingestion_date"]


def clean_posts(raw: pd.DataFrame, run_date: date, valid_user_ids: pd.Series) -> CleanResult:
    raw = raw.reset_index(drop=True)
    q = Quarantine(raw)
    stats = StageStats(ENTITY, str(run_date), rows_in=len(raw))
    df = blank_to_na(raw, [c for c in raw.columns if c != "caption"])

    df = q.take(df, df[REQUIRED].isna().any(axis=1), "missing_required_field")
    df = df.assign(platform=normalize_platform(df["platform"]),
                   content_type=df["content_type"].str.lower())
    df = q.take(df, ~df["platform"].isin(PLATFORMS), "invalid_platform")
    df = df.assign(posted_at=parse_to_utc(df["posted_at"]))
    df = q.take(df, df["posted_at"].isna(), "invalid_timestamp")
    df = q.take(df, df.duplicated("post_id", keep="first"), "duplicate_post_id")
    df = q.take(df, ~df["user_id"].isin(valid_user_ids), "orphan_user_id")

    n_null = int(df["caption"].isna().sum())
    if n_null:
        stats.nulls_filled["caption"] = n_null
    df["caption"] = df["caption"].fillna("").astype(str).str.strip()

    df = df.assign(**{c: df[c].astype(object) for c in ("platform",)})
    df = with_ingestion_date(df, run_date)[OUTPUT_COLUMNS].reset_index(drop=True)
    rejects = q.frame()
    stats.rows_out, stats.rows_rejected, stats.reject_reasons = len(df), len(rejects), q.reasons()
    return CleanResult(df, rejects, stats)


def run(client, settings: Settings, run_date) -> StageStats:
    users = read_silver_column(client, settings.r2_bucket, "users", "user_id")
    return process_entity(client, settings, ENTITY, run_date, clean_posts, valid_user_ids=users)


__all__ = ["clean_posts", "run", "SILVER"]
