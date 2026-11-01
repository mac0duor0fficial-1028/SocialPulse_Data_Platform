"""Bronze -> Silver for post hashtags (lower-case, no '#', de-duplicated)."""
from __future__ import annotations

from datetime import date

import pandas as pd

from config.settings import Settings
from transformations.common import (
    CleanResult,
    Quarantine,
    StageStats,
    blank_to_na,
    process_entity,
    read_silver_column,
    with_ingestion_date,
)

ENTITY = "hashtags"
OUTPUT_COLUMNS = ["post_id", "hashtag", "ingestion_date"]


def clean_hashtags(raw: pd.DataFrame, run_date: date, valid_post_ids: pd.Series) -> CleanResult:
    raw = raw.reset_index(drop=True)
    q = Quarantine(raw)
    stats = StageStats(ENTITY, str(run_date), rows_in=len(raw))
    df = blank_to_na(raw)

    df = q.take(df, df[["post_id", "hashtag"]].isna().any(axis=1), "missing_required_field")
    tag = df["hashtag"].str.lstrip("#").str.strip().str.lower().str.replace(r"\s+", "", regex=True)
    df = df.assign(hashtag=tag)
    df = q.take(df, df["hashtag"].eq(""), "empty_hashtag")
    df = q.take(df, df.duplicated(["post_id", "hashtag"], keep="first"), "duplicate_post_hashtag")
    df = q.take(df, ~df["post_id"].isin(valid_post_ids), "orphan_post_id")

    df = with_ingestion_date(df, run_date)[OUTPUT_COLUMNS].reset_index(drop=True)
    rejects = q.frame()
    stats.rows_out, stats.rows_rejected, stats.reject_reasons = len(df), len(rejects), q.reasons()
    return CleanResult(df, rejects, stats)


def run(client, settings: Settings, run_date) -> StageStats:
    posts = read_silver_column(client, settings.r2_bucket, "posts", "post_id")
    return process_entity(client, settings, ENTITY, run_date, clean_hashtags, valid_post_ids=posts)
