"""Bronze -> Silver for daily follower snapshots."""
from __future__ import annotations

from datetime import date

import pandas as pd

from config.settings import Settings
from transformations.common import (
    CleanResult,
    Quarantine,
    StageStats,
    blank_to_na,
    parse_date,
    process_entity,
    read_silver_column,
    with_ingestion_date,
)

ENTITY = "followers"
REQUIRED = ["user_id", "follower_count", "snapshot_date"]
OUTPUT_COLUMNS = ["user_id", "follower_count", "snapshot_date", "ingestion_date"]


def clean_followers(raw: pd.DataFrame, run_date: date, valid_user_ids: pd.Series) -> CleanResult:
    raw = raw.reset_index(drop=True)
    q = Quarantine(raw)
    stats = StageStats(ENTITY, str(run_date), rows_in=len(raw))
    df = blank_to_na(raw)

    df = q.take(df, df[REQUIRED].isna().any(axis=1), "missing_required_field")
    counts = pd.to_numeric(df["follower_count"], errors="coerce")
    df = q.take(df, counts.isna(), "invalid_follower_count")
    counts = counts.loc[df.index]
    df = q.take(df, counts < 0, "negative_follower_count")
    df = df.assign(follower_count=counts.loc[df.index].astype("int64"),
                   snapshot_date=parse_date(df["snapshot_date"]))
    df = q.take(df, df["snapshot_date"].isna(), "invalid_snapshot_date")
    df = q.take(df, df.duplicated(["user_id", "snapshot_date"], keep="first"), "duplicate_snapshot")
    df = q.take(df, ~df["user_id"].isin(valid_user_ids), "orphan_user_id")

    df = with_ingestion_date(df, run_date)[OUTPUT_COLUMNS].reset_index(drop=True)
    rejects = q.frame()
    stats.rows_out, stats.rows_rejected, stats.reject_reasons = len(df), len(rejects), q.reasons()
    return CleanResult(df, rejects, stats)


def run(client, settings: Settings, run_date) -> StageStats:
    users = read_silver_column(client, settings.r2_bucket, "users", "user_id")
    return process_entity(client, settings, ENTITY, run_date, clean_followers, valid_user_ids=users)
