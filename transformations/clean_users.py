"""Bronze -> Silver for users."""
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
    with_ingestion_date,
)

ENTITY = "users"
OUTPUT_COLUMNS = ["user_id", "username", "city", "niche", "joined_date", "ingestion_date"]


def clean_users(raw: pd.DataFrame, run_date: date) -> CleanResult:
    raw = raw.reset_index(drop=True)
    q = Quarantine(raw)
    stats = StageStats(ENTITY, str(run_date), rows_in=len(raw))
    df = blank_to_na(raw)

    df = q.take(df, df[["user_id", "username"]].isna().any(axis=1), "missing_required_field")
    df = df.assign(joined_date=parse_date(df["joined_date"]))
    df = q.take(df, df["joined_date"].isna(), "invalid_joined_date")
    df = q.take(df, df.duplicated("user_id", keep="first"), "duplicate_user_id")

    for col in ("city", "niche"):
        n_null = int(df[col].isna().sum())
        if n_null:
            stats.nulls_filled[col] = n_null  # logged, then filled with an explicit sentinel
        df[col] = df[col].fillna("Unknown").astype(str).str.title()

    df = with_ingestion_date(df, run_date)[OUTPUT_COLUMNS].reset_index(drop=True)
    rejects = q.frame()
    stats.rows_out, stats.rows_rejected, stats.reject_reasons = len(df), len(rejects), q.reasons()
    return CleanResult(df, rejects, stats)


def run(client, settings: Settings, run_date) -> StageStats:
    return process_entity(client, settings, ENTITY, run_date, clean_users)
