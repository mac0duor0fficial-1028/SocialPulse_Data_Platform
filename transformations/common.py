"""Shared building blocks for Silver cleaning."""
from __future__ import annotations

import json
from collections.abc import Callable
from dataclasses import asdict, dataclass, field
from datetime import date
from zoneinfo import ZoneInfo

import pandas as pd
from botocore.client import BaseClient

from config.logging_config import get_logger
from config.settings import LOCAL_TZ, LOGS, PLATFORMS, SILVER, Settings
from data_generation.common import to_date
from ingestion.download_data import fetch_bronze
from ingestion.r2_client import (
    delete_keys,
    list_keys,
    put_bytes,
    put_parquet,
    read_parquet_prefix,
)
from quality.checks import check_not_empty, check_null_rates, check_reject_rate, check_row_reconciliation
from quality.schemas import validate_silver

_NAIROBI = ZoneInfo(LOCAL_TZ)
PLATFORM_ALIASES = {"twitter": "x", "ig": "instagram", "insta": "instagram", "tik tok": "tiktok"}


# ----------------------------------------------------------------------------- stats
@dataclass
class StageStats:
    entity: str
    run_date: str
    rows_in: int = 0
    rows_out: int = 0
    rows_rejected: int = 0
    nulls_filled: dict[str, int] = field(default_factory=dict)
    reject_reasons: dict[str, int] = field(default_factory=dict)

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class CleanResult:
    clean: pd.DataFrame
    rejects: pd.DataFrame
    stats: StageStats


class Quarantine:
    """Collects rejected rows (with the raw values and a reason) so that
    rows_in == rows_out + rows_rejected always holds."""

    def __init__(self, raw: pd.DataFrame):
        self.raw = raw
        self._parts: list[pd.DataFrame] = []

    def take(self, df: pd.DataFrame, mask: pd.Series, reason: str) -> pd.DataFrame:
        """Remove rows where `mask` is True from `df`, record them, return the rest."""
        mask = mask.fillna(False).astype(bool)
        if mask.any():
            part = self.raw.loc[df.index[mask]].copy()
            part["reject_reason"] = reason
            self._parts.append(part)
        return df[~mask]

    def frame(self) -> pd.DataFrame:
        if not self._parts:
            return pd.DataFrame(columns=[*self.raw.columns, "reject_reason"])
        return pd.concat(self._parts).sort_index()

    def reasons(self) -> dict[str, int]:
        f = self.frame()
        return {} if f.empty else f["reject_reason"].value_counts().to_dict()


# ----------------------------------------------------------------------------- cleaning helpers
def blank_to_na(df: pd.DataFrame, cols: list[str] | None = None) -> pd.DataFrame:
    """Strip whitespace on string columns and turn empty strings into NA."""
    df = df.copy()
    for c in cols or list(df.columns):
        s = df[c]
        df[c] = s.where(s.isna(), s.astype(str).str.strip()).replace("", pd.NA)
    return df


def normalize_platform(s: pd.Series) -> pd.Series:
    out = s.astype("string").str.strip().str.lower()
    return out.replace(PLATFORM_ALIASES)


def parse_to_utc(s: pd.Series) -> pd.Series:
    """Parse mixed timestamp strings into tz-aware UTC.

    * values with an explicit offset / 'Z' are converted exactly;
    * naive values are interpreted as Nairobi local time (EAT, UTC+3) – the source system's zone;
    * unparseable values become NaT (and are rejected by the caller).
    """
    cache: dict[str, pd.Timestamp] = {}

    def one(v):
        if v is None or v is pd.NA or (isinstance(v, float) and v != v):
            return pd.NaT
        if v in cache:
            return cache[v]
        try:
            ts = pd.Timestamp(v)
            ts = ts.tz_localize(_NAIROBI) if ts.tzinfo is None else ts
            res = ts.tz_convert("UTC")
        except (ValueError, TypeError):
            res = pd.NaT
        cache[v] = res
        return res

    return pd.to_datetime(pd.Series([one(v) for v in s], index=s.index), utc=True).astype("datetime64[ns, UTC]")


def parse_date(s: pd.Series) -> pd.Series:
    return pd.to_datetime(s, errors="coerce", format="%Y-%m-%d").astype("datetime64[ns]")


def with_ingestion_date(df: pd.DataFrame, run_date: date) -> pd.DataFrame:
    df = df.copy()
    df["ingestion_date"] = str(run_date)
    return df


# ----------------------------------------------------------------------------- storage
def layer_key(layer: str, entity: str, run_date: date, name: str | None = None) -> str:
    return f"{layer}/{entity}/ingestion_date={run_date:%Y-%m-%d}/{name or entity + '.parquet'}"


def reject_key(entity: str, run_date: date) -> str:
    return f"{SILVER}/_rejects/{entity}/ingestion_date={run_date:%Y-%m-%d}/rejects.parquet"


def read_silver_column(client: BaseClient, bucket: str, entity: str, column: str) -> pd.Series:
    """Distinct values of one column across all Silver partitions (reference-key lookup)."""
    df = read_parquet_prefix(client, bucket, f"{SILVER}/{entity}/", columns=[column])
    return df[column].drop_duplicates()


def write_transform_log(settings: Settings, client: BaseClient, stage: str, run_date: date,
                        payload: list[dict]) -> None:
    body = json.dumps({"stage": stage, "run_date": str(run_date), "entities": payload}, indent=2)
    local = settings.log_dir / "transformations" / f"{stage}_{run_date:%Y%m%d}.json"
    local.parent.mkdir(parents=True, exist_ok=True)
    local.write_text(body)
    put_bytes(client, settings.r2_bucket,
              f"{LOGS}/transformations/ingestion_date={run_date:%Y-%m-%d}/{stage}.json",
              body.encode(), "application/json")


CleanFn = Callable[..., CleanResult]


def process_entity(client: BaseClient, settings: Settings, entity: str, run_date: date | str,
                   cleaner: CleanFn, **refs) -> StageStats:
    """Bronze -> clean -> quality gates -> Silver Parquet (+ quarantine file).

    Idempotent: output keys depend only on (entity, run_date), so a re-run overwrites."""
    run_date = to_date(run_date)
    log = get_logger("socialpulse.silver", settings.log_dir, "transformations", str(run_date))
    fetch = fetch_bronze(client, settings.r2_bucket, entity, run_date)
    result = cleaner(fetch.df, run_date, **refs)
    stats = result.stats

    # Gates run BEFORE anything is written to Silver.
    check_row_reconciliation(entity, stats.rows_in, stats.rows_out, stats.rows_rejected, fetch.source_rows)
    check_reject_rate(entity, stats.rows_in, stats.rows_rejected, settings.max_reject_rate)
    check_not_empty(f"silver.{entity}", result.clean)
    check_null_rates(entity, result.clean)
    validate_silver(entity, result.clean)

    put_parquet(client, settings.r2_bucket, layer_key(SILVER, entity, run_date), result.clean,
                {"rows": str(len(result.clean))})
    rkey = reject_key(entity, run_date)
    if result.rejects.empty:  # keep re-runs idempotent: remove a stale quarantine file
        delete_keys(client, settings.r2_bucket, list_keys(client, settings.r2_bucket, rkey))
    else:
        put_parquet(client, settings.r2_bucket, rkey, result.rejects.astype(str))

    log.info("silver %-12s in=%-7d out=%-7d rejected=%-5d reasons=%s filled=%s", entity, stats.rows_in,
             stats.rows_out, stats.rows_rejected, stats.reject_reasons, stats.nulls_filled)
    return stats


__all__ = [
    "PLATFORMS", "CleanResult", "Quarantine", "StageStats", "blank_to_na", "layer_key",
    "normalize_platform", "parse_date", "parse_to_utc", "process_entity", "read_silver_column",
    "reject_key", "with_ingestion_date", "write_transform_log",
]
