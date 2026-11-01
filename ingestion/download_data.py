"""List and fetch Bronze objects; print the MVP daily engagement summary.

    python -m ingestion.download_data --date 2026-10-30 --summary
    python -m ingestion.download_data --date 2026-10-30 --entity posts --out data/downloads
"""
from __future__ import annotations

import argparse
import io
from dataclasses import dataclass
from datetime import date
from pathlib import Path

import pandas as pd
from botocore.client import BaseClient

from config.settings import BRONZE, ENTITIES, Settings
from data_generation.common import to_date
from ingestion.r2_client import get_bytes, get_client, list_keys


class NoBronzeDataError(RuntimeError):
    pass


@dataclass
class BronzeFetch:
    df: pd.DataFrame
    keys: list[str]
    source_rows: int | None  # row_count recorded at ingestion time (None if metadata absent)


def list_bronze(client: BaseClient, bucket: str, entity: str, run_date: date | str) -> list[str]:
    run_date = to_date(run_date)
    return list_keys(client, bucket, f"{BRONZE}/{entity}/ingestion_date={run_date:%Y-%m-%d}/")


def _parse(body: bytes, ext: str) -> pd.DataFrame:
    if ext == "csv":
        return pd.read_csv(io.BytesIO(body), dtype=str)
    # dtype=False/convert_dates=False keeps raw strings so cleaning is explicit in Silver
    return pd.read_json(io.BytesIO(body), lines=True, dtype=False, convert_dates=False)


def fetch_bronze(client: BaseClient, bucket: str, entity: str, run_date: date | str) -> BronzeFetch:
    """Download and concatenate all Bronze objects of `entity` for `run_date`."""
    keys = list_bronze(client, bucket, entity, run_date)
    if not keys:
        raise NoBronzeDataError(f"No Bronze objects for {entity} on {run_date}")
    frames, source_rows = [], 0
    have_meta = True
    for key in keys:
        obj = client.get_object(Bucket=bucket, Key=key)
        meta = obj.get("Metadata", {})
        if "row_count" in meta:
            source_rows += int(meta["row_count"])
        else:
            have_meta = False
        frames.append(_parse(obj["Body"].read(), key.rsplit(".", 1)[-1]))
    return BronzeFetch(pd.concat(frames, ignore_index=True), keys, source_rows if have_meta else None)


def daily_engagement_summary(engagements: pd.DataFrame) -> pd.DataFrame:
    """MVP summary: engagement counts by platform x type (raw data, so defects included)."""
    df = engagements.copy()
    df["platform"] = df["platform"].fillna("unknown").astype(str).str.strip().str.lower()
    df["engagement_type"] = df["engagement_type"].fillna("unknown")  # keep defects visible, never drop
    pivot = (
        df.pivot_table(index="platform", columns="engagement_type", values="engagement_id",
                       aggfunc="count", fill_value=0)
    )
    pivot["total"] = pivot.sum(axis=1)
    return pivot.sort_values("total", ascending=False)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--date", default=str(date.today()))
    ap.add_argument("--entity", choices=ENTITIES)
    ap.add_argument("--out", type=Path, help="Download raw objects into this directory")
    ap.add_argument("--summary", action="store_true", help="Print the daily engagement summary")
    args = ap.parse_args()

    settings = Settings.from_env()
    client = get_client(settings)
    for entity in [args.entity] if args.entity else ENTITIES:
        keys = list_bronze(client, settings.r2_bucket, entity, args.date)
        print(f"{entity}: {len(keys)} object(s)")
        for key in keys:
            print("  ", key)
            if args.out:
                dest = args.out / key
                dest.parent.mkdir(parents=True, exist_ok=True)
                dest.write_bytes(get_bytes(client, settings.r2_bucket, key))
    if args.summary:
        fetch = fetch_bronze(client, settings.r2_bucket, "engagements", args.date)
        print(f"\nEngagements on {args.date}: {len(fetch.df):,} raw records "
              f"(source rows recorded at ingestion: {fetch.source_rows})")
        print(daily_engagement_summary(fetch.df).to_string())


if __name__ == "__main__":
    main()
