"""Run the whole Bronze -> Silver stage in dependency order.

    python -m transformations.run_silver --date 2026-10-30
"""
from __future__ import annotations

import argparse
from datetime import date

from botocore.client import BaseClient

from config.settings import Settings
from data_generation.common import to_date
from ingestion.r2_client import get_client
from transformations import clean_engagements, clean_followers, clean_hashtags, clean_posts, clean_users
from transformations.common import StageStats, write_transform_log

# Parents before children so referential-integrity lookups see today's parents.
ORDER = (clean_users, clean_posts, clean_engagements, clean_followers, clean_hashtags)


def transform_silver(settings: Settings, run_date: date | str,
                     client: BaseClient | None = None) -> list[StageStats]:
    run_date = to_date(run_date)
    client = client or get_client(settings)
    stats = [module.run(client, settings, run_date) for module in ORDER]
    write_transform_log(settings, client, "silver", run_date, [s.to_dict() for s in stats])
    return stats


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--date", default=str(date.today()))
    args = ap.parse_args()
    transform_silver(Settings.from_env(), args.date)


if __name__ == "__main__":
    main()
