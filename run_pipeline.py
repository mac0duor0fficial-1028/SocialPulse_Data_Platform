"""Run the pipeline end to end (or selected stages) from the command line.

    python run_pipeline.py --date 2026-10-30
    python run_pipeline.py --start 2026-10-01 --end 2026-10-30          # chronological backfill
    python run_pipeline.py --date 2026-10-30 --stages silver gold
"""
from __future__ import annotations

import argparse
from datetime import date, timedelta

from config.logging_config import get_logger
from config.settings import Settings
from data_generation.common import to_date
from data_generation.run_generation import generate_all
from ingestion.r2_client import get_client
from ingestion.upload_data import upload_run
from transformations.build_gold import build_gold
from transformations.run_silver import transform_silver
from warehouse.load_gold import load_warehouse

STAGES = ("generate", "bronze", "silver", "gold", "warehouse")


def run_day(settings: Settings, run_date: date, stages=STAGES) -> None:
    client = get_client(settings)
    if "generate" in stages:
        generate_all(settings, run_date)
    if "bronze" in stages:
        upload_run(settings, run_date, client=client)
    if "silver" in stages:
        transform_silver(settings, run_date, client=client)
    if "gold" in stages:
        build_gold(settings, run_date, client=client)
    if "warehouse" in stages:
        load_warehouse(settings, run_date, client=client)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--date", help="Single run date YYYY-MM-DD")
    ap.add_argument("--start")
    ap.add_argument("--end")
    ap.add_argument("--stages", nargs="+", choices=STAGES, default=list(STAGES))
    args = ap.parse_args()

    settings = Settings.from_env()
    log = get_logger("socialpulse.pipeline", settings.log_dir, "pipeline")
    if args.date:
        days = [to_date(args.date)]
    elif args.start and args.end:
        start, end = to_date(args.start), to_date(args.end)
        days = [start + timedelta(days=i) for i in range((end - start).days + 1)]
    else:
        days = [date.today() - timedelta(days=1)]
    for d in days:  # chronological order matters for SCD2 and growth calculations
        log.info("=== pipeline run for %s stages=%s ===", d, args.stages)
        run_day(settings, d, tuple(args.stages))


if __name__ == "__main__":
    main()
