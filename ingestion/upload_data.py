"""Upload a day of raw data to the R2 Bronze layer.

    python -m ingestion.upload_data --date 2026-10-30

Key layout:  bronze/<entity>/ingestion_date=YYYY-MM-DD/<entity>_<MMDD>.<ext>
Re-running a date overwrites the same keys (idempotent).
"""
from __future__ import annotations

import argparse
import json
from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone
from pathlib import Path

from botocore.client import BaseClient
from botocore.exceptions import BotoCoreError, ClientError

from config.logging_config import get_logger
from config.settings import BRONZE, BRONZE_FORMATS, LOGS, Settings
from data_generation.common import to_date
from data_generation.run_generation import MANIFEST
from ingestion.r2_client import get_client, put_bytes

SOURCE = "synthetic-faker"
CONTENT_TYPES = {"csv": "text/csv", "json": "application/x-ndjson"}


class IngestionError(RuntimeError):
    """One or more files failed to upload."""


@dataclass
class UploadResult:
    entity: str
    key: str
    rows: int
    bytes: int
    ok: bool
    error: str | None = None


def bronze_key(entity: str, run_date: date, ext: str | None = None) -> str:
    ext = ext or BRONZE_FORMATS[entity]
    return f"{BRONZE}/{entity}/ingestion_date={run_date:%Y-%m-%d}/{entity}_{run_date:%m%d}.{ext}"


def upload_run(settings: Settings, run_date: date | str, local_dir: Path | None = None,
               client: BaseClient | None = None) -> list[UploadResult]:
    """Upload every entity file for `run_date`. All files are attempted; if any fail an
    IngestionError is raised afterwards (so Airflow marks the task failed and retries)."""
    run_date = to_date(run_date)
    local_dir = Path(local_dir) if local_dir else settings.raw_dir(run_date)
    log = get_logger("socialpulse.ingestion", settings.log_dir, "ingestion", str(run_date))
    client = client or get_client(settings)

    manifest_path = local_dir / MANIFEST
    if not manifest_path.exists():
        raise IngestionError(f"No manifest at {manifest_path}; run data generation first.")
    manifest = json.loads(manifest_path.read_text())
    ingestion_ts = datetime.now(timezone.utc).isoformat(timespec="seconds")

    results: list[UploadResult] = []
    for entity, info in manifest["files"].items():
        path = local_dir / info["file"]
        key = bronze_key(entity, run_date, info["format"])
        try:
            body = path.read_bytes()
            put_bytes(
                client, settings.r2_bucket, key, body, CONTENT_TYPES[info["format"]],
                metadata={
                    "ingestion_ts": ingestion_ts,
                    "source": SOURCE,
                    "row_count": str(info["rows"]),
                    "sha256": info["sha256"],
                    "run_date": str(run_date),
                },
            )
            head = client.head_object(Bucket=settings.r2_bucket, Key=key)  # verify the write
            if head["ContentLength"] != len(body):
                raise OSError(f"size mismatch: local={len(body)} remote={head['ContentLength']}")
            results.append(UploadResult(entity, key, info["rows"], len(body), True))
            log.info("uploaded %-12s rows=%-7d -> s3://%s/%s", entity, info["rows"], settings.r2_bucket, key)
        except (ClientError, BotoCoreError, OSError) as exc:
            results.append(UploadResult(entity, key, info["rows"], 0, False, str(exc)))
            log.error("FAILED upload %s -> %s: %s", entity, key, exc)

    _write_run_log(settings, client, run_date, ingestion_ts, results, log)
    failed = [r for r in results if not r.ok]
    if failed:
        raise IngestionError(
            f"{len(failed)}/{len(results)} uploads failed: " + "; ".join(f"{r.entity}: {r.error}" for r in failed)
        )
    return results


def _write_run_log(settings, client, run_date, ingestion_ts, results, log) -> None:
    """Persist a JSON run summary locally and (best effort) to R2 logs/ingestion/."""
    payload = json.dumps(
        {"run_date": str(run_date), "ingestion_ts": ingestion_ts, "results": [asdict(r) for r in results]},
        indent=2,
    )
    local = settings.log_dir / "ingestion" / f"run_{run_date:%Y%m%d}.json"
    local.parent.mkdir(parents=True, exist_ok=True)
    local.write_text(payload)
    try:
        put_bytes(client, settings.r2_bucket,
                  f"{LOGS}/ingestion/ingestion_date={run_date:%Y-%m-%d}/run_summary.json",
                  payload.encode(), "application/json")
    except (ClientError, BotoCoreError) as exc:  # logging must never mask the real outcome
        log.warning("could not write run log to R2: %s", exc)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--date", default=str(date.today()))
    args = ap.parse_args()
    upload_run(Settings.from_env(), args.date)


if __name__ == "__main__":
    main()
