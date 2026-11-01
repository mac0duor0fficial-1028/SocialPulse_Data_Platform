"""Generate one day of synthetic Bronze data into data/raw/<date>/ and write a manifest.

    python -m data_generation.run_generation --date 2026-10-30
"""
from __future__ import annotations

import argparse
import hashlib
import json
from datetime import date, datetime, timezone
from pathlib import Path

from config.logging_config import get_logger
from config.settings import BRONZE_FORMATS, Settings
from data_generation import dirtiness
from data_generation.common import date_int, rng_for, to_date
from data_generation.generate_engagements import generate_engagements
from data_generation.generate_followers import generate_followers
from data_generation.generate_posts import generate_posts
from data_generation.generate_users import generate_users

MANIFEST = "_manifest.json"


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def generate_all(settings: Settings, run_date: date | str, out_dir: Path | None = None) -> dict:
    """Generate all five datasets for `run_date`. Returns the manifest dict.

    The output is a pure function of (settings.seed, run_date, sizes) so re-running a day
    produces byte-identical files – the basis for idempotent re-runs."""
    run_date = to_date(run_date)
    out_dir = Path(out_dir) if out_dir else settings.raw_dir(run_date)
    out_dir.mkdir(parents=True, exist_ok=True)
    log = get_logger("socialpulse.generate", settings.log_dir, "generation", str(run_date))

    users = generate_users(settings.n_users, settings.seed)
    posts, hashtags = generate_posts(users, run_date, settings.n_posts_per_day, settings.seed)
    engagements = generate_engagements(
        posts, users["user_id"].to_numpy(), run_date, settings.n_engagements_per_day, settings.seed
    )
    followers = generate_followers(users, run_date, settings.seed)
    frames = {
        "users": users, "posts": posts, "engagements": engagements,
        "followers": followers, "hashtags": hashtags,
    }

    if settings.inject_dirty_data:
        rng = rng_for(settings.seed, 9, date_int(run_date))
        rate = settings.dirty_rate
        frames["users"] = dirtiness.dirty_users(frames["users"], rng, rate)
        frames["posts"] = dirtiness.dirty_posts(frames["posts"], rng, rate)
        frames["engagements"] = dirtiness.dirty_engagements(frames["engagements"], rng, rate)
        frames["followers"] = dirtiness.dirty_followers(frames["followers"], rng, rate)
        frames["hashtags"] = dirtiness.dirty_hashtags(frames["hashtags"], rng, rate)

    manifest: dict = {
        "run_date": str(run_date),
        "seed": settings.seed,
        "inject_dirty_data": settings.inject_dirty_data,
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "files": {},
    }
    for entity, df in frames.items():
        ext = BRONZE_FORMATS[entity]
        path = out_dir / f"{entity}_{run_date:%m%d}.{ext}"
        if ext == "csv":
            df.to_csv(path, index=False)
        else:  # JSON Lines: one engagement event per line
            df.to_json(path, orient="records", lines=True)
        manifest["files"][entity] = {
            "file": path.name, "rows": int(len(df)), "sha256": _sha256(path), "format": ext,
        }
        log.info("generated %-12s rows=%-7d -> %s", entity, len(df), path)

    (out_dir / MANIFEST).write_text(json.dumps(manifest, indent=2))
    return manifest


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--date", default=str(date.today()), help="Run date YYYY-MM-DD")
    args = ap.parse_args()
    generate_all(Settings.from_env(), args.date)


if __name__ == "__main__":
    main()
