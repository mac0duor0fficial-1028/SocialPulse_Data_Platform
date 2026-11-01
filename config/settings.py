"""Central configuration. All secrets come from the environment (.env locally)."""
from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date
from pathlib import Path

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[1]

# Medallion layers (prefixes inside the single R2 bucket)
BRONZE = "bronze"
SILVER = "silver"
GOLD = "gold"
LOGS = "logs"

ENTITIES = ("users", "posts", "engagements", "followers", "hashtags")
# Bronze keeps data exactly as generated: CSV for four entities, JSON Lines for engagements.
BRONZE_FORMATS = {
    "users": "csv",
    "posts": "csv",
    "followers": "csv",
    "hashtags": "csv",
    "engagements": "json",
}
GOLD_DATASETS = (
    "creator_performance",
    "post_performance",
    "hashtag_performance",
    "audience_growth",
)

PLATFORMS = ("instagram", "tiktok", "x")
ENGAGEMENT_TYPES = ("like", "comment", "share", "view")
LOCAL_TZ = "Africa/Nairobi"  # source timestamps without an offset are Nairobi local time (EAT, UTC+3)


class ConfigError(RuntimeError):
    """Raised when required configuration is missing or invalid."""


def _as_bool(value: str) -> bool:
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


@dataclass(frozen=True)
class Settings:
    r2_access_key_id: str
    r2_secret_access_key: str
    r2_endpoint: str
    r2_bucket: str
    pg_host: str = "localhost"
    pg_port: int = 5432
    pg_database: str = "socialpulse"
    pg_user: str = "postgres"
    pg_password: str = "postgres"
    seed: int = 42
    n_users: int = 500
    n_posts_per_day: int = 1500
    n_engagements_per_day: int = 10_000
    inject_dirty_data: bool = True
    dirty_rate: float = 0.03
    max_reject_rate: float = 0.10
    data_dir: Path = PROJECT_ROOT / "data"
    log_dir: Path = PROJECT_ROOT / "logs"

    @property
    def pg_dsn(self) -> str:
        return (
            f"host={self.pg_host} port={self.pg_port} dbname={self.pg_database} "
            f"user={self.pg_user} password={self.pg_password}"
        )

    def raw_dir(self, run_date: date | str) -> Path:
        return self.data_dir / "raw" / str(run_date)

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> Settings:
        """Build settings from `env` (defaults to os.environ after loading .env)."""
        if env is None:
            load_dotenv(PROJECT_ROOT / ".env", override=False)
            env = os.environ

        required = ("R2_ACCESS_KEY_ID", "R2_SECRET_ACCESS_KEY", "R2_ENDPOINT", "R2_BUCKET")
        missing = [k for k in required if not env.get(k)]
        if missing:
            raise ConfigError(
                f"Missing required environment variables: {', '.join(missing)}. "
                "Copy .env.example to .env and fill them in."
            )

        def _path(key: str, default: str) -> Path:
            p = Path(env.get(key, default))
            return p if p.is_absolute() else PROJECT_ROOT / p

        try:
            return cls(
                r2_access_key_id=env["R2_ACCESS_KEY_ID"],
                r2_secret_access_key=env["R2_SECRET_ACCESS_KEY"],
                r2_endpoint=env["R2_ENDPOINT"].rstrip("/"),
                r2_bucket=env["R2_BUCKET"],
                pg_host=env.get("PG_HOST", "localhost"),
                pg_port=int(env.get("PG_PORT", "5432")),
                pg_database=env.get("PG_DATABASE", "socialpulse"),
                pg_user=env.get("PG_USER", "postgres"),
                pg_password=env.get("PG_PASSWORD", "postgres"),
                seed=int(env.get("SEED", "42")),
                n_users=int(env.get("N_USERS", "500")),
                n_posts_per_day=int(env.get("N_POSTS_PER_DAY", "1500")),
                n_engagements_per_day=int(env.get("N_ENGAGEMENTS_PER_DAY", "10000")),
                inject_dirty_data=_as_bool(env.get("INJECT_DIRTY_DATA", "true")),
                dirty_rate=float(env.get("DIRTY_RATE", "0.03")),
                max_reject_rate=float(env.get("MAX_REJECT_RATE", "0.10")),
                data_dir=_path("DATA_DIR", "data"),
                log_dir=_path("LOG_DIR", "logs"),
            )
        except ValueError as exc:  # bad int/float
            raise ConfigError(f"Invalid numeric configuration value: {exc}") from exc
