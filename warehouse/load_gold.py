"""Load Gold Parquet (+ Silver users for the creator dimension) into PostgreSQL.

    python -m warehouse.load_gold --date 2026-10-30

Design
* One transaction: either the whole day loads or nothing does.
* Idempotent: dimensions upsert, facts use INSERT .. ON CONFLICT DO UPDATE, so re-running a
  date never duplicates rows.
* dim_creator is SCD Type 2 (surrogate key + valid_from / valid_to / is_current). Facts are
  linked to the creator version that was valid on the fact's date.
* Run dates must be loaded in chronological order (the DAG enforces max_active_runs=1).
"""
from __future__ import annotations

import argparse
from datetime import date
from pathlib import Path

import pandas as pd
import psycopg2
from botocore.client import BaseClient
from psycopg2.extras import execute_values

from config.logging_config import get_logger
from config.settings import GOLD, SILVER, Settings
from data_generation.common import to_date
from ingestion.r2_client import get_client, read_parquet
from quality.checks import DataQualityError
from transformations.common import layer_key

SCHEMA_FILE = Path(__file__).with_name("schema.sql")


class WarehouseLoadError(RuntimeError):
    pass


# ----------------------------------------------------------------------------- helpers
def _rows(df: pd.DataFrame) -> list[tuple]:
    """DataFrame -> list of tuples with NaN/NaT -> None and numpy scalars -> Python scalars."""
    obj = df.astype(object).where(pd.notna(df), None)
    return [tuple(r) for r in obj.itertuples(index=False, name=None)]


def _stage(cur, name: str, ddl_cols: str, df: pd.DataFrame) -> int:
    """Create a transaction-scoped staging table and bulk-insert `df` into it."""
    cur.execute(f"CREATE TEMP TABLE {name} ({ddl_cols}) ON COMMIT DROP")
    cols = ", ".join(df.columns)
    if len(df):
        execute_values(cur, f"INSERT INTO {name} ({cols}) VALUES %s", _rows(df), page_size=5000)
    return len(df)


def _audit(cur, run_date: date, table: str, rows: int) -> None:
    cur.execute("INSERT INTO etl_load_audit (run_date, target_table, rows_loaded) VALUES (%s, %s, %s)",
                (run_date, table, rows))


def _require_all_matched(cur, stage: str, join_sql: str, what: str) -> None:
    cur.execute(f"SELECT count(*) FROM {stage} s {join_sql}")
    missing = cur.fetchone()[0]
    if missing:
        raise DataQualityError(f"{stage}: {missing} row(s) have no matching {what} in the warehouse")


def apply_schema(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(SCHEMA_FILE.read_text())


# ----------------------------------------------------------------------------- dimensions
def load_dim_location(cur, cities: pd.Series) -> None:
    execute_values(cur, "INSERT INTO dim_location (city) VALUES %s ON CONFLICT (city) DO NOTHING",
                   [(c,) for c in sorted(cities.dropna().unique())])


def load_dim_platform(cur, platforms: pd.Series) -> None:
    execute_values(cur, "INSERT INTO dim_platform (platform_name) VALUES %s ON CONFLICT (platform_name) DO NOTHING",
                   [(p,) for p in sorted(platforms.dropna().unique())])


def load_dim_creator_scd2(cur, users: pd.DataFrame, run_date: date) -> dict[str, int]:
    """SCD Type 2 merge. Returns {'updated_in_place', 'closed', 'inserted'} counts."""
    stg = users[["user_id", "username", "niche", "city", "joined_date"]].copy()
    stg["joined_date"] = stg["joined_date"].dt.date
    _stage(cur, "stg_creator",
           "user_id TEXT, username TEXT, niche TEXT, city TEXT, joined_date DATE", stg)

    tracked = "(d.username, d.niche, d.city) IS DISTINCT FROM (s.username, s.niche, s.city)"
    # 1) same-day correction of the version opened today: overwrite instead of versioning
    cur.execute(f"""
        UPDATE dim_creator d
           SET username = s.username, niche = s.niche, city = s.city,
               location_key = (SELECT location_key FROM dim_location WHERE city = s.city)
          FROM stg_creator s
         WHERE d.user_id = s.user_id AND d.is_current AND d.valid_from = %s AND {tracked}""", (run_date,))
    updated = cur.rowcount
    # 2) close current versions whose tracked attributes changed
    cur.execute(f"""
        UPDATE dim_creator d SET valid_to = %s, is_current = FALSE
          FROM stg_creator s
         WHERE d.user_id = s.user_id AND d.is_current AND {tracked}""", (run_date,))
    closed = cur.rowcount
    # 3) open a version for new creators and for creators closed above
    cur.execute("""
        INSERT INTO dim_creator (user_id, username, niche, city, location_key, joined_date, valid_from)
        SELECT s.user_id, s.username, s.niche, s.city, l.location_key, s.joined_date,
               CASE WHEN EXISTS (SELECT 1 FROM dim_creator p WHERE p.user_id = s.user_id)
                    THEN %s::DATE ELSE DATE '1970-01-01' END
          FROM stg_creator s JOIN dim_location l ON l.city = s.city
         WHERE NOT EXISTS (SELECT 1 FROM dim_creator c WHERE c.user_id = s.user_id AND c.is_current)""",
                (run_date,))
    inserted = cur.rowcount
    return {"updated_in_place": updated, "closed": closed, "inserted": inserted}


def load_dim_post(cur, post_perf: pd.DataFrame) -> int:
    stg = post_perf[["post_id", "content_type", "caption", "posted_at", "posted_hour_eat", "posted_dow",
                     "posted_day_name"]].rename(columns={"posted_dow": "posted_day_of_week"})
    n = _stage(cur, "stg_post", "post_id TEXT, content_type TEXT, caption TEXT, posted_at TIMESTAMPTZ, "
                                "posted_hour_eat SMALLINT, posted_day_of_week SMALLINT, posted_day_name TEXT", stg)
    cur.execute("""
        INSERT INTO dim_post (post_id, content_type, caption, posted_at, posted_hour_eat,
                              posted_day_of_week, posted_day_name)
        SELECT post_id, content_type, caption, posted_at, posted_hour_eat, posted_day_of_week, posted_day_name
          FROM stg_post
        ON CONFLICT (post_id) DO UPDATE SET
            content_type = EXCLUDED.content_type, caption = EXCLUDED.caption, posted_at = EXCLUDED.posted_at,
            posted_hour_eat = EXCLUDED.posted_hour_eat, posted_day_of_week = EXCLUDED.posted_day_of_week,
            posted_day_name = EXCLUDED.posted_day_name""")
    return n


# ----------------------------------------------------------------------------- facts
_CREATOR_JOIN = ("JOIN dim_creator c ON c.user_id = s.user_id "
                 "AND s.fact_date >= c.valid_from AND s.fact_date < c.valid_to")


def _date_key(d: pd.Series) -> pd.Series:
    return (d.dt.year * 10_000 + d.dt.month * 100 + d.dt.day).astype("int64")


def load_fact_post_engagement(cur, post_perf: pd.DataFrame) -> int:
    stg = post_perf[["post_id", "user_id", "platform", "posted_date", "likes", "comments", "shares",
                     "impressions", "total_engagement"]].copy()
    stg["fact_date"] = stg.pop("posted_date").dt.date
    n = _stage(cur, "stg_fpe", "post_id TEXT, user_id TEXT, platform TEXT, likes INT, comments INT, shares INT, "
                               "impressions INT, total_engagement INT, fact_date DATE", stg)
    _require_all_matched(cur, "stg_fpe", "LEFT JOIN dim_creator c ON c.user_id = s.user_id "
                         "AND s.fact_date >= c.valid_from AND s.fact_date < c.valid_to "
                         "WHERE c.creator_key IS NULL", "creator")
    cur.execute(f"""
        INSERT INTO fact_post_engagement (post_key, creator_key, platform_key, date_key, likes, comments,
                                          shares, impressions, total_engagement)
        SELECT p.post_key, c.creator_key, pl.platform_key, to_char(s.fact_date, 'YYYYMMDD')::INT,
               s.likes, s.comments, s.shares, s.impressions, s.total_engagement
          FROM stg_fpe s
          JOIN dim_post p ON p.post_id = s.post_id
          {_CREATOR_JOIN}
          JOIN dim_platform pl ON pl.platform_name = s.platform
        ON CONFLICT (post_key, date_key) DO UPDATE SET
            creator_key = EXCLUDED.creator_key, platform_key = EXCLUDED.platform_key,
            likes = EXCLUDED.likes, comments = EXCLUDED.comments, shares = EXCLUDED.shares,
            impressions = EXCLUDED.impressions, total_engagement = EXCLUDED.total_engagement,
            loaded_at = now()""")
    return n


def load_fact_follower_snapshots(cur, growth: pd.DataFrame) -> int:
    stg = growth[["user_id", "snapshot_date", "follower_count", "previous_follower_count",
                  "growth_rate_pct"]].copy()
    stg["fact_date"] = stg.pop("snapshot_date").dt.date
    n = _stage(cur, "stg_ffs", "user_id TEXT, follower_count BIGINT, previous_follower_count BIGINT, "
                               "growth_rate_pct NUMERIC, fact_date DATE", stg)
    _require_all_matched(cur, "stg_ffs", "LEFT JOIN dim_creator c ON c.user_id = s.user_id "
                         "AND s.fact_date >= c.valid_from AND s.fact_date < c.valid_to "
                         "WHERE c.creator_key IS NULL", "creator")
    cur.execute(f"""
        INSERT INTO fact_follower_snapshots (creator_key, date_key, follower_count, previous_follower_count,
                                             growth_rate_pct)
        SELECT c.creator_key, to_char(s.fact_date, 'YYYYMMDD')::INT, s.follower_count,
               s.previous_follower_count, round(s.growth_rate_pct, 4)
          FROM stg_ffs s {_CREATOR_JOIN}
        ON CONFLICT (creator_key, date_key) DO UPDATE SET
            follower_count = EXCLUDED.follower_count, previous_follower_count = EXCLUDED.previous_follower_count,
            growth_rate_pct = EXCLUDED.growth_rate_pct, loaded_at = now()""")
    return n


def load_fact_creator_daily(cur, perf: pd.DataFrame) -> int:
    stg = perf[["user_id", "snapshot_date", "posts_count", "likes", "comments", "shares", "impressions",
                "total_engagement", "follower_count", "engagement_rate_pct"]].copy()
    stg["fact_date"] = stg.pop("snapshot_date").dt.date
    n = _stage(cur, "stg_fcd", "user_id TEXT, posts_count INT, likes BIGINT, comments BIGINT, shares BIGINT, "
                               "impressions BIGINT, total_engagement BIGINT, follower_count BIGINT, "
                               "engagement_rate_pct NUMERIC, fact_date DATE", stg)
    _require_all_matched(cur, "stg_fcd", "LEFT JOIN dim_creator c ON c.user_id = s.user_id "
                         "AND s.fact_date >= c.valid_from AND s.fact_date < c.valid_to "
                         "WHERE c.creator_key IS NULL", "creator")
    cur.execute(f"""
        INSERT INTO fact_creator_daily (creator_key, date_key, posts_count, likes, comments, shares,
                                        impressions, total_engagement, follower_count, engagement_rate_pct)
        SELECT c.creator_key, to_char(s.fact_date, 'YYYYMMDD')::INT, s.posts_count, s.likes, s.comments,
               s.shares, s.impressions, s.total_engagement, s.follower_count, round(s.engagement_rate_pct, 4)
          FROM stg_fcd s {_CREATOR_JOIN}
        ON CONFLICT (creator_key, date_key) DO UPDATE SET
            posts_count = EXCLUDED.posts_count, likes = EXCLUDED.likes, comments = EXCLUDED.comments,
            shares = EXCLUDED.shares, impressions = EXCLUDED.impressions,
            total_engagement = EXCLUDED.total_engagement, follower_count = EXCLUDED.follower_count,
            engagement_rate_pct = EXCLUDED.engagement_rate_pct, loaded_at = now()""")
    return n


def load_fact_hashtag_performance(cur, tags: pd.DataFrame, run_date: date) -> int:
    stg = tags[["hashtag", "platform", "posts_used", "likes", "comments", "shares", "impressions",
                "total_engagement"]].copy()
    stg["fact_date"] = run_date
    n = _stage(cur, "stg_fhp", "hashtag TEXT, platform TEXT, posts_used INT, likes BIGINT, comments BIGINT, "
                               "shares BIGINT, impressions BIGINT, total_engagement BIGINT, fact_date DATE", stg)
    cur.execute("""
        INSERT INTO fact_hashtag_performance (hashtag, platform_key, date_key, posts_used, likes, comments,
                                              shares, impressions, total_engagement)
        SELECT s.hashtag, pl.platform_key, to_char(s.fact_date, 'YYYYMMDD')::INT, s.posts_used, s.likes,
               s.comments, s.shares, s.impressions, s.total_engagement
          FROM stg_fhp s JOIN dim_platform pl ON pl.platform_name = s.platform
        ON CONFLICT (hashtag, platform_key, date_key) DO UPDATE SET
            posts_used = EXCLUDED.posts_used, likes = EXCLUDED.likes, comments = EXCLUDED.comments,
            shares = EXCLUDED.shares, impressions = EXCLUDED.impressions,
            total_engagement = EXCLUDED.total_engagement, loaded_at = now()""")
    return n


# ----------------------------------------------------------------------------- orchestration
def load_frames(conn, run_date: date, users: pd.DataFrame, gold: dict[str, pd.DataFrame]) -> dict[str, int]:
    """Load already-read frames inside ONE transaction. Separated from I/O so it is testable."""
    counts: dict[str, int] = {}
    try:
        apply_schema(conn)
        with conn.cursor() as cur:
            load_dim_location(cur, users["city"])
            load_dim_platform(cur, gold["post_performance"]["platform"])
            scd = load_dim_creator_scd2(cur, users, run_date)
            counts["dim_creator"] = scd["inserted"] + scd["closed"] + scd["updated_in_place"]
            counts["dim_post"] = load_dim_post(cur, gold["post_performance"])
            counts["fact_post_engagement"] = load_fact_post_engagement(cur, gold["post_performance"])
            counts["fact_follower_snapshots"] = load_fact_follower_snapshots(cur, gold["audience_growth"])
            counts["fact_creator_daily"] = load_fact_creator_daily(cur, gold["creator_performance"])
            counts["fact_hashtag_performance"] = load_fact_hashtag_performance(
                cur, gold["hashtag_performance"], run_date)
            for table, rows in counts.items():
                _audit(cur, run_date, table, rows)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    return counts


def load_warehouse(settings: Settings, run_date: date | str, client: BaseClient | None = None,
                   conn=None) -> dict[str, int]:
    run_date = to_date(run_date)
    log = get_logger("socialpulse.warehouse", settings.log_dir, "warehouse", str(run_date))
    client = client or get_client(settings)
    users = read_parquet(client, settings.r2_bucket, layer_key(SILVER, "users", run_date))
    gold = {
        name: read_parquet(client, settings.r2_bucket, layer_key(GOLD, name, run_date))
        for name in ("post_performance", "creator_performance", "hashtag_performance", "audience_growth")
    }
    own_conn = conn is None
    conn = conn or psycopg2.connect(settings.pg_dsn, connect_timeout=15)
    try:
        counts = load_frames(conn, run_date, users, gold)
    finally:
        if own_conn:
            conn.close()
    for table, rows in counts.items():
        log.info("warehouse %-26s rows=%d", table, rows)
    return counts


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--date", default=str(date.today()))
    args = ap.parse_args()
    load_warehouse(Settings.from_env(), args.date)


if __name__ == "__main__":
    main()
