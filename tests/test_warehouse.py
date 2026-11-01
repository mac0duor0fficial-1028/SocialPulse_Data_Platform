"""Warehouse integration tests. Need a PostgreSQL database: set TEST_PG_DSN, e.g.
   TEST_PG_DSN="host=localhost dbname=socialpulse_test user=postgres password=postgres"
The tests wipe the public schema of that database - never point it at real data."""
import os
from datetime import date
from pathlib import Path

import pandas as pd
import pytest

from config.settings import GOLD, SILVER
from data_generation.run_generation import generate_all
from ingestion.r2_client import read_parquet
from ingestion.upload_data import upload_run
from quality.checks import DataQualityError
from transformations.build_gold import build_gold
from transformations.common import layer_key
from transformations.run_silver import transform_silver
from warehouse.load_gold import apply_schema, load_frames, load_warehouse

DSN = os.environ.get("TEST_PG_DSN")
pytestmark = pytest.mark.skipif(not DSN, reason="TEST_PG_DSN not set")
psycopg2 = pytest.importorskip("psycopg2")

D1, D2 = date(2026, 10, 29), date(2026, 10, 30)
GOLD_NAMES = ("post_performance", "creator_performance", "hashtag_performance", "audience_growth")


@pytest.fixture
def conn():
    c = psycopg2.connect(DSN)
    with c, c.cursor() as cur:
        cur.execute("DROP SCHEMA public CASCADE; CREATE SCHEMA public;")
    yield c
    c.close()


@pytest.fixture
def lake(settings, r2):
    for d in (D1, D2):
        generate_all(settings, d)
        upload_run(settings, d, client=r2)
        transform_silver(settings, d, client=r2)
        build_gold(settings, d, client=r2)
    return r2


def q(conn, sql, *args):
    with conn.cursor() as cur:
        cur.execute(sql, args)
        return cur.fetchall()


def test_schema_is_idempotent(conn):
    apply_schema(conn)
    apply_schema(conn)
    conn.commit()
    assert q(conn, "SELECT count(*) FROM dim_date")[0][0] > 4000


def test_load_is_idempotent_and_facts_reconcile(settings, lake, conn):
    first = load_warehouse(settings, D2, client=lake, conn=conn)
    snapshot = {t: q(conn, f"SELECT count(*) FROM {t}")[0][0] for t in
                ("dim_creator", "dim_post", "fact_post_engagement", "fact_follower_snapshots",
                 "fact_creator_daily", "fact_hashtag_performance")}
    load_warehouse(settings, D2, client=lake, conn=conn)  # re-run same day
    for t, n in snapshot.items():
        assert q(conn, f"SELECT count(*) FROM {t}")[0][0] == n, t
    gold = read_parquet(lake, settings.r2_bucket, layer_key(GOLD, "post_performance", D2))
    assert snapshot["fact_post_engagement"] == len(gold) == first["fact_post_engagement"]
    assert q(conn, "SELECT sum(total_engagement) FROM fact_post_engagement")[0][0] == gold["total_engagement"].sum()
    # every fact row resolves through every dimension (no orphans)
    assert q(conn, """SELECT count(*) FROM fact_post_engagement f
                      JOIN dim_post p USING (post_key) JOIN dim_creator c USING (creator_key)
                      JOIN dim_platform USING (platform_key) JOIN dim_date USING (date_key)""")[0][0] == len(gold)
    # audit trail records both loads
    assert q(conn, "SELECT count(*) FROM etl_load_audit WHERE target_table='fact_post_engagement'")[0][0] == 2


def test_scd2_creates_history_and_links_facts_to_the_right_version(settings, lake, conn):
    load_warehouse(settings, D1, client=lake, conn=conn)
    users = read_parquet(lake, settings.r2_bucket, layer_key(SILVER, "users", D2))
    gold = {n: read_parquet(lake, settings.r2_bucket, layer_key(GOLD, n, D2)) for n in GOLD_NAMES}
    mover = gold["creator_performance"].query("posts_count > 0")["user_id"].iloc[0]
    old_city = users.loc[users["user_id"] == mover, "city"].iloc[0]
    users.loc[users["user_id"] == mover, "city"] = "Thika" if old_city != "Thika" else "Mombasa"
    new_city = users.loc[users["user_id"] == mover, "city"].iloc[0]

    counts = load_frames(conn, D2, users, gold)
    assert counts["dim_creator"] == 2  # one closed + one opened
    rows = q(conn, "SELECT city, valid_from, valid_to, is_current FROM dim_creator WHERE user_id=%s ORDER BY valid_from", mover)
    assert [r[0] for r in rows] == [old_city, new_city]
    assert rows[0][2] == D2 and rows[0][3] is False and rows[1][3] is True
    # D1 fact -> old version, D2 fact -> new version
    cities = q(conn, """SELECT f.date_key, c.city FROM fact_creator_daily f JOIN dim_creator c USING (creator_key)
                        WHERE c.user_id=%s ORDER BY f.date_key""", mover)
    assert cities == [(20261029, old_city), (20261030, new_city)]
    # exactly one current version per creator
    assert q(conn, "SELECT count(*) FROM (SELECT user_id FROM dim_creator WHERE is_current "
                   "GROUP BY user_id HAVING count(*) > 1) x")[0][0] == 0


def test_unmatched_creator_rolls_back_whole_load(settings, lake, conn):
    apply_schema(conn)
    conn.commit()
    users = read_parquet(lake, settings.r2_bucket, layer_key(SILVER, "users", D2))
    gold = {n: read_parquet(lake, settings.r2_bucket, layer_key(GOLD, n, D2)) for n in GOLD_NAMES}
    gold["post_performance"].loc[0, "user_id"] = "USER_DOES_NOT_EXIST"
    with pytest.raises(DataQualityError, match="creator"):
        load_frames(conn, D2, users, gold)
    assert q(conn, "SELECT count(*) FROM dim_creator")[0][0] == 0  # nothing partially committed


def test_analytics_sql_runs_and_matches_pandas(settings, lake, conn):
    load_warehouse(settings, D1, client=lake, conn=conn)
    load_warehouse(settings, D2, client=lake, conn=conn)
    sql = (Path(__file__).resolve().parents[1] / "warehouse" / "analytics.sql").read_text()
    statements = [s.strip() for s in sql.split(";") if s.strip() and not s.strip().startswith("--\n")]
    with conn.cursor() as cur:
        for stmt in statements:
            cur.execute(stmt)  # every documented query must execute
            cur.fetchall()
    # Q4 cross-check against the Gold parquet
    gold = pd.concat(read_parquet(lake, settings.r2_bucket, layer_key(GOLD, "post_performance", d)) for d in (D1, D2))
    expected = gold.groupby("platform")["total_engagement"].sum().to_dict()
    got = dict(q(conn, "SELECT pl.platform_name, sum(f.total_engagement) FROM fact_post_engagement f "
                       "JOIN dim_platform pl USING (platform_key) GROUP BY 1"))
    assert {k: int(v) for k, v in got.items()} == expected
