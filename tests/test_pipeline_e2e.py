"""Whole pipeline (generate -> Bronze -> Silver -> Gold) against a mocked R2 bucket."""
from datetime import date

import pandas as pd
import pytest

from config.settings import GOLD, SILVER
from data_generation.run_generation import generate_all
from ingestion.r2_client import list_keys, read_parquet, read_parquet_prefix
from ingestion.upload_data import upload_run
from quality.checks import DataQualityError
from transformations.build_gold import build_audience_growth, build_gold
from transformations.common import layer_key, reject_key
from transformations.run_silver import transform_silver

D1, D2 = date(2026, 10, 29), date(2026, 10, 30)


def run_day(settings, r2, d):
    generate_all(settings, d)
    upload_run(settings, d, client=r2)
    stats = transform_silver(settings, d, client=r2)
    counts = build_gold(settings, d, client=r2)
    return stats, counts


def test_full_pipeline_and_gold_reconciles(settings, r2):
    stats, counts = run_day(settings, r2, D2)
    for s in stats:
        assert s.rows_in == s.rows_out + s.rows_rejected
    eng_stats = next(s for s in stats if s.entity == "engagements")
    assert eng_stats.reject_reasons.get("duplicate_engagement_id", 0) > 0

    # quarantine file exists and explains every rejected row
    rejects = read_parquet(r2, settings.r2_bucket, reject_key("engagements", D2))
    assert len(rejects) == eng_stats.rows_rejected

    silver = read_parquet(r2, settings.r2_bucket, layer_key(SILVER, "engagements", D2))
    perf = read_parquet(r2, settings.r2_bucket, layer_key(GOLD, "post_performance", D2))
    assert perf["total_engagement"].sum() == (silver["engagement_type"] != "view").sum()
    assert perf["impressions"].sum() == (silver["engagement_type"] == "view").sum()
    assert (perf["total_engagement"] == perf["likes"] + perf["comments"] + perf["shares"]).all()
    assert set(counts) == {"post_performance", "creator_performance", "hashtag_performance", "audience_growth"}


def test_rerun_is_idempotent(settings, r2):
    run_day(settings, r2, D2)
    keys_before = list_keys(r2, settings.r2_bucket, "")
    gold1 = read_parquet(r2, settings.r2_bucket, layer_key(GOLD, "post_performance", D2))
    run_day(settings, r2, D2)
    assert list_keys(r2, settings.r2_bucket, "") == keys_before
    pd.testing.assert_frame_equal(
        gold1, read_parquet(r2, settings.r2_bucket, layer_key(GOLD, "post_performance", D2)))


def test_audience_growth_uses_previous_snapshot(settings, r2):
    run_day(settings, r2, D1)
    run_day(settings, r2, D2)
    followers = read_parquet_prefix(r2, settings.r2_bucket, f"{SILVER}/followers/")
    growth = build_audience_growth(followers, D2)
    row = growth.dropna(subset=["previous_follower_count"]).iloc[0]
    expected = (row.follower_count - row.previous_follower_count) / row.previous_follower_count * 100
    assert row.growth_rate_pct == pytest.approx(expected)
    # first-ever snapshot has no previous value
    assert build_audience_growth(followers, D1)["previous_follower_count"].isna().all()


def test_reject_rate_circuit_breaker_stops_bad_source(settings, r2):
    import dataclasses
    broken = dataclasses.replace(settings, dirty_rate=0.2)  # simulate a badly broken source
    generate_all(broken, D2)
    upload_run(broken, D2, client=r2)
    with pytest.raises(DataQualityError, match="reject rate"):
        transform_silver(broken, D2, client=r2)
    # nothing from the failed entity reached Silver
    assert not list_keys(r2, broken.r2_bucket, "silver/engagements/")
