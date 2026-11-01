from datetime import date

import pandas as pd
import pytest

from quality.checks import (
    DataQualityError,
    check_null_rates,
    check_reject_rate,
    check_row_reconciliation,
)
from quality.schemas import validate_silver
from transformations.clean_engagements import clean_engagements
from transformations.clean_followers import clean_followers
from transformations.clean_hashtags import clean_hashtags
from transformations.clean_posts import clean_posts
from transformations.clean_users import clean_users
from transformations.common import parse_to_utc

D = date(2026, 10, 30)
POSTS = pd.Series(["P1", "P2"])
USERS = pd.Series(["U1", "U2"])


def eng(rows):
    cols = ["engagement_id", "post_id", "user_id", "engagement_type", "platform", "timestamp"]
    return pd.DataFrame(rows, columns=cols)


def test_parse_to_utc_handles_all_formats():
    s = pd.Series(["2026-10-30T10:30:00", "2026-10-30 10:30:00", "2026-10-30T10:30:00+03:00",
                   "2026-10-30T07:30:00Z", "garbage", None])
    out = parse_to_utc(s)
    expected = pd.Timestamp("2026-10-30T07:30:00", tz="UTC")  # 10:30 Nairobi == 07:30 UTC
    assert (out.iloc[:4] == expected).all()
    assert out.iloc[4:].isna().all()


def test_engagements_dedupe_standardise_and_quarantine():
    raw = eng([
        ["E1", "P1", "U1", "LIKE", " Instagram ", "2026-10-30T10:00:00"],
        ["E1", "P1", "U1", "like", "instagram", "2026-10-30T10:00:00"],      # duplicate id
        ["E2", "P1", "U2", "view", "Twitter", "2026-10-30T11:00:00Z"],       # alias + UTC
        ["E3", "P9", "U1", "like", "x", "2026-10-30T11:00:00"],             # orphan post
        ["E4", "P1", "U9", "like", "x", "2026-10-30T11:00:00"],             # orphan user
        ["E5", "P1", None, "like", "x", "2026-10-30T11:00:00"],              # missing field
        ["E6", "P1", "U1", "poke", "x", "2026-10-30T11:00:00"],              # bad type
        ["E7", "P1", "U1", "like", "myspace", "2026-10-30T11:00:00"],        # bad platform
        ["E8", "P1", "U1", "like", "x", "not-a-date"],                       # bad timestamp
    ])
    res = clean_engagements(raw, D, POSTS, USERS)
    assert list(res.clean["engagement_id"]) == ["E1", "E2"]
    assert list(res.clean["platform"]) == ["instagram", "x"]
    assert str(res.clean["timestamp"].dt.tz) == "UTC"
    assert res.clean.loc[0, "timestamp"] == pd.Timestamp("2026-10-30T07:00:00", tz="UTC")
    assert res.stats.reject_reasons == {
        "duplicate_engagement_id": 1, "orphan_post_id": 1, "orphan_user_id": 1,
        "missing_required_field": 1, "invalid_engagement_type": 1, "invalid_platform": 1,
        "invalid_timestamp": 1,
    }
    # reconciliation invariant and Silver contract
    assert res.stats.rows_in == res.stats.rows_out + res.stats.rows_rejected == 9
    assert set(res.rejects["reject_reason"]) == set(res.stats.reject_reasons)
    validate_silver("engagements", res.clean)


def test_users_fill_nulls_and_title_case_with_logging():
    raw = pd.DataFrame({
        "user_id": ["U1", "U2", "U2", None], "username": ["a", "b", "b", "c"],
        "city": [" nairobi ", None, None, "kisumu"], "niche": ["Food", "FOOD", "FOOD", "Food"],
        "joined_date": ["2024-01-01"] * 3 + ["2024-02-02"],
    })
    res = clean_users(raw, D)
    assert list(res.clean["city"]) == ["Nairobi", "Unknown"]
    assert res.stats.nulls_filled == {"city": 1}
    assert res.stats.reject_reasons == {"missing_required_field": 1, "duplicate_user_id": 1}
    validate_silver("users", res.clean)


def test_posts_fill_caption_and_reject_orphan_users():
    raw = pd.DataFrame({
        "post_id": ["P1", "P2", "P3"], "user_id": ["U1", "U1", "U9"], "platform": ["TikTok", "x", "x"],
        "content_type": ["Video", "text", "text"], "caption": [None, "hi", "hi"],
        "posted_at": ["2026-10-30T09:00:00"] * 3,
    })
    res = clean_posts(raw, D, USERS)
    assert list(res.clean["post_id"]) == ["P1", "P2"]
    assert res.clean.loc[0, "caption"] == "" and res.clean.loc[0, "platform"] == "tiktok"
    assert res.stats.nulls_filled == {"caption": 1}
    assert res.stats.reject_reasons == {"orphan_user_id": 1}
    validate_silver("posts", res.clean)


def test_followers_reject_negative_duplicates_orphans():
    raw = pd.DataFrame({
        "user_id": ["U1", "U1", "U2", "U9", "U2"], "follower_count": ["100", "100", "-1", "5", "abc"],
        "snapshot_date": ["2026-10-30"] * 5,
    })
    res = clean_followers(raw, D, USERS)
    assert list(res.clean["user_id"]) == ["U1"] and res.clean["follower_count"].dtype == "int64"
    assert res.stats.reject_reasons == {
        "invalid_follower_count": 1, "negative_follower_count": 1, "duplicate_snapshot": 1, "orphan_user_id": 1}
    validate_silver("followers", res.clean)


def test_hashtags_normalise_and_dedupe():
    raw = pd.DataFrame({"post_id": ["P1", "P1", "P1", "P9", "P2"],
                        "hashtag": ["#Kenya", "kenya", " NAIROBI ", "x", "#"]})
    res = clean_hashtags(raw, D, POSTS)
    assert list(res.clean["hashtag"]) == ["kenya", "nairobi"]
    assert res.stats.reject_reasons == {"empty_hashtag": 1, "duplicate_post_hashtag": 1, "orphan_post_id": 1}
    validate_silver("hashtags", res.clean)


def test_quality_gates():
    check_row_reconciliation("x", 10, 8, 2, source_rows=10)
    with pytest.raises(DataQualityError):
        check_row_reconciliation("x", 10, 8, 1)
    with pytest.raises(DataQualityError, match="recorded at ingestion"):
        check_row_reconciliation("x", 10, 8, 2, source_rows=11)
    check_reject_rate("x", 100, 10, 0.10)
    with pytest.raises(DataQualityError, match="reject rate"):
        check_reject_rate("x", 100, 11, 0.10)
    df = pd.DataFrame({"a": [1, None], "b": [1, 2]})
    with pytest.raises(DataQualityError, match="a="):
        check_null_rates("x", df)
    assert check_null_rates("x", df, {"a": 0.5})["a"] == 0.5
