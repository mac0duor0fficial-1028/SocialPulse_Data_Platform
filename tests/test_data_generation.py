from datetime import date

import pandas as pd

from data_generation.generate_engagements import generate_engagements
from data_generation.generate_followers import generate_followers
from data_generation.generate_posts import generate_posts
from data_generation.generate_users import generate_users
from data_generation.run_generation import generate_all

D = date(2026, 10, 30)


def test_users_are_deterministic_and_valid():
    a, b = generate_users(100, 42), generate_users(100, 42)
    pd.testing.assert_frame_equal(a, b)
    assert a["user_id"].is_unique and a["username"].is_unique
    assert set(a["city"]) <= {"Nairobi", "Mombasa", "Kisumu", "Nakuru", "Eldoret", "Thika"}
    assert not generate_users(100, 7).equals(a)


def test_engagements_reference_posts_and_are_after_publication():
    users = generate_users(50)
    posts, tags = generate_posts(users, D, 100)
    eng = generate_engagements(posts, users["user_id"], D, 1000)
    assert len(eng) == 1000 and eng["engagement_id"].is_unique
    assert eng["post_id"].isin(posts["post_id"]).all()
    merged = eng.merge(posts[["post_id", "posted_at", "platform"]], on="post_id", suffixes=("", "_p"))
    assert (pd.to_datetime(merged["timestamp"]) >= pd.to_datetime(merged["posted_at"])).all()
    assert (merged["platform"] == merged["platform_p"]).all()
    assert set(eng["engagement_type"]) <= {"like", "comment", "share", "view"}
    assert tags["post_id"].isin(posts["post_id"]).all()


def test_followers_reproducible_per_date():
    users = generate_users(20)
    pd.testing.assert_frame_equal(generate_followers(users, D), generate_followers(users, D))
    assert (generate_followers(users, D)["follower_count"] >= 0).all()


def test_generate_all_is_byte_identical_on_rerun(settings, tmp_path):
    m1 = generate_all(settings, D, tmp_path / "a")
    m2 = generate_all(settings, D, tmp_path / "b")
    for entity in m1["files"]:
        assert m1["files"][entity]["sha256"] == m2["files"][entity]["sha256"]


def test_dirty_data_is_injected_only_when_enabled(settings, tmp_path):
    import dataclasses
    clean = dataclasses.replace(settings, inject_dirty_data=False)
    dirty = generate_all(settings, D, tmp_path / "d")
    ok = generate_all(clean, D, tmp_path / "c")
    assert ok["files"]["engagements"]["rows"] == settings.n_engagements_per_day
    assert dirty["files"]["engagements"]["rows"] > settings.n_engagements_per_day
