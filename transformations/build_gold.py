"""Silver -> Gold: business-ready aggregates.

    python -m transformations.build_gold --date 2026-10-30

Datasets (each partitioned by ingestion_date, written idempotently):
  post_performance    one row per post published on the run date, with engagement counts
  creator_performance one row per creator: posts, engagement and engagement rate for the day
  hashtag_performance one row per (hashtag, platform) for posts published on the run date
  audience_growth     one row per creator: follower count vs the previous snapshot

Definitions
  total_engagement    = likes + comments + shares   (views are impressions, not engagement)
  engagement_rate_pct = total_engagement / follower_count * 100
  growth_rate_pct     = (current - previous) / previous * 100
"""
from __future__ import annotations

import argparse
from datetime import date

import numpy as np
import pandas as pd
from botocore.client import BaseClient

from config.logging_config import get_logger
from config.settings import ENGAGEMENT_TYPES, GOLD, LOCAL_TZ, SILVER, Settings
from data_generation.common import to_date
from ingestion.r2_client import get_client, put_parquet, read_parquet, read_parquet_prefix
from quality.checks import DataQualityError, check_not_empty
from quality.schemas import validate_gold
from transformations.common import layer_key, write_transform_log

_COUNT_COLS = ["likes", "comments", "shares", "impressions"]
_TYPE_TO_COL = {"like": "likes", "comment": "comments", "share": "shares", "view": "impressions"}


def build_post_performance(posts: pd.DataFrame, engagements: pd.DataFrame, run_date: date) -> pd.DataFrame:
    e = engagements[engagements["post_id"].isin(posts["post_id"])]
    counts = (
        e.groupby(["post_id", "engagement_type"]).size().unstack(fill_value=0)
        .reindex(columns=list(ENGAGEMENT_TYPES), fill_value=0)
        .rename(columns=_TYPE_TO_COL)
    )
    df = posts.drop(columns="ingestion_date").merge(counts, left_on="post_id", right_index=True, how="left")
    df[_COUNT_COLS] = df[_COUNT_COLS].fillna(0).astype("int64")
    df["total_engagement"] = (df["likes"] + df["comments"] + df["shares"]).astype("int64")

    local = df["posted_at"].dt.tz_convert(LOCAL_TZ)
    df["posted_date"] = local.dt.tz_localize(None).dt.normalize().astype("datetime64[ns]")
    df["posted_hour_eat"] = local.dt.hour.astype("int64")
    df["posted_dow"] = (local.dt.dayofweek + 1).astype("int64")  # ISO: Mon=1 .. Sun=7
    df["posted_day_name"] = local.dt.day_name().astype(object)
    df["ingestion_date"] = str(run_date)
    cols = ["post_id", "user_id", "platform", "content_type", "caption", "posted_at", "posted_date",
            "posted_hour_eat", "posted_dow", "posted_day_name", *_COUNT_COLS, "total_engagement",
            "ingestion_date"]
    return df[cols].sort_values("post_id").reset_index(drop=True)


def build_creator_performance(users: pd.DataFrame, post_perf: pd.DataFrame, followers: pd.DataFrame,
                              run_date: date) -> pd.DataFrame:
    agg = post_perf.groupby("user_id").agg(
        posts_count=("post_id", "count"), likes=("likes", "sum"), comments=("comments", "sum"),
        shares=("shares", "sum"), impressions=("impressions", "sum"),
        total_engagement=("total_engagement", "sum"),
    )
    today = followers[followers["snapshot_date"] == pd.Timestamp(run_date)][["user_id", "follower_count"]]
    df = (users[["user_id", "username", "city", "niche"]]
          .merge(agg, left_on="user_id", right_index=True, how="left")
          .merge(today, on="user_id", how="left"))
    num = ["posts_count", *_COUNT_COLS, "total_engagement", "follower_count"]
    df[num] = df[num].fillna(0).astype("int64")
    df["engagement_rate_pct"] = np.where(
        df["follower_count"] > 0, df["total_engagement"] / df["follower_count"].replace(0, np.nan) * 100, np.nan
    )
    df["snapshot_date"] = pd.Timestamp(run_date)
    df["ingestion_date"] = str(run_date)
    cols = ["user_id", "username", "city", "niche", "snapshot_date", "posts_count", "likes", "comments",
            "shares", "impressions", "total_engagement", "follower_count", "engagement_rate_pct",
            "ingestion_date"]
    return df[cols].sort_values("user_id").reset_index(drop=True)


def build_hashtag_performance(hashtags: pd.DataFrame, post_perf: pd.DataFrame, run_date: date) -> pd.DataFrame:
    j = hashtags[["post_id", "hashtag"]].merge(
        post_perf[["post_id", "platform", *_COUNT_COLS, "total_engagement"]], on="post_id", how="inner"
    )
    df = j.groupby(["hashtag", "platform"], as_index=False).agg(
        posts_used=("post_id", "nunique"), likes=("likes", "sum"), comments=("comments", "sum"),
        shares=("shares", "sum"), impressions=("impressions", "sum"),
        total_engagement=("total_engagement", "sum"),
    )
    df["avg_engagement_per_post"] = (df["total_engagement"] / df["posts_used"]).astype("float64")
    df["platform"] = df["platform"].astype(object)
    df["ingestion_date"] = str(run_date)
    return df.sort_values(["hashtag", "platform"]).reset_index(drop=True)


def build_audience_growth(followers: pd.DataFrame, run_date: date) -> pd.DataFrame:
    f = followers[["user_id", "snapshot_date", "follower_count"]].sort_values(["user_id", "snapshot_date"])
    f = f[f["snapshot_date"] <= pd.Timestamp(run_date)].copy()
    f["previous_follower_count"] = f.groupby("user_id")["follower_count"].shift(1).astype("float64")
    cur = f[f["snapshot_date"] == pd.Timestamp(run_date)].copy()
    prev = cur["previous_follower_count"]
    cur["follower_change"] = (cur["follower_count"] - prev).astype("float64")
    cur["growth_rate_pct"] = np.where(prev > 0, cur["follower_change"] / prev * 100, np.nan)
    cur["ingestion_date"] = str(run_date)
    return cur.reset_index(drop=True)


def build_gold(settings: Settings, run_date: date | str, client: BaseClient | None = None) -> dict[str, int]:
    run_date = to_date(run_date)
    client = client or get_client(settings)
    bucket = settings.r2_bucket
    log = get_logger("socialpulse.gold", settings.log_dir, "transformations", str(run_date))

    users = read_parquet(client, bucket, layer_key(SILVER, "users", run_date))
    posts = read_parquet(client, bucket, layer_key(SILVER, "posts", run_date))
    hashtags = read_parquet(client, bucket, layer_key(SILVER, "hashtags", run_date))
    # Engagement and follower history span all partitions: late engagements and growth
    # calculations need more than today's file.
    engagements = read_parquet_prefix(client, bucket, f"{SILVER}/engagements/")
    followers = read_parquet_prefix(client, bucket, f"{SILVER}/followers/")
    check_not_empty("silver.posts", posts)

    post_perf = build_post_performance(posts, engagements, run_date)
    datasets = {
        "post_performance": post_perf,
        "creator_performance": build_creator_performance(users, post_perf, followers, run_date),
        "hashtag_performance": build_hashtag_performance(hashtags, post_perf, run_date),
        "audience_growth": build_audience_growth(followers, run_date),
    }

    # Reconciliation: Gold must agree with Silver (engagement on today's posts).
    e = engagements[engagements["post_id"].isin(posts["post_id"])]
    expected = int((e["engagement_type"] != "view").sum())
    if int(post_perf["total_engagement"].sum()) != expected:
        raise DataQualityError("gold.post_performance total_engagement does not reconcile with silver")
    if int(datasets["creator_performance"]["total_engagement"].sum()) != expected:
        raise DataQualityError("gold.creator_performance total_engagement does not reconcile with silver")

    counts = {}
    for name, df in datasets.items():
        validate_gold(name, df)
        put_parquet(client, bucket, layer_key(GOLD, name, run_date), df, {"rows": str(len(df))})
        counts[name] = len(df)
        log.info("gold %-20s rows=%d", name, len(df))
    write_transform_log(settings, client, "gold", run_date, [{"dataset": k, "rows": v} for k, v in counts.items()])
    return counts


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--date", default=str(date.today()))
    args = ap.parse_args()
    build_gold(Settings.from_env(), args.date)


if __name__ == "__main__":
    main()
