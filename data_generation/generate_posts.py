"""Generate one day of posts and their hashtags."""
from __future__ import annotations

from datetime import date, datetime, time

import numpy as np
import pandas as pd
from faker import Faker

from data_generation.common import (
    CONTENT_TYPES,
    GENERAL_HASHTAGS,
    HOUR_WEIGHTS,
    NICHE_HASHTAGS,
    PLATFORM_WEIGHTS,
    PLATFORMS,
    TS_FMT,
    date_int,
    rng_for,
)


def _creator_activity(n_users: int, seed: int) -> np.ndarray:
    """Heavy-tailed, *stable* posting propensity per creator (a few prolific creators)."""
    w = rng_for(seed, 2).pareto(1.5, n_users) + 1.0
    return w / w.sum()


def generate_posts(users: pd.DataFrame, run_date: date, n_posts: int, seed: int = 42):
    """Return (posts_df, hashtags_df) for `run_date`. Deterministic for (users, run_date, seed)."""
    rng = rng_for(seed, 3, date_int(run_date))
    fake = Faker()
    fake.seed_instance(seed + date_int(run_date))

    user_idx = rng.choice(len(users), n_posts, p=_creator_activity(len(users), seed))
    platforms = rng.choice(PLATFORMS, n_posts, p=PLATFORM_WEIGHTS)
    content_types = [rng.choice(CONTENT_TYPES[p]) for p in platforms]

    hours = rng.choice(24, n_posts, p=HOUR_WEIGHTS)
    minutes = rng.integers(0, 60, n_posts)
    seconds = rng.integers(0, 60, n_posts)
    posted_at = [
        datetime.combine(run_date, time(int(h), int(m), int(s))).strftime(TS_FMT)
        for h, m, s in zip(hours, minutes, seconds, strict=True)
    ]

    day = date_int(run_date)
    post_ids = [f"POST{day}{i:05d}" for i in range(1, n_posts + 1)]
    user_ids = users["user_id"].to_numpy()[user_idx]
    niches = users["niche"].to_numpy()[user_idx]

    posts = pd.DataFrame(
        {
            "post_id": post_ids,
            "user_id": user_ids,
            "platform": platforms,
            "content_type": content_types,
            "caption": [fake.sentence(nb_words=int(rng.integers(5, 14))) for _ in range(n_posts)],
            "posted_at": posted_at,
        }
    )

    rows: list[tuple[str, str]] = []
    for post_id, niche in zip(post_ids, niches, strict=True):
        pool = NICHE_HASHTAGS[niche] + GENERAL_HASHTAGS
        k = int(rng.integers(1, 6))
        for tag in rng.choice(pool, k, replace=False):
            rows.append((post_id, str(tag)))
    hashtags = pd.DataFrame(rows, columns=["post_id", "hashtag"])
    return posts, hashtags
