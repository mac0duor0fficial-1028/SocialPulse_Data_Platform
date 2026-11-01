"""Generate one day of engagement events (like / comment / share / view)."""
from __future__ import annotations

from datetime import date, datetime, time

import numpy as np
import pandas as pd

from data_generation.common import (
    ENGAGEMENT_TYPES,
    ENGAGEMENT_WEIGHTS,
    TS_FMT,
    date_int,
    rng_for,
)


def generate_engagements(posts: pd.DataFrame, user_ids: np.ndarray | list[str], run_date: date,
                         n: int, seed: int = 42) -> pd.DataFrame:
    """Return `n` engagement events for `run_date`.

    Post popularity is log-normal (a few viral posts); delays after publishing are exponential
    (most engagement happens within hours) and capped at the end of the day so each daily
    partition is self-contained.
    """
    rng = rng_for(seed, 4, date_int(run_date))
    user_ids = np.asarray(user_ids)

    virality = rng.lognormal(mean=0.0, sigma=1.2, size=len(posts))
    post_idx = rng.choice(len(posts), n, p=virality / virality.sum())
    types = rng.choice(ENGAGEMENT_TYPES, n, p=ENGAGEMENT_WEIGHTS)
    engager = rng.choice(user_ids, n)

    posted = pd.to_datetime(posts["posted_at"]).to_numpy()[post_idx]
    delay_s = rng.exponential(scale=4 * 3600, size=n).astype("int64")
    end_of_day = np.datetime64(datetime.combine(run_date, time(23, 59, 59)), "s")
    ts = np.minimum(posted.astype("datetime64[s]") + delay_s.astype("timedelta64[s]"), end_of_day)

    day = date_int(run_date)
    return pd.DataFrame(
        {
            "engagement_id": [f"ENG{day}{i:07d}" for i in range(1, n + 1)],
            "post_id": posts["post_id"].to_numpy()[post_idx],
            "user_id": engager,
            "engagement_type": types,
            "platform": posts["platform"].to_numpy()[post_idx],
            "timestamp": pd.Series(ts).dt.strftime(TS_FMT).to_numpy(),
        }
    )
