"""Generate the daily follower snapshot for every creator."""
from __future__ import annotations

from datetime import date

import numpy as np
import pandas as pd

from data_generation.common import DATE_FMT, date_int, rng_for

_EPOCH = date(2026, 1, 1)


def generate_followers(users: pd.DataFrame, run_date: date, seed: int = 42) -> pd.DataFrame:
    """Follower counts follow a per-creator compound growth curve plus daily noise.

    Because the base count and growth rate are derived from the seed, any date can be
    generated independently (backfills and re-runs are reproducible)."""
    n = len(users)
    rng_static = rng_for(seed, 5)
    base = np.clip(rng_static.lognormal(np.log(3000), 1.3, n), 200, 2_000_000)
    growth = rng_static.uniform(-0.0005, 0.004, n)  # daily growth rate, a few creators shrink

    days = (run_date - _EPOCH).days
    noise = rng_for(seed, 6, date_int(run_date)).normal(0, 0.002, n)
    counts = np.maximum(0, np.rint(base * (1 + growth) ** days * (1 + noise))).astype("int64")
    return pd.DataFrame(
        {
            "user_id": users["user_id"].to_numpy(),
            "follower_count": counts,
            "snapshot_date": run_date.strftime(DATE_FMT),
        }
    )
