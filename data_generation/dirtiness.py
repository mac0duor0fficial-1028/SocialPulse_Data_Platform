"""Inject realistic data-quality problems into Bronze data.

Real sources are messy. Injecting known defects (at a configurable rate) gives the Silver
layer something genuine to clean and lets the tests/README prove the pipeline handles them.
Every function is pure and deterministic given the RNG.
"""
from __future__ import annotations

import numpy as np
import pandas as pd


def _pick(rng: np.random.Generator, n: int, rate: float) -> np.ndarray:
    k = int(round(n * rate))
    return rng.choice(n, size=min(k, n), replace=False) if k > 0 else np.array([], dtype=int)


def add_duplicates(df: pd.DataFrame, rng: np.random.Generator, rate: float) -> pd.DataFrame:
    dupes = df.iloc[_pick(rng, len(df), rate)]
    out = pd.concat([df, dupes], ignore_index=True)
    return out.iloc[rng.permutation(len(out))].reset_index(drop=True)


def set_null(df: pd.DataFrame, col: str, rng: np.random.Generator, rate: float) -> pd.DataFrame:
    df = df.copy()
    idx = _pick(rng, len(df), rate)
    df.loc[df.index[idx], col] = None
    return df


def messy_platform(df: pd.DataFrame, rng: np.random.Generator, rate: float, col: str = "platform"):
    """Upper/title-case, padded whitespace and the legacy name 'twitter' for 'x'."""
    df = df.copy()
    idx = _pick(rng, len(df), rate)
    variants = []
    for v in df.loc[df.index[idx], col]:
        choice = rng.integers(0, 4)
        if choice == 0:
            variants.append(v.upper())
        elif choice == 1:
            variants.append(v.title())
        elif choice == 2:
            variants.append(f" {v} ")
        else:
            variants.append("Twitter" if v == "x" else v.capitalize())
    df.loc[df.index[idx], col] = variants
    return df


def messy_timestamps(df: pd.DataFrame, col: str, rng: np.random.Generator, rate: float,
                     tz_offset_hours: int = 3) -> pd.DataFrame:
    """Re-express a share of Nairobi-local timestamps in other (still correct) formats:
    space separator, explicit '+03:00' offset, or true UTC with a 'Z' suffix."""
    df = df.copy()
    idx = _pick(rng, len(df), rate)
    out = []
    for v in df.loc[df.index[idx], col]:
        kind = rng.integers(0, 3)
        if kind == 0:
            out.append(v.replace("T", " "))
        elif kind == 1:
            out.append(f"{v}+0{tz_offset_hours}:00")
        else:
            utc = pd.Timestamp(v) - pd.Timedelta(hours=tz_offset_hours)
            out.append(utc.strftime("%Y-%m-%dT%H:%M:%SZ"))
    df.loc[df.index[idx], col] = out
    return df


def orphan_reference(df: pd.DataFrame, col: str, prefix: str, rng: np.random.Generator, rate: float):
    """Point a share of rows at a parent key that does not exist."""
    df = df.copy()
    idx = _pick(rng, len(df), rate)
    df.loc[df.index[idx], col] = [f"{prefix}ORPHAN{i:05d}" for i in range(len(idx))]
    return df


def dirty_users(df: pd.DataFrame, rng: np.random.Generator, rate: float) -> pd.DataFrame:
    df = df.copy()
    idx = _pick(rng, len(df), rate * 3)
    df.loc[df.index[idx], "city"] = [
        rng.choice([c.lower(), c.upper(), f"  {c} "]) for c in df.loc[df.index[idx], "city"]
    ]
    # NB: nulls are deliberately NOT injected into dimension attributes - day-to-day flapping to
    # 'Unknown' would create spurious SCD2 versions. Null handling is covered by unit tests.
    return add_duplicates(df, rng, rate / 3)


def dirty_posts(df: pd.DataFrame, rng: np.random.Generator, rate: float) -> pd.DataFrame:
    df = messy_platform(df, rng, rate * 3)
    df = messy_timestamps(df, "posted_at", rng, rate * 2)
    df = set_null(df, "caption", rng, rate)
    df = orphan_reference(df, "user_id", "USER", rng, rate / 6)
    return add_duplicates(df, rng, rate)


def dirty_engagements(df: pd.DataFrame, rng: np.random.Generator, rate: float) -> pd.DataFrame:
    df = messy_platform(df, rng, rate * 3)
    df = messy_timestamps(df, "timestamp", rng, rate * 2)
    df = set_null(df, "user_id", rng, rate / 3)
    df = set_null(df, "engagement_type", rng, rate / 6)
    df = orphan_reference(df, "post_id", "POST", rng, rate / 4)
    return add_duplicates(df, rng, rate)


def dirty_followers(df: pd.DataFrame, rng: np.random.Generator, rate: float) -> pd.DataFrame:
    df = df.copy()
    idx = _pick(rng, len(df), rate / 3)
    df.loc[df.index[idx], "follower_count"] = -1  # impossible value
    df = orphan_reference(df, "user_id", "USER", rng, rate / 3)
    return add_duplicates(df, rng, rate / 3)


def dirty_hashtags(df: pd.DataFrame, rng: np.random.Generator, rate: float) -> pd.DataFrame:
    df = df.copy()
    idx = _pick(rng, len(df), rate * 3)
    df.loc[df.index[idx], "hashtag"] = [
        rng.choice([f"#{h}", h.upper(), f" #{h.title()} "]) for h in df.loc[df.index[idx], "hashtag"]
    ]
    df = orphan_reference(df, "post_id", "POST", rng, rate / 6)
    return add_duplicates(df, rng, rate)
