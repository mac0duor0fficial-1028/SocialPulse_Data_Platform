"""Shared constants and RNG helpers for the synthetic data generators."""
from __future__ import annotations

from datetime import date, datetime

import numpy as np

CITIES = ["Nairobi", "Mombasa", "Kisumu", "Nakuru", "Eldoret", "Thika"]
CITY_WEIGHTS = [0.45, 0.17, 0.10, 0.10, 0.10, 0.08]
NICHES = ["Technology", "Fashion", "Food", "Education", "Entertainment", "Travel", "Business"]

PLATFORMS = ["instagram", "tiktok", "x"]
PLATFORM_WEIGHTS = [0.40, 0.35, 0.25]
CONTENT_TYPES = {
    "instagram": ["image", "reel", "carousel", "story"],
    "tiktok": ["video"],
    "x": ["text", "image", "video", "poll"],
}

ENGAGEMENT_TYPES = ["like", "comment", "share", "view"]
ENGAGEMENT_WEIGHTS = [0.35, 0.09, 0.06, 0.50]

# Share of posts published in each Nairobi-local hour (peaks: morning, lunch, evening).
HOUR_WEIGHTS = np.array(
    [1, 1, 1, 1, 1, 2, 4, 7, 8, 6, 5, 6, 8, 8, 6, 5, 5, 6, 8, 10, 11, 9, 5, 2], dtype=float
)
HOUR_WEIGHTS = HOUR_WEIGHTS / HOUR_WEIGHTS.sum()

GENERAL_HASHTAGS = ["kenya", "nairobi", "254", "kenyatwitter", "madeinkenya", "africa", "trending", "viral"]
NICHE_HASHTAGS = {
    "Technology": ["techke", "siliconsavannah", "ai", "startupke", "coding", "fintech"],
    "Fashion": ["kenyanfashion", "ootd", "style", "africanprint", "thrift"],
    "Food": ["nyamachoma", "ugali", "foodie", "chapati", "kenyancuisine", "streetfood"],
    "Education": ["studytips", "kcse", "university", "scholarship", "learning"],
    "Entertainment": ["gengetone", "comedy", "music", "nollywood", "kenyanmusic"],
    "Travel": ["magicalkenya", "safari", "maasaimara", "diani", "travelke", "nakurulakes"],
    "Business": ["hustle", "smeke", "entrepreneur", "mpesa", "investing", "jobs"],
}

DATE_FMT = "%Y-%m-%d"
TS_FMT = "%Y-%m-%dT%H:%M:%S"


def to_date(value: date | datetime | str) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return datetime.strptime(value, DATE_FMT).date()


def date_int(d: date) -> int:
    return d.year * 10_000 + d.month * 100 + d.day


def rng_for(seed: int, *parts: int) -> np.random.Generator:
    """Independent, reproducible generator per (seed, purpose, date)."""
    return np.random.default_rng([seed, *parts])
