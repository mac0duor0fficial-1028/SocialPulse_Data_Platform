"""Generate the (stable) universe of fictional Kenyan creators."""
from __future__ import annotations

from datetime import date

import pandas as pd
from faker import Faker

from data_generation.common import CITIES, CITY_WEIGHTS, NICHES, rng_for

FIRST_NAMES = [
    "wanjiku", "otieno", "achieng", "kamau", "njeri", "mwangi", "akinyi", "kiprop", "chebet",
    "mutiso", "wairimu", "odhiambo", "naliaka", "barasa", "zawadi", "juma", "amina", "baraka",
    "wambui", "ouma", "faith", "brian", "mercy", "kevin", "lilian", "dennis", "esther", "ian",
]
LAST_NAMES = [
    "kariuki", "omondi", "mutua", "wekesa", "korir", "kimani", "ndungu", "owino", "cheruiyot",
    "mwende", "gathoni", "okoth", "kiptoo", "maina", "atieno", "kilonzo", "nyambura", "rotich",
]


def generate_users(n: int, seed: int = 42) -> pd.DataFrame:
    """Return `n` users. Output depends only on (n, seed) so every daily run sees the same creators."""
    rng = rng_for(seed, 1)
    Faker.seed(seed)
    fake = Faker()
    fake.seed_instance(seed)

    first = rng.choice(FIRST_NAMES, n)
    last = rng.choice(LAST_NAMES, n)
    cities = rng.choice(CITIES, n, p=CITY_WEIGHTS)
    niches = rng.choice(NICHES, n)
    joined = [
        fake.date_between(start_date=date(2023, 1, 1), end_date=date(2025, 12, 31)).isoformat()
        for _ in range(n)
    ]
    width = max(4, len(str(n)))
    return pd.DataFrame(
        {
            "user_id": [f"USER{i:0{width}d}" for i in range(1, n + 1)],
            "username": [f"{f}_{ln}{i}" for i, (f, ln) in enumerate(zip(first, last, strict=True), 1)],
            "city": cities,
            "niche": niches,
            "joined_date": joined,
        }
    )
