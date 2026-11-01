"""Regenerate the small committed sample under sample_data/ (50 creators, 1 day)."""
import dataclasses
from datetime import date

from config.settings import Settings
from data_generation.run_generation import generate_all

settings = Settings.from_env({
    "R2_ACCESS_KEY_ID": "x", "R2_SECRET_ACCESS_KEY": "x", "R2_ENDPOINT": "https://example.invalid",
    "R2_BUCKET": "x", "N_USERS": "50", "N_POSTS_PER_DAY": "120", "N_ENGAGEMENTS_PER_DAY": "500",
})
generate_all(dataclasses.replace(settings), date(2026, 10, 30), out_dir="sample_data")
