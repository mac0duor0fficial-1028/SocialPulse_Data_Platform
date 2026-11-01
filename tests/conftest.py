import os

import boto3
import pytest
from moto import mock_aws

ENDPOINT = "https://testaccount.r2.cloudflarestorage.com"
os.environ.setdefault("MOTO_S3_CUSTOM_ENDPOINTS", ENDPOINT)
os.environ["AWS_ACCESS_KEY_ID"] = "testing"
os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"

from config.settings import Settings  # noqa: E402


@pytest.fixture
def settings(tmp_path):
    return Settings.from_env({
        "R2_ACCESS_KEY_ID": "testing", "R2_SECRET_ACCESS_KEY": "testing", "R2_ENDPOINT": ENDPOINT,
        "R2_BUCKET": "test-lake", "DATA_DIR": str(tmp_path / "data"), "LOG_DIR": str(tmp_path / "logs"),
        "N_USERS": "60", "N_POSTS_PER_DAY": "200", "N_ENGAGEMENTS_PER_DAY": "2000",
    })


@pytest.fixture
def r2(settings):
    """A moto-backed bucket reachable through the project's own get_client()."""
    from ingestion.r2_client import get_client
    with mock_aws():
        boto3.client("s3", region_name="us-east-1").create_bucket(Bucket=settings.r2_bucket)
        yield get_client(settings)
