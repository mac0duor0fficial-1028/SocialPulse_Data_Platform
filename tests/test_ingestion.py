import json
from datetime import date

import pytest
from botocore.exceptions import ClientError

from config.settings import ConfigError, Settings
from data_generation.run_generation import generate_all
from ingestion.download_data import NoBronzeDataError, daily_engagement_summary, fetch_bronze, list_bronze
from ingestion.upload_data import IngestionError, bronze_key, upload_run

D = date(2026, 10, 30)


def test_bronze_key_layout():
    assert bronze_key("engagements", D) == \
        "bronze/engagements/ingestion_date=2026-10-30/engagements_1030.json"
    assert bronze_key("users", D) == "bronze/users/ingestion_date=2026-10-30/users_1030.csv"


def test_upload_writes_all_keys_with_metadata(settings, r2):
    manifest = generate_all(settings, D)
    results = upload_run(settings, D, client=r2)
    assert {r.entity for r in results} == set(manifest["files"]) and all(r.ok for r in results)
    for entity, info in manifest["files"].items():
        head = r2.head_object(Bucket=settings.r2_bucket, Key=bronze_key(entity, D, info["format"]))
        meta = head["Metadata"]
        assert meta["source"] == "synthetic-faker" and meta["ingestion_ts"]
        assert meta["row_count"] == str(info["rows"]) and meta["sha256"] == info["sha256"]
    # run summary written to logs/ingestion/ both locally and in the lake
    assert (settings.log_dir / "ingestion" / "run_20261030.json").exists()
    r2.head_object(Bucket=settings.r2_bucket, Key="logs/ingestion/ingestion_date=2026-10-30/run_summary.json")


def test_upload_is_idempotent(settings, r2):
    generate_all(settings, D)
    upload_run(settings, D, client=r2)
    upload_run(settings, D, client=r2)
    assert len(list_bronze(r2, settings.r2_bucket, "engagements", D)) == 1


def test_upload_failure_is_logged_and_raised(settings, r2, monkeypatch):
    generate_all(settings, D)
    real = r2.put_object

    def flaky(**kw):
        if "/posts/" in kw["Key"]:
            raise ClientError({"Error": {"Code": "InternalError", "Message": "boom"}}, "PutObject")
        return real(**kw)

    monkeypatch.setattr(r2, "put_object", flaky)
    with pytest.raises(IngestionError, match="posts"):
        upload_run(settings, D, client=r2)
    summary = json.loads((settings.log_dir / "ingestion" / "run_20261030.json").read_text())
    failed = [x for x in summary["results"] if not x["ok"]]
    assert [x["entity"] for x in failed] == ["posts"]
    # other entities were still uploaded (partial progress is not lost)
    assert list_bronze(r2, settings.r2_bucket, "users", D)


def test_upload_without_manifest_fails_fast(settings, r2):
    with pytest.raises(IngestionError, match="manifest"):
        upload_run(settings, D, client=r2)


def test_download_roundtrip_and_summary(settings, r2):
    manifest = generate_all(settings, D)
    upload_run(settings, D, client=r2)
    fetched = fetch_bronze(r2, settings.r2_bucket, "engagements", D)
    assert len(fetched.df) == manifest["files"]["engagements"]["rows"] == fetched.source_rows
    summary = daily_engagement_summary(fetched.df)
    assert summary["total"].sum() == len(fetched.df)
    assert {"like", "comment", "share", "view"} <= set(summary.columns)


def test_fetch_missing_date_raises(settings, r2):
    with pytest.raises(NoBronzeDataError):
        fetch_bronze(r2, settings.r2_bucket, "users", date(2020, 1, 1))


def test_settings_require_r2_credentials():
    with pytest.raises(ConfigError, match="R2_BUCKET"):
        Settings.from_env({"R2_ACCESS_KEY_ID": "a", "R2_SECRET_ACCESS_KEY": "b", "R2_ENDPOINT": "https://x"})
