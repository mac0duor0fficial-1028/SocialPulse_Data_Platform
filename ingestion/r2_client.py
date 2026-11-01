"""Cloudflare R2 access through the S3-compatible API (boto3)."""
from __future__ import annotations

import io
from collections.abc import Iterator

import boto3
import pandas as pd
from botocore.client import BaseClient
from botocore.config import Config

from config.settings import Settings


def get_client(settings: Settings) -> BaseClient:
    """Create an S3 client pointed at R2.

    * region 'auto' + SigV4 + path addressing are what R2 expects.
    * Checksums are only sent when required: recent boto3 versions add CRC checksums by
      default, which R2 rejects for some operations.
    * Standard retry mode gives exponential back-off on throttling / 5xx.
    """
    return boto3.client(
        "s3",
        endpoint_url=settings.r2_endpoint,
        aws_access_key_id=settings.r2_access_key_id,
        aws_secret_access_key=settings.r2_secret_access_key,
        region_name="auto",
        config=Config(
            signature_version="s3v4",
            retries={"max_attempts": 5, "mode": "standard"},
            s3={"addressing_style": "path"},
            request_checksum_calculation="when_required",
            response_checksum_validation="when_required",
        ),
    )


def list_keys(client: BaseClient, bucket: str, prefix: str) -> list[str]:
    """All object keys under `prefix` (handles >1000 objects via pagination)."""
    keys: list[str] = []
    for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
        keys.extend(obj["Key"] for obj in page.get("Contents", []))
    return sorted(keys)


def get_bytes(client: BaseClient, bucket: str, key: str) -> bytes:
    return client.get_object(Bucket=bucket, Key=key)["Body"].read()


def put_bytes(client: BaseClient, bucket: str, key: str, body: bytes,
              content_type: str = "application/octet-stream", metadata: dict[str, str] | None = None) -> None:
    client.put_object(Bucket=bucket, Key=key, Body=body, ContentType=content_type,
                      Metadata=metadata or {})


def delete_keys(client: BaseClient, bucket: str, keys: list[str]) -> None:
    for i in range(0, len(keys), 1000):
        chunk = keys[i:i + 1000]
        client.delete_objects(Bucket=bucket, Delete={"Objects": [{"Key": k} for k in chunk]})


def put_parquet(client: BaseClient, bucket: str, key: str, df: pd.DataFrame,
                metadata: dict[str, str] | None = None) -> None:
    buf = io.BytesIO()
    df.to_parquet(buf, engine="pyarrow", index=False, compression="snappy")
    put_bytes(client, bucket, key, buf.getvalue(), "application/vnd.apache.parquet", metadata)


def read_parquet(client: BaseClient, bucket: str, key: str, columns: list[str] | None = None) -> pd.DataFrame:
    return pd.read_parquet(io.BytesIO(get_bytes(client, bucket, key)), columns=columns)


def read_parquet_prefix(client: BaseClient, bucket: str, prefix: str,
                        columns: list[str] | None = None) -> pd.DataFrame:
    """Concatenate every .parquet object under `prefix` (all date partitions)."""
    keys = [k for k in list_keys(client, bucket, prefix) if k.endswith(".parquet")]
    if not keys:
        return pd.DataFrame(columns=columns or [])
    return pd.concat((read_parquet(client, bucket, k, columns) for k in keys), ignore_index=True)


def iter_objects(client: BaseClient, bucket: str, prefix: str) -> Iterator[dict]:
    for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
        yield from page.get("Contents", [])
