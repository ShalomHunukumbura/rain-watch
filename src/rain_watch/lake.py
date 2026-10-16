"""The data lake: files in an S3 bucket, or in a local folder for tests.

Everything is addressed by a key such as "clean/rainfall/date=2026-10-15/data.parquet".
The same code works against SeaweedFS on a laptop, or MinIO, Cloudflare R2 or AWS S3,
because they all speak the S3 protocol.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

import duckdb


@dataclass
class S3Settings:
    bucket: str
    endpoint: str  # host:port, such as "seaweedfs:8333"
    key_id: str
    secret: str
    region: str = "us-east-1"
    use_ssl: bool = False

    @classmethod
    def from_env(cls) -> S3Settings:
        return cls(
            bucket=os.environ["RAIN_WATCH_BUCKET"],
            endpoint=os.environ["RAIN_WATCH_S3_ENDPOINT"],
            key_id=os.environ["RAIN_WATCH_S3_KEY_ID"],
            secret=os.environ["RAIN_WATCH_S3_SECRET"],
            region=os.environ.get("RAIN_WATCH_S3_REGION", "us-east-1"),
            use_ssl=os.environ.get("RAIN_WATCH_S3_SSL", "false") == "true",
        )


class Lake:
    def put(self, key: str, data: bytes) -> None:
        raise NotImplementedError

    def get(self, key: str) -> bytes:
        raise NotImplementedError

    def exists(self, key: str) -> bool:
        raise NotImplementedError

    def keys(self, prefix: str) -> list[str]:
        raise NotImplementedError

    def uri(self, key: str) -> str:
        """How DuckDB should refer to a key (globs such as * allowed)."""
        raise NotImplementedError

    def connect(self) -> duckdb.DuckDBPyConnection:
        """A DuckDB connection that can read this lake."""
        return duckdb.connect()


class LocalLake(Lake):
    def __init__(self, root: Path) -> None:
        self.root = root

    def _path(self, key: str) -> Path:
        return self.root / key

    def put(self, key: str, data: bytes) -> None:
        path = self._path(key)
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_name(path.name + ".tmp")
        tmp.write_bytes(data)
        os.replace(tmp, path)

    def get(self, key: str) -> bytes:
        return self._path(key).read_bytes()

    def exists(self, key: str) -> bool:
        return self._path(key).exists()

    def keys(self, prefix: str) -> list[str]:
        base = self._path(prefix)
        if not base.exists():
            return []
        return sorted(str(p.relative_to(self.root)) for p in base.rglob("*") if p.is_file())

    def uri(self, key: str) -> str:
        return str(self._path(key))


class S3Lake(Lake):
    def __init__(self, settings: S3Settings) -> None:
        import boto3

        self.settings = settings
        scheme = "https" if settings.use_ssl else "http"
        self.client = boto3.client(
            "s3",
            endpoint_url=f"{scheme}://{settings.endpoint}",
            aws_access_key_id=settings.key_id,
            aws_secret_access_key=settings.secret,
            region_name=settings.region,
        )

    def ensure_bucket(self) -> None:
        names = {b["Name"] for b in self.client.list_buckets().get("Buckets", [])}
        if self.settings.bucket not in names:
            self.client.create_bucket(Bucket=self.settings.bucket)

    def put(self, key: str, data: bytes) -> None:
        # A single PUT is atomic: readers see the old object or the new one, never half.
        self.client.put_object(Bucket=self.settings.bucket, Key=key, Body=data)

    def get(self, key: str) -> bytes:
        return self.client.get_object(Bucket=self.settings.bucket, Key=key)["Body"].read()

    def exists(self, key: str) -> bool:
        response = self.client.list_objects_v2(Bucket=self.settings.bucket, Prefix=key, MaxKeys=1)
        return any(obj["Key"] == key for obj in response.get("Contents", []))

    def keys(self, prefix: str) -> list[str]:
        keys: list[str] = []
        for page in self.client.get_paginator("list_objects_v2").paginate(
            Bucket=self.settings.bucket, Prefix=prefix
        ):
            keys += [obj["Key"] for obj in page.get("Contents", [])]
        return sorted(keys)

    def uri(self, key: str) -> str:
        return f"s3://{self.settings.bucket}/{key}"

    def connect(self) -> duckdb.DuckDBPyConnection:
        s = self.settings
        conn = duckdb.connect()
        conn.execute("LOAD httpfs")
        conn.execute(
            "CREATE SECRET lake (TYPE s3, KEY_ID ?, SECRET ?, ENDPOINT ?, REGION ?, "
            "URL_STYLE 'path', USE_SSL ?)",
            [s.key_id, s.secret, s.endpoint, s.region, s.use_ssl],
        )
        return conn


def lake_from_env() -> Lake:
    """S3 if RAIN_WATCH_BUCKET is set, otherwise a local folder."""
    if os.environ.get("RAIN_WATCH_BUCKET"):
        lake = S3Lake(S3Settings.from_env())
        lake.ensure_bucket()
        return lake
    return LocalLake(Path(os.environ.get("RAIN_WATCH_LAKE_DIR", "lake")))
