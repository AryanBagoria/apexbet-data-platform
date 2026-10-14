"""Storage abstraction for the data lake (Bronze/Silver/Gold).

One interface, two backends:
  * ``local`` -- writes under a root directory (default ``./lake``). Zero deps.
  * ``s3``    -- writes via boto3 to an S3-compatible endpoint (LocalStack S3
                 or MinIO). Same key layout.

Keys are POSIX-style relative paths, e.g.
``bronze/event_type=bet_placed/event_date=2026-01-01/part-0001.parquet``.
"""

from __future__ import annotations

import io
import os
from dataclasses import dataclass
from typing import Protocol


@dataclass
class ObjectInfo:
    key: str
    size_bytes: int


class ObjectStore(Protocol):
    def put_bytes(
        self, key: str, data: bytes, content_type: str = "application/octet-stream"
    ) -> None: ...

    def get_bytes(self, key: str) -> bytes: ...

    def exists(self, key: str) -> bool: ...

    def list(self, prefix: str) -> list[ObjectInfo]: ...

    def delete(self, key: str) -> None: ...

    def health(self) -> dict[str, object]: ...


class LocalObjectStore:
    """Filesystem-backed object store. Atomic writes via temp+rename."""

    name = "local"

    def __init__(self, root: str) -> None:
        self.root = os.path.abspath(root)
        os.makedirs(self.root, exist_ok=True)

    def _path(self, key: str) -> str:
        safe = key.lstrip("/")
        full = os.path.abspath(os.path.join(self.root, safe))
        if not full.startswith(self.root):
            raise ValueError(f"path traversal detected for key {key!r}")
        return full

    def put_bytes(
        self, key: str, data: bytes, content_type: str = "application/octet-stream"
    ) -> None:
        path = self._path(key)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        tmp = f"{path}.tmp-{os.getpid()}"
        with open(tmp, "wb") as fh:
            fh.write(data)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp, path)

    def get_bytes(self, key: str) -> bytes:
        with open(self._path(key), "rb") as fh:
            return fh.read()

    def exists(self, key: str) -> bool:
        return os.path.exists(self._path(key))

    def list(self, prefix: str) -> list[ObjectInfo]:
        base = self._path(prefix)
        out: list[ObjectInfo] = []
        if os.path.isfile(base):
            return [ObjectInfo(prefix, os.path.getsize(base))]
        if os.path.isdir(base):
            walk_root = base
            key_prefix = prefix.replace(os.sep, "/").rstrip("/")
        else:
            # Prefix is not a literal directory (e.g. "bronze/event_type=").
            # Walk the closest existing ancestor and filter by string prefix.
            walk_root = os.path.dirname(base)
            while walk_root and not os.path.isdir(walk_root):
                walk_root = os.path.dirname(walk_root)
            if not walk_root or not os.path.isdir(walk_root):
                return []
            key_prefix = prefix.replace(os.sep, "/").lstrip("/")
        for dirpath, _dirs, files in os.walk(walk_root):
            for f in files:
                if f.endswith(".tmp") or ".tmp-" in f:
                    continue
                full = os.path.join(dirpath, f)
                key = os.path.relpath(full, self.root).replace(os.sep, "/")
                if not key.startswith(key_prefix):
                    continue
                out.append(ObjectInfo(key, os.path.getsize(full)))
        return sorted(out, key=lambda o: o.key)

    def delete(self, key: str) -> None:
        p = self._path(key)
        if os.path.exists(p):
            os.remove(p)

    def health(self) -> dict[str, object]:
        try:
            probe = ".health/probe"
            self.put_bytes(probe, b"ok")
            ok = self.get_bytes(probe) == b"ok"
            self.delete(probe)
            return {"backend": self.name, "status": "OK" if ok else "DEGRADED", "root": self.root}
        except Exception as exc:  # pragma: no cover
            return {"backend": self.name, "status": "DOWN", "root": self.root, "error": str(exc)}


class S3ObjectStore:
    """boto3-backed S3 / S3-compatible store (LocalStack or MinIO)."""

    name = "s3"

    def __init__(
        self,
        bucket: str,
        prefix: str = "",
        *,
        endpoint_url: str | None = None,
        region: str = "us-east-1",
        access_key: str = "test",
        secret_key: str = "test",
        auto_create: bool = True,
    ) -> None:
        import boto3

        self.bucket = bucket
        self.prefix = prefix.strip("/")
        self._client = boto3.client(
            "s3",
            region_name=region,
            endpoint_url=endpoint_url,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
        )
        if auto_create:
            self._ensure_bucket()

    def _ensure_bucket(self) -> None:
        from botocore.exceptions import ClientError

        try:
            self._client.head_bucket(Bucket=self.bucket)
        except ClientError:
            try:
                self._client.create_bucket(Bucket=self.bucket)
            except ClientError as exc:  # pragma: no cover
                raise RuntimeError(f"could not create bucket {self.bucket}: {exc}") from exc

    def _key(self, key: str) -> str:
        k = key.lstrip("/")
        return f"{self.prefix}/{k}" if self.prefix else k

    def put_bytes(
        self, key: str, data: bytes, content_type: str = "application/octet-stream"
    ) -> None:
        self._client.put_object(
            Bucket=self.bucket, Key=self._key(key), Body=data, ContentType=content_type
        )

    def get_bytes(self, key: str) -> bytes:
        resp = self._client.get_object(Bucket=self.bucket, Key=self._key(key))
        data: bytes = resp["Body"].read()
        return data

    def exists(self, key: str) -> bool:
        from botocore.exceptions import ClientError

        try:
            self._client.head_object(Bucket=self.bucket, Key=self._key(key))
            return True
        except ClientError:
            return False

    def list(self, prefix: str) -> list[ObjectInfo]:
        out: list[ObjectInfo] = []
        token: str | None = None
        full_prefix = self._key(prefix)
        while True:
            kwargs: dict[str, object] = {"Bucket": self.bucket, "Prefix": full_prefix}
            if token:
                kwargs["ContinuationToken"] = token
            resp = self._client.list_objects_v2(**kwargs)
            for obj in resp.get("Contents", []):
                rel = obj["Key"]
                if self.prefix and rel.startswith(self.prefix + "/"):
                    rel = rel[len(self.prefix) + 1 :]
                out.append(ObjectInfo(rel, obj["Size"]))
            if resp.get("IsTruncated"):
                token = resp.get("NextContinuationToken")
            else:
                break
        return sorted(out, key=lambda o: o.key)

    def delete(self, key: str) -> None:
        self._client.delete_object(Bucket=self.bucket, Key=self._key(key))

    def health(self) -> dict[str, object]:
        try:
            probe = self._key(".health/probe")
            self._client.put_object(Bucket=self.bucket, Key=probe, Body=b"ok")
            ok = self._client.get_object(Bucket=self.bucket, Key=probe)["Body"].read() == b"ok"
            self._client.delete_object(Bucket=self.bucket, Key=probe)
            return {
                "backend": self.name,
                "status": "OK" if ok else "DEGRADED",
                "bucket": self.bucket,
                "prefix": self.prefix,
            }
        except Exception as exc:  # pragma: no cover
            return {
                "backend": self.name,
                "status": "DOWN",
                "bucket": self.bucket,
                "error": str(exc),
            }


def build_store() -> ObjectStore:
    """Build the store from settings."""
    from apexbet.config import get_settings

    s = get_settings()
    if s.storage_backend == "s3":
        return S3ObjectStore(
            bucket=s.s3_bucket,
            prefix=s.s3_prefix,
            endpoint_url=None if s.use_real_aws else s.aws_endpoint_url,
            region=s.aws_region,
            access_key=s.aws_access_key_id,
            secret_key=s.aws_secret_access_key,
        )
    return LocalObjectStore(s.lake_root)


def parquet_bytes(rows: list[dict]) -> bytes:
    """Encode rows as a single Parquet file in memory using pyarrow."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    if not rows:
        return b""
    table = pa.Table.from_pylist(rows)
    buf = io.BytesIO()
    pq.write_table(table, buf, compression="snappy")
    return buf.getvalue()


def read_parquet_bytes(data: bytes) -> list[dict]:
    import pyarrow.parquet as pq

    table = pq.read_table(io.BytesIO(data))
    rows: list[dict] = table.to_pylist()
    return rows
