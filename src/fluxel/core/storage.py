# Fluxel Guardrails (Permanent):
# - DO NOT optimize for ML throughput in canonical storage (`blobs/`): no sharding, tarballs, or parquet in the blob layer.
# - DO NOT read blob data for metadata-only operations (diff, list, log, status).
# - DO NOT introduce a server, daemon, or central database; Fluxel is 100% client-side/serverless.
# - DO NOT use SHA-1/SHA-256; use Blake3 for all content hashing.
# - PREFER JSONL manifests to preserve streaming and O(1) memory usage.

from __future__ import annotations

from pathlib import Path
from typing import Iterator, Protocol

import boto3
from botocore.exceptions import ClientError


class OptimisticLockError(RuntimeError):
    pass


class StorageBackend(Protocol):
    def read_bytes(self, relative_path: str) -> bytes: ...

    def write_bytes(
        self,
        relative_path: str,
        data: bytes,
        *,
        if_none_match: bool = False,
    ) -> None: ...

    def exists(self, relative_path: str) -> bool: ...

    def ensure_dir(self, relative_path: str) -> None: ...

    def iter_keys(self, prefix: str = "") -> Iterator[str]: ...

    def etag(self, relative_path: str) -> str | None: ...


class LocalStorageBackend:
    def __init__(self, root: str | Path) -> None:
        self.root = Path(root).resolve()

    def _full_path(self, relative_path: str) -> Path:
        return self.root / relative_path

    def read_bytes(self, relative_path: str) -> bytes:
        return self._full_path(relative_path).read_bytes()

    def write_bytes(
        self,
        relative_path: str,
        data: bytes,
        *,
        if_none_match: bool = False,
    ) -> None:
        file_path = self._full_path(relative_path)
        file_path.parent.mkdir(parents=True, exist_ok=True)
        if if_none_match and file_path.exists():
            raise OptimisticLockError(f"Path already exists: {relative_path}")
        file_path.write_bytes(data)

    def exists(self, relative_path: str) -> bool:
        return self._full_path(relative_path).exists()

    def ensure_dir(self, relative_path: str) -> None:
        self._full_path(relative_path).mkdir(parents=True, exist_ok=True)

    def iter_keys(self, prefix: str = "") -> Iterator[str]:
        base = self._full_path(prefix)
        if not base.exists():
            return
        for file_path in base.rglob("*"):
            if file_path.is_file():
                yield str(file_path.relative_to(self.root))

    def etag(self, relative_path: str) -> str | None:
        if not self.exists(relative_path):
            return None
        stat = self._full_path(relative_path).stat()
        return f"{stat.st_mtime_ns}-{stat.st_size}"


class S3StorageBackend:
    def __init__(
        self,
        bucket: str,
        prefix: str = "",
        *,
        client: object | None = None,
    ) -> None:
        self.bucket = bucket
        self.prefix = prefix.strip("/")
        self.client = client or boto3.client("s3")

    def _key(self, relative_path: str) -> str:
        if self.prefix:
            return f"{self.prefix}/{relative_path.lstrip('/')}"
        return relative_path.lstrip("/")

    def read_bytes(self, relative_path: str) -> bytes:
        response = self.client.get_object(Bucket=self.bucket, Key=self._key(relative_path))
        return response["Body"].read()

    def write_bytes(
        self,
        relative_path: str,
        data: bytes,
        *,
        if_none_match: bool = False,
    ) -> None:
        kwargs = {
            "Bucket": self.bucket,
            "Key": self._key(relative_path),
            "Body": data,
        }
        if if_none_match:
            kwargs["IfNoneMatch"] = "*"
        try:
            self.client.put_object(**kwargs)
        except ClientError as error:
            code = error.response.get("Error", {}).get("Code", "")
            if if_none_match and code in {"PreconditionFailed", "412"}:
                raise OptimisticLockError(f"Path already exists: {relative_path}") from error
            raise

    def exists(self, relative_path: str) -> bool:
        try:
            self.client.head_object(Bucket=self.bucket, Key=self._key(relative_path))
            return True
        except ClientError as error:
            code = error.response.get("Error", {}).get("Code", "")
            if code in {"404", "NoSuchKey", "NotFound"}:
                return False
            raise

    def ensure_dir(self, relative_path: str) -> None:
        return None

    def iter_keys(self, prefix: str = "") -> Iterator[str]:
        paginator = self.client.get_paginator("list_objects_v2")
        key_prefix = self._key(prefix)
        for page in paginator.paginate(Bucket=self.bucket, Prefix=key_prefix):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                if self.prefix and key.startswith(f"{self.prefix}/"):
                    key = key[len(self.prefix) + 1 :]
                yield key

    def etag(self, relative_path: str) -> str | None:
        try:
            response = self.client.head_object(Bucket=self.bucket, Key=self._key(relative_path))
        except ClientError as error:
            code = error.response.get("Error", {}).get("Code", "")
            if code in {"404", "NoSuchKey", "NotFound"}:
                return None
            raise
        return response.get("ETag", "").strip('"') or None
