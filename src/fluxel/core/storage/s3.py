from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, BinaryIO, Iterator
from urllib.parse import unquote, urlparse

import boto3
import fsspec
from botocore.exceptions import ClientError

from ..domain import OptimisticLockError
from .base import S3ObjectMetadata, SourceObjectMetadata


def _s3_is_404(error: ClientError) -> bool:
    return error.response.get("Error", {}).get("Code", "") in {"404", "NoSuchKey", "NotFound"}


def _s3_is_precondition_failed(error: ClientError) -> bool:
    return error.response.get("Error", {}).get("Code", "") in {"PreconditionFailed", "412"}


def parse_s3_uri(uri: str) -> tuple[str, str]:
    parsed = urlparse(uri)
    if parsed.scheme != "s3" or not parsed.netloc:
        raise ValueError(f"Invalid S3 URI: {uri}")
    return parsed.netloc, parsed.path.lstrip("/")


def _mtime_ns(value: object) -> int:
    if isinstance(value, datetime):
        timestamp = value
        if timestamp.tzinfo is None:
            timestamp = timestamp.replace(tzinfo=timezone.utc)
        return int(timestamp.timestamp() * 1_000_000_000)
    return 0


def iter_s3_objects(
    source_uri: str,
    *,
    client: Any | None = None,
) -> Iterator[S3ObjectMetadata]:
    bucket, prefix = parse_s3_uri(source_uri)
    s3_client = client or boto3.client("s3")
    paginator = s3_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = str(obj["Key"])
            yield S3ObjectMetadata(
                bucket=bucket,
                key=key,
                size=int(obj.get("Size", 0)),
                mtime_ns=_mtime_ns(obj.get("LastModified")),
            )


def describe_source_uri(
    source_uri: str,
    *,
    client: Any | None = None,
) -> SourceObjectMetadata:
    if source_uri.startswith("s3://"):
        bucket, key = parse_s3_uri(source_uri)
        if not key or key.endswith("/"):
            raise ValueError(f"S3 source must be an object, not a prefix: {source_uri}")
        s3_client = client or boto3.client("s3")
        response = s3_client.head_object(Bucket=bucket, Key=key)
        return SourceObjectMetadata(
            source_uri=source_uri,
            size=int(response.get("ContentLength", 0)),
            mtime_ns=_mtime_ns(response.get("LastModified")),
        )

    parsed = urlparse(source_uri)
    if parsed.scheme == "file":
        path = Path(unquote(parsed.path)).resolve()
    else:
        path = Path(source_uri).expanduser().resolve()
    if not path.exists() or not path.is_file():
        raise FileNotFoundError(f"Cannot stage missing file: {path}")
    stat = path.stat()
    return SourceObjectMetadata(
        source_uri=path.as_uri(),
        size=stat.st_size,
        mtime_ns=stat.st_mtime_ns,
    )


@contextmanager
def open_source_uri(
    source_uri: str,
    *,
    client: Any | None = None,
) -> Iterator[BinaryIO]:
    if source_uri.startswith("s3://"):
        bucket, key = parse_s3_uri(source_uri)
        s3_client = client or boto3.client("s3")
        response = s3_client.get_object(Bucket=bucket, Key=key)
        body = response["Body"]
        try:
            yield body
        finally:
            body.close()
        return
    with fsspec.open(source_uri, mode="rb") as handle:
        yield handle  # type: ignore[invalid-yield]


class S3StorageBackend:
    def __init__(
        self,
        bucket: str,
        prefix: str = "",
        *,
        client: Any | None = None,
    ) -> None:
        self.bucket = bucket
        self.prefix = prefix.strip("/")
        self.client = client or boto3.client("s3")

    def _key(self, relative_path: str) -> str:
        if self.prefix:
            return f"{self.prefix}/{relative_path.lstrip('/')}"
        return relative_path.lstrip("/")

    def read_bytes(self, relative_path: str) -> bytes:
        response = self.client.get_object(
            Bucket=self.bucket, Key=self._key(relative_path)
        )
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
            if if_none_match and _s3_is_precondition_failed(error):
                raise OptimisticLockError(
                    f"Path already exists: {relative_path}"
                ) from error
            raise

    def exists(self, relative_path: str) -> bool:
        try:
            self.client.head_object(Bucket=self.bucket, Key=self._key(relative_path))
            return True
        except ClientError as error:
            if _s3_is_404(error):
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

    def delete(self, relative_path: str) -> None:
        self.client.delete_object(Bucket=self.bucket, Key=self._key(relative_path))

    def etag(self, relative_path: str) -> str | None:
        try:
            response = self.client.head_object(
                Bucket=self.bucket, Key=self._key(relative_path)
            )
        except ClientError as error:
            if _s3_is_404(error):
                return None
            raise
        return response.get("ETag", "").strip('"') or None
