from __future__ import annotations

import os
import subprocess
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import BinaryIO, Iterator, Protocol
from urllib.parse import unquote, urlparse

import boto3
import fsspec
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


@dataclass(frozen=True)
class S3ObjectMetadata:
    bucket: str
    key: str
    size: int
    mtime_ns: int

    @property
    def source_uri(self) -> str:
        return f"s3://{self.bucket}/{self.key}"


@dataclass(frozen=True)
class SourceObjectMetadata:
    source_uri: str
    size: int
    mtime_ns: int


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
    client: object | None = None,
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
    client: object | None = None,
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
    client: object | None = None,
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
    with fsspec.open(source_uri, mode="rb").open() as handle:
        yield handle


class BlobTransferBackend(Protocol):
    """Interface for blob storage transfer operations.

    Implementations can use any tool (boto3, s5cmd, awscli, etc.)
    Swappable to suit different environments and performance needs.
    """

    def upload(
        self,
        local_path: str,
        remote_uri: str,
        *,
        if_not_exists: bool = False,
    ) -> None: ...

    def download(self, remote_uri: str, local_path: str) -> None: ...

    def list_objects(self, uri_prefix: str) -> Iterator[S3ObjectMetadata]: ...

    def delete(self, remote_uri: str) -> None: ...

    def exists(self, remote_uri: str) -> bool: ...


class S3BlobTransferBackend:
    """Blob transfer backend using boto3 directly."""

    def __init__(self, client: object | None = None) -> None:
        self._client = client or boto3.client("s3")
        self._backends: dict[str, S3StorageBackend] = {}

    def _backend(self, remote_uri: str) -> tuple[S3StorageBackend, str]:
        bucket, key = parse_s3_uri(remote_uri)
        if bucket not in self._backends:
            self._backends[bucket] = S3StorageBackend(
                bucket, prefix="", client=self._client
            )
        return self._backends[bucket], key

    def upload(
        self,
        local_path: str,
        remote_uri: str,
        *,
        if_not_exists: bool = False,
    ) -> None:
        backend, key = self._backend(remote_uri)
        backend.write_bytes(
            key,
            Path(local_path).read_bytes(),
            if_none_match=if_not_exists,
        )

    def download(self, remote_uri: str, local_path: str) -> None:
        backend, key = self._backend(remote_uri)
        Path(local_path).write_bytes(backend.read_bytes(key))

    def list_objects(self, uri_prefix: str) -> Iterator[S3ObjectMetadata]:
        bucket, prefix = parse_s3_uri(uri_prefix)
        paginator = self._client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                yield S3ObjectMetadata(
                    bucket=bucket,
                    key=str(obj["Key"]),
                    size=int(obj.get("Size", 0)),
                    mtime_ns=_mtime_ns(obj.get("LastModified")),
                )

    def delete(self, remote_uri: str) -> None:
        backend, key = self._backend(remote_uri)
        backend.delete(key)

    def exists(self, remote_uri: str) -> bool:
        backend, key = self._backend(remote_uri)
        return backend.exists(key)


class S5CmdBlobTransferBackend:
    """Blob transfer backend using s5cmd for high-throughput S3 operations.

    Uses subprocess to shell out to s5cmd. Best for bulk operations
    where s5cmd's parallel transfer engine provides significant speedups.

    Requires s5cmd to be installed and available on PATH.
    """

    def __init__(
        self,
        s5cmd_path: str = "s5cmd",
        endpoint_url: str | None = None,
    ) -> None:
        self._s5cmd_path = s5cmd_path
        self._endpoint = endpoint_url

    def _run(
        self,
        args: list[str],
        input_data: str | None = None,
    ) -> subprocess.CompletedProcess[str]:
        cmd = [self._s5cmd_path]
        if self._endpoint:
            cmd.extend(["--endpoint-url", self._endpoint])
        cmd.extend(args)
        return subprocess.run(
            cmd,
            text=True,
            capture_output=True,
            input=input_data,
            check=True,
        )

    def upload(
        self,
        local_path: str,
        remote_uri: str,
        *,
        if_not_exists: bool = False,
    ) -> None:
        flag = "--if-not-exists" if if_not_exists else ""
        parts = ["cp", flag, local_path, remote_uri]
        self._run([p for p in parts if p])

    def download(self, remote_uri: str, local_path: str) -> None:
        Path(local_path).parent.mkdir(parents=True, exist_ok=True)
        self._run(["cp", remote_uri, local_path])

    def list_objects(self, uri_prefix: str) -> Iterator[S3ObjectMetadata]:
        bucket, prefix = parse_s3_uri(uri_prefix)
        result = self._run(["ls", uri_prefix])
        for line in result.stdout.strip().splitlines():
            if not line.strip():
                continue
            parts = line.split(None, 3)
            if len(parts) < 4:
                continue
            # s5cmd ls output: "2024-01-15 10:30:00         1234 s3://bucket/key"
            try:
                size = int(parts[2])
            except (ValueError, IndexError):
                try:
                    size = int(parts[1])
                except (ValueError, IndexError):
                    size = 0
            key = parts[-1]
            key_path = key[len(f"s3://{bucket}/"):] if key.startswith(f"s3://{bucket}/") else key.lstrip("/")
            yield S3ObjectMetadata(
                bucket=bucket,
                key=key_path,
                size=size,
                mtime_ns=0,
            )

    def delete(self, remote_uri: str) -> None:
        self._run(["rm", remote_uri])

    def exists(self, remote_uri: str) -> bool:
        try:
            result = self._run(["ls", remote_uri])
            return bool(result.stdout.strip())
        except subprocess.CalledProcessError:
            return False


def build_blob_transfer_backend(
    backend_type: str = "boto3",
    **kwargs: object,
) -> BlobTransferBackend:
    """Factory to create a BlobTransferBackend by name.

    Args:
        backend_type: "boto3" (default) or "s5cmd"
        **kwargs: Passed to the backend constructor.

    Returns:
        A BlobTransferBackend instance.
    """
    if backend_type == "boto3":
        return S3BlobTransferBackend(**kwargs)  # type: ignore[arg-type]
    if backend_type == "s5cmd":
        return S5CmdBlobTransferBackend(**kwargs)  # type: ignore[arg-type]
    raise ValueError(f"Unknown blob transfer backend: {backend_type}")
