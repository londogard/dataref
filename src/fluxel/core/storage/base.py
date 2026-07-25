from __future__ import annotations

from dataclasses import dataclass
from typing import BinaryIO, Iterator, Protocol


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
