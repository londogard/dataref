from __future__ import annotations

from pathlib import Path
from typing import BinaryIO, Iterator, Protocol

from ..domain import BranchRefState, RepositoryObjectKind
from ..manifest import ManifestEntry
from ..manifest_index import ManifestIndex
from ..storage import BlobTransferBackend


class RepositoryStore(Protocol):
    def read_commit_bytes(self, commit_id: str) -> bytes | None: ...

    def write_commit_bytes(
        self,
        commit_id: str,
        payload: bytes,
        *,
        if_missing: bool = False,
    ) -> None: ...

    def iter_manifest_entries(self, manifest_hash: str) -> Iterator[ManifestEntry]: ...

    def write_manifest_file(
        self,
        manifest_hash: str,
        source_path: str | Path,
        *,
        if_missing: bool = False,
    ) -> None: ...

    def write_manifest_index_file(
        self,
        manifest_hash: str,
        source_path: str | Path,
        *,
        if_missing: bool = False,
    ) -> None: ...

    def read_manifest_index_bytes(self, manifest_hash: str) -> bytes | None: ...

    def branch_path(self, branch: str) -> Path: ...

    def read_branch_ref(self, branch: str) -> BranchRefState | None: ...

    def write_branch_ref(self, branch: str, commit_id: str | None) -> None: ...

    def compare_and_set_branch_ref(
        self,
        branch: str,
        commit_id: str | None,
        *,
        expected_version_token: str | None,
        expected_commit_id: str | None = None,
    ) -> bool: ...

    def read_blob_bytes(self, blob_hash: str) -> bytes: ...

    def open_blob(self, blob_hash: str) -> BinaryIO: ...

    def lookup_manifest_entry(
        self,
        manifest_hash: str,
        logical_path: str,
        *,
        manifest_index: ManifestIndex | None = None,
    ) -> ManifestEntry | None: ...

    def iter_manifest_entries_for_prefix(
        self,
        manifest_hash: str,
        logical_prefix: str,
        *,
        manifest_index: ManifestIndex | None = None,
    ) -> Iterator[ManifestEntry]: ...

    def write_blob_file(
        self,
        blob_hash: str,
        source_path: str | Path,
        *,
        if_missing: bool = True,
    ) -> None: ...

    def write_blob_stream(
        self,
        blob_hash: str,
        source: BinaryIO,
        *,
        if_missing: bool = True,
    ) -> None: ...

    def object_exists(self, kind: RepositoryObjectKind, object_id: str) -> bool: ...

    def version_token(
        self, kind: RepositoryObjectKind, object_id: str
    ) -> str | None: ...
