"""The ``ObjectStore`` protocol: object IO, CAS refs, and tree-walk queries.

This is the single store protocol of Fluxel v2 (§1/§12 of
docs/architecture.md): immutable object IO (blobs, trees, commits, footers),
pure-CAS branch refs (§6), and tree-walk lookups (§3).  Index sidecars and
branch locks are gone, so there is nothing left to compose — one protocol,
two adapters (``LocalObjectStore`` / ``S3ObjectStore`` in this package).
"""

from __future__ import annotations

from pathlib import Path
from typing import BinaryIO, Iterator, Protocol

from ..domain import BranchRefState, RepositoryObjectKind
from ..manifest import ManifestEntry


class ObjectStore(Protocol):
    """A complete repository object store: object IO, refs, tree queries."""

    # ── Commits ─────────────────────────────────────────────────────────

    def read_commit_bytes(self, commit_id: str) -> bytes | None: ...

    def write_commit_bytes(
        self,
        commit_id: str,
        payload: bytes,
        *,
        if_missing: bool = False,
    ) -> None: ...

    # ── Trees ───────────────────────────────────────────────────────────

    def read_tree_bytes(self, tree_hash: str) -> bytes | None: ...

    def write_tree_file(
        self,
        tree_hash: str,
        source_path: str | Path,
        *,
        if_missing: bool = True,
    ) -> None: ...

    # ── Footers ─────────────────────────────────────────────────────────

    def read_footer_bytes(self, footer_hash: str) -> bytes | None: ...

    def write_footer_file(
        self,
        footer_hash: str,
        source_path: str | Path,
        *,
        if_missing: bool = True,
    ) -> None: ...

    # ── Blobs ───────────────────────────────────────────────────────────

    def read_blob_bytes(self, blob_hash: str) -> bytes: ...

    def open_blob(self, blob_hash: str) -> BinaryIO: ...

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

    # ── Refs (pure CAS, §6) ─────────────────────────────────────────────

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

    # ── Tree-walk queries (single lookup core, §3) ─────────────────────

    def iter_all_entries(self, tree_hash: str) -> Iterator[ManifestEntry]: ...

    def lookup_entry(
        self, tree_hash: str, logical_path: str
    ) -> ManifestEntry | None: ...

    def iter_entries_for_prefix(
        self, tree_hash: str, logical_prefix: str
    ) -> Iterator[ManifestEntry]: ...
