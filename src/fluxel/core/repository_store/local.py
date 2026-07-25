from __future__ import annotations

from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import BinaryIO, Iterator

from ..domain import BranchRefState, OptimisticLockError, RepositoryObjectKind
from ..layout import blob_relpath, initialize_fluxel_layout
from ..manifest import ManifestEntry, ManifestReader
from ..manifest_index import (
    ManifestIndex,
    iter_manifest_index_entry_jsons,
    lookup_manifest_index_entry_json,
)


class LocalRepositoryStore:
    def __init__(self, root: str | Path) -> None:
        self.layout = initialize_fluxel_layout(root)

    def read_commit_bytes(self, commit_id: str) -> bytes | None:
        commit_path = self.commit_path(commit_id)
        if not commit_path.exists():
            return None
        return commit_path.read_bytes()

    def write_commit_bytes(
        self,
        commit_id: str,
        payload: bytes,
        *,
        if_missing: bool = False,
    ) -> None:
        commit_path = self.commit_path(commit_id)
        commit_path.parent.mkdir(parents=True, exist_ok=True)
        if if_missing and commit_path.exists():
            raise OptimisticLockError(f"Commit already exists: {commit_id}")
        commit_path.write_bytes(payload)

    def iter_manifest_entries(self, manifest_hash: str) -> Iterator[ManifestEntry]:
        yield from ManifestReader(self.manifest_path(manifest_hash)).iter_entries()

    def write_manifest_file(
        self,
        manifest_hash: str,
        source_path: str | Path,
        *,
        if_missing: bool = False,
    ) -> None:
        manifest_path = self.manifest_path(manifest_hash)
        source = Path(source_path)
        manifest_path.parent.mkdir(parents=True, exist_ok=True)
        if if_missing and manifest_path.exists():
            raise OptimisticLockError(f"Manifest already exists: {manifest_hash}")
        source.replace(manifest_path)

    def write_manifest_index_file(
        self,
        manifest_hash: str,
        source_path: str | Path,
        *,
        if_missing: bool = False,
    ) -> None:
        index_path = self.manifest_index_path(manifest_hash)
        source = Path(source_path)
        index_path.parent.mkdir(parents=True, exist_ok=True)
        if if_missing and index_path.exists():
            raise OptimisticLockError(f"Manifest index already exists: {manifest_hash}")
        source.replace(index_path)

    def read_manifest_index_bytes(self, manifest_hash: str) -> bytes | None:
        index_path = self.manifest_index_path(manifest_hash)
        if not index_path.exists():
            return None
        return index_path.read_bytes()

    def read_branch_ref(self, branch: str) -> BranchRefState | None:
        branch_path = self.branch_path(branch)
        if not branch_path.exists():
            return None
        commit_id = branch_path.read_text(encoding="utf-8").strip() or None
        return BranchRefState(
            branch=branch,
            commit_id=commit_id,
            version_token=self.version_token("ref", branch),
        )

    def write_branch_ref(self, branch: str, commit_id: str | None) -> None:
        branch_path = self.branch_path(branch)
        branch_path.parent.mkdir(parents=True, exist_ok=True)
        payload = f"{commit_id}\n" if commit_id else ""
        branch_path.write_text(payload, encoding="utf-8")

    def compare_and_set_branch_ref(
        self,
        branch: str,
        commit_id: str | None,
        *,
        expected_version_token: str | None,
        expected_commit_id: str | None = None,
    ) -> bool:
        current_token = self.version_token("ref", branch)
        if current_token != expected_version_token:
            return False
        current_state = self.read_branch_ref(branch)
        current_commit_id = current_state.commit_id if current_state else None
        if current_commit_id != expected_commit_id:
            return False
        self.write_branch_ref(branch, commit_id)
        return True

    def read_blob_bytes(self, blob_hash: str) -> bytes:
        return self.blob_path(blob_hash).read_bytes()

    def open_blob(self, blob_hash: str) -> BinaryIO:
        return self.blob_path(blob_hash).open("rb")

    def lookup_manifest_entry(
        self,
        manifest_hash: str,
        logical_path: str,
        *,
        manifest_index: ManifestIndex | None = None,
    ) -> ManifestEntry | None:
        manifest_path = self.manifest_path(manifest_hash)
        if manifest_index is not None:
            entry_json = lookup_manifest_index_entry_json(
                logical_path,
                read_range=lambda start, end: self._read_manifest_range(
                    manifest_path, start, end
                ),
                index=manifest_index,
            )
        else:
            index_path = self.manifest_index_path(manifest_hash)
            if not index_path.exists():
                return None
            entry_json = lookup_manifest_index_entry_json(
                logical_path,
                read_range=lambda start, end: self._read_manifest_range(
                    manifest_path, start, end
                ),
                index_path=index_path,
            )
        if entry_json is None:
            return None
        return ManifestEntry.deserialize(entry_json)

    def iter_manifest_entries_for_prefix(
        self,
        manifest_hash: str,
        logical_prefix: str,
        *,
        manifest_index: ManifestIndex | None = None,
    ) -> Iterator[ManifestEntry]:
        normalized_prefix = logical_prefix.strip("/")
        manifest_path = self.manifest_path(manifest_hash)
        if manifest_index is not None:
            iter_jsons = iter_manifest_index_entry_jsons(
                logical_prefix=normalized_prefix or None,
                read_range=lambda start, end: self._read_manifest_range(
                    manifest_path, start, end
                ),
                index=manifest_index,
            )
        else:
            index_path = self.manifest_index_path(manifest_hash)
            if not index_path.exists():
                return
            iter_jsons = iter_manifest_index_entry_jsons(
                logical_prefix=normalized_prefix or None,
                read_range=lambda start, end: self._read_manifest_range(
                    manifest_path, start, end
                ),
                index_path=index_path,
            )
        for entry_json in iter_jsons:
            yield ManifestEntry.deserialize(entry_json)

    def write_blob_file(
        self,
        blob_hash: str,
        source_path: str | Path,
        *,
        if_missing: bool = True,
    ) -> None:
        blob_path = self.blob_path(blob_hash)
        if if_missing and blob_path.exists():
            return
        blob_path.parent.mkdir(parents=True, exist_ok=True)
        with Path(source_path).open("rb") as src, blob_path.open("wb") as dst:
            while chunk := src.read(1024 * 1024):
                dst.write(chunk)

    def write_blob_stream(
        self,
        blob_hash: str,
        source: BinaryIO,
        *,
        if_missing: bool = True,
    ) -> None:
        blob_path = self.blob_path(blob_hash)
        if if_missing and blob_path.exists():
            return
        blob_path.parent.mkdir(parents=True, exist_ok=True)
        with NamedTemporaryFile(dir=blob_path.parent, delete=False) as temp:
            temp_path = Path(temp.name)
            while chunk := source.read(1024 * 1024):
                temp.write(chunk)
        try:
            if if_missing and blob_path.exists():
                return
            temp_path.replace(blob_path)
        finally:
            temp_path.unlink(missing_ok=True)

    def object_exists(self, kind: RepositoryObjectKind, object_id: str) -> bool:
        return self._path_for(kind, object_id).exists()

    def version_token(self, kind: RepositoryObjectKind, object_id: str) -> str | None:
        path = self._path_for(kind, object_id)
        if not path.exists():
            return None
        stat = path.stat()
        return f"{stat.st_mtime_ns}-{stat.st_size}"

    def blob_path(self, blob_hash: str) -> Path:
        return self.layout.blobs_dir / blob_relpath(blob_hash)

    def commit_path(self, commit_id: str) -> Path:
        return self.layout.commits_dir / f"{commit_id}.json"

    def manifest_path(self, manifest_hash: str) -> Path:
        return self.layout.manifests_dir / f"{manifest_hash}.jsonl"

    def manifest_index_path(self, manifest_hash: str) -> Path:
        return self.layout.manifests_dir / f"{manifest_hash}.idx"

    def branch_path(self, branch: str) -> Path:
        return self.layout.heads_dir / branch

    def _path_for(self, kind: RepositoryObjectKind, object_id: str) -> Path:
        if kind == "blob":
            return self.blob_path(object_id)
        if kind == "commit":
            return self.commit_path(object_id)
        if kind == "manifest":
            return self.manifest_path(object_id)
        if kind == "manifest-index":
            return self.manifest_index_path(object_id)
        return self.branch_path(object_id)

    def _read_manifest_range(self, manifest_path: Path, start: int, end: int) -> bytes:
        if end <= start:
            return b""
        with manifest_path.open("rb") as handle:
            handle.seek(start)
            return handle.read(end - start)
