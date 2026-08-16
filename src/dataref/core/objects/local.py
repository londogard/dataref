from __future__ import annotations

from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import BinaryIO, Iterator

from ..domain import BranchRefState, OptimisticLockError, RepositoryObjectKind
from ..layout import blob_relpath, initialize_dataref_layout
from ..manifest import ManifestEntry
from .query import TreeCache, TreeWalker
from .tree import parse_tree_object


class LocalObjectStore:
    def __init__(self, root: str | Path) -> None:
        self.layout = initialize_dataref_layout(root)
        self.tree_cache = TreeCache()
        self._walker = TreeWalker(
            read_tree=self.read_tree_bytes,
            cache=self.tree_cache,
        )

    # ── Commits ──────────────────────────────────────────────────────────

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

    # ── Trees ────────────────────────────────────────────────────────────

    def read_tree_bytes(self, tree_hash: str) -> bytes | None:
        tree_path = self.tree_path(tree_hash)
        if not tree_path.exists():
            return None
        return tree_path.read_bytes()

    def write_tree_file(
        self,
        tree_hash: str,
        source_path: str | Path,
        *,
        if_missing: bool = True,
    ) -> None:
        tree_path = self.tree_path(tree_hash)
        source = Path(source_path)
        tree_path.parent.mkdir(parents=True, exist_ok=True)
        if if_missing and tree_path.exists():
            return
        source.replace(tree_path)

    # ── Tree-walk queries ────────────────────────────────────────────────

    def iter_all_entries(self, tree_hash: str) -> Iterator[ManifestEntry]:
        yield from self._walker.iter_all_entries(tree_hash)

    def lookup_entry(
        self, tree_hash: str, logical_path: str
    ) -> ManifestEntry | None:
        return self._walker.lookup_entry(tree_hash, logical_path)

    def iter_entries_for_prefix(
        self, tree_hash: str, logical_prefix: str
    ) -> Iterator[ManifestEntry]:
        yield from self._walker.iter_entries_for_prefix(tree_hash, logical_prefix)

    # ── Footers ──────────────────────────────────────────────────────────

    def read_footer_bytes(self, footer_hash: str) -> bytes | None:
        footer_path = self.footer_path(footer_hash)
        if not footer_path.exists():
            return None
        return footer_path.read_bytes()

    def write_footer_file(
        self,
        footer_hash: str,
        source_path: str | Path,
        *,
        if_missing: bool = True,
    ) -> None:
        footer_path = self.footer_path(footer_hash)
        source = Path(source_path)
        footer_path.parent.mkdir(parents=True, exist_ok=True)
        if if_missing and footer_path.exists():
            return
        source.replace(footer_path)

    # ── Refs ─────────────────────────────────────────────────────────────

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
        if expected_commit_id is not None:
            current_state = self.read_branch_ref(branch)
            current_commit_id = current_state.commit_id if current_state else None
            if current_commit_id != expected_commit_id:
                return False
        self.write_branch_ref(branch, commit_id)
        return True

    # ── Blobs ────────────────────────────────────────────────────────────

    def read_blob_bytes(self, blob_hash: str) -> bytes:
        return self.blob_path(blob_hash).read_bytes()

    def open_blob(self, blob_hash: str) -> BinaryIO:
        return self.blob_path(blob_hash).open("rb")

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

    # ── Enumeration (GC) ────────────────────────────────────────────────

    def iter_branches(self) -> Iterator[str]:
        for path in self.layout.heads_dir.iterdir():
            # Skip HEAD + client-state snapshots (refs/heads/<branch>.json).
            if (
                path.is_file()
                and not path.name.startswith(".")
                and not path.name.endswith(".json")
            ):
                yield path.name

    def iter_object_ids(self, kind: RepositoryObjectKind) -> Iterator[str]:
        if kind == "blob":
            for shard in self.layout.blobs_dir.iterdir():
                if shard.is_dir():
                    for path in shard.iterdir():
                        yield shard.name + path.name
        elif kind == "tree":
            yield from (p.name for p in self.layout.trees_dir.iterdir() if p.is_file())
        elif kind == "footer":
            yield from (p.name for p in self.layout.footers_dir.iterdir() if p.is_file())
        elif kind == "commit":
            yield from (p.stem for p in self.layout.commits_dir.glob("*.json"))

    def delete_object(self, kind: RepositoryObjectKind, object_id: str) -> None:
        self._path_for(kind, object_id).unlink(missing_ok=True)

    def object_path(self, kind: RepositoryObjectKind, object_id: str) -> Path:
        return self._path_for(kind, object_id)

    # ── Paths ────────────────────────────────────────────────────────────

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

    def tree_path(self, tree_hash: str) -> Path:
        return self.layout.trees_dir / tree_hash

    def footer_path(self, footer_hash: str) -> Path:
        return self.layout.footers_dir / footer_hash

    def branch_path(self, branch: str) -> Path:
        return self.layout.heads_dir / branch

    def _path_for(self, kind: RepositoryObjectKind, object_id: str) -> Path:
        if kind == "blob":
            return self.blob_path(object_id)
        if kind == "commit":
            return self.commit_path(object_id)
        if kind == "tree":
            return self.tree_path(object_id)
        if kind == "footer":
            return self.footer_path(object_id)
        return self.branch_path(object_id)
