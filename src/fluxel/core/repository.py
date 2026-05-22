# Fluxel Guardrails (Permanent):
# - DO NOT optimize for ML throughput in canonical storage (`blobs/`): no sharding, tarballs, or parquet in the blob layer.
# - DO NOT read blob data for metadata-only operations (diff, list, log, status).
# - DO NOT introduce a server, daemon, or central database; Fluxel is 100% client-side/serverless.
# - DO NOT use SHA-1/SHA-256; use Blake3 for all content hashing.
# - PREFER JSONL manifests to preserve streaming and O(1) memory usage.

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from pathlib import PurePosixPath
from tempfile import NamedTemporaryFile
from typing import BinaryIO, Iterator, Literal

from blake3 import blake3

from .client_state import LocalClientState
from .config import load_config, validate_config
from .hashing import DEFAULT_CHUNK_SIZE, blake3_digest_file
from .layout import blob_relpath, initialize_fluxel_layout
from .manifest import ManifestEntry, ManifestWriter, walk_files, FileEntry
from .repository_store import (
    BranchRefState,
    LocalRepositoryStore,
    RepositoryStore,
    S3RepositoryStore,
    build_manifest_index_file,
)
from .repository_support import (
    matches_any_logical_path,
    matches_import_patterns,
    matches_logical_path,
    metadata_identity,
    move_logical_path,
    normalize_import_patterns,
    normalize_logical_paths,
    normalize_repository_path,
    normalize_s3_import_path,
    relocate_manifest_entry,
)
from .storage import (
    BlobTransferBackend,
    build_blob_transfer_backend,
    iter_s3_objects,
    open_source_uri,
    parse_s3_uri,
)
from .storage import describe_source_uri


@dataclass(frozen=True)
class CommitObject:
    id: str
    message: str
    manifest: str
    parent: str | None
    created_at: str
    branch: str


@dataclass(frozen=True)
class DiffEntry:
    path: str
    change: str
    before_hash: str | None
    after_hash: str | None
    before_size: int | None
    after_size: int | None


@dataclass(frozen=True)
class VerifyResult:
    commit_id: str
    verified_entries: int
    candidate_entries: int
    total_entries: int
    created_commit: bool
    dry_run: bool


@dataclass(frozen=True)
class MergeResult:
    source_ref: str
    target_ref: str
    commit_id: str
    updated: bool


@dataclass(frozen=True)
class RemoveResult:
    ref: str
    commit_id: str
    removed_paths: list[str]


@dataclass(frozen=True)
class MoveResult:
    ref: str
    commit_id: str
    source_path: str
    destination_path: str
    moved_paths: list[str]


@dataclass(frozen=True)
class StageChange:
    path: str
    action: str
    identity_mode: str | None = None
    source_uri: str | None = None
    blob_hash: str | None = None
    size: int | None = None

    @staticmethod
    def from_dict(data: dict[str, object]) -> "StageChange":
        action = str(data["action"])
        identity_mode_raw = data.get("identity_mode")
        source_uri_raw = data.get("source_uri")
        blob_hash_raw = data.get("blob_hash")
        size_raw = data.get("size")
        return StageChange(
            path=str(data["path"]),
            action=action,
            identity_mode=(
                str(identity_mode_raw) if identity_mode_raw is not None else None
            ),
            source_uri=str(source_uri_raw) if source_uri_raw is not None else None,
            blob_hash=str(blob_hash_raw) if blob_hash_raw is not None else None,
            size=int(size_raw) if size_raw is not None else None,
        )


@dataclass(frozen=True)
class StageStatus:
    ref: str
    added: list[str]
    removed: list[str]
    modified: list[str] = ()
    working_tree_added: list[str] = ()
    working_tree_removed: list[str] = ()
    working_tree_modified: list[str] = ()

    def __post_init__(self) -> None:
        object.__setattr__(self, "added", list(self.added))
        object.__setattr__(self, "removed", list(self.removed))
        object.__setattr__(self, "modified", list(self.modified))
        object.__setattr__(self, "working_tree_added", list(self.working_tree_added))
        object.__setattr__(self, "working_tree_removed", list(self.working_tree_removed))
        object.__setattr__(self, "working_tree_modified", list(self.working_tree_modified))


class RefConflictError(RuntimeError):
    def __init__(
        self,
        *,
        branch: str,
        operation: str,
        expected_commit_id: str | None,
        current_commit_id: str | None,
    ) -> None:
        expected = expected_commit_id or "<empty>"
        current = current_commit_id or "<empty>"
        super().__init__(
            f"Branch update conflict for '{branch}' during {operation}: expected {expected}, found {current}"
        )
        self.branch = branch
        self.operation = operation
        self.expected_commit_id = expected_commit_id
        self.current_commit_id = current_commit_id


class FluxelRepository:
    def __init__(
        self,
        root: str | Path,
        *,
        store: RepositoryStore | None = None,
        client_state: LocalClientState | None = None,
        blob_transfer: BlobTransferBackend | None = None,
    ) -> None:
        self.layout = initialize_fluxel_layout(root)
        config = load_config(root)
        if config is not None:
            validate_config(config)
        self.store = store or LocalRepositoryStore(self.layout.root)
        if isinstance(self.store, S3RepositoryStore) and blob_transfer is not None:
            self.store = S3RepositoryStore(
                self.store.bucket,
                self.store.prefix,
                client=self.store.client,
                branch_root=self.store.branch_root,
                lock_timeout_seconds=self.store.lock_timeout_seconds,
                blob_transfer=blob_transfer,
            )
        self.client_state = client_state or LocalClientState(self.layout.root)
        self._resolved_ref_cache: dict[str, BranchRefState] = {}
        self._commit_cache: dict[str, CommitObject] = {}
        self._ensure_head(default_branch="main")

    @property
    def root(self) -> Path:
        return self.layout.root

    def _branch_path(self, branch_name: str) -> Path:
        return self.store.branch_path(branch_name)

    def _ensure_head(self, default_branch: str) -> None:
        self.client_state.ensure_current_branch(default_branch)
        if self.store.read_branch_ref(default_branch) is None:
            self.store.write_branch_ref(default_branch, None)

    def current_branch(self) -> str:
        return self.client_state.current_branch()

    def set_current_branch(self, branch: str) -> None:
        self._ensure_branch_exists(branch)
        self.client_state.set_current_branch(branch)

    def head_commit(self) -> str | None:
        branch_ref = self.store.read_branch_ref(self.current_branch())
        if branch_ref is None:
            return None
        return branch_ref.commit_id

    def resolve_ref(self, branch_or_commit: str) -> str:
        cached_branch_ref = self._resolved_ref_cache.get(branch_or_commit)
        if cached_branch_ref is not None:
            current_token = self.store.version_token("ref", branch_or_commit)
            if current_token == cached_branch_ref.version_token:
                if not cached_branch_ref.commit_id:
                    raise ValueError(f"Branch has no commits: {branch_or_commit}")
                return cached_branch_ref.commit_id

        branch_ref = self.store.read_branch_ref(branch_or_commit)
        if branch_ref is not None:
            self._resolved_ref_cache[branch_or_commit] = branch_ref
            if not branch_ref.commit_id:
                raise ValueError(f"Branch has no commits: {branch_or_commit}")
            return branch_ref.commit_id
        if self.store.object_exists("commit", branch_or_commit):
            return branch_or_commit
        raise ValueError(f"Unknown branch or commit: {branch_or_commit}")

    def branch(self, name: str) -> Path:
        if not name or "/" in name or name.startswith("."):
            raise ValueError("Invalid branch name")
        if self.store.read_branch_ref(name) is not None:
            raise ValueError(f"Branch already exists: {name}")
        head_commit = self.head_commit() or ""
        if not self.store.compare_and_set_branch_ref(
            name,
            head_commit or None,
            expected_version_token=None,
            expected_commit_id=None,
        ):
            raise ValueError(f"Branch already exists: {name}")
        created_state = self.store.read_branch_ref(name)
        if created_state is not None:
            self.client_state.write_branch_snapshot(
                name,
                commit_id=created_state.commit_id,
                version_token=created_state.version_token,
            )
        self._resolved_ref_cache.pop(name, None)
        return self._branch_path(name)

    def merge(self, source_ref: str, target_ref: str) -> MergeResult:
        if not source_ref:
            raise ValueError("Source ref cannot be empty")
        if not target_ref:
            raise ValueError("Target ref cannot be empty")

        target_branch_state = self._require_branch_state(target_ref)
        source_commit = self.resolve_ref(source_ref)
        target_commit = self.resolve_ref(target_ref)

        if source_commit == target_commit:
            return MergeResult(
                source_ref=source_ref,
                target_ref=target_ref,
                commit_id=target_commit,
                updated=False,
            )

        if not self._is_ancestor(
            ancestor_commit=target_commit, descendant_commit=source_commit
        ):
            raise ValueError(
                f"Cannot fast-forward {target_ref} to {source_ref}: target is not an ancestor"
            )

        self._update_branch_ref(
            branch=target_ref,
            commit_id=source_commit,
            expected_version_token=target_branch_state.version_token,
            expected_commit_id=target_branch_state.commit_id,
            operation="merge",
        )
        return MergeResult(
            source_ref=source_ref,
            target_ref=target_ref,
            commit_id=source_commit,
            updated=True,
        )

    def commit(
        self,
        message: str,
        identity_mode: Literal["blake3", "meta"] = "blake3",
        *,
        staged: bool = False,
        ref: str | None = None,
    ) -> str:
        if not message.strip():
            raise ValueError("Commit message cannot be empty")
        if identity_mode not in {"blake3", "meta"}:
            raise ValueError("identity_mode must be one of: blake3, meta")
        branch = ref or self.current_branch()
        branch_state = self._require_branch_state(branch)

        if staged:
            return self._commit_staged(
                message=message,
                branch_state=branch_state,
            )

        temp_manifest = self._write_temp_manifest(
            self._materialize_blobs_and_entries(identity_mode=identity_mode)
        )
        manifest_hash = blake3_digest_file(temp_manifest)
        self._persist_manifest(temp_manifest, manifest_hash)

        parent_commit = branch_state.commit_id
        return self._write_commit_object(
            branch=branch,
            message=message,
            parent_commit=parent_commit,
            manifest_hash=manifest_hash,
            expected_version_token=branch_state.version_token,
            operation="commit",
        )

    def import_s3(
        self,
        source_uri: str,
        message: str,
        identity_mode: Literal["blake3", "meta"] = "blake3",
        *,
        path_patterns: list[str] | None = None,
        ref: str | None = None,
    ) -> str:
        if not message.strip():
            raise ValueError("Commit message cannot be empty")
        if identity_mode not in {"blake3", "meta"}:
            raise ValueError("identity_mode must be one of: blake3, meta")
        branch = ref or self.current_branch()
        branch_state = self._require_branch_state(branch)
        parent_commit = branch_state.commit_id

        parent_manifest: str | None = None
        if parent_commit:
            parent = self.read_commit(parent_commit)
            parent_manifest = parent.manifest

        temp_manifest = self._write_temp_manifest(
            _merge_sorted_entry_streams(
                base_entries=(
                    self.store.iter_manifest_entries(parent_manifest)
                    if parent_manifest
                    else None
                ),
                new_entries=self._materialize_s3_entries(
                    source_uri=source_uri,
                    identity_mode=identity_mode,
                    path_patterns=path_patterns,
                ),
            )
        )
        manifest_hash = blake3_digest_file(temp_manifest)
        self._persist_manifest(temp_manifest, manifest_hash)

        return self._write_commit_object(
            branch=branch,
            message=message,
            parent_commit=parent_commit,
            manifest_hash=manifest_hash,
            expected_version_token=branch_state.version_token,
            operation="commit",
        )

    def add(
        self,
        paths: list[str],
        *,
        ref: str | None = None,
        identity_mode: str = "blake3",
        destination_path: str | None = None,
    ) -> StageStatus:
        if identity_mode not in {"blake3", "meta"}:
            raise ValueError("identity_mode must be one of: blake3, meta")
        branch = ref or self.current_branch()
        self._ensure_branch_exists(branch)
        staged = self._load_stage(branch)
        if destination_path is not None and len(paths) != 1:
            raise ValueError("--as can only be used when staging exactly one source")
        for raw_source in paths:
            for logical_path, source_uri in self._expand_stage_source(
                raw_source,
                destination_path=destination_path,
            ):
                staged[logical_path] = StageChange(
                    path=logical_path,
                    action="add",
                    identity_mode=identity_mode,
                    source_uri=source_uri,
                )
        self._save_stage(branch, staged)
        return self.status(ref=branch)

    def rm(self, paths: list[str], *, ref: str | None = None) -> StageStatus:
        branch = ref or self.current_branch()
        self._ensure_branch_exists(branch)
        staged = self._load_stage(branch)
        for path in paths:
            normalized = normalize_repository_path(path)
            staged[normalized] = StageChange(path=normalized, action="remove")
        self._save_stage(branch, staged)
        return self.status(ref=branch)

    def remove_paths(
        self,
        paths: list[str],
        message: str,
        *,
        ref: str | None = None,
    ) -> RemoveResult:
        if not message.strip():
            raise ValueError("Commit message cannot be empty")
        branch = ref or self.current_branch()
        branch_state = self._require_branch_state(branch)
        base_commit = self._require_commit_for_metadata_mutation(branch_state.branch)
        normalized_paths = normalize_logical_paths(paths)
        removed_paths: set[str] = set()

        def iter_entries() -> Iterator[ManifestEntry]:
            nonlocal removed_paths
            for entry in self.store.iter_manifest_entries(base_commit.manifest):
                if matches_any_logical_path(entry.path, normalized_paths):
                    removed_paths.add(entry.path)
                    continue
                yield entry

        temp_manifest = self._write_temp_manifest(iter_entries())
        if not removed_paths:
            temp_manifest.unlink(missing_ok=True)
            missing = ", ".join(normalized_paths)
            raise FileNotFoundError(f"Path not found in branch '{branch}': {missing}")

        manifest_hash = blake3_digest_file(temp_manifest)
        self._persist_manifest(temp_manifest, manifest_hash)
        commit_id = self._write_commit_object(
            branch=branch,
            message=message,
            parent_commit=branch_state.commit_id,
            manifest_hash=manifest_hash,
            expected_version_token=branch_state.version_token,
            operation="rm",
        )
        return RemoveResult(
            ref=branch,
            commit_id=commit_id,
            removed_paths=sorted(removed_paths),
        )

    def move(
        self,
        source_path: str,
        destination_path: str,
        message: str,
        *,
        ref: str | None = None,
    ) -> MoveResult:
        if not message.strip():
            raise ValueError("Commit message cannot be empty")
        branch = ref or self.current_branch()
        branch_state = self._require_branch_state(branch)
        base_commit = self._require_commit_for_metadata_mutation(branch_state.branch)
        source = normalize_repository_path(source_path)
        destination = normalize_repository_path(destination_path)

        if source == destination:
            raise ValueError("Source and destination paths must differ")
        if destination.startswith(f"{source}/"):
            raise ValueError("Cannot move a path into itself")

        existing_paths: set[str] = set()
        source_paths: list[str] = []
        moved_paths: list[str] = []
        path_map: dict[str, str] = {}

        for entry in self.store.iter_manifest_entries(base_commit.manifest):
            existing_paths.add(entry.path)
            if not matches_logical_path(entry.path, source):
                continue
            moved_path = move_logical_path(
                entry.path,
                source_path=source,
                destination_path=destination,
            )
            source_paths.append(entry.path)
            moved_paths.append(moved_path)
            path_map[entry.path] = moved_path

        if not path_map:
            raise FileNotFoundError(f"Path not found in branch '{branch}': {source}")
        if len(set(moved_paths)) != len(moved_paths):
            raise ValueError("Move would create duplicate logical paths")

        source_path_set = set(source_paths)
        for moved_path in moved_paths:
            if moved_path in existing_paths and moved_path not in source_path_set:
                raise ValueError(
                    f"Destination already exists in branch '{branch}': {moved_path}"
                )

        updated_entries: dict[str, ManifestEntry] = {}
        for entry in self.store.iter_manifest_entries(base_commit.manifest):
            moved_path = path_map.get(entry.path)
            if moved_path is None:
                updated_entries[entry.path] = entry
                continue
            relocated_entry = relocate_manifest_entry(entry, moved_path)
            updated_entries[relocated_entry.path] = relocated_entry

        def iter_entries() -> Iterator[ManifestEntry]:
            for path in sorted(updated_entries):
                yield updated_entries[path]

        temp_manifest = self._write_temp_manifest(iter_entries())
        manifest_hash = blake3_digest_file(temp_manifest)
        self._persist_manifest(temp_manifest, manifest_hash)
        commit_id = self._write_commit_object(
            branch=branch,
            message=message,
            parent_commit=branch_state.commit_id,
            manifest_hash=manifest_hash,
            expected_version_token=branch_state.version_token,
            operation="mv",
        )
        return MoveResult(
            ref=branch,
            commit_id=commit_id,
            source_path=source,
            destination_path=destination,
            moved_paths=sorted(moved_paths),
        )

    def status(self, *, ref: str | None = None, working_tree: bool = False) -> StageStatus:
        branch = ref or self.current_branch()
        self._ensure_branch_exists(branch)
        staged = self._load_stage(branch)
        added = sorted(
            change.path for change in staged.values() if change.action == "add"
        )
        removed = sorted(
            change.path for change in staged.values() if change.action == "remove"
        )

        wt_added: list[str] = []
        wt_removed: list[str] = []
        wt_modified: list[str] = []
        if working_tree:
            wt_added, wt_removed, wt_modified = self._compare_working_tree(branch)

        return StageStatus(
            ref=branch,
            added=added,
            removed=removed,
            modified=[],
            working_tree_added=wt_added,
            working_tree_removed=wt_removed,
            working_tree_modified=wt_modified,
        )

    def _compare_working_tree(self, branch: str) -> tuple[list[str], list[str], list[str]]:
        head_commit = self.head_commit()
        if head_commit is None:
            manifest_paths: set[str] = set()
            manifest_by_path: dict[str, ManifestEntry] = {}
        else:
            commit_obj = self.read_commit(head_commit)
            manifest_by_path = {
                entry.path: entry
                for entry in self.store.iter_manifest_entries(commit_obj.manifest)
            }
            manifest_paths = set(manifest_by_path)

        working_files: dict[str, FileEntry] = {}
        for file_entry in walk_files(self.root):
            relative = file_entry.path.relative_to(self.root).as_posix()
            working_files[relative] = file_entry
        working_paths = set(working_files)

        added_paths: list[str] = []
        removed_paths: list[str] = []
        modified_paths: list[str] = []

        for path in sorted(working_paths - manifest_paths):
            added_paths.append(path)

        for path in sorted(manifest_paths - working_paths):
            removed_paths.append(path)

        for path in sorted(working_paths & manifest_paths):
            manifest_entry = manifest_by_path[path]
            working_file = working_files[path]
            current_hash = blake3_digest_file(working_file.path)
            if current_hash != manifest_entry.hash or working_file.size != manifest_entry.size:
                modified_paths.append(path)

        return added_paths, removed_paths, modified_paths

    def read_commit(self, commit_id: str) -> CommitObject:
        cached_commit = self._commit_cache.get(commit_id)
        if cached_commit is not None:
            return cached_commit
        commit_payload = self.store.read_commit_bytes(commit_id)
        if commit_payload is None:
            raise ValueError(f"Unknown commit: {commit_id}")
        data = json.loads(commit_payload.decode("utf-8"))
        commit = CommitObject(
            id=str(data["id"]),
            message=str(data["message"]),
            manifest=str(data["manifest"]),
            parent=str(data["parent"]) if data["parent"] else None,
            created_at=str(data["created_at"]),
            branch=str(data["branch"]),
        )
        self._commit_cache[commit_id] = commit
        return commit

    def log(self, ref: str) -> Iterator[CommitObject]:
        try:
            commit_id = self.resolve_ref(ref)
        except ValueError as e:
            if "Branch has no commits:" in str(e):
                return
            raise

        while commit_id:
            commit = self.read_commit(commit_id)
            yield commit
            commit_id = commit.parent

    def diff(self, from_ref: str, to_ref: str) -> list[DiffEntry]:
        from_commit = self.read_commit(self.resolve_ref(from_ref))
        to_commit = self.read_commit(self.resolve_ref(to_ref))

        from_iter = self.store.iter_manifest_entries(from_commit.manifest)
        to_iter = self.store.iter_manifest_entries(to_commit.manifest)

        changes: list[DiffEntry] = []
        from_entry = next(from_iter, None)
        to_entry = next(to_iter, None)

        while from_entry is not None or to_entry is not None:
            if from_entry is None:
                changes.append(
                    DiffEntry(
                        path=to_entry.path,
                        change="added",
                        before_hash=None,
                        after_hash=to_entry.hash,
                        before_size=None,
                        after_size=to_entry.size,
                    )
                )
                to_entry = next(to_iter, None)
            elif to_entry is None:
                changes.append(
                    DiffEntry(
                        path=from_entry.path,
                        change="removed",
                        before_hash=from_entry.hash,
                        after_hash=None,
                        before_size=from_entry.size,
                        after_size=None,
                    )
                )
                from_entry = next(from_iter, None)
            elif from_entry.path == to_entry.path:
                if (
                    from_entry.hash != to_entry.hash
                    or from_entry.size != to_entry.size
                ):
                    changes.append(
                        DiffEntry(
                            path=from_entry.path,
                            change="modified",
                            before_hash=from_entry.hash,
                            after_hash=to_entry.hash,
                            before_size=from_entry.size,
                            after_size=to_entry.size,
                        )
                    )
                from_entry = next(from_iter, None)
                to_entry = next(to_iter, None)
            elif from_entry.path < to_entry.path:
                changes.append(
                    DiffEntry(
                        path=from_entry.path,
                        change="removed",
                        before_hash=from_entry.hash,
                        after_hash=None,
                        before_size=from_entry.size,
                        after_size=None,
                    )
                )
                from_entry = next(from_iter, None)
            else:
                changes.append(
                    DiffEntry(
                        path=to_entry.path,
                        change="added",
                        before_hash=None,
                        after_hash=to_entry.hash,
                        before_size=None,
                        after_size=to_entry.size,
                    )
                )
                to_entry = next(to_iter, None)

        return changes

    def verify(
        self,
        ref: str = "main",
        path_prefixes: list[str] | None = None,
        *,
        dry_run: bool = False,
    ) -> VerifyResult:
        branch_state = self._require_branch_state(ref)

        base_commit_id = self.resolve_ref(ref)
        base_commit = self.read_commit(base_commit_id)
        normalized_prefixes = [
            prefix.strip("/") for prefix in (path_prefixes or []) if prefix.strip("/")
        ]

        verified_entries = 0
        candidate_entries = 0
        total_entries = 0

        def should_verify(entry_path: str) -> bool:
            if not normalized_prefixes:
                return True
            return any(
                entry_path == prefix or entry_path.startswith(f"{prefix}/")
                for prefix in normalized_prefixes
            )

        def iter_verified_entries() -> Iterator[ManifestEntry]:
            nonlocal verified_entries, candidate_entries, total_entries
            for entry in self.store.iter_manifest_entries(base_commit.manifest):
                total_entries += 1
                if not should_verify(entry.path):
                    yield entry
                    continue
                if entry.blob_hash:
                    yield entry
                    continue
                candidate_entries += 1
                if dry_run:
                    yield entry
                    continue
                if not entry.source_uri:
                    raise FileNotFoundError(
                        f"Cannot verify '{entry.path}' because source_uri is missing"
                    )
                digest = self._store_blob_from_source_uri(entry.source_uri)
                verified_entries += 1
                yield ManifestEntry(
                    path=entry.path,
                    hash=digest,
                    size=entry.size,
                    mtime_ns=entry.mtime_ns,
                    identity_mode="blake3",
                    identity_value=digest,
                    blob_hash=digest,
                    source_uri=entry.source_uri,
                )

        temp_manifest = self._write_temp_manifest(iter_verified_entries())
        if verified_entries == 0:
            temp_manifest.unlink(missing_ok=True)
            return VerifyResult(
                commit_id=base_commit_id,
                verified_entries=0,
                candidate_entries=candidate_entries,
                total_entries=total_entries,
                created_commit=False,
                dry_run=dry_run,
            )

        manifest_hash = blake3_digest_file(temp_manifest)
        self._persist_manifest(temp_manifest, manifest_hash)

        commit_body = {
            "message": f"verify {ref}",
            "manifest": manifest_hash,
            "parent": base_commit_id,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "branch": ref,
        }
        canonical = json.dumps(
            commit_body, sort_keys=True, separators=(",", ":")
        ).encode("utf-8")
        commit_id = blake3(canonical).hexdigest()
        commit_object = CommitObject(id=commit_id, **commit_body)
        self._commit_cache[commit_id] = commit_object

        self.store.write_commit_bytes(
            commit_id,
            (json.dumps(asdict(commit_object), indent=2, sort_keys=True) + "\n").encode(
                "utf-8"
            ),
        )
        self._update_branch_ref(
            branch=ref,
            commit_id=commit_id,
            expected_version_token=branch_state.version_token,
            expected_commit_id=branch_state.commit_id,
            operation="verify",
        )
        return VerifyResult(
            commit_id=commit_id,
            verified_entries=verified_entries,
            candidate_entries=candidate_entries,
            total_entries=total_entries,
            created_commit=True,
            dry_run=dry_run,
        )

    def _manifest_index(self, manifest_hash: str) -> dict[str, ManifestEntry]:
        index: dict[str, ManifestEntry] = {}
        for entry in self.store.iter_manifest_entries(manifest_hash):
            index[entry.path] = entry
        return index

    def resolve_entries(
        self, ref: str, *, include_staging: bool = False
    ) -> dict[str, ManifestEntry]:
        commit = self.read_commit(self.resolve_ref(ref))
        index = self._manifest_index(commit.manifest)
        if include_staging:
            for change in self._load_stage(ref).values():
                if change.action == "remove":
                    index.pop(change.path, None)
                    continue
                if change.action == "add":
                    index[change.path] = self._entry_from_stage_change(
                        change,
                        change.identity_mode or "blake3",
                        store_blob=False,
                    )
        return index

    def resolve_entries_for_prefix(
        self,
        ref: str,
        logical_prefix: str,
        *,
        include_staging: bool = False,
        commit_id: str | None = None,
    ) -> dict[str, ManifestEntry]:
        normalized_prefix = logical_prefix.strip("/")
        resolved_commit_id = commit_id or self.resolve_ref(ref)
        commit = self.read_commit(resolved_commit_id)

        if not normalized_prefix:
            index = self._manifest_index(commit.manifest)
        else:
            index = {
                entry.path: entry
                for entry in self.store.iter_manifest_entries_for_prefix(
                    commit.manifest,
                    normalized_prefix,
                )
            }

        if include_staging:
            for change in self._load_stage(ref).values():
                if normalized_prefix and not _matches_logical_prefix(
                    change.path,
                    normalized_prefix,
                ):
                    continue
                if change.action == "remove":
                    index.pop(change.path, None)
                    continue
                if change.action == "add":
                    index[change.path] = self._entry_from_stage_change(
                        change,
                        change.identity_mode or "blake3",
                        store_blob=False,
                    )
        return index

    def resolve_entry(
        self,
        ref: str,
        logical_path: str,
        *,
        include_staging: bool = False,
        commit_id: str | None = None,
    ) -> ManifestEntry | None:
        normalized_path = logical_path.strip("/")
        if not normalized_path:
            return None

        if include_staging:
            change = self._load_stage(ref).get(normalized_path)
            if change is not None:
                if change.action == "remove":
                    return None
                if change.action == "add":
                    return self._entry_from_stage_change(
                        change,
                        change.identity_mode or "blake3",
                        store_blob=False,
                    )

        resolved_commit_id = commit_id or self.resolve_ref(ref)
        commit = self.read_commit(resolved_commit_id)
        return self.store.lookup_manifest_entry(commit.manifest, normalized_path)

    def _materialize_blobs_and_entries(
        self, *, identity_mode: str
    ) -> Iterator[str]:
        for entry in walk_files(self.root):
            relative_path = entry.path.relative_to(self.root).as_posix()
            if identity_mode == "blake3":
                identity_value = blake3_digest_file(entry.path)
                self._store_blob(entry.path, identity_value)
                yield json.dumps(
                    ["b", relative_path, identity_value, entry.size, entry.mtime_ns],
                    separators=(",", ":"),
                )
            else:
                identity_value = metadata_identity(relative_path, entry.size)
                yield json.dumps(
                    ["m", relative_path, identity_value, entry.size, entry.mtime_ns, entry.path.as_uri()],
                    separators=(",", ":"),
                )

    def _materialize_s3_entries(
        self,
        *,
        source_uri: str,
        identity_mode: str,
        path_patterns: list[str] | None = None,
    ) -> Iterator[ManifestEntry]:
        _, prefix = parse_s3_uri(source_uri)
        normalized_prefix = prefix.strip("/")
        normalized_patterns = normalize_import_patterns(path_patterns)
        for obj in iter_s3_objects(source_uri):
            relative_path = normalize_s3_import_path(
                key=obj.key,
                prefix=normalized_prefix,
                size=obj.size,
            )
            if relative_path is None:
                continue
            if not matches_import_patterns(relative_path, normalized_patterns):
                continue
            if identity_mode == "blake3":
                identity_value = self._store_blob_from_source_uri(obj.source_uri)
                blob_hash = identity_value
            elif identity_mode == "meta":
                identity_value = metadata_identity(relative_path, obj.size)
                blob_hash = None
            else:
                raise ValueError("identity_mode must be one of: blake3, meta")
            yield ManifestEntry(
                path=relative_path,
                hash=identity_value,
                size=obj.size,
                mtime_ns=obj.mtime_ns,
                identity_mode=identity_mode,
                identity_value=identity_value,
                blob_hash=blob_hash,
                source_uri=obj.source_uri,
            )

    def _entry_from_working_path(
        self,
        relative_path: str,
        identity_mode: str,
        *,
        store_blob: bool,
    ) -> ManifestEntry:
        source_path = self.root / relative_path
        if not source_path.exists() or not source_path.is_file():
            raise FileNotFoundError(f"Cannot stage missing file: {relative_path}")
        stat = source_path.stat()
        source_uri = source_path.as_uri()
        if identity_mode == "blake3":
            identity_value = blake3_digest_file(source_path)
            blob_hash = identity_value
            if store_blob:
                self._store_blob(source_path, blob_hash)
            else:
                blob_hash = None
        elif identity_mode == "meta":
            identity_value = metadata_identity(relative_path, stat.st_size)
            blob_hash = None
        else:
            raise ValueError("identity_mode must be one of: blake3, meta")
        return ManifestEntry(
            path=relative_path,
            hash=identity_value,
            size=stat.st_size,
            mtime_ns=stat.st_mtime_ns,
            identity_mode=identity_mode,
            identity_value=identity_value,
            blob_hash=blob_hash,
            source_uri=source_uri,
        )

    def _entry_from_stage_change(
        self,
        change: StageChange,
        identity_mode: str,
        *,
        store_blob: bool,
    ) -> ManifestEntry:
        if change.source_uri is not None:
            return self._entry_from_source_uri(
                logical_path=change.path,
                source_uri=change.source_uri,
                identity_mode=identity_mode,
                store_blob=store_blob,
            )
        source_path = self.root / change.path
        if source_path.exists():
            return self._entry_from_working_path(
                change.path,
                identity_mode,
                store_blob=store_blob,
            )
        if change.blob_hash and identity_mode == "blake3":
            return ManifestEntry(
                path=change.path,
                hash=change.blob_hash,
                size=change.size or 0,
                mtime_ns=0,
                identity_mode="blake3",
                identity_value=change.blob_hash,
                blob_hash=change.blob_hash,
                source_uri=None,
            )
        raise FileNotFoundError(f"Cannot stage missing file: {change.path}")

    def _entry_from_source_uri(
        self,
        *,
        logical_path: str,
        source_uri: str,
        identity_mode: str,
        store_blob: bool,
    ) -> ManifestEntry:
        metadata = describe_source_uri(source_uri)
        if identity_mode == "blake3":
            identity_value = self._store_blob_from_source_uri(source_uri)
            blob_hash = identity_value if store_blob else None
        elif identity_mode == "meta":
            identity_value = metadata_identity(logical_path, metadata.size)
            blob_hash = None
        else:
            raise ValueError("identity_mode must be one of: blake3, meta")
        return ManifestEntry(
            path=logical_path,
            hash=identity_value,
            size=metadata.size,
            mtime_ns=metadata.mtime_ns,
            identity_mode=identity_mode,
            identity_value=identity_value,
            blob_hash=blob_hash,
            source_uri=metadata.source_uri,
        )

    def _expand_stage_source(
        self,
        raw_source: str,
        *,
        destination_path: str | None,
    ) -> list[tuple[str, str]]:
        if raw_source.startswith("s3://"):
            return self._expand_s3_stage_source(
                raw_source,
                destination_path=destination_path,
            )

        return self._expand_local_stage_source(
            raw_source,
            destination_path=destination_path,
        )

    def _expand_local_stage_source(
        self,
        raw_source: str,
        *,
        destination_path: str | None,
    ) -> list[tuple[str, str]]:
        raw_path = Path(raw_source).expanduser()
        source_path = (
            raw_path.resolve()
            if raw_path.is_absolute()
            else (self.root / raw_path).resolve()
        )
        if not source_path.exists():
            raise FileNotFoundError(f"Cannot stage missing path: {raw_source}")

        if source_path.is_file():
            logical_path = self._single_local_logical_path(
                source_path,
                destination_path=destination_path,
            )
            return [(logical_path, source_path.as_uri())]

        if not source_path.is_dir():
            raise FileNotFoundError(f"Cannot stage unsupported path: {raw_source}")

        repo_relative_dir: str | None
        try:
            repo_relative_dir = normalize_repository_path(
                source_path.relative_to(self.root).as_posix()
            )
        except ValueError:
            repo_relative_dir = None

        destination_prefix = (
            normalize_repository_path(destination_path)
            if destination_path is not None
            else repo_relative_dir or normalize_repository_path(source_path.name)
        )

        staged_entries: list[tuple[str, str]] = []
        for file_entry in walk_files(source_path):
            relative_suffix = file_entry.path.relative_to(source_path).as_posix()
            logical_path = normalize_repository_path(
                PurePosixPath(destination_prefix, relative_suffix).as_posix()
            )
            staged_entries.append((logical_path, file_entry.path.as_uri()))

        if not staged_entries:
            raise FileNotFoundError(f"Cannot stage empty directory: {raw_source}")
        return staged_entries

    def _single_local_logical_path(
        self,
        source_path: Path,
        *,
        destination_path: str | None,
    ) -> str:
        if destination_path is not None:
            return normalize_repository_path(destination_path)
        try:
            return normalize_repository_path(
                source_path.relative_to(self.root).as_posix()
            )
        except ValueError:
            return normalize_repository_path(source_path.name)

    def _expand_s3_stage_source(
        self,
        raw_source: str,
        *,
        destination_path: str | None,
    ) -> list[tuple[str, str]]:
        bucket, key = parse_s3_uri(raw_source)
        normalized_key = key.strip("/")
        if not normalized_key:
            raise ValueError(f"S3 source cannot be bucket root: {raw_source}")

        objects = list(iter_s3_objects(raw_source))
        if not objects:
            raise FileNotFoundError(f"Cannot stage missing S3 path: {raw_source}")

        exact_object_uri = f"s3://{bucket}/{normalized_key}"
        exact_object_matches = [
            obj for obj in objects if obj.source_uri == exact_object_uri
        ]
        is_single_object = len(exact_object_matches) == 1 and len(objects) == 1

        if is_single_object:
            logical_path = (
                normalize_repository_path(destination_path)
                if destination_path is not None
                else normalize_repository_path(PurePosixPath(normalized_key).name)
            )
            return [(logical_path, exact_object_uri)]

        prefix = normalized_key
        destination_prefix = (
            normalize_repository_path(destination_path)
            if destination_path is not None
            else normalize_repository_path(PurePosixPath(prefix).name)
        )

        staged_entries: list[tuple[str, str]] = []
        for obj in objects:
            relative_path = normalize_s3_import_path(
                key=obj.key,
                prefix=prefix,
                size=obj.size,
            )
            if relative_path is None:
                continue
            logical_path = normalize_repository_path(
                PurePosixPath(destination_prefix, relative_path).as_posix()
            )
            staged_entries.append((logical_path, obj.source_uri))

        if not staged_entries and exact_object_matches:
            logical_path = (
                normalize_repository_path(destination_path)
                if destination_path is not None
                else normalize_repository_path(PurePosixPath(normalized_key).name)
            )
            return [(logical_path, exact_object_uri)]
        if not staged_entries:
            raise FileNotFoundError(f"Cannot stage empty S3 prefix: {raw_source}")
        return staged_entries

    def _load_stage(self, branch: str) -> dict[str, StageChange]:
        stage_payload = self.client_state.read_staging_payload(branch)
        if stage_payload is None:
            return {}
        raw = json.loads(stage_payload)
        if not isinstance(raw, list):
            raise ValueError("Stage file must contain a list")
        staged: dict[str, StageChange] = {}
        for item in raw:
            if not isinstance(item, dict):
                continue
            change = StageChange.from_dict(item)
            staged[change.path] = change
        return staged

    def _save_stage(self, branch: str, staged: dict[str, StageChange]) -> None:
        if not staged:
            self.client_state.write_staging_payload(branch, None)
            return
        payload = [
            _stage_change_dict(change)
            for change in sorted(staged.values(), key=lambda item: item.path)
        ]
        self.client_state.write_staging_payload(
            branch,
            json.dumps(payload, indent=2) + "\n",
        )

    def _ensure_branch_exists(self, branch: str) -> None:
        self._require_branch_state(branch)

    def _require_branch_state(self, branch: str) -> BranchRefState:
        cached_state = self.client_state.read_branch_snapshot(branch)
        if cached_state is not None:
            return BranchRefState(
                branch=branch,
                commit_id=cached_state.commit_id,
                version_token=cached_state.version_token,
            )

        branch_state = self.store.read_branch_ref(branch)
        if branch_state is None:
            raise ValueError(f"Unknown branch: {branch}")
        self.client_state.write_branch_snapshot(
            branch,
            commit_id=branch_state.commit_id,
            version_token=branch_state.version_token,
        )
        return branch_state

    def _require_commit_for_metadata_mutation(self, branch: str) -> CommitObject:
        branch_state = self._require_branch_state(branch)
        if not branch_state.commit_id:
            raise FileNotFoundError(
                f"Branch '{branch}' has no committed manifest to mutate"
            )
        return self.read_commit(branch_state.commit_id)

    def _branch_head_commit(self, branch: str) -> str | None:
        branch_ref = self.store.read_branch_ref(branch)
        if branch_ref is None:
            return None
        return branch_ref.commit_id

    def _is_ancestor(self, *, ancestor_commit: str, descendant_commit: str) -> bool:
        current_commit: str | None = descendant_commit
        while current_commit:
            if current_commit == ancestor_commit:
                return True
            current_commit = self.read_commit(current_commit).parent
        return False

    def _write_commit_object(
        self,
        *,
        branch: str,
        message: str,
        parent_commit: str | None,
        manifest_hash: str,
        expected_version_token: str | None,
        operation: str,
    ) -> str:
        commit_body = {
            "message": message,
            "manifest": manifest_hash,
            "parent": parent_commit,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "branch": branch,
        }
        canonical = json.dumps(
            commit_body, sort_keys=True, separators=(",", ":")
        ).encode("utf-8")
        commit_id = blake3(canonical).hexdigest()
        commit_object = CommitObject(id=commit_id, **commit_body)
        self._commit_cache[commit_id] = commit_object
        self.store.write_commit_bytes(
            commit_id,
            (json.dumps(asdict(commit_object), indent=2, sort_keys=True) + "\n").encode(
                "utf-8"
            ),
        )
        self._update_branch_ref(
            branch=branch,
            commit_id=commit_id,
            expected_version_token=expected_version_token,
            expected_commit_id=parent_commit,
            operation=operation,
        )
        return commit_id

    def _commit_staged(self, *, message: str, branch_state: BranchRefState) -> str:
        staged = self._load_stage(branch_state.branch)
        if not staged:
            raise ValueError(f"No staged changes for branch: {branch_state.branch}")

        parent_commit = branch_state.commit_id

        removed_paths: set[str] = set()
        added_entries: list[ManifestEntry] = []
        for change in staged.values():
            if change.action == "remove":
                removed_paths.add(change.path)
            elif change.action == "add":
                added_entries.append(
                    self._entry_from_stage_change(
                        change,
                        change.identity_mode or "blake3",
                        store_blob=True,
                    )
                )
            else:
                raise ValueError(f"Unknown stage action: {change.action}")
        added_entries.sort(key=lambda e: e.path)

        parent_manifest: str | None = None
        if parent_commit:
            parent = self.read_commit(parent_commit)
            parent_manifest = parent.manifest

        temp_manifest = self._write_temp_manifest(
            _merge_sorted_entry_streams(
                base_entries=(
                    self.store.iter_manifest_entries(parent_manifest)
                    if parent_manifest
                    else None
                ),
                new_entries=iter(added_entries),
                removed_paths=removed_paths,
            )
        )
        manifest_hash = blake3_digest_file(temp_manifest)
        self._persist_manifest(temp_manifest, manifest_hash)

        commit_id = self._write_commit_object(
            branch=branch_state.branch,
            message=message,
            parent_commit=parent_commit,
            manifest_hash=manifest_hash,
            expected_version_token=branch_state.version_token,
            operation="commit",
        )
        self._save_stage(branch_state.branch, {})
        return commit_id

    def _store_blob(self, source_file: Path, content_hash: str) -> None:
        self.store.write_blob_file(content_hash, source_file, if_missing=True)

    def _store_blob_from_source_uri(self, source_uri: str) -> str:
        with NamedTemporaryFile(mode="wb", delete=False) as temp:
            temp_path = Path(temp.name)
            hasher = blake3()
            with open_source_uri(source_uri) as source:
                while True:
                    chunk = source.read(DEFAULT_CHUNK_SIZE)
                    if not chunk:
                        break
                    hasher.update(chunk)
                    temp.write(chunk)
        digest = hasher.hexdigest()
        if self.store.object_exists("blob", digest):
            temp_path.unlink(missing_ok=True)
            return digest
        self.store.write_blob_file(digest, temp_path, if_missing=True)
        temp_path.unlink(missing_ok=True)
        return digest

    def read_blob(self, blob_hash: str) -> bytes:
        return self.store.read_blob_bytes(blob_hash)

    def open_blob_stream(self, blob_hash: str) -> BinaryIO:
        return self.store.open_blob(blob_hash)

    def restore_files(
        self,
        ref: str,
        paths: list[str] | None = None,
        *,
        force: bool = False,
    ) -> list[str]:
        commit_id = self.resolve_ref(ref)
        commit = self.read_commit(commit_id)

        if paths:
            entries: dict[str, ManifestEntry] = {}
            for p in paths:
                entry = self.store.lookup_manifest_entry(commit.manifest, p)
                if entry is not None:
                    entries[p] = entry
        else:
            entries = {
                entry.path: entry
                for entry in self.store.iter_manifest_entries(commit.manifest)
            }

        restored: list[str] = []
        for logical_path, entry in sorted(entries.items()):
            target = self.root / logical_path
            if target.exists() and not force:
                continue
            target.parent.mkdir(parents=True, exist_ok=True)
            if entry.blob_hash:
                target.write_bytes(self.read_blob(entry.blob_hash))
                restored.append(logical_path)
            elif entry.source_uri:
                source_file = next(open_source_uri(entry.source_uri))
                with source_file:
                    target.write_bytes(source_file.read())
                restored.append(logical_path)
        return restored

    def generate_transfer_commands(
        self,
        ref: str | None = None,
        *,
        mode: str = "upload",
        include_metadata: bool = False,
    ) -> list[str]:
        config = load_config(self.root)
        if config is None or config.backend != "s3" or config.s3 is None:
            raise ValueError("S3 backend not configured in repo config")
        bucket = config.s3.bucket

        branch = ref or self.current_branch()
        commit_id = self.resolve_ref(branch)
        commit = self.read_commit(commit_id)
        s3_prefix = config.s3.prefix or ""

        local_store = LocalRepositoryStore(self.root)
        commands: list[str] = []
        seen: set[str] = set()

        for entry in local_store.iter_manifest_entries(commit.manifest):
            if entry.blob_hash and entry.blob_hash not in seen:
                seen.add(entry.blob_hash)
                rel = blob_relpath(entry.blob_hash).as_posix()
                s3_key = f"blobs/{rel}"
                if s3_prefix:
                    s3_key = f"{s3_prefix}/{s3_key}"
                s3_uri = f"s3://{bucket}/{s3_key}"
                local = str(self.layout.blobs_dir / rel)
                if mode == "upload":
                    commands.append(f"cp {local} {s3_uri}")
                else:
                    commands.append(f"cp {s3_uri} {local}")

        if include_metadata:
            for manifest_hash, extension in [(commit.manifest, "jsonl"), (commit.manifest, "idx")]:
                s3_key = f"manifests/{manifest_hash}.{extension}"
                if s3_prefix:
                    s3_key = f"{s3_prefix}/{s3_key}"
                s3_uri = f"s3://{bucket}/{s3_key}"
                local = str(self.layout.manifests_dir / f"{manifest_hash}.{extension}")
                if mode == "upload":
                    commands.append(f"cp {local} {s3_uri}")
                else:
                    commands.append(f"cp {s3_uri} {local}")

            commit_s3_key = f"commits/{commit_id}.json"
            if s3_prefix:
                commit_s3_key = f"{s3_prefix}/{commit_s3_key}"
            commit_s3_uri = f"s3://{bucket}/{commit_s3_key}"
            commit_local = str(self.layout.commits_dir / f"{commit_id}.json")
            if mode == "upload":
                commands.append(f"cp {commit_local} {commit_s3_uri}")
            else:
                commands.append(f"cp {commit_s3_uri} {commit_local}")

        return commands

    def _persist_manifest(self, temp_manifest: Path, manifest_hash: str) -> None:
        temp_index = self._write_temp_manifest_index(temp_manifest)
        try:
            if self.store.object_exists("manifest", manifest_hash):
                temp_manifest.unlink(missing_ok=True)
            else:
                self.store.write_manifest_file(
                    manifest_hash, temp_manifest, if_missing=True
                )

            if self.store.object_exists("manifest-index", manifest_hash):
                temp_index.unlink(missing_ok=True)
            else:
                self.store.write_manifest_index_file(
                    manifest_hash,
                    temp_index,
                    if_missing=True,
                )
        finally:
            temp_manifest.unlink(missing_ok=True)
            temp_index.unlink(missing_ok=True)

    def _update_branch_ref(
        self,
        *,
        branch: str,
        commit_id: str | None,
        expected_version_token: str | None,
        expected_commit_id: str | None,
        operation: str,
    ) -> None:
        updated = self.store.compare_and_set_branch_ref(
            branch,
            commit_id,
            expected_version_token=expected_version_token,
            expected_commit_id=expected_commit_id,
        )
        if updated:
            self._resolved_ref_cache.pop(branch, None)
            current_state = self.store.read_branch_ref(branch)
            if current_state is not None:
                self._resolved_ref_cache[branch] = current_state
                self.client_state.write_branch_snapshot(
                    branch,
                    commit_id=current_state.commit_id,
                    version_token=current_state.version_token,
                )
            return
        current_state = self.store.read_branch_ref(branch)
        if current_state is not None:
            self.client_state.write_branch_snapshot(
                branch,
                commit_id=current_state.commit_id,
                version_token=current_state.version_token,
            )
        raise RefConflictError(
            branch=branch,
            operation=operation,
            expected_commit_id=expected_commit_id,
            current_commit_id=current_state.commit_id if current_state else None,
        )

    def _write_temp_manifest(self, entries: Iterator[ManifestEntry | str]) -> Path:
        with NamedTemporaryFile(
            mode="w", suffix=".jsonl", delete=False, encoding="utf-8"
        ) as temp:
            temp_path = Path(temp.name)
        writer = ManifestWriter(temp_path)
        writer.write_entries(entries)
        return temp_path

    def _write_temp_manifest_index(self, manifest_path: Path) -> Path:
        return build_manifest_index_file(manifest_path)


def _default_remote_client_root(worktree_root: Path, repo_uri: str) -> Path:
    repo_id = blake3(repo_uri.encode("utf-8")).hexdigest()[:16]
    return worktree_root / ".fluxel" / "clients" / repo_id


def _matches_logical_prefix(path: str, logical_prefix: str) -> bool:
    return path == logical_prefix or path.startswith(f"{logical_prefix}/")


def _stage_change_dict(change: StageChange) -> dict[str, object]:
    d: dict[str, object] = {
        "path": change.path,
        "action": change.action,
    }
    if change.identity_mode is not None:
        d["identity_mode"] = change.identity_mode
    if change.source_uri is not None:
        d["source_uri"] = change.source_uri
    if change.blob_hash is not None:
        d["blob_hash"] = change.blob_hash
    if change.size is not None:
        d["size"] = change.size
    return d


def _path_is_removed(path: str, removed: set[str]) -> bool:
    if path in removed:
        return True
    for rp in removed:
        if path.startswith(f"{rp}/"):
            return True
    return False


def _merge_sorted_entry_streams(
    base_entries: Iterator[ManifestEntry] | None,
    new_entries: Iterator[ManifestEntry],
    removed_paths: set[str] = frozenset(),
) -> Iterator[ManifestEntry]:
    """Merge two sorted entry streams without loading either fully in memory.

    Both iterators MUST yield entries sorted by path in ascending order.
    new_entries wins on path conflict (overwrite semantics).
    Paths in removed_paths (and their descendants) are excluded from the base stream.

    O(1) memory, O(N) time.
    """
    if base_entries is None:
        yield from new_entries
        return

    base = next(base_entries, None)
    new = next(new_entries, None)

    while base is not None or new is not None:
        if base is None:
            yield new
            new = next(new_entries, None)
        elif new is None:
            if not _path_is_removed(base.path, removed_paths):
                yield base
            base = next(base_entries, None)
        elif base.path == new.path:
            # New wins: skip the base entry, emit the new one
            if new.path not in removed_paths:
                yield new
            base = next(base_entries, None)
            new = next(new_entries, None)
        elif base.path < new.path:
            if not _path_is_removed(base.path, removed_paths):
                yield base
            base = next(base_entries, None)
        else:
            if new.path not in removed_paths:
                yield new
            new = next(new_entries, None)


def open_repository(
    root: str | Path,
    *,
    worktree: str | Path | None = None,
    client_root: str | Path | None = None,
    s3_client: object | None = None,
    blob_transfer: BlobTransferBackend | str | None = None,
) -> FluxelRepository:
    if isinstance(blob_transfer, str):
        blob_transfer = build_blob_transfer_backend(blob_transfer)

    if isinstance(root, str) and root.startswith("s3://"):
        bucket, prefix = parse_s3_uri(root)
        worktree_root = Path(worktree or ".").resolve()
        resolved_client_root = (
            Path(client_root).resolve()
            if client_root
            else _default_remote_client_root(worktree_root, root)
        )
        return FluxelRepository(
            worktree_root,
            store=S3RepositoryStore(
                bucket,
                prefix,
                client=s3_client,
                branch_root=worktree_root / ".fluxel" / "refs" / "heads",
                blob_transfer=blob_transfer,
            ),
            client_state=LocalClientState(resolved_client_root),
        )

    repo_root = Path(root).resolve()
    config = load_config(repo_root)

    if config is not None and config.backend == "s3" and config.s3 is not None:
        bucket = config.s3.bucket
        prefix = config.s3.prefix
        worktree_root = Path(worktree or repo_root)
        if worktree or client_root:
            resolved_client_root = (
                Path(client_root).resolve() if client_root else worktree_root
            )
        else:
            resolved_client_root = worktree_root
        return FluxelRepository(
            worktree_root,
            store=S3RepositoryStore(
                bucket,
                prefix,
                client=s3_client,
                branch_root=worktree_root / ".fluxel" / "refs" / "heads",
                blob_transfer=blob_transfer,
            ),
            client_state=LocalClientState(resolved_client_root),
        )

    worktree_root = Path(worktree).resolve() if worktree else repo_root
    resolved_client_root = Path(client_root).resolve() if client_root else repo_root
    return FluxelRepository(
        worktree_root,
        store=LocalRepositoryStore(repo_root),
        client_state=LocalClientState(resolved_client_root),
    )


def commit(
    root: str | Path,
    message: str,
    identity_mode: str = "blake3",
    *,
    staged: bool = False,
    ref: str | None = None,
    blob_transfer: BlobTransferBackend | str | None = None,
) -> str:
    return open_repository(root, blob_transfer=blob_transfer).commit(
        message,
        identity_mode=identity_mode,
        staged=staged,
        ref=ref,
    )


def import_s3(
    root: str | Path,
    source_uri: str,
    message: str,
    identity_mode: str = "blake3",
    *,
    path_patterns: list[str] | None = None,
    ref: str | None = None,
    blob_transfer: BlobTransferBackend | str | None = None,
) -> str:
    return open_repository(root, blob_transfer=blob_transfer).import_s3(
        source_uri,
        message,
        identity_mode=identity_mode,
        path_patterns=path_patterns,
        ref=ref,
    )


def branch(root: str | Path, name: str) -> Path:
    return open_repository(root).branch(name)


def merge(root: str | Path, source_ref: str, target_ref: str) -> MergeResult:
    return open_repository(root).merge(source_ref, target_ref)


def add(
    root: str | Path,
    paths: list[str],
    *,
    ref: str | None = None,
    identity_mode: str = "blake3",
    destination_path: str | None = None,
    blob_transfer: BlobTransferBackend | str | None = None,
) -> StageStatus:
    return open_repository(root, blob_transfer=blob_transfer).add(
        paths,
        ref=ref,
        identity_mode=identity_mode,
        destination_path=destination_path,
    )


def rm(root: str | Path, paths: list[str], *, ref: str | None = None) -> StageStatus:
    return open_repository(root).rm(paths, ref=ref)


def remove(
    root: str | Path,
    paths: list[str],
    message: str,
    *,
    ref: str | None = None,
) -> RemoveResult:
    return open_repository(root).remove_paths(paths, message, ref=ref)


def move(
    root: str | Path,
    source_path: str,
    destination_path: str,
    message: str,
    *,
    ref: str | None = None,
) -> MoveResult:
    return open_repository(root).move(
        source_path,
        destination_path,
        message,
        ref=ref,
    )


def move_staged(
    root: str | Path,
    source_path: str,
    destination_path: str,
    *,
    ref: str | None = None,
) -> StageStatus:
    """Stage a move operation without committing."""
    repo = open_repository(root)
    branch = ref or repo.current_branch()
    repo._ensure_branch_exists(branch)
    stage = repo._load_stage(branch)

    source = normalize_repository_path(source_path)
    destination = normalize_repository_path(destination_path)

    if source == destination:
        raise ValueError("Source and destination paths must differ")

    # Find all entries matching the source path in the current manifest
    base_commit = repo.read_commit(repo.resolve_ref(branch))
    moved_count = 0

    for entry in repo.store.iter_manifest_entries(base_commit.manifest):
        if not matches_logical_path(entry.path, source):
            continue

        moved_path = move_logical_path(
            entry.path,
            source_path=source,
            destination_path=destination,
        )

        # Stage the remove + add as separate operations
        stage[entry.path] = StageChange(
            path=entry.path,
            action="remove",
            identity_mode=None,
            source_uri=None,
        )
        stage[moved_path] = StageChange(
            path=moved_path,
            action="add",
            identity_mode=entry.identity_mode,
            source_uri=entry.source_uri,
            blob_hash=entry.blob_hash,
            size=entry.size,
        )
        moved_count += 1

    if moved_count == 0:
        raise FileNotFoundError(f"Path not found in branch '{branch}': {source}")

    repo._save_stage(branch, stage)
    return repo.status(ref=branch)


def status(root: str | Path, *, ref: str | None = None, working_tree: bool = True) -> StageStatus:
    return open_repository(root).status(ref=ref, working_tree=working_tree)


def diff(root: str | Path, from_ref: str, to_ref: str) -> list[DiffEntry]:
    return open_repository(root).diff(from_ref=from_ref, to_ref=to_ref)


def verify(
    root: str | Path,
    ref: str = "main",
    path_prefixes: list[str] | None = None,
    *,
    dry_run: bool = False,
    blob_transfer: BlobTransferBackend | str | None = None,
) -> VerifyResult:
    return open_repository(root, blob_transfer=blob_transfer).verify(
        ref=ref, path_prefixes=path_prefixes, dry_run=dry_run
    )


def log(root: str | Path, ref: str) -> Iterator[CommitObject]:
    return open_repository(root).log(ref)


def checkout(root: str | Path, branch: str) -> None:
    open_repository(root).set_current_branch(branch)


def restore_files(
    root: str | Path,
    ref: str,
    paths: list[str] | None = None,
    *,
    force: bool = False,
    blob_transfer: BlobTransferBackend | str | None = None,
) -> list[str]:
    return open_repository(root, blob_transfer=blob_transfer).restore_files(
        ref, paths=paths, force=force
    )


def generate_transfer_commands(
    root: str | Path,
    ref: str | None = None,
    *,
    mode: str = "upload",
    include_metadata: bool = False,
) -> list[str]:
    repo_root = Path(root).resolve()
    store = LocalRepositoryStore(repo_root)
    repo = FluxelRepository(
        repo_root,
        store=store,
        client_state=LocalClientState(repo_root),
    )
    return repo.generate_transfer_commands(
        ref, mode=mode, include_metadata=include_metadata
    )
