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
from tempfile import NamedTemporaryFile
from typing import Iterator, Literal

from blake3 import blake3

from .client_state import LocalClientState
from .hashing import DEFAULT_CHUNK_SIZE, blake3_digest_file
from .layout import initialize_fluxel_layout
from .manifest import ManifestEntry, ManifestWriter, walk_files
from .repository_store import (
    BranchRefState,
    LocalRepositoryStore,
    RepositoryStore,
    S3RepositoryStore,
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
from .storage import iter_s3_objects, open_source_uri, parse_s3_uri


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

    @staticmethod
    def from_dict(data: dict[str, object]) -> "StageChange":
        action = str(data["action"])
        identity_mode_raw = data.get("identity_mode")
        return StageChange(
            path=str(data["path"]),
            action=action,
            identity_mode=(
                str(identity_mode_raw) if identity_mode_raw is not None else None
            ),
        )


@dataclass(frozen=True)
class StageStatus:
    ref: str
    added: list[str]
    removed: list[str]


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
    ) -> None:
        self.layout = initialize_fluxel_layout(root)
        self.store = store or LocalRepositoryStore(self.layout.root)
        self.client_state = client_state or LocalClientState(self.layout.root)
        self._ensure_head(default_branch="main")

    @property
    def root(self) -> Path:
        return self.layout.root

    def _branch_path(self, branch_name: str) -> Path:
        if isinstance(self.store, LocalRepositoryStore):
            return self.store.branch_path(branch_name)
        return self.layout.heads_dir / branch_name

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
        branch_ref = self.store.read_branch_ref(branch_or_commit)
        if branch_ref is not None:
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
        ):
            raise ValueError(f"Branch already exists: {name}")
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

        index: dict[str, ManifestEntry]
        if parent_commit:
            parent = self.read_commit(parent_commit)
            index = self._manifest_index(parent.manifest)
        else:
            index = {}

        for entry in self._materialize_s3_entries(
            source_uri=source_uri,
            identity_mode=identity_mode,
            path_patterns=path_patterns,
        ):
            index[entry.path] = entry

        def iter_entries() -> Iterator[ManifestEntry]:
            for path in sorted(index):
                yield index[path]

        temp_manifest = self._write_temp_manifest(iter_entries())
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
    ) -> StageStatus:
        if identity_mode not in {"blake3", "meta"}:
            raise ValueError("identity_mode must be one of: blake3, meta")
        branch = ref or self.current_branch()
        self._ensure_branch_exists(branch)
        staged = self._load_stage(branch)
        for path in paths:
            normalized = normalize_repository_path(path)
            source = self.root / normalized
            if not source.exists() or not source.is_file():
                raise FileNotFoundError(f"Cannot stage missing file: {normalized}")
            staged[normalized] = StageChange(
                path=normalized,
                action="add",
                identity_mode=identity_mode,
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

        def iter_entries() -> Iterator[ManifestEntry]:
            for entry in self.store.iter_manifest_entries(base_commit.manifest):
                moved_path = path_map.get(entry.path)
                if moved_path is None:
                    yield entry
                    continue
                yield relocate_manifest_entry(entry, moved_path)

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

    def status(self, *, ref: str | None = None) -> StageStatus:
        branch = ref or self.current_branch()
        self._ensure_branch_exists(branch)
        staged = self._load_stage(branch)
        added = sorted(
            change.path for change in staged.values() if change.action == "add"
        )
        removed = sorted(
            change.path for change in staged.values() if change.action == "remove"
        )
        return StageStatus(ref=branch, added=added, removed=removed)

    def read_commit(self, commit_id: str) -> CommitObject:
        commit_payload = self.store.read_commit_bytes(commit_id)
        if commit_payload is None:
            raise ValueError(f"Unknown commit: {commit_id}")
        data = json.loads(commit_payload.decode("utf-8"))
        return CommitObject(
            id=str(data["id"]),
            message=str(data["message"]),
            manifest=str(data["manifest"]),
            parent=str(data["parent"]) if data["parent"] else None,
            created_at=str(data["created_at"]),
            branch=str(data["branch"]),
        )

    def diff(self, from_ref: str, to_ref: str) -> list[DiffEntry]:
        from_commit = self.read_commit(self.resolve_ref(from_ref))
        to_commit = self.read_commit(self.resolve_ref(to_ref))
        from_entries = self._manifest_index(from_commit.manifest)
        to_entries = self._manifest_index(to_commit.manifest)

        changes: list[DiffEntry] = []

        from_only = sorted(set(from_entries) - set(to_entries))
        for path in from_only:
            before = from_entries[path]
            changes.append(
                DiffEntry(
                    path=path,
                    change="removed",
                    before_hash=before.hash,
                    after_hash=None,
                    before_size=before.size,
                    after_size=None,
                )
            )

        to_only = sorted(set(to_entries) - set(from_entries))
        for path in to_only:
            after = to_entries[path]
            changes.append(
                DiffEntry(
                    path=path,
                    change="added",
                    before_hash=None,
                    after_hash=after.hash,
                    before_size=None,
                    after_size=after.size,
                )
            )

        shared = sorted(set(from_entries) & set(to_entries))
        for path in shared:
            before = from_entries[path]
            after = to_entries[path]
            if before.hash == after.hash and before.size == after.size:
                continue
            changes.append(
                DiffEntry(
                    path=path,
                    change="modified",
                    before_hash=before.hash,
                    after_hash=after.hash,
                    before_size=before.size,
                    after_size=after.size,
                )
            )

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
                    index[change.path] = self._entry_from_working_path(
                        change.path,
                        change.identity_mode or "blake3",
                        store_blob=False,
                    )
        return index

    def _materialize_blobs_and_entries(
        self, *, identity_mode: str
    ) -> Iterator[ManifestEntry]:
        for file_path in walk_files(self.root):
            stat = file_path.stat()
            relative_path = file_path.relative_to(self.root).as_posix()
            source_uri = file_path.as_uri()
            if identity_mode == "blake3":
                identity_value = blake3_digest_file(file_path)
                blob_hash = identity_value
                self._store_blob(file_path, blob_hash)
            else:
                identity_value = metadata_identity(relative_path, stat.st_size)
                blob_hash = None
            yield ManifestEntry(
                path=relative_path,
                hash=identity_value,
                size=stat.st_size,
                mtime_ns=stat.st_mtime_ns,
                identity_mode=identity_mode,
                identity_value=identity_value,
                blob_hash=blob_hash,
                source_uri=source_uri,
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
            {
                "path": change.path,
                "action": change.action,
                "identity_mode": change.identity_mode,
            }
            for change in sorted(staged.values(), key=lambda item: item.path)
        ]
        self.client_state.write_staging_payload(
            branch,
            json.dumps(payload, indent=2) + "\n",
        )

    def _ensure_branch_exists(self, branch: str) -> None:
        self._require_branch_state(branch)

    def _require_branch_state(self, branch: str) -> BranchRefState:
        branch_state = self.store.read_branch_ref(branch)
        if branch_state is None:
            raise ValueError(f"Unknown branch: {branch}")
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
        index: dict[str, ManifestEntry]
        if parent_commit:
            parent = self.read_commit(parent_commit)
            index = self._manifest_index(parent.manifest)
        else:
            index = {}

        for change in staged.values():
            if change.action == "remove":
                index.pop(change.path, None)
                continue
            if change.action == "add":
                index[change.path] = self._entry_from_working_path(
                    change.path,
                    change.identity_mode or "blake3",
                    store_blob=True,
                )
                continue
            raise ValueError(f"Unknown stage action: {change.action}")

        def iter_entries() -> Iterator[ManifestEntry]:
            for path in sorted(index):
                yield index[path]

        temp_manifest = self._write_temp_manifest(iter_entries())
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

    def _persist_manifest(self, temp_manifest: Path, manifest_hash: str) -> None:
        if self.store.object_exists("manifest", manifest_hash):
            temp_manifest.unlink(missing_ok=True)
            return
        self.store.write_manifest_file(manifest_hash, temp_manifest, if_missing=True)

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
        )
        if updated:
            return
        current_state = self.store.read_branch_ref(branch)
        raise RefConflictError(
            branch=branch,
            operation=operation,
            expected_commit_id=expected_commit_id,
            current_commit_id=current_state.commit_id if current_state else None,
        )

    def _write_temp_manifest(self, entries: Iterator[ManifestEntry]) -> Path:
        with NamedTemporaryFile(
            mode="w", suffix=".jsonl", delete=False, encoding="utf-8"
        ) as temp:
            temp_path = Path(temp.name)
        writer = ManifestWriter(temp_path)
        writer.write_entries(entries)
        return temp_path


def _default_remote_client_root(worktree_root: Path, repo_uri: str) -> Path:
    repo_id = blake3(repo_uri.encode("utf-8")).hexdigest()[:16]
    return worktree_root / ".fluxel" / "clients" / repo_id


def open_repository(
    root: str | Path,
    *,
    worktree: str | Path | None = None,
    client_root: str | Path | None = None,
    s3_client: object | None = None,
) -> FluxelRepository:
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
            store=S3RepositoryStore(bucket, prefix, client=s3_client),
            client_state=LocalClientState(resolved_client_root),
        )

    repo_root = Path(root).resolve()
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
) -> str:
    return open_repository(root).commit(
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
) -> str:
    return open_repository(root).import_s3(
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
) -> StageStatus:
    return open_repository(root).add(paths, ref=ref, identity_mode=identity_mode)


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


def status(root: str | Path, *, ref: str | None = None) -> StageStatus:
    return open_repository(root).status(ref=ref)


def diff(root: str | Path, from_ref: str, to_ref: str) -> list[DiffEntry]:
    return open_repository(root).diff(from_ref=from_ref, to_ref=to_ref)


def verify(
    root: str | Path,
    ref: str = "main",
    path_prefixes: list[str] | None = None,
    *,
    dry_run: bool = False,
) -> VerifyResult:
    return open_repository(root).verify(
        ref=ref, path_prefixes=path_prefixes, dry_run=dry_run
    )
