"""Operation methods extracted from FluxelRepository.

Each function takes a FluxelRepository as its first argument and implements
one of the larger "operation" methods that were previously methods on the class.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Iterator, Literal
from .repository import StageChange
from .hashing import blake3_digest_file

from .manifest import ManifestEntry
from .repository_support import (
    matches_any_logical_path,
    matches_logical_path,
    move_logical_path,
    normalize_logical_paths,
    normalize_repository_path,
    relocate_manifest_entry,
)

if TYPE_CHECKING:
    from .repository import (
        FluxelRepository,
        MergeResult,
        MoveResult,
        RemoveResult,
        StageStatus,
        VerifyResult,
    )


def repo_add(
    repo: FluxelRepository,
    paths: list[str],
    *,
    ref: str | None = None,
    identity_mode: str = "blake3",
    destination_path: str | None = None,
) -> StageStatus:
    if identity_mode not in {"blake3", "meta"}:
        raise ValueError("identity_mode must be one of: blake3, meta")
    branch = ref or repo.current_branch()
    repo._ensure_branch_exists(branch)
    staged = repo._load_stage(branch)
    if destination_path is not None and len(paths) != 1:
        raise ValueError("--as can only be used when staging exactly one source")
    for raw_source in paths:
        for logical_path, source_uri in repo._expand_stage_source(
            raw_source,
            destination_path=destination_path,
        ):
            staged[logical_path] = StageChange(
                path=logical_path,
                action="add",
                identity_mode=identity_mode,
                source_uri=source_uri,
            )
    repo._save_stage(branch, staged)
    return repo.status(ref=branch)


def repo_import_s3(
    repo: FluxelRepository,
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
    branch = ref or repo.current_branch()
    branch_state = repo._require_branch_state(branch)
    parent_commit = branch_state.commit_id

    parent_manifest: str | None = None
    if parent_commit:
        parent = repo.read_commit(parent_commit)
        parent_manifest = parent.manifest

    from .repository import _merge_sorted_entry_streams

    temp_manifest = repo._write_temp_manifest(
        _merge_sorted_entry_streams(
            base_entries=(
                repo.store.iter_manifest_entries(parent_manifest)
                if parent_manifest
                else None
            ),
            new_entries=repo._materialize_s3_entries(
                source_uri=source_uri,
                identity_mode=identity_mode,
                path_patterns=path_patterns,
            ),
        )
    )
    manifest_hash = blake3_digest_file(temp_manifest)
    repo._persist_manifest(temp_manifest, manifest_hash)

    return repo._write_commit_object(
        branch=branch,
        message=message,
        parent_commit=parent_commit,
        manifest_hash=manifest_hash,
        expected_version_token=branch_state.version_token,
        operation="commit",
    )


def repo_rm(
    repo: FluxelRepository, paths: list[str], *, ref: str | None = None
) -> StageStatus:
    branch = ref or repo.current_branch()
    repo._ensure_branch_exists(branch)
    staged = repo._load_stage(branch)
    for path in paths:
        normalized = normalize_repository_path(path)
        staged[normalized] = StageChange(path=normalized, action="remove")
    repo._save_stage(branch, staged)
    return repo.status(ref=branch)


def repo_remove_paths(
    repo: FluxelRepository,
    paths: list[str],
    message: str,
    *,
    ref: str | None = None,
) -> RemoveResult:
    if not message.strip():
        raise ValueError("Commit message cannot be empty")
    branch = ref or repo.current_branch()
    branch_state = repo._require_branch_state(branch)
    base_commit = repo._require_commit_for_metadata_mutation(branch_state.branch)
    normalized_paths = normalize_logical_paths(paths)
    removed_paths: set[str] = set()

    def iter_entries() -> Iterator[ManifestEntry]:
        nonlocal removed_paths
        for entry in repo.store.iter_manifest_entries(base_commit.manifest):
            if matches_any_logical_path(entry.path, normalized_paths):
                removed_paths.add(entry.path)
                continue
            yield entry

    temp_manifest = repo._write_temp_manifest(iter_entries())
    if not removed_paths:
        temp_manifest.unlink(missing_ok=True)
        missing = ", ".join(normalized_paths)
        raise FileNotFoundError(f"Path not found in branch '{branch}': {missing}")

    manifest_hash = blake3_digest_file(temp_manifest)
    repo._persist_manifest(temp_manifest, manifest_hash)
    commit_id = repo._write_commit_object(
        branch=branch,
        message=message,
        parent_commit=branch_state.commit_id,
        manifest_hash=manifest_hash,
        expected_version_token=branch_state.version_token,
        operation="rm",
    )
    from .repository import RemoveResult as RR

    return RR(
        ref=branch,
        commit_id=commit_id,
        removed_paths=sorted(removed_paths),
    )


def repo_move(
    repo: FluxelRepository,
    source_path: str,
    destination_path: str,
    message: str,
    *,
    ref: str | None = None,
) -> MoveResult:
    if not message.strip():
        raise ValueError("Commit message cannot be empty")
    branch = ref or repo.current_branch()
    branch_state = repo._require_branch_state(branch)
    base_commit = repo._require_commit_for_metadata_mutation(branch_state.branch)
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
    updated_entries: dict[str, ManifestEntry] = {}

    for entry in repo.store.iter_manifest_entries(base_commit.manifest):
        existing_paths.add(entry.path)
        if not matches_logical_path(entry.path, source):
            updated_entries[entry.path] = entry
            continue
        moved_path = move_logical_path(
            entry.path,
            source_path=source,
            destination_path=destination,
        )
        source_paths.append(entry.path)
        moved_paths.append(moved_path)
        path_map[entry.path] = moved_path
        relocated_entry = relocate_manifest_entry(entry, moved_path)
        updated_entries[relocated_entry.path] = relocated_entry

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
        for path in sorted(updated_entries):
            yield updated_entries[path]

    temp_manifest = repo._write_temp_manifest(iter_entries())
    manifest_hash = blake3_digest_file(temp_manifest)
    repo._persist_manifest(temp_manifest, manifest_hash)
    commit_id = repo._write_commit_object(
        branch=branch,
        message=message,
        parent_commit=branch_state.commit_id,
        manifest_hash=manifest_hash,
        expected_version_token=branch_state.version_token,
        operation="mv",
    )
    from .repository import MoveResult as MR

    return MR(
        ref=branch,
        commit_id=commit_id,
        source_path=source,
        destination_path=destination,
        moved_paths=sorted(moved_paths),
    )


def repo_move_staged(
    repo: FluxelRepository,
    source_path: str,
    destination_path: str,
    *,
    ref: str | None = None,
) -> StageStatus:
    """Stage a move operation without committing."""
    branch = ref or repo.current_branch()
    repo._ensure_branch_exists(branch)
    stage = repo._load_stage(branch)

    source = normalize_repository_path(source_path)
    destination = normalize_repository_path(destination_path)

    if source == destination:
        raise ValueError("Source and destination paths must differ")

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


def repo_verify(
    repo: FluxelRepository,
    ref: str | None = None,
    path_prefixes: list[str] | None = None,
    *,
    dry_run: bool = False,
) -> VerifyResult:
    if ref is None:
        ref = repo.current_branch()
    branch_state = repo._require_branch_state(ref)

    base_commit_id = repo.resolve_ref(ref)
    base_commit = repo.read_commit(base_commit_id)
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
        for entry in repo.store.iter_manifest_entries(base_commit.manifest):
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
            digest = repo._store_blob_from_source_uri(entry.source_uri)
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

    temp_manifest = repo._write_temp_manifest(iter_verified_entries())
    if verified_entries == 0:
        temp_manifest.unlink(missing_ok=True)
        from .repository import VerifyResult as VR

        return VR(
            commit_id=base_commit_id,
            verified_entries=0,
            candidate_entries=candidate_entries,
            total_entries=total_entries,
            created_commit=False,
            dry_run=dry_run,
        )

    manifest_hash = blake3_digest_file(temp_manifest)
    repo._persist_manifest(temp_manifest, manifest_hash)

    commit_id = repo._write_commit_object(
        branch=ref,
        message=f"verify {ref}",
        parent_commit=base_commit_id,
        manifest_hash=manifest_hash,
        expected_version_token=branch_state.version_token,
        operation="verify",
    )
    from .repository import VerifyResult as VR

    return VR(
        commit_id=commit_id,
        verified_entries=verified_entries,
        candidate_entries=candidate_entries,
        total_entries=total_entries,
        created_commit=True,
        dry_run=dry_run,
    )


def repo_restore_files(
    repo: FluxelRepository,
    ref: str,
    paths: list[str] | None = None,
    *,
    force: bool = False,
) -> list[str]:
    commit_id = repo.resolve_ref(ref)
    commit = repo.read_commit(commit_id)

    if paths:
        entries: dict[str, ManifestEntry] = {}
        for p in paths:
            entry = repo.store.lookup_manifest_entry(
                commit.manifest, p, manifest_index=commit.manifest_index
            )
            if entry is not None:
                entries[p] = entry
    else:
        entries = {
            entry.path: entry
            for entry in repo.store.iter_manifest_entries(commit.manifest)
        }

    restored: list[str] = []
    for logical_path, entry in sorted(entries.items()):
        target = repo.root / logical_path
        if target.exists() and not force:
            continue
        target.parent.mkdir(parents=True, exist_ok=True)
        if entry.blob_hash:
            target.write_bytes(repo.read_blob(entry.blob_hash))
            restored.append(logical_path)
        elif entry.source_uri:
            from .storage import open_source_uri

            with open_source_uri(entry.source_uri) as source_file:
                target.write_bytes(source_file.read())
            restored.append(logical_path)
    return restored


def repo_generate_transfer_commands(
    repo: FluxelRepository,
    ref: str | None = None,
    *,
    mode: str = "upload",
    include_metadata: bool = False,
) -> list[str]:
    from .config import BaseConfig, S3Config
    from .layout import blob_relpath
    from .repository_store import LocalRepositoryStore

    config = BaseConfig.load(repo.root)
    if not isinstance(config, S3Config):
        raise ValueError("S3 backend not configured in repo config")
    bucket = config.bucket

    branch = ref or repo.current_branch()
    commit_id = repo.resolve_ref(branch)
    commit = repo.read_commit(commit_id)
    s3_prefix = config.prefix or ""

    local_store = LocalRepositoryStore(repo.root)
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
            local = str(repo.layout.blobs_dir / rel)
            if mode == "upload":
                commands.append(f"cp {local} {s3_uri}")
            else:
                commands.append(f"cp {s3_uri} {local}")

    if include_metadata:
        for manifest_hash, extension in [
            (commit.manifest, "jsonl"),
            (commit.manifest, "idx"),
        ]:
            s3_key = f"manifests/{manifest_hash}.{extension}"
            if s3_prefix:
                s3_key = f"{s3_prefix}/{s3_key}"
            s3_uri = f"s3://{bucket}/{s3_key}"
            local = str(repo.layout.manifests_dir / f"{manifest_hash}.{extension}")
            if mode == "upload":
                commands.append(f"cp {local} {s3_uri}")
            else:
                commands.append(f"cp {s3_uri} {local}")

        commit_s3_key = f"commits/{commit_id}.json"
        if s3_prefix:
            commit_s3_key = f"{s3_prefix}/{commit_s3_key}"
        commit_s3_uri = f"s3://{bucket}/{commit_s3_key}"
        commit_local = str(repo.layout.commits_dir / f"{commit_id}.json")
        if mode == "upload":
            commands.append(f"cp {commit_local} {commit_s3_uri}")
        else:
            commands.append(f"cp {commit_s3_uri} {commit_local}")

    return commands
