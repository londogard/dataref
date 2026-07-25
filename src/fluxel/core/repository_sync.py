"""Push, pull, and fetch operations for syncing between local and S3 repos."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .repository import FluxelRepository


from .domain import FetchResult, PullResult, PushResult


def _copy_blob(
    src_repo: FluxelRepository, dst_repo: FluxelRepository, blob_hash: str
) -> None:
    """Stream a known blob from src_repo's store to dst_repo's store."""
    if dst_repo.store.object_exists("blob", blob_hash):
        return
    source = src_repo.store.open_blob(blob_hash)
    try:
        dst_repo.store.write_blob_stream(blob_hash, source, if_missing=True)
    finally:
        source.close()


def _copy_manifest(
    src_repo: FluxelRepository, dst_repo: FluxelRepository, manifest_hash: str
) -> None:
    """Copy a manifest JSONL and its sidecar index from src to dst store."""
    if dst_repo.store.object_exists("manifest", manifest_hash):
        return

    with NamedTemporaryFile(
        mode="w", suffix=".jsonl", delete=False, encoding="utf-8"
    ) as tmp:
        tmp_path = Path(tmp.name)
        for entry in src_repo.store.iter_manifest_entries(manifest_hash):
            tmp.write(entry.serialize() + "\n")
    try:
        dst_repo.store.write_manifest_file(manifest_hash, tmp_path, if_missing=True)
    finally:
        tmp_path.unlink(missing_ok=True)

    index_bytes = src_repo.store.read_manifest_index_bytes(manifest_hash)
    if index_bytes is not None:
        with NamedTemporaryFile(mode="wb", suffix=".idx", delete=False) as tmp:
            tmp_path = Path(tmp.name)
            tmp.write(index_bytes)
        try:
            dst_repo.store.write_manifest_index_file(
                manifest_hash, tmp_path, if_missing=True
            )
        finally:
            tmp_path.unlink(missing_ok=True)


def _collect_commits_to_push(
    src_repo: FluxelRepository, dst_repo: FluxelRepository, commit_id: str
) -> list[str]:
    """Walk local commit chain and return commits not on remote, oldest first."""
    commits: list[str] = []
    current: str | None = commit_id
    while current:
        if dst_repo.store.object_exists("commit", current):
            break
        commits.append(current)
        commit_obj = src_repo.read_commit(current)
        current = commit_obj.parent
    commits.reverse()
    return commits


def _is_ancestor_in(
    ancestor: str,
    descendant: str,
    repo: FluxelRepository,
) -> bool:
    """Check whether *ancestor* is an ancestor of *descendant* by walking
    the commit chain stored in *repo*."""
    current: str | None = descendant
    while current:
        if current == ancestor:
            return True
        commit_obj = repo.read_commit(current)
        current = commit_obj.parent
    return False


def _verify_push_fast_forward(
    local_repo: FluxelRepository,
    remote_repo: FluxelRepository,
    branch: str,
    local_commit_id: str,
) -> bool:
    """Verify that the remote head is an ancestor of the local head.

    Returns False when already up to date.  Raises NonFastForwardError
    when the histories have diverged.
    """
    from .repository import NonFastForwardError

    remote_state = remote_repo.store.read_branch_ref(branch)
    remote_head = remote_state.commit_id if remote_state else None
    if remote_head == local_commit_id:
        return False
    if remote_head is not None and not _is_ancestor_in(
        remote_head,
        local_commit_id,
        local_repo,
    ):
        raise NonFastForwardError(
            branch=branch,
            current_commit=remote_head,
            target_commit=local_commit_id,
        )
    return True


def _verify_pull_fast_forward(
    local_repo: FluxelRepository,
    remote_repo: FluxelRepository,
    branch: str,
    remote_commit_id: str,
) -> bool:
    """Verify that the local head is an ancestor of the remote head.

    Returns False when already up to date.  Raises NonFastForwardError
    when the histories have diverged.
    """
    from .repository import NonFastForwardError

    local_state = local_repo.store.read_branch_ref(branch)
    local_head = local_state.commit_id if local_state else None
    if local_head == remote_commit_id:
        return False
    if local_head is not None and not _is_ancestor_in(
        local_head,
        remote_commit_id,
        remote_repo,
    ):
        raise NonFastForwardError(
            branch=branch,
            current_commit=local_head,
            target_commit=remote_commit_id,
        )
    return True


def push(
    repo: FluxelRepository,
    remote_uri: str,
    ref: str | None = None,
    *,
    blob_transfer: str | None = None,
) -> PushResult:
    """Push commits, blobs, and manifests from local repo to a remote S3 repo.

    Rejects divergent history *before* copying any objects.  The final
    ref update uses CAS; a race between the pre-check and the CAS will
    result in a RefConflictError rather than a silent overwrite.

    Args:
        repo: Local FluxelRepository.
        remote_uri: S3 URI of the remote repository (e.g. s3://bucket/prefix).
        ref: Branch to push (default: current branch).
        blob_transfer: Blob transfer backend name (e.g. "boto3", "s5cmd").
    """
    from .repository import open_repository

    branch = ref or repo.current_branch()
    if not repo.store.object_exists("ref", branch):
        raise ValueError(f"Unknown branch: {branch}")
    branch_state = repo.store.read_branch_ref(branch)
    if branch_state is None or branch_state.commit_id is None:
        raise ValueError(f"Branch '{branch}' has no commits to push")

    local_commit_id = branch_state.commit_id

    # Open remote repo
    remote_repo = open_repository(
        remote_uri,
        worktree=repo.root,
        blob_transfer=blob_transfer,
    )

    # Ensure remote branch exists
    if remote_repo.store.read_branch_ref(branch) is None:
        remote_repo.store.write_branch_ref(branch, None)

    # ── Pre-check: reject divergent history before copying any objects ──
    can_fast_forward = _verify_push_fast_forward(
        repo,
        remote_repo,
        branch,
        local_commit_id,
    )
    if not can_fast_forward:
        return PushResult(
            source_branch=branch,
            remote_uri=remote_uri,
            pushed_commits=0,
            pushed_blobs=0,
            updated=False,
        )

    commits_to_push = _collect_commits_to_push(repo, remote_repo, local_commit_id)
    if not commits_to_push:
        # All objects already exist; CAS the branch ref for idempotency.
        updated = remote_repo.fast_forward_branch(
            branch, local_commit_id, operation="push"
        )
        return PushResult(
            source_branch=branch,
            remote_uri=remote_uri,
            pushed_commits=0,
            pushed_blobs=0,
            updated=updated,
        )

    # Publish immutable objects first. The ref moves last, after the remote
    # verifies that the update is a fast-forward.
    pushed_blobs: set[str] = set()
    for commit_id in commits_to_push:
        commit_obj = repo.read_commit(commit_id)

        # Push manifest
        _copy_manifest(repo, remote_repo, commit_obj.manifest)

        # Push blobs
        for entry in repo.store.iter_manifest_entries(commit_obj.manifest):
            if entry.blob_hash and entry.blob_hash not in pushed_blobs:
                _copy_blob(repo, remote_repo, entry.blob_hash)
                pushed_blobs.add(entry.blob_hash)

        # Push commit object
        commit_bytes = repo.store.read_commit_bytes(commit_id)
        if commit_bytes is not None:
            remote_repo.store.write_commit_bytes(
                commit_id, commit_bytes, if_missing=True
            )

    updated = remote_repo.fast_forward_branch(branch, local_commit_id, operation="push")

    return PushResult(
        source_branch=branch,
        remote_uri=remote_uri,
        pushed_commits=len(commits_to_push),
        pushed_blobs=len(pushed_blobs),
        updated=updated,
    )


def pull(
    repo: FluxelRepository,
    remote_uri: str,
    ref: str | None = None,
    *,
    blob_transfer: str | None = None,
) -> PullResult:
    """Pull commits, blobs, and manifests from a remote S3 repo to local.

    Rejects divergent history *before* copying any objects.  The final
    ref update uses CAS; a race between the pre-check and the CAS will
    result in a RefConflictError rather than a silent overwrite.

    Args:
        repo: Local FluxelRepository.
        remote_uri: S3 URI of the remote repository.
        ref: Branch to pull (default: current branch).
        blob_transfer: Blob transfer backend name (e.g. "boto3", "s5cmd").
    """
    from .repository import open_repository

    branch = ref or repo.current_branch()

    # Open remote repo
    remote_repo = open_repository(
        remote_uri, worktree=repo.root, blob_transfer=blob_transfer
    )

    # Resolve remote ref
    remote_commit_id = remote_repo.resolve_ref(branch)

    # ── Pre-check: reject divergent history before copying any objects ──
    # Ensure local branch exists for the check
    if repo.store.read_branch_ref(branch) is None:
        repo.store.write_branch_ref(branch, None)

    can_fast_forward = _verify_pull_fast_forward(
        repo,
        remote_repo,
        branch,
        remote_commit_id,
    )
    if not can_fast_forward:
        return PullResult(
            source_branch=branch,
            remote_uri=remote_uri,
            pulled_commits=0,
            pulled_blobs=0,
            updated=False,
        )

    commits_to_pull = _collect_commits_to_push(remote_repo, repo, remote_commit_id)
    if not commits_to_pull:
        updated = repo.fast_forward_branch(branch, remote_commit_id, operation="pull")
        return PullResult(
            source_branch=branch,
            remote_uri=remote_uri,
            pulled_commits=0,
            pulled_blobs=0,
            updated=updated,
        )

    pulled_blobs: set[str] = set()
    for commit_id in commits_to_pull:
        commit_obj = remote_repo.read_commit(commit_id)

        _copy_manifest(remote_repo, repo, commit_obj.manifest)

        for entry in remote_repo.store.iter_manifest_entries(commit_obj.manifest):
            if entry.blob_hash and entry.blob_hash not in pulled_blobs:
                _copy_blob(remote_repo, repo, entry.blob_hash)
                pulled_blobs.add(entry.blob_hash)

        commit_bytes = remote_repo.store.read_commit_bytes(commit_id)
        if commit_bytes is not None:
            repo.store.write_commit_bytes(commit_id, commit_bytes, if_missing=True)

    # CAS the local branch ref (branch was ensured to exist during pre-check).
    updated = repo.fast_forward_branch(branch, remote_commit_id, operation="pull")

    return PullResult(
        source_branch=branch,
        remote_uri=remote_uri,
        pulled_commits=len(commits_to_pull),
        pulled_blobs=len(pulled_blobs),
        updated=updated,
    )


def fetch(
    repo: FluxelRepository,
    remote_uri: str,
    ref: str | None = None,
    *,
    blob_transfer: str | None = None,
) -> FetchResult:
    """Fetch objects from a remote S3 repo without updating local branch refs.

    Args:
        repo: Local FluxelRepository.
        remote_uri: S3 URI of the remote repository.
        ref: Branch to fetch (default: current branch).
        blob_transfer: Blob transfer backend name (e.g. "boto3", "s5cmd").
    """
    from .repository import open_repository

    branch = ref or repo.current_branch()
    remote_repo = open_repository(
        remote_uri, worktree=repo.root, blob_transfer=blob_transfer
    )
    remote_commit_id = remote_repo.resolve_ref(branch)

    commits_to_fetch = _collect_commits_to_push(remote_repo, repo, remote_commit_id)

    fetched_blobs: set[str] = set()
    for commit_id in commits_to_fetch:
        commit_obj = remote_repo.read_commit(commit_id)

        _copy_manifest(remote_repo, repo, commit_obj.manifest)

        for entry in remote_repo.store.iter_manifest_entries(commit_obj.manifest):
            if entry.blob_hash and entry.blob_hash not in fetched_blobs:
                _copy_blob(remote_repo, repo, entry.blob_hash)
                fetched_blobs.add(entry.blob_hash)

        commit_bytes = remote_repo.store.read_commit_bytes(commit_id)
        if commit_bytes is not None:
            repo.store.write_commit_bytes(commit_id, commit_bytes, if_missing=True)

    return FetchResult(
        remote_uri=remote_uri,
        branch=branch,
        fetched_commits=len(commits_to_fetch),
        fetched_blobs=len(fetched_blobs),
    )
