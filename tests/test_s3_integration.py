from __future__ import annotations

from pathlib import Path
from uuid import uuid4

import pytest

from fluxel.core import FluxelRepository, RefConflictError, open_repository


pytestmark = pytest.mark.integration


def _open_remote_repo(
    repo_uri: str,
    *,
    worktree: Path,
    client_root: Path,
    s3_client: object,
) -> FluxelRepository:
    return open_repository(
        repo_uri,
        worktree=worktree,
        client_root=client_root,
        s3_client=s3_client,
    )


def test_s3_integration_branch_commit_and_fast_forward_merge(
    tmp_path: Path,
    ministack_client,
    s3_repo_root: str,
) -> None:
    main_worktree = tmp_path / "main-worktree"
    feature_worktree = tmp_path / "feature-worktree"
    main_client = tmp_path / "main-client"
    feature_client = tmp_path / "feature-client"
    for path in (main_worktree, feature_worktree, main_client, feature_client):
        path.mkdir(parents=True)

    (main_worktree / "shared.txt").write_text("base")
    repo_main = _open_remote_repo(
        s3_repo_root,
        worktree=main_worktree,
        client_root=main_client,
        s3_client=ministack_client,
    )
    base_commit = repo_main.commit("base")
    assert base_commit

    repo_main.branch("feature")

    (feature_worktree / "shared.txt").write_text("base")
    (feature_worktree / "feature.txt").write_text("feature")
    repo_feature = _open_remote_repo(
        s3_repo_root,
        worktree=feature_worktree,
        client_root=feature_client,
        s3_client=ministack_client,
    )
    repo_feature.set_current_branch("feature")
    repo_feature.add(["feature.txt"])
    feature_commit = repo_feature.commit("feature commit", staged=True)

    merge_result = repo_main.merge("feature", "main")
    assert merge_result.updated is True
    assert merge_result.commit_id == feature_commit

    changes = repo_main.diff(base_commit, "main")
    assert [(entry.path, entry.change) for entry in changes] == [
        ("feature.txt", "added")
    ]


def test_s3_integration_metadata_import_verify_remove_and_move(
    tmp_path: Path,
    ministack_client,
    s3_repo_root: str,
) -> None:
    source_bucket = s3_repo_root.split("//", maxsplit=1)[1].split("/", maxsplit=1)[0]
    source_prefix = f"imports/{uuid4().hex}"
    worktree = tmp_path / "client-worktree"
    client_root = tmp_path / "client-state"
    worktree.mkdir(parents=True)
    client_root.mkdir(parents=True)

    ministack_client.put_object(
        Bucket=source_bucket,
        Key=f"{source_prefix}/root.txt",
        Body=b"root",
    )
    ministack_client.put_object(
        Bucket=source_bucket,
        Key=f"{source_prefix}/images/cat.jpg",
        Body=b"cat",
    )
    ministack_client.put_object(
        Bucket=source_bucket,
        Key=f"{source_prefix}/images/dog.jpg",
        Body=b"dog",
    )
    ministack_client.put_object(
        Bucket=source_bucket,
        Key=f"{source_prefix}/docs/readme.md",
        Body=b"readme",
    )

    repo = _open_remote_repo(
        s3_repo_root,
        worktree=worktree,
        client_root=client_root,
        s3_client=ministack_client,
    )
    source_uri = f"s3://{source_bucket}/{source_prefix}"
    imported_commit = repo.import_s3(
        source_uri,
        "metadata import",
        identity_mode="meta",
        path_patterns=["root.txt", "**/*.jpg"],
    )
    assert imported_commit

    imported_entries = repo.resolve_entries("main")
    assert sorted(imported_entries) == ["images/cat.jpg", "images/dog.jpg", "root.txt"]
    assert all(entry.blob_hash is None for entry in imported_entries.values())

    verify_result = repo.verify(
        ref="main",
        path_prefixes=["root.txt", "images/cat.jpg"],
    )
    assert verify_result.created_commit is True
    assert verify_result.verified_entries == 2

    verified_entries = repo.resolve_entries("main")
    assert verified_entries["root.txt"].blob_hash is not None
    assert verified_entries["images/cat.jpg"].blob_hash is not None
    assert verified_entries["images/dog.jpg"].blob_hash is None

    move_result = repo.move("images", "photos", "rename image prefix")
    assert move_result.moved_paths == ["photos/cat.jpg", "photos/dog.jpg"]

    remove_result = repo.remove_paths(["root.txt"], "remove root")
    assert remove_result.removed_paths == ["root.txt"]

    final_entries = repo.resolve_entries("main")
    assert sorted(final_entries) == ["photos/cat.jpg", "photos/dog.jpg"]
    assert final_entries["photos/cat.jpg"].blob_hash is not None
    assert final_entries["photos/dog.jpg"].blob_hash is None


def test_s3_integration_reports_optimistic_concurrency_conflicts(
    tmp_path: Path,
    ministack_client,
    s3_repo_root: str,
) -> None:
    client_a_worktree = tmp_path / "client-a-worktree"
    client_b_worktree = tmp_path / "client-b-worktree"
    client_a_root = tmp_path / "client-a-state"
    client_b_root = tmp_path / "client-b-state"
    for path in (
        client_a_worktree,
        client_b_worktree,
        client_a_root,
        client_b_root,
    ):
        path.mkdir(parents=True)

    (client_a_worktree / "data.txt").write_text("alpha")
    repo_a = _open_remote_repo(
        s3_repo_root,
        worktree=client_a_worktree,
        client_root=client_a_root,
        s3_client=ministack_client,
    )
    base_commit = repo_a.commit("base")
    assert base_commit

    (client_a_worktree / "data.txt").write_text("beta")

    (client_b_worktree / "data.txt").write_text("alpha")
    repo_b = _open_remote_repo(
        s3_repo_root,
        worktree=client_b_worktree,
        client_root=client_b_root,
        s3_client=ministack_client,
    )
    (client_b_worktree / "data.txt").write_text("gamma")
    winning_commit = repo_b.commit("winning update")
    assert winning_commit != base_commit

    with pytest.raises(RefConflictError) as error_info:
        repo_a.commit("stale update")

    error = error_info.value
    assert error.operation == "commit"
    assert error.expected_commit_id == base_commit
    assert error.current_commit_id == winning_commit
