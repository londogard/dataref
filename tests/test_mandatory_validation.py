from __future__ import annotations

import tracemalloc
from pathlib import Path

from blake3 import blake3
import pytest

from fluxel.core import (
    FluxelFileSystem,
    FluxelRepository,
    LocalClientState,
    LocalRepositoryStore,
    ManifestEntry,
    ManifestReader,
    ManifestWriter,
    RefConflictError,
    build_analytical_index,
    drop_analytical_index,
    open_repository,
    query_analytical_index,
)


class ConflictOnceLocalRepositoryStore(LocalRepositoryStore):
    def __init__(self, root: str | Path, *, conflict_commit_id: str) -> None:
        super().__init__(root)
        self.conflict_commit_id = conflict_commit_id
        self.conflict_next_ref_update = False

    def compare_and_set_branch_ref(
        self,
        branch: str,
        commit_id: str | None,
        *,
        expected_version_token: str | None,
        expected_commit_id: str | None = None,
    ) -> bool:
        if self.conflict_next_ref_update:
            self.conflict_next_ref_update = False
            self.write_branch_ref(branch, self.conflict_commit_id)
            return False
        return super().compare_and_set_branch_ref(
            branch,
            commit_id,
            expected_version_token=expected_version_token,
            expected_commit_id=expected_commit_id,
        )


def test_metadata_only_diff_does_not_read_blobs(tmp_path: Path, monkeypatch) -> None:
    (tmp_path / "a.txt").write_text("alpha")
    repo = FluxelRepository(tmp_path)
    commit_a = repo.commit("initial")

    (tmp_path / "a.txt").write_text("beta")
    (tmp_path / "b.txt").write_text("new")
    commit_b = repo.commit("update")

    touched_blob_reads: list[Path] = []
    original_read_bytes = Path.read_bytes

    def tracking_read_bytes(path: Path) -> bytes:
        path_obj = Path(path)
        if ".fluxel" in path_obj.parts and "blobs" in path_obj.parts:
            touched_blob_reads.append(path_obj)
        return original_read_bytes(path_obj)

    monkeypatch.setattr(Path, "read_bytes", tracking_read_bytes)

    changes = repo.diff(commit_a, commit_b)
    assert {(entry.path, entry.change) for entry in changes} == {
        ("a.txt", "modified"),
        ("b.txt", "added"),
    }
    assert touched_blob_reads == []


def test_metadata_only_remove_does_not_read_blobs(tmp_path: Path, monkeypatch) -> None:
    (tmp_path / "a.txt").write_text("alpha")
    (tmp_path / "b.txt").write_text("beta")
    repo = FluxelRepository(tmp_path)
    repo.commit("initial")

    touched_blob_reads: list[Path] = []
    original_read_bytes = Path.read_bytes

    def tracking_read_bytes(path: Path) -> bytes:
        path_obj = Path(path)
        if ".fluxel" in path_obj.parts and "blobs" in path_obj.parts:
            touched_blob_reads.append(path_obj)
        return original_read_bytes(path_obj)

    monkeypatch.setattr(Path, "read_bytes", tracking_read_bytes)

    result = repo.remove_paths(["a.txt"], "remove a")

    assert result.removed_paths == ["a.txt"]
    assert set(repo.resolve_entries("main")) == {"b.txt"}
    assert touched_blob_reads == []


def test_metadata_only_move_updates_meta_identity_without_blob_read(
    tmp_path: Path, monkeypatch
) -> None:
    nested = tmp_path / "dir"
    nested.mkdir()
    (nested / "file.txt").write_text("payload")
    repo = FluxelRepository(tmp_path)
    repo.commit("metadata only", identity_mode="meta")
    before_entry = repo.resolve_entries("main")["dir/file.txt"]

    touched_blob_reads: list[Path] = []
    original_read_bytes = Path.read_bytes

    def tracking_read_bytes(path: Path) -> bytes:
        path_obj = Path(path)
        if ".fluxel" in path_obj.parts and "blobs" in path_obj.parts:
            touched_blob_reads.append(path_obj)
        return original_read_bytes(path_obj)

    monkeypatch.setattr(Path, "read_bytes", tracking_read_bytes)

    result = repo.move("dir", "archive", "rename prefix")
    after_entries = repo.resolve_entries("main")
    moved_entry = after_entries["archive/file.txt"]
    expected_identity = blake3(
        f"archive/file.txt\n{before_entry.size}".encode("utf-8")
    ).hexdigest()

    assert result.moved_paths == ["archive/file.txt"]
    assert "dir/file.txt" not in after_entries
    assert moved_entry.identity_mode == "meta"
    assert moved_entry.hash == expected_identity
    assert moved_entry.identity_value == expected_identity
    assert moved_entry.blob_hash is None
    assert moved_entry.source_uri == before_entry.source_uri
    assert touched_blob_reads == []


def test_memory_safe_manifesting_100k_entries(tmp_path: Path) -> None:
    entry_count = 100_000
    manifest_path = tmp_path / ".fluxel" / "manifests" / "large.jsonl"

    def entries():
        for i in range(entry_count):
            yield ManifestEntry(
                path=f"dir/file_{i}.dat",
                hash=f"{i:064x}",
                size=i,
                mtime_ns=i,
            )

    writer = ManifestWriter(manifest_path)
    tracemalloc.start()
    written = writer.write_entries(entries())
    _, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()

    assert written == entry_count
    assert peak < 150 * 1024 * 1024

    reader = ManifestReader(manifest_path)
    first = next(reader.iter_entries())
    assert first.path == "dir/file_0.dat"
    assert sum(1 for _ in ManifestReader(manifest_path).iter_entries()) == entry_count


def test_manifest_entry_validation_rejects_invalid_payloads() -> None:
    invalid_payloads = [
        (
            {
                "path": "../escape.txt",
                "hash": "a" * 64,
                "size": 1,
                "mtime_ns": 1,
            },
            "normalized relative path",
        ),
        (
            {
                "path": "valid.txt",
                "hash": "g" * 64,
                "size": 1,
                "mtime_ns": 1,
            },
            "64-character hex digest",
        ),
        (
            {
                "path": "valid.txt",
                "hash": "a" * 64,
                "size": -1,
                "mtime_ns": 1,
            },
            "size cannot be negative",
        ),
        (
            {
                "path": "meta.txt",
                "hash": "a" * 64,
                "size": 1,
                "mtime_ns": 1,
                "identity_mode": "meta",
            },
            "must include source_uri",
        ),
    ]

    for payload, message in invalid_payloads:
        with pytest.raises(ValueError, match=message):
            ManifestEntry.from_dict(payload)


def test_manifest_reader_reports_corrupt_json_with_line_context(
    tmp_path: Path,
) -> None:
    manifest_path = tmp_path / "broken.jsonl"
    manifest_path.write_text(
        '{"path":"ok.txt","hash":"' + ("a" * 64) + '","size":1,"mtime_ns":1}\n'
        '{"path": invalid json}\n',
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match=r"Corrupt manifest JSON at line 2"):
        list(ManifestReader(manifest_path).iter_entries())


def test_local_client_state_writes_use_atomic_replace(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    state = LocalClientState(tmp_path)
    replaced_targets: list[Path] = []
    original_replace = Path.replace

    def tracking_replace(path: Path, target: str | Path) -> Path:
        replaced_targets.append(Path(target))
        return original_replace(path, target)

    monkeypatch.setattr(Path, "replace", tracking_replace)

    state.set_current_branch("feature")
    state.write_staging_payload("feature", '[{"path":"a.txt","action":"add"}]\n')

    assert state.current_branch() == "feature"
    assert (
        state.read_staging_payload("feature") == '[{"path":"a.txt","action":"add"}]\n'
    )
    assert state.head_path in replaced_targets
    assert state.stage_path("feature") in replaced_targets
    assert not list(state.fluxel_dir.rglob("*.tmp"))


def test_uri_routing_reads_expected_blob_bytes(tmp_path: Path) -> None:
    dataset_root = tmp_path / "my_data"
    dataset_root.mkdir(parents=True)
    (dataset_root / "test.csv").write_text("col\n123\n")

    repo = FluxelRepository(dataset_root)
    repo.commit("add file")

    fs = FluxelFileSystem(dataset_roots={"my_data": dataset_root})
    with fs.open("fluxel://my_data@main/test.csv", "rb") as handle:
        assert handle.read() == b"col\n123\n"


def test_exact_lookup_uses_manifest_sidecar_without_full_scan(tmp_path: Path) -> None:
    (tmp_path / "a.txt").write_text("alpha")
    repo = FluxelRepository(tmp_path)
    repo.commit("initial")

    manifest_indexes = list((tmp_path / ".fluxel" / "manifests").glob("*.idx"))
    assert len(manifest_indexes) == 1

    original_iter_manifest_entries = repo.store.iter_manifest_entries

    def fail_iter_manifest_entries(manifest_hash: str):
        raise AssertionError(f"unexpected full manifest scan for {manifest_hash}")

    repo.store.iter_manifest_entries = fail_iter_manifest_entries  # type: ignore[method-assign]
    try:
        entry = repo.resolve_entry("main", "a.txt")
    finally:
        repo.store.iter_manifest_entries = original_iter_manifest_entries  # type: ignore[method-assign]

    assert entry is not None
    assert entry.path == "a.txt"


def test_uri_routing_uses_manifest_sidecar_for_point_reads(tmp_path: Path) -> None:
    dataset_root = tmp_path / "sidecar_data"
    dataset_root.mkdir(parents=True)
    (dataset_root / "test.csv").write_text("value\n7\n")

    repo = FluxelRepository(dataset_root)
    repo.commit("add file")

    fs = FluxelFileSystem(dataset_roots={"sidecar_data": dataset_root})
    cached_repo = fs._repository(dataset_root)
    original_iter_manifest_entries = cached_repo.store.iter_manifest_entries

    def fail_iter_manifest_entries(manifest_hash: str):
        raise AssertionError(f"unexpected full manifest scan for {manifest_hash}")

    cached_repo.store.iter_manifest_entries = fail_iter_manifest_entries  # type: ignore[method-assign]
    try:
        with fs.open("fluxel://sidecar_data@main/test.csv", "rb") as handle:
            assert handle.read() == b"value\n7\n"
    finally:
        cached_repo.store.iter_manifest_entries = original_iter_manifest_entries  # type: ignore[method-assign]


def test_uri_listing_uses_manifest_sidecar_for_prefix_reads(tmp_path: Path) -> None:
    dataset_root = tmp_path / "listing_data"
    dataset_root.mkdir(parents=True)
    (dataset_root / "logs").mkdir()
    (dataset_root / "logs" / "a.txt").write_text("a")
    (dataset_root / "logs" / "b.txt").write_text("b")
    (dataset_root / "other.txt").write_text("c")

    repo = FluxelRepository(dataset_root)
    repo.commit("add files")

    fs = FluxelFileSystem(dataset_roots={"listing_data": dataset_root})
    cached_repo = fs._repository(dataset_root)
    original_iter_manifest_entries = cached_repo.store.iter_manifest_entries

    def fail_iter_manifest_entries(manifest_hash: str):
        raise AssertionError(f"unexpected full manifest scan for {manifest_hash}")

    cached_repo.store.iter_manifest_entries = fail_iter_manifest_entries  # type: ignore[method-assign]
    try:
        paths = fs.ls("fluxel://listing_data@main/logs", detail=False)
    finally:
        cached_repo.store.iter_manifest_entries = original_iter_manifest_entries  # type: ignore[method-assign]

    assert paths == [
        "fluxel://listing_data@main/logs/a.txt",
        "fluxel://listing_data@main/logs/b.txt",
    ]


def test_repeated_exact_lookup_reuses_cached_commit(tmp_path: Path) -> None:
    (tmp_path / "a.txt").write_text("alpha")
    repo = FluxelRepository(tmp_path)
    repo.commit("initial")

    first = repo.resolve_entry("main", "a.txt")
    assert first is not None

    original_read_commit_bytes = repo.store.read_commit_bytes

    def fail_read_commit_bytes(commit_id: str):
        raise AssertionError(f"unexpected commit reread for {commit_id}")

    repo.store.read_commit_bytes = fail_read_commit_bytes  # type: ignore[method-assign]
    try:
        second = repo.resolve_entry("main", "a.txt")
    finally:
        repo.store.read_commit_bytes = original_read_commit_bytes  # type: ignore[method-assign]

    assert second is not None
    assert second.path == "a.txt"


def test_remote_exact_lookup_uses_manifest_sidecar(
    tmp_path: Path, fake_s3_installer
) -> None:
    client = fake_s3_installer({})
    worktree = tmp_path / "worktree"
    client_root = tmp_path / "client"
    worktree.mkdir(parents=True)
    client_root.mkdir(parents=True)
    (worktree / "remote.txt").write_text("payload")

    repo = open_repository(
        "s3://demo-bucket/repos/demo",
        worktree=worktree,
        client_root=client_root,
        s3_client=client,
    )
    repo.commit("initial")

    assert any(
        key.startswith("repos/demo/manifests/") and key.endswith(".idx")
        for key in client._objects
    )

    original_iter_manifest_entries = repo.store.iter_manifest_entries

    def fail_iter_manifest_entries(manifest_hash: str):
        raise AssertionError(f"unexpected full manifest scan for {manifest_hash}")

    repo.store.iter_manifest_entries = fail_iter_manifest_entries  # type: ignore[method-assign]
    try:
        entry = repo.resolve_entry("main", "remote.txt")
    finally:
        repo.store.iter_manifest_entries = original_iter_manifest_entries  # type: ignore[method-assign]

    assert entry is not None
    assert entry.path == "remote.txt"


def test_remote_prefix_listing_uses_manifest_sidecar(
    tmp_path: Path, fake_s3_installer
) -> None:
    client = fake_s3_installer({})
    worktree = tmp_path / "worktree-list"
    client_root = tmp_path / "client-list"
    worktree.mkdir(parents=True)
    client_root.mkdir(parents=True)
    (worktree / "logs").mkdir()
    (worktree / "logs" / "a.txt").write_text("a")
    (worktree / "logs" / "b.txt").write_text("b")
    (worktree / "other.txt").write_text("c")

    repo = open_repository(
        "s3://demo-bucket/repos/demo-prefix",
        worktree=worktree,
        client_root=client_root,
        s3_client=client,
    )
    repo.commit("initial")

    original_iter_manifest_entries = repo.store.iter_manifest_entries

    def fail_iter_manifest_entries(manifest_hash: str):
        raise AssertionError(f"unexpected full manifest scan for {manifest_hash}")

    repo.store.iter_manifest_entries = fail_iter_manifest_entries  # type: ignore[method-assign]
    try:
        entries = repo.resolve_entries_for_prefix("main", "logs")
    finally:
        repo.store.iter_manifest_entries = original_iter_manifest_entries  # type: ignore[method-assign]

    assert sorted(entries) == ["logs/a.txt", "logs/b.txt"]


def test_s3_compare_and_set_checks_expected_commit_id_under_lock(
    tmp_path: Path, fake_s3_installer
) -> None:
    client = fake_s3_installer({})
    client.fixed_etag = '"static-etag"'

    worktree = tmp_path / "store-worktree"
    client_root = tmp_path / "store-client"
    worktree.mkdir(parents=True)
    client_root.mkdir(parents=True)

    repo = open_repository(
        "s3://demo-bucket/repos/conflict-store",
        worktree=worktree,
        client_root=client_root,
        s3_client=client,
    )
    store = repo.store

    store.write_branch_ref("main", "base")
    base_state = store.read_branch_ref("main")
    assert base_state is not None
    assert base_state.commit_id == "base"

    store.write_branch_ref("main", "winning")

    updated = store.compare_and_set_branch_ref(
        "main",
        "stale",
        expected_version_token=base_state.version_token,
        expected_commit_id=base_state.commit_id,
    )

    assert updated is False
    current_state = store.read_branch_ref("main")
    assert current_state is not None
    assert current_state.commit_id == "winning"


def test_remote_repo_commit_detects_conflict_even_if_s3_etag_is_unchanged(
    tmp_path: Path, fake_s3_installer
) -> None:
    client = fake_s3_installer({})
    client.fixed_etag = '"static-etag"'

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
    repo_a = open_repository(
        "s3://demo-bucket/repos/conflict-repo",
        worktree=client_a_worktree,
        client_root=client_a_root,
        s3_client=client,
    )
    base_commit = repo_a.commit("base")

    (client_b_worktree / "data.txt").write_text("alpha")
    repo_b = open_repository(
        "s3://demo-bucket/repos/conflict-repo",
        worktree=client_b_worktree,
        client_root=client_b_root,
        s3_client=client,
    )
    (client_b_worktree / "data.txt").write_text("gamma")
    winning_commit = repo_b.commit("winning update")

    (client_a_worktree / "data.txt").write_text("beta")
    with pytest.raises(RefConflictError) as error_info:
        repo_a.commit("stale update")

    error = error_info.value
    assert error.operation == "commit"
    assert error.expected_commit_id == base_commit
    assert error.current_commit_id == winning_commit


def test_disposable_analytical_index(tmp_path: Path) -> None:
    (tmp_path / "a.jpg").write_bytes(b"x" * 10)
    (tmp_path / "b.txt").write_text("hello")

    repo = FluxelRepository(tmp_path)
    commit_id = repo.commit("indexable commit")

    paths = build_analytical_index(tmp_path, "main", export_parquet=True)
    assert paths.database_path.exists()
    assert paths.parquet_path is not None and paths.parquet_path.exists()

    rows = query_analytical_index(
        paths.database_path,
        "SELECT path FROM files WHERE path LIKE '%.jpg' AND size > 2 ORDER BY path",
    )
    assert rows == [("a.jpg",)]

    drop_analytical_index(paths.database_path)
    assert not paths.database_path.exists()

    commit = repo.read_commit(commit_id)
    manifest_path = tmp_path / ".fluxel" / "manifests" / f"{commit.manifest}.jsonl"
    assert manifest_path.exists()


def test_uri_routing_supports_metadata_identity_entries(tmp_path: Path) -> None:
    dataset_root = tmp_path / "meta_data"
    dataset_root.mkdir(parents=True)
    (dataset_root / "test.csv").write_text("value\n42\n")

    repo = FluxelRepository(dataset_root)
    repo.commit("metadata only", identity_mode="meta")

    assert not any((dataset_root / ".fluxel" / "blobs").rglob("*"))

    fs = FluxelFileSystem(dataset_roots={"meta_data": dataset_root})
    with fs.open("fluxel://meta_data@main/test.csv", "rb") as handle:
        assert handle.read() == b"value\n42\n"


def test_repository_mutations_can_use_separate_local_store(tmp_path: Path) -> None:
    dataset_root = tmp_path / "dataset"
    store_root = tmp_path / "repo-state"
    dataset_root.mkdir(parents=True)
    store_root.mkdir(parents=True)
    (dataset_root / "sample.txt").write_text("payload")

    repo = FluxelRepository(
        dataset_root,
        store=LocalRepositoryStore(store_root),
    )
    commit_id = repo.commit("initial")

    assert commit_id
    assert list((dataset_root / ".fluxel" / "commits").glob("*.json")) == []
    assert list((dataset_root / ".fluxel" / "manifests").glob("*.jsonl")) == []
    assert not any((dataset_root / ".fluxel" / "blobs").rglob("*"))

    assert list((store_root / ".fluxel" / "commits").glob("*.json"))
    manifest_paths = list((store_root / ".fluxel" / "manifests").glob("*.jsonl"))
    assert manifest_paths
    assert any((store_root / ".fluxel" / "blobs").rglob("*"))
    assert (
        store_root / ".fluxel" / "refs" / "heads" / "main"
    ).read_text().strip() == commit_id

    manifest_entries = list(ManifestReader(manifest_paths[0]).iter_entries())
    assert [entry.path for entry in manifest_entries] == ["sample.txt"]


def test_current_branch_preference_is_local_per_client(tmp_path: Path) -> None:
    dataset_root = tmp_path / "dataset"
    store_root = tmp_path / "repo-state"
    client_a_root = tmp_path / "client-a"
    client_b_root = tmp_path / "client-b"
    dataset_root.mkdir(parents=True)
    store_root.mkdir(parents=True)
    client_a_root.mkdir(parents=True)
    client_b_root.mkdir(parents=True)
    (dataset_root / "shared.txt").write_text("base")

    repo_a = FluxelRepository(
        dataset_root,
        store=LocalRepositoryStore(store_root),
        client_state=LocalClientState(client_a_root),
    )
    repo_b = FluxelRepository(
        dataset_root,
        store=LocalRepositoryStore(store_root),
        client_state=LocalClientState(client_b_root),
    )

    repo_a.commit("base")
    repo_a.branch("feature")
    repo_a.set_current_branch("feature")
    repo_b.set_current_branch("main")

    assert repo_a.current_branch() == "feature"
    assert repo_b.current_branch() == "main"
    assert (
        client_a_root / ".fluxel" / "refs" / "HEAD"
    ).read_text().strip() == "refs/heads/feature"
    assert (
        client_b_root / ".fluxel" / "refs" / "HEAD"
    ).read_text().strip() == "refs/heads/main"
    assert not (store_root / ".fluxel" / "refs" / "HEAD").exists()


def test_staging_state_is_local_per_client(tmp_path: Path) -> None:
    dataset_root = tmp_path / "dataset"
    store_root = tmp_path / "repo-state"
    client_a_root = tmp_path / "client-a"
    client_b_root = tmp_path / "client-b"
    dataset_root.mkdir(parents=True)
    store_root.mkdir(parents=True)
    client_a_root.mkdir(parents=True)
    client_b_root.mkdir(parents=True)
    (dataset_root / "shared.txt").write_text("base")
    (dataset_root / "feature.txt").write_text("feature")

    repo_a = FluxelRepository(
        dataset_root,
        store=LocalRepositoryStore(store_root),
        client_state=LocalClientState(client_a_root),
    )
    repo_b = FluxelRepository(
        dataset_root,
        store=LocalRepositoryStore(store_root),
        client_state=LocalClientState(client_b_root),
    )

    repo_a.commit("base")
    repo_a.branch("feature")
    repo_a.set_current_branch("feature")
    repo_b.set_current_branch("feature")

    repo_a.add(["feature.txt"])

    assert repo_a.status().added == ["feature.txt"]
    assert repo_b.status().added == []
    assert (client_a_root / ".fluxel" / "staging" / "feature.json").exists()
    assert not (client_b_root / ".fluxel" / "staging" / "feature.json").exists()
    assert not (store_root / ".fluxel" / "staging" / "feature.json").exists()


def test_commit_fails_clearly_on_branch_update_conflict(tmp_path: Path) -> None:
    conflict_commit_id = "f" * 64
    store = ConflictOnceLocalRepositoryStore(
        tmp_path / "repo-state",
        conflict_commit_id=conflict_commit_id,
    )
    (tmp_path / "data.txt").write_text("alpha")

    repo = FluxelRepository(tmp_path, store=store)
    base_commit = repo.commit("base")

    (tmp_path / "data.txt").write_text("beta")
    store.conflict_next_ref_update = True

    try:
        repo.commit("update")
    except RefConflictError as error:
        assert str(error) == (
            f"Branch update conflict for 'main' during commit: expected {base_commit}, found {conflict_commit_id}"
        )
    else:
        raise AssertionError("Expected RefConflictError")

    assert store.read_branch_ref("main") is not None
    assert store.read_branch_ref("main").commit_id == conflict_commit_id


def test_merge_fails_clearly_on_branch_update_conflict(tmp_path: Path) -> None:
    conflict_commit_id = "e" * 64
    store = ConflictOnceLocalRepositoryStore(
        tmp_path / "repo-state",
        conflict_commit_id=conflict_commit_id,
    )
    (tmp_path / "shared.txt").write_text("base")

    repo = FluxelRepository(tmp_path, store=store)
    base_commit = repo.commit("base")
    repo.branch("feature")

    (tmp_path / "feature.txt").write_text("feature")
    repo.add(["feature.txt"], ref="feature")
    feature_commit = repo.commit("feature commit", staged=True, ref="feature")
    assert feature_commit != base_commit

    store.conflict_next_ref_update = True

    try:
        repo.merge("feature", "main")
    except RefConflictError as error:
        assert str(error) == (
            f"Branch update conflict for 'main' during merge: expected {base_commit}, found {conflict_commit_id}"
        )
    else:
        raise AssertionError("Expected RefConflictError")
