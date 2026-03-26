from __future__ import annotations

import tracemalloc
from pathlib import Path

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
    ) -> bool:
        if self.conflict_next_ref_update:
            self.conflict_next_ref_update = False
            self.write_branch_ref(branch, self.conflict_commit_id)
            return False
        return super().compare_and_set_branch_ref(
            branch,
            commit_id,
            expected_version_token=expected_version_token,
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


def test_uri_routing_reads_expected_blob_bytes(tmp_path: Path) -> None:
    dataset_root = tmp_path / "my_data"
    dataset_root.mkdir(parents=True)
    (dataset_root / "test.csv").write_text("col\n123\n")

    repo = FluxelRepository(dataset_root)
    repo.commit("add file")

    fs = FluxelFileSystem(dataset_roots={"my_data": dataset_root})
    with fs.open("fluxel://my_data@main/test.csv", "rb") as handle:
        assert handle.read() == b"col\n123\n"


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
