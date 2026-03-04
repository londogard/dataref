from __future__ import annotations

import tracemalloc
from pathlib import Path

from fluxel.core import (
    FluxelFileSystem,
    FluxelRepository,
    ManifestEntry,
    ManifestReader,
    ManifestWriter,
    build_analytical_index,
    drop_analytical_index,
    query_analytical_index,
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
