from __future__ import annotations

import io
import json
from datetime import datetime, timezone
from pathlib import Path

from botocore.exceptions import ClientError

from fluxel.core import FluxelFileSystem
from fluxel import run_cli
from fluxel.core import ManifestReader


class FakeStreamingBody:
    def __init__(self, payload: bytes) -> None:
        self._buffer = io.BytesIO(payload)

    def read(self, size: int = -1) -> bytes:
        return self._buffer.read(size)

    def iter_lines(self) -> list[bytes]:
        return self._buffer.getvalue().splitlines()

    def close(self) -> None:
        self._buffer.close()


class FakeS3Paginator:
    def __init__(self, objects: dict[str, dict[str, object]]) -> None:
        self._objects = objects

    def paginate(self, *, Bucket: str, Prefix: str) -> list[dict[str, object]]:
        contents = []
        for key, metadata in sorted(self._objects.items()):
            if not key.startswith(Prefix):
                continue
            contents.append(
                {
                    "Key": key,
                    "Size": len(metadata["Body"]),
                    "LastModified": metadata["LastModified"],
                }
            )
        return [{"Contents": contents}]


class FakeS3Client:
    def __init__(self, objects: dict[str, dict[str, object]]) -> None:
        self._objects = objects

    def get_paginator(self, operation_name: str) -> FakeS3Paginator:
        assert operation_name == "list_objects_v2"
        return FakeS3Paginator(self._objects)

    def get_object(self, *, Bucket: str, Key: str) -> dict[str, object]:
        assert Bucket == "demo-bucket"
        metadata = self._objects.get(Key)
        if metadata is None:
            raise self._client_error("NoSuchKey")
        return {
            "Body": FakeStreamingBody(metadata["Body"]),
            "ETag": metadata["ETag"],
        }

    def put_object(
        self,
        *,
        Bucket: str,
        Key: str,
        Body: object,
        IfNoneMatch: str | None = None,
    ) -> dict[str, object]:
        assert Bucket == "demo-bucket"
        if IfNoneMatch == "*" and Key in self._objects:
            raise self._client_error("PreconditionFailed")

        payload = Body.read() if hasattr(Body, "read") else Body
        if not isinstance(payload, bytes):
            payload = bytes(payload)
        self._objects[Key] = {
            "Body": payload,
            "LastModified": datetime.now(timezone.utc),
            "ETag": self._etag(payload),
        }
        return {"ETag": self._objects[Key]["ETag"]}

    def head_object(self, *, Bucket: str, Key: str) -> dict[str, object]:
        assert Bucket == "demo-bucket"
        metadata = self._objects.get(Key)
        if metadata is None:
            raise self._client_error("404")
        return {"ETag": metadata["ETag"]}

    def delete_object(self, *, Bucket: str, Key: str) -> dict[str, object]:
        assert Bucket == "demo-bucket"
        self._objects.pop(Key, None)
        return {}

    def _etag(self, payload: bytes) -> str:
        return f'"{len(payload):x}-{sum(payload):x}"'

    def _client_error(self, code: str) -> ClientError:
        return ClientError({"Error": {"Code": code, "Message": code}}, "fake_s3")


def install_fake_s3(monkeypatch, objects: dict[str, bytes]) -> FakeS3Client:
    client = FakeS3Client(
        {
            key: {
                "Body": payload,
                "LastModified": datetime(2026, 1, 2, tzinfo=timezone.utc),
                "ETag": f'"{len(payload):x}-{sum(payload):x}"',
            }
            for key, payload in objects.items()
        }
    )
    monkeypatch.setattr("fluxel.core.storage.boto3.client", lambda service_name: client)
    monkeypatch.setattr(
        "fluxel.core.repository_store.boto3.client",
        lambda service_name: client,
    )
    return client


def test_cli_supports_global_repo_flag_for_s3_repositories(
    tmp_path: Path, capsys, monkeypatch
) -> None:
    client = install_fake_s3(monkeypatch, {})
    monkeypatch.chdir(tmp_path)
    repo_uri = "s3://demo-bucket/repos/demo"

    (tmp_path / "a.txt").write_text("one")
    assert run_cli(["--repo", repo_uri, "commit", "-m", "initial"]) == 0
    commit_a = capsys.readouterr().out.strip()
    assert len(commit_a) == 64

    assert run_cli(["--repo", repo_uri, "branch", "feature"]) == 0
    branch_out = capsys.readouterr().out.strip()
    assert branch_out.endswith("/.fluxel/refs/heads/feature")

    (tmp_path / "a.txt").write_text("two")
    assert run_cli(["--repo", repo_uri, "commit", "-m", "update"]) == 0
    commit_b = capsys.readouterr().out.strip()
    assert len(commit_b) == 64
    assert commit_a != commit_b

    assert run_cli(["--repo", repo_uri, "diff", commit_a, commit_b]) == 0
    diff_payload = json.loads(capsys.readouterr().out)
    assert [entry["path"] for entry in diff_payload] == ["a.txt"]
    assert diff_payload[0]["change"] == "modified"

    assert run_cli(["--repo", repo_uri, "index", "build"]) == 0
    build_payload = json.loads(capsys.readouterr().out)
    db_path = Path(build_payload["database_path"])
    assert db_path.exists()
    assert "clients" in db_path.as_posix()

    assert (
        run_cli(
            ["index", "query", "--db", str(db_path), "--sql", "SELECT path FROM files"]
        )
        == 0
    )
    query_payload = json.loads(capsys.readouterr().out)
    assert query_payload == [["a.txt"]]

    assert not list((tmp_path / ".fluxel" / "commits").glob("*.json"))
    assert f"repos/demo/commits/{commit_b}.json" in client._objects
    assert "repos/demo/refs/heads/main" in client._objects


def test_cli_commit_branch_and_diff(tmp_path: Path, capsys) -> None:
    (tmp_path / "a.txt").write_text("one")

    assert run_cli(["commit", "--root", str(tmp_path), "-m", "initial"]) == 0
    commit_a = capsys.readouterr().out.strip()
    assert len(commit_a) == 64

    (tmp_path / "a.txt").write_text("two")
    assert run_cli(["commit", "--root", str(tmp_path), "-m", "update"]) == 0
    commit_b = capsys.readouterr().out.strip()
    assert len(commit_b) == 64

    assert run_cli(["branch", "--root", str(tmp_path), "exp"]) == 0
    branch_out = capsys.readouterr().out.strip()
    assert branch_out.endswith("/.fluxel/refs/heads/exp")

    assert run_cli(["diff", "--root", str(tmp_path), commit_a, commit_b]) == 0
    diff_payload = json.loads(capsys.readouterr().out)
    assert diff_payload == [
        {
            "path": "a.txt",
            "change": "modified",
            "before_hash": diff_payload[0]["before_hash"],
            "after_hash": diff_payload[0]["after_hash"],
            "before_size": 3,
            "after_size": 3,
        }
    ]
    assert diff_payload[0]["before_hash"] != diff_payload[0]["after_hash"]


def test_cli_index_build_query_drop(tmp_path: Path, capsys) -> None:
    (tmp_path / "x.jpg").write_bytes(b"img")
    (tmp_path / "y.txt").write_text("text")

    assert run_cli(["commit", "--root", str(tmp_path), "-m", "seed"]) == 0
    capsys.readouterr()

    assert run_cli(["index", "build", "--root", str(tmp_path), "--parquet"]) == 0
    build_payload = json.loads(capsys.readouterr().out)
    db_path = Path(build_payload["database_path"])
    parquet_path = Path(build_payload["parquet_path"])
    assert db_path.exists()
    assert parquet_path.exists()

    assert (
        run_cli(
            [
                "index",
                "query",
                "--db",
                str(db_path),
                "--sql",
                "SELECT COUNT(*) FROM files",
            ]
        )
        == 0
    )
    query_payload = json.loads(capsys.readouterr().out)
    assert query_payload == [[2]]

    assert run_cli(["index", "drop", "--db", str(db_path)]) == 0
    drop_output = capsys.readouterr().out.strip()
    assert drop_output == "ok"
    assert not db_path.exists()


def test_cli_commit_with_metadata_identity_mode(tmp_path: Path, capsys) -> None:
    (tmp_path / "a.txt").write_text("payload")

    assert (
        run_cli(["commit", "--root", str(tmp_path), "-m", "meta", "--identity", "meta"])
        == 0
    )
    capsys.readouterr()

    manifest_paths = sorted((tmp_path / ".fluxel" / "manifests").glob("*.jsonl"))
    assert len(manifest_paths) == 1
    entries = list(ManifestReader(manifest_paths[0]).iter_entries())
    assert len(entries) == 1
    entry = entries[0]
    assert entry.identity_mode == "meta"
    assert entry.identity_value is not None
    assert entry.blob_hash is None
    assert entry.source_uri is not None


def test_cli_verify_promotes_metadata_entries(tmp_path: Path, capsys) -> None:
    (tmp_path / "a.txt").write_text("payload")

    assert (
        run_cli(["commit", "--root", str(tmp_path), "-m", "meta", "--identity", "meta"])
        == 0
    )
    first_commit = capsys.readouterr().out.strip()
    assert first_commit

    assert run_cli(["verify", "--root", str(tmp_path), "--ref", "main"]) == 0
    verify_payload = json.loads(capsys.readouterr().out)
    assert verify_payload["created_commit"] is True
    assert verify_payload["verified_entries"] == 1
    assert verify_payload["commit_id"] != first_commit

    commit_payload = json.loads(
        (
            tmp_path / ".fluxel" / "commits" / f"{verify_payload['commit_id']}.json"
        ).read_text()
    )
    manifest_path = (
        tmp_path / ".fluxel" / "manifests" / f"{commit_payload['manifest']}.jsonl"
    )
    latest_entries = list(ManifestReader(manifest_path).iter_entries())
    assert latest_entries[0].identity_mode == "blake3"
    assert latest_entries[0].blob_hash is not None

    assert run_cli(["verify", "--root", str(tmp_path), "--ref", "main"]) == 0
    verify_again_payload = json.loads(capsys.readouterr().out)
    assert verify_again_payload["created_commit"] is False
    assert verify_again_payload["verified_entries"] == 0


def test_cli_verify_dry_run_reports_without_changes(tmp_path: Path, capsys) -> None:
    (tmp_path / "a.txt").write_text("payload")

    assert (
        run_cli(["commit", "--root", str(tmp_path), "-m", "meta", "--identity", "meta"])
        == 0
    )
    first_commit = capsys.readouterr().out.strip()

    assert (
        run_cli(["verify", "--root", str(tmp_path), "--ref", "main", "--dry-run"]) == 0
    )
    payload = json.loads(capsys.readouterr().out)
    assert payload["dry_run"] is True
    assert payload["created_commit"] is False
    assert payload["verified_entries"] == 0
    assert payload["candidate_entries"] == 1
    assert payload["commit_id"] == first_commit


def test_cli_staging_commit_is_branch_scoped(tmp_path: Path, capsys) -> None:
    (tmp_path / "shared.txt").write_text("base")
    assert run_cli(["commit", "--root", str(tmp_path), "-m", "base"]) == 0
    base_commit = capsys.readouterr().out.strip()
    assert base_commit

    assert run_cli(["branch", "--root", str(tmp_path), "feature"]) == 0
    capsys.readouterr()

    (tmp_path / "feature.txt").write_text("feature")
    assert (
        run_cli(
            [
                "add",
                "--root",
                str(tmp_path),
                "--ref",
                "feature",
                "feature.txt",
            ]
        )
        == 0
    )
    add_payload = json.loads(capsys.readouterr().out)
    assert add_payload["added"] == ["feature.txt"]

    fs = FluxelFileSystem(dataset_roots={"demo": tmp_path})
    staged_root_listing = fs.ls("fluxel://demo@feature+staged/", detail=False)
    assert "fluxel://demo@feature/shared.txt" in staged_root_listing
    assert "fluxel://demo@feature/feature.txt" in staged_root_listing

    staged_wildcard_listing = fs.ls("fluxel://demo@feature+staged/*", detail=False)
    assert "fluxel://demo@feature/shared.txt" in staged_wildcard_listing
    assert "fluxel://demo@feature/feature.txt" in staged_wildcard_listing

    with fs.open("fluxel://demo@feature+staged/feature.txt", "rb") as handle:
        assert handle.read() == b"feature"

    assert (
        run_cli(
            [
                "commit",
                "--root",
                str(tmp_path),
                "--ref",
                "feature",
                "--staged",
                "-m",
                "feature only",
            ]
        )
        == 0
    )
    feature_commit = capsys.readouterr().out.strip()
    assert feature_commit and feature_commit != base_commit

    assert run_cli(["status", "--root", str(tmp_path), "--ref", "feature"]) == 0
    status_payload = json.loads(capsys.readouterr().out)
    assert status_payload["added"] == []
    assert status_payload["removed"] == []

    assert run_cli(["diff", "--root", str(tmp_path), "main", "feature"]) == 0
    diff_payload = json.loads(capsys.readouterr().out)
    assert diff_payload == [
        {
            "path": "feature.txt",
            "change": "added",
            "before_hash": None,
            "after_hash": diff_payload[0]["after_hash"],
            "before_size": None,
            "after_size": 7,
        }
    ]


def test_cli_merge_fast_forwards_target_branch(tmp_path: Path, capsys) -> None:
    (tmp_path / "shared.txt").write_text("base")
    assert run_cli(["commit", "--root", str(tmp_path), "-m", "base"]) == 0
    base_commit = capsys.readouterr().out.strip()
    assert base_commit

    assert run_cli(["branch", "--root", str(tmp_path), "feature"]) == 0
    capsys.readouterr()

    (tmp_path / "feature.txt").write_text("feature")
    assert (
        run_cli(
            [
                "add",
                "--root",
                str(tmp_path),
                "--ref",
                "feature",
                "feature.txt",
            ]
        )
        == 0
    )
    capsys.readouterr()

    assert (
        run_cli(
            [
                "commit",
                "--root",
                str(tmp_path),
                "--ref",
                "feature",
                "--staged",
                "-m",
                "feature commit",
            ]
        )
        == 0
    )
    feature_commit = capsys.readouterr().out.strip()
    assert feature_commit and feature_commit != base_commit

    assert run_cli(["merge", "--root", str(tmp_path), "feature", "main"]) == 0
    merge_payload = json.loads(capsys.readouterr().out)
    assert merge_payload == {
        "source_ref": "feature",
        "target_ref": "main",
        "commit_id": feature_commit,
        "updated": True,
    }

    assert run_cli(["diff", "--root", str(tmp_path), "main", "feature"]) == 0
    assert json.loads(capsys.readouterr().out) == []


def test_cli_merge_rejects_non_fast_forward(tmp_path: Path, capsys) -> None:
    (tmp_path / "shared.txt").write_text("base")
    assert run_cli(["commit", "--root", str(tmp_path), "-m", "base"]) == 0
    capsys.readouterr()

    assert run_cli(["branch", "--root", str(tmp_path), "feature"]) == 0
    capsys.readouterr()

    (tmp_path / "main.txt").write_text("main")
    assert run_cli(["commit", "--root", str(tmp_path), "-m", "main commit"]) == 0
    main_commit = capsys.readouterr().out.strip()
    assert main_commit

    (tmp_path / "feature.txt").write_text("feature")
    assert (
        run_cli(
            [
                "add",
                "--root",
                str(tmp_path),
                "--ref",
                "feature",
                "feature.txt",
            ]
        )
        == 0
    )
    capsys.readouterr()

    assert (
        run_cli(
            [
                "commit",
                "--root",
                str(tmp_path),
                "--ref",
                "feature",
                "--staged",
                "-m",
                "feature commit",
            ]
        )
        == 0
    )
    feature_commit = capsys.readouterr().out.strip()
    assert feature_commit and feature_commit != main_commit

    assert run_cli(["merge", "--root", str(tmp_path), "feature", "main"]) == 2
    stderr = capsys.readouterr().err
    assert "merge error: Cannot fast-forward" in stderr


def test_cli_import_s3_writes_manifest_and_blobs(
    tmp_path: Path, capsys, monkeypatch
) -> None:
    install_fake_s3(
        monkeypatch,
        {
            "bootstrap/a.txt": b"alpha",
            "bootstrap/nested/b.txt": b"beta",
            "bootstrap/": b"",
        },
    )

    assert (
        run_cli(
            [
                "import",
                "--root",
                str(tmp_path),
                "s3://demo-bucket/bootstrap",
                "-m",
                "bootstrap",
            ]
        )
        == 0
    )
    commit_id = capsys.readouterr().out.strip()
    assert len(commit_id) == 64

    manifest_paths = sorted((tmp_path / ".fluxel" / "manifests").glob("*.jsonl"))
    assert len(manifest_paths) == 1
    entries = list(ManifestReader(manifest_paths[0]).iter_entries())
    assert [entry.path for entry in entries] == ["a.txt", "nested/b.txt"]
    assert all(entry.identity_mode == "blake3" for entry in entries)
    assert all(entry.blob_hash for entry in entries)
    assert {entry.source_uri for entry in entries} == {
        "s3://demo-bucket/bootstrap/a.txt",
        "s3://demo-bucket/bootstrap/nested/b.txt",
    }
    for entry in entries:
        blob_path = (
            tmp_path / ".fluxel" / "blobs" / entry.blob_hash[:2] / entry.blob_hash[2:]
        )
        assert blob_path.exists()


def test_cli_import_s3_metadata_entries_can_be_read_and_verified(
    tmp_path: Path, capsys, monkeypatch
) -> None:
    install_fake_s3(
        monkeypatch,
        {
            "imports/a.txt": b"alpha",
            "imports/nested/b.txt": b"beta",
        },
    )

    assert (
        run_cli(
            [
                "import",
                "--root",
                str(tmp_path),
                "s3://demo-bucket/imports",
                "-m",
                "metadata import",
                "--identity",
                "meta",
            ]
        )
        == 0
    )
    first_commit = capsys.readouterr().out.strip()
    assert len(first_commit) == 64
    assert not any((tmp_path / ".fluxel" / "blobs").rglob("*"))

    manifest_path = next((tmp_path / ".fluxel" / "manifests").glob("*.jsonl"))
    entries = list(ManifestReader(manifest_path).iter_entries())
    assert [entry.identity_mode for entry in entries] == ["meta", "meta"]
    assert [entry.blob_hash for entry in entries] == [None, None]

    fs = FluxelFileSystem(dataset_roots={"demo": tmp_path})
    with fs.open("fluxel://demo@main/nested/b.txt", "rb") as handle:
        assert handle.read() == b"beta"

    assert run_cli(["verify", "--root", str(tmp_path), "--ref", "main"]) == 0
    verify_payload = json.loads(capsys.readouterr().out)
    assert verify_payload["created_commit"] is True
    assert verify_payload["verified_entries"] == 2
    assert verify_payload["commit_id"] != first_commit


def test_cli_import_s3_supports_repeated_path_filters(
    tmp_path: Path, capsys, monkeypatch
) -> None:
    install_fake_s3(
        monkeypatch,
        {
            "gallery/root.jpg": b"root-jpg",
            "gallery/root.txt": b"root-txt",
            "gallery/nested/photo.jpg": b"nested-jpg",
            "gallery/nested/notes.txt": b"nested-txt",
        },
    )

    assert (
        run_cli(
            [
                "import",
                "--root",
                str(tmp_path),
                "s3://demo-bucket/gallery",
                "-m",
                "filtered import",
                "--path",
                "**/*.jpg",
                "--path",
                "root.txt",
            ]
        )
        == 0
    )
    commit_id = capsys.readouterr().out.strip()
    assert len(commit_id) == 64

    manifest_path = next((tmp_path / ".fluxel" / "manifests").glob("*.jsonl"))
    entries = list(ManifestReader(manifest_path).iter_entries())
    assert [entry.path for entry in entries] == [
        "nested/photo.jpg",
        "root.jpg",
        "root.txt",
    ]


def test_cli_import_s3_path_star_imports_all_entries(
    tmp_path: Path, capsys, monkeypatch
) -> None:
    install_fake_s3(
        monkeypatch,
        {
            "all/a.txt": b"a",
            "all/nested/b.jpg": b"b",
        },
    )

    assert (
        run_cli(
            [
                "import",
                "--root",
                str(tmp_path),
                "s3://demo-bucket/all",
                "-m",
                "all entries",
                "--path",
                "*",
            ]
        )
        == 0
    )
    capsys.readouterr()

    manifest_path = next((tmp_path / ".fluxel" / "manifests").glob("*.jsonl"))
    entries = list(ManifestReader(manifest_path).iter_entries())
    assert [entry.path for entry in entries] == ["a.txt", "nested/b.jpg"]


def test_cli_dataset_can_mix_s3_meta_local_blake3_and_verified_s3_entries(
    tmp_path: Path, capsys, monkeypatch
) -> None:
    install_fake_s3(
        monkeypatch,
        {
            "dataset/root.txt": b"root-from-s3",
            "dataset/images/cat.jpg": b"cat-image",
            "dataset/images/dog.jpg": b"dog-image",
            "dataset/docs/readme.md": b"ignored",
        },
    )

    assert (
        run_cli(
            [
                "import",
                "--root",
                str(tmp_path),
                "s3://demo-bucket/dataset",
                "-m",
                "bootstrap metadata import",
                "--identity",
                "meta",
                "--path",
                "root.txt",
                "--path",
                "**/*.jpg",
            ]
        )
        == 0
    )
    first_commit = capsys.readouterr().out.strip()
    assert len(first_commit) == 64
    assert (tmp_path / ".fluxel").exists()

    (tmp_path / "local").mkdir()
    (tmp_path / "local" / "one.txt").write_text("one")
    (tmp_path / "local" / "two.txt").write_text("two")

    assert (
        run_cli(
            [
                "add",
                "--root",
                str(tmp_path),
                "--identity",
                "blake3",
                "local/one.txt",
                "local/two.txt",
            ]
        )
        == 0
    )
    add_payload = json.loads(capsys.readouterr().out)
    assert add_payload["added"] == ["local/one.txt", "local/two.txt"]

    assert (
        run_cli(
            [
                "commit",
                "--root",
                str(tmp_path),
                "--staged",
                "-m",
                "add local blake3 files",
            ]
        )
        == 0
    )
    second_commit = capsys.readouterr().out.strip()
    assert len(second_commit) == 64
    assert second_commit != first_commit

    assert (
        run_cli(
            [
                "verify",
                "--root",
                str(tmp_path),
                "--ref",
                "main",
                "--path",
                "root.txt",
                "--path",
                "images/cat.jpg",
            ]
        )
        == 0
    )
    verify_payload = json.loads(capsys.readouterr().out)
    assert verify_payload["created_commit"] is True
    assert verify_payload["verified_entries"] == 2

    commit_payload = json.loads(
        (
            tmp_path / ".fluxel" / "commits" / f"{verify_payload['commit_id']}.json"
        ).read_text()
    )
    manifest_path = (
        tmp_path / ".fluxel" / "manifests" / f"{commit_payload['manifest']}.jsonl"
    )
    entries = {
        entry.path: entry for entry in ManifestReader(manifest_path).iter_entries()
    }

    assert sorted(entries) == [
        "images/cat.jpg",
        "images/dog.jpg",
        "local/one.txt",
        "local/two.txt",
        "root.txt",
    ]

    assert entries["root.txt"].identity_mode == "blake3"
    assert entries["root.txt"].blob_hash is not None
    assert entries["images/cat.jpg"].identity_mode == "blake3"
    assert entries["images/cat.jpg"].blob_hash is not None
    assert entries["images/dog.jpg"].identity_mode == "meta"
    assert entries["images/dog.jpg"].blob_hash is None
    assert entries["local/one.txt"].identity_mode == "blake3"
    assert entries["local/one.txt"].blob_hash is not None
    assert entries["local/two.txt"].identity_mode == "blake3"
    assert entries["local/two.txt"].blob_hash is not None

    assert run_cli(["index", "build", "--root", str(tmp_path)]) == 0
    build_payload = json.loads(capsys.readouterr().out)
    db_path = Path(build_payload["database_path"])
    assert db_path.exists()

    assert (
        run_cli(
            [
                "index",
                "query",
                "--db",
                str(db_path),
                "--sql",
                "SELECT path FROM files ORDER BY path",
            ]
        )
        == 0
    )
    query_payload = json.loads(capsys.readouterr().out)
    assert query_payload == [
        ["images/cat.jpg"],
        ["images/dog.jpg"],
        ["local/one.txt"],
        ["local/two.txt"],
        ["root.txt"],
    ]


def test_cli_meta_import_branch_removal_and_fast_forward_merge(
    tmp_path: Path, capsys, monkeypatch
) -> None:
    install_fake_s3(
        monkeypatch,
        {
            "images/image0.jpg": b"image-0",
            "images/image1.jpg": b"image-1",
            "images/nested/photo0.jpg": b"photo-0",
            "images/nested/photo1.jpg": b"photo-1",
            "images/notes.txt": b"ignore-me",
        },
    )

    assert (
        run_cli(
            [
                "import",
                "--root",
                str(tmp_path),
                "s3://demo-bucket/images",
                "-m",
                "import jpg metadata",
                "--identity",
                "meta",
                "--path",
                "**/*.jpg",
            ]
        )
        == 0
    )
    main_commit = capsys.readouterr().out.strip()
    assert len(main_commit) == 64

    assert run_cli(["branch", "--root", str(tmp_path), "feature"]) == 0
    capsys.readouterr()

    assert (
        run_cli(
            [
                "rm",
                "--root",
                str(tmp_path),
                "--ref",
                "feature",
                "image0.jpg",
                "nested/photo0.jpg",
            ]
        )
        == 0
    )
    rm_payload = json.loads(capsys.readouterr().out)
    assert rm_payload["removed"] == ["image0.jpg", "nested/photo0.jpg"]

    assert (
        run_cli(
            [
                "commit",
                "--root",
                str(tmp_path),
                "--ref",
                "feature",
                "--staged",
                "-m",
                "remove zero-suffixed jpgs",
            ]
        )
        == 0
    )
    feature_commit = capsys.readouterr().out.strip()
    assert len(feature_commit) == 64
    assert feature_commit != main_commit

    fs = FluxelFileSystem(dataset_roots={"demo": tmp_path})
    main_listing = sorted(fs.ls("fluxel://demo@main/*", detail=False))
    assert main_listing == [
        "fluxel://demo@main/image0.jpg",
        "fluxel://demo@main/image1.jpg",
        "fluxel://demo@main/nested/photo0.jpg",
        "fluxel://demo@main/nested/photo1.jpg",
    ]

    feature_listing = sorted(fs.ls("fluxel://demo@feature/*", detail=False))
    assert feature_listing == [
        "fluxel://demo@feature/image1.jpg",
        "fluxel://demo@feature/nested/photo1.jpg",
    ]

    main_ref_path = tmp_path / ".fluxel" / "refs" / "heads" / "main"
    main_ref_path.write_text(f"{feature_commit}\n", encoding="utf-8")

    merged_main_listing = sorted(fs.ls("fluxel://demo@main/*", detail=False))
    assert merged_main_listing == [
        "fluxel://demo@main/image1.jpg",
        "fluxel://demo@main/nested/photo1.jpg",
    ]

    assert run_cli(["diff", "--root", str(tmp_path), "main", "feature"]) == 0
    diff_payload = json.loads(capsys.readouterr().out)
    assert diff_payload == []
