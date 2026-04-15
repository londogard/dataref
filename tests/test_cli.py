from __future__ import annotations

import json
import shutil
from datetime import datetime, timezone
from pathlib import Path

from fluxel.core import FluxelFileSystem
from fluxel import run_cli
from fluxel.core import ManifestReader


def test_cli_supports_command_local_repo_flag_for_s3_repositories(
    tmp_path: Path, capsys, monkeypatch, fake_s3_installer
) -> None:
    client = fake_s3_installer({})
    monkeypatch.chdir(tmp_path)
    repo_uri = "s3://demo-bucket/repos/demo"

    (tmp_path / "a.txt").write_text("one")
    assert run_cli(["commit", "--repo", repo_uri, "-m", "initial"]) == 0
    commit_a = capsys.readouterr().out.strip()
    assert len(commit_a) == 64

    assert run_cli(["branch", "--repo", repo_uri, "feature"]) == 0
    branch_out = capsys.readouterr().out.strip()
    assert branch_out.endswith("/.fluxel/refs/heads/feature")

    (tmp_path / "a.txt").write_text("two")
    assert run_cli(["commit", "--repo", repo_uri, "-m", "update"]) == 0
    commit_b = capsys.readouterr().out.strip()
    assert len(commit_b) == 64
    assert commit_a != commit_b

    assert run_cli(["diff", "--repo", repo_uri, commit_a, commit_b]) == 0
    diff_payload = json.loads(capsys.readouterr().out)
    assert [entry["path"] for entry in diff_payload] == ["a.txt"]
    assert diff_payload[0]["change"] == "modified"

    assert run_cli(["index", "build", "--repo", repo_uri]) == 0
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
    assert any(key.startswith("repos/demo/manifests/") for key in client._objects)
    assert any(
        key.startswith("repos/demo/manifests/") and key.endswith(".idx")
        for key in client._objects
    )
    assert "repos/demo/refs/heads/main" in client._objects


def test_cli_metadata_only_rm_and_mv_work_for_s3_repositories(
    tmp_path: Path, capsys, monkeypatch, fake_s3_installer
) -> None:
    client = fake_s3_installer({})
    monkeypatch.chdir(tmp_path)
    repo_uri = "s3://demo-bucket/repos/demo"

    (tmp_path / "a.txt").write_text("one")
    (tmp_path / "dir").mkdir()
    (tmp_path / "dir" / "b.txt").write_text("two")

    assert run_cli(["commit", "--repo", repo_uri, "-m", "initial"]) == 0
    initial_commit = capsys.readouterr().out.strip()
    assert initial_commit

    (tmp_path / "a.txt").unlink()
    shutil.rmtree(tmp_path / "dir")

    assert (
        run_cli(
            [
                "mv",
                "--repo",
                repo_uri,
                "a.txt",
                "renamed.txt",
                "-m",
                "rename a",
            ]
        )
        == 0
    )
    mv_payload = json.loads(capsys.readouterr().out)
    assert mv_payload["moved_paths"] == ["renamed.txt"]
    moved_commit = mv_payload["commit_id"]

    assert run_cli(["rm", "--repo", repo_uri, "dir", "-m", "remove dir"]) == 0
    rm_payload = json.loads(capsys.readouterr().out)
    assert rm_payload["removed_paths"] == ["dir/b.txt"]
    removed_commit = rm_payload["commit_id"]
    assert removed_commit != moved_commit

    assert run_cli(["diff", "--repo", repo_uri, initial_commit, removed_commit]) == 0
    diff_payload = json.loads(capsys.readouterr().out)
    assert [(entry["path"], entry["change"]) for entry in diff_payload] == [
        ("a.txt", "removed"),
        ("dir/b.txt", "removed"),
        ("renamed.txt", "added"),
    ]

    assert not list((tmp_path / ".fluxel" / "commits").glob("*.json"))
    assert f"repos/demo/commits/{removed_commit}.json" in client._objects


def test_cli_commit_branch_and_diff(tmp_path: Path, capsys) -> None:
    (tmp_path / "a.txt").write_text("one")

    assert run_cli(["commit", "--repo", str(tmp_path), "-m", "initial"]) == 0
    commit_a = capsys.readouterr().out.strip()
    assert len(commit_a) == 64

    (tmp_path / "a.txt").write_text("two")
    assert run_cli(["commit", "--repo", str(tmp_path), "-m", "update"]) == 0
    commit_b = capsys.readouterr().out.strip()
    assert len(commit_b) == 64

    assert run_cli(["branch", "--repo", str(tmp_path), "exp"]) == 0
    branch_out = capsys.readouterr().out.strip()
    assert branch_out.endswith("/.fluxel/refs/heads/exp")

    assert run_cli(["diff", "--repo", str(tmp_path), commit_a, commit_b]) == 0
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

    assert run_cli(["commit", "--repo", str(tmp_path), "-m", "seed"]) == 0
    capsys.readouterr()

    assert run_cli(["index", "build", "--repo", str(tmp_path), "--parquet"]) == 0
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
        run_cli(["commit", "--repo", str(tmp_path), "-m", "meta", "--identity", "meta"])
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


def test_cli_add_reports_missing_source_cleanly(tmp_path: Path, capsys) -> None:
    repo_root = tmp_path / "repo"
    repo_root.mkdir()

    assert run_cli(["add", "--repo", str(repo_root), "missing.txt"]) == 2
    stderr = capsys.readouterr().err
    assert "add error: Cannot stage missing path: missing.txt" in stderr


def test_cli_import_rejects_invalid_path_filter_cleanly(tmp_path: Path, capsys) -> None:
    assert (
        run_cli(
            [
                "import",
                "--repo",
                str(tmp_path),
                "s3://demo-bucket/bootstrap",
                "-m",
                "filtered import",
                "--path",
                "../bad",
            ]
        )
        == 2
    )
    stderr = capsys.readouterr().err
    assert (
        "import error: Import path filter cannot traverse outside repository root"
        in stderr
    )


def test_cli_verify_reports_missing_source_cleanly(tmp_path: Path, capsys) -> None:
    (tmp_path / "a.txt").write_text("payload")
    assert (
        run_cli(["commit", "--repo", str(tmp_path), "-m", "meta", "--identity", "meta"])
        == 0
    )
    capsys.readouterr()

    (tmp_path / "a.txt").unlink()

    assert run_cli(["verify", "--repo", str(tmp_path), "--ref", "main"]) == 2
    stderr = capsys.readouterr().err
    assert "verify error:" in stderr
    assert "a.txt" in stderr


def test_cli_verify_promotes_metadata_entries(tmp_path: Path, capsys) -> None:
    (tmp_path / "a.txt").write_text("payload")

    assert (
        run_cli(["commit", "--repo", str(tmp_path), "-m", "meta", "--identity", "meta"])
        == 0
    )
    first_commit = capsys.readouterr().out.strip()
    assert first_commit

    assert run_cli(["verify", "--repo", str(tmp_path), "--ref", "main"]) == 0
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

    assert run_cli(["verify", "--repo", str(tmp_path), "--ref", "main"]) == 0
    verify_again_payload = json.loads(capsys.readouterr().out)
    assert verify_again_payload["created_commit"] is False
    assert verify_again_payload["verified_entries"] == 0


def test_cli_verify_dry_run_reports_without_changes(tmp_path: Path, capsys) -> None:
    (tmp_path / "a.txt").write_text("payload")

    assert (
        run_cli(["commit", "--repo", str(tmp_path), "-m", "meta", "--identity", "meta"])
        == 0
    )
    first_commit = capsys.readouterr().out.strip()

    assert (
        run_cli(["verify", "--repo", str(tmp_path), "--ref", "main", "--dry-run"]) == 0
    )
    payload = json.loads(capsys.readouterr().out)
    assert payload["dry_run"] is True
    assert payload["created_commit"] is False
    assert payload["verified_entries"] == 0
    assert payload["candidate_entries"] == 1
    assert payload["commit_id"] == first_commit


def test_cli_staging_commit_is_branch_scoped(tmp_path: Path, capsys) -> None:
    (tmp_path / "shared.txt").write_text("base")
    assert run_cli(["commit", "--repo", str(tmp_path), "-m", "base"]) == 0
    base_commit = capsys.readouterr().out.strip()
    assert base_commit

    assert run_cli(["branch", "--repo", str(tmp_path), "feature"]) == 0
    capsys.readouterr()

    (tmp_path / "feature.txt").write_text("feature")
    assert (
        run_cli(
            [
                "add",
                "--repo",
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
                "--repo",
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

    assert run_cli(["status", "--repo", str(tmp_path), "--ref", "feature"]) == 0
    status_payload = json.loads(capsys.readouterr().out)
    assert status_payload["added"] == []
    assert status_payload["removed"] == []

    assert run_cli(["diff", "--repo", str(tmp_path), "main", "feature"]) == 0
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


def test_cli_add_supports_arbitrary_local_source_with_logical_destination(
    tmp_path: Path, capsys
) -> None:
    repo_root = tmp_path / "repo"
    repo_root.mkdir()
    external_file = tmp_path / "external.txt"
    external_file.write_text("external payload")

    assert (
        run_cli(
            [
                "add",
                "--repo",
                str(repo_root),
                "--as",
                "imports/external.txt",
                str(external_file),
            ]
        )
        == 0
    )
    add_payload = json.loads(capsys.readouterr().out)
    assert add_payload["added"] == ["imports/external.txt"]

    assert (
        run_cli(
            [
                "commit",
                "--repo",
                str(repo_root),
                "--staged",
                "-m",
                "ingest external",
            ]
        )
        == 0
    )
    commit_id = capsys.readouterr().out.strip()
    assert len(commit_id) == 64

    commit_payload = json.loads(
        (repo_root / ".fluxel" / "commits" / f"{commit_id}.json").read_text()
    )
    manifest_path = (
        repo_root / ".fluxel" / "manifests" / f"{commit_payload['manifest']}.jsonl"
    )
    entries = list(ManifestReader(manifest_path).iter_entries())
    assert [entry.path for entry in entries] == ["imports/external.txt"]
    assert entries[0].source_uri is None


def test_cli_add_supports_s3_source_with_staged_read_and_logical_destination(
    tmp_path: Path, capsys, fake_s3_installer
) -> None:
    fake_s3_installer({"incoming/source.txt": b"remote payload"})
    repo_root = tmp_path / "repo"
    repo_root.mkdir()

    (repo_root / "base.txt").write_text("base")
    assert run_cli(["commit", "--repo", str(repo_root), "-m", "base"]) == 0
    capsys.readouterr()

    assert (
        run_cli(
            [
                "add",
                "--repo",
                str(repo_root),
                "--identity",
                "meta",
                "--as",
                "imports/source.txt",
                "s3://demo-bucket/incoming/source.txt",
            ]
        )
        == 0
    )
    add_payload = json.loads(capsys.readouterr().out)
    assert add_payload["added"] == ["imports/source.txt"]

    fs = FluxelFileSystem(dataset_roots={"demo": repo_root})
    with fs.open("fluxel://demo@main+staged/imports/source.txt", "rb") as handle:
        assert handle.read() == b"remote payload"

    assert (
        run_cli(
            [
                "commit",
                "--repo",
                str(repo_root),
                "--staged",
                "-m",
                "ingest remote",
            ]
        )
        == 0
    )
    commit_id = capsys.readouterr().out.strip()
    assert len(commit_id) == 64

    commit_payload = json.loads(
        (repo_root / ".fluxel" / "commits" / f"{commit_id}.json").read_text()
    )
    manifest_path = (
        repo_root / ".fluxel" / "manifests" / f"{commit_payload['manifest']}.jsonl"
    )
    entries = list(ManifestReader(manifest_path).iter_entries())
    assert {entry.path for entry in entries} == {"base.txt", "imports/source.txt"}
    imported_entry = next(
        entry for entry in entries if entry.path == "imports/source.txt"
    )
    assert imported_entry.identity_mode == "meta"
    assert imported_entry.blob_hash is None
    assert imported_entry.source_uri == "s3://demo-bucket/incoming/source.txt"


def test_cli_add_supports_local_directory_source_with_destination_prefix(
    tmp_path: Path, capsys
) -> None:
    repo_root = tmp_path / "repo"
    repo_root.mkdir()
    external_dir = tmp_path / "bundle"
    (external_dir / "nested").mkdir(parents=True)
    (external_dir / "a.txt").write_text("alpha")
    (external_dir / "nested" / "b.txt").write_text("beta")

    assert (
        run_cli(
            [
                "add",
                "--repo",
                str(repo_root),
                "--as",
                "imports/bundle",
                str(external_dir),
            ]
        )
        == 0
    )
    add_payload = json.loads(capsys.readouterr().out)
    assert add_payload["added"] == [
        "imports/bundle/a.txt",
        "imports/bundle/nested/b.txt",
    ]

    assert (
        run_cli(
            [
                "commit",
                "--repo",
                str(repo_root),
                "--staged",
                "-m",
                "ingest bundle",
            ]
        )
        == 0
    )
    commit_id = capsys.readouterr().out.strip()
    commit_payload = json.loads(
        (repo_root / ".fluxel" / "commits" / f"{commit_id}.json").read_text()
    )
    manifest_path = (
        repo_root / ".fluxel" / "manifests" / f"{commit_payload['manifest']}.jsonl"
    )
    assert [entry.path for entry in ManifestReader(manifest_path).iter_entries()] == [
        "imports/bundle/a.txt",
        "imports/bundle/nested/b.txt",
    ]


def test_cli_add_supports_s3_prefix_with_destination_prefix(
    tmp_path: Path, capsys, fake_s3_installer
) -> None:
    fake_s3_installer(
        {
            "incoming/batch/a.txt": b"alpha",
            "incoming/batch/nested/b.txt": b"beta",
        }
    )
    repo_root = tmp_path / "repo"
    repo_root.mkdir()
    (repo_root / "base.txt").write_text("base")
    assert run_cli(["commit", "--repo", str(repo_root), "-m", "base"]) == 0
    capsys.readouterr()

    assert (
        run_cli(
            [
                "add",
                "--repo",
                str(repo_root),
                "--identity",
                "meta",
                "--as",
                "imports/batch",
                "s3://demo-bucket/incoming/batch",
            ]
        )
        == 0
    )
    add_payload = json.loads(capsys.readouterr().out)
    assert add_payload["added"] == [
        "imports/batch/a.txt",
        "imports/batch/nested/b.txt",
    ]

    fs = FluxelFileSystem(dataset_roots={"demo": repo_root})
    with fs.open(
        "fluxel://demo@main+staged/imports/batch/nested/b.txt", "rb"
    ) as handle:
        assert handle.read() == b"beta"

    assert (
        run_cli(
            [
                "commit",
                "--repo",
                str(repo_root),
                "--staged",
                "-m",
                "ingest batch",
            ]
        )
        == 0
    )
    capsys.readouterr()


def test_cli_remote_staged_add_preserves_existing_entries_and_uploads_one_blob(
    tmp_path: Path, capsys, monkeypatch, fake_s3_installer
) -> None:
    client = fake_s3_installer({})
    monkeypatch.chdir(tmp_path)
    repo_uri = "s3://demo-bucket/repos/demo"

    (tmp_path / "a.txt").write_text("alpha")
    assert run_cli(["commit", "--repo", repo_uri, "-m", "initial"]) == 0
    first_commit = capsys.readouterr().out.strip()
    assert len(first_commit) == 64

    initial_blob_keys = {
        key for key in client._objects if key.startswith("repos/demo/blobs/")
    }
    assert len(initial_blob_keys) == 1

    (tmp_path / "a.txt").unlink()
    (tmp_path / "b.txt").write_text("beta")

    assert run_cli(["add", "--repo", repo_uri, "b.txt"]) == 0
    add_payload = json.loads(capsys.readouterr().out)
    assert add_payload["added"] == ["b.txt"]

    assert run_cli(["commit", "--repo", repo_uri, "--staged", "-m", "add b"]) == 0
    second_commit = capsys.readouterr().out.strip()
    assert len(second_commit) == 64
    assert second_commit != first_commit

    new_blob_keys = {
        key for key in client._objects if key.startswith("repos/demo/blobs/")
    }
    assert len(new_blob_keys - initial_blob_keys) == 1

    assert run_cli(["diff", "--repo", repo_uri, first_commit, second_commit]) == 0
    diff_payload = json.loads(capsys.readouterr().out)
    assert diff_payload == [
        {
            "path": "b.txt",
            "change": "added",
            "before_hash": None,
            "after_hash": diff_payload[0]["after_hash"],
            "before_size": None,
            "after_size": 4,
        }
    ]


def test_cli_remote_commit_recovers_from_stale_branch_lock(
    tmp_path: Path, capsys, monkeypatch, fake_s3_installer
) -> None:
    client = fake_s3_installer({})
    monkeypatch.chdir(tmp_path)
    repo_uri = "s3://demo-bucket/repos/demo"
    lock_key = "repos/demo/locks/refs/heads/main.lock"
    client._objects[lock_key] = {
        "Body": b"legacy-stale-lock",
        "LastModified": datetime(2025, 1, 1, tzinfo=timezone.utc),
        "ETag": '"11-11"',
    }

    (tmp_path / "a.txt").write_text("alpha")
    assert run_cli(["commit", "--repo", repo_uri, "-m", "initial"]) == 0
    commit_id = capsys.readouterr().out.strip()

    assert len(commit_id) == 64
    assert lock_key not in client._objects
    assert "repos/demo/refs/heads/main" in client._objects


def test_cli_merge_fast_forwards_target_branch(tmp_path: Path, capsys) -> None:
    (tmp_path / "shared.txt").write_text("base")
    assert run_cli(["commit", "--repo", str(tmp_path), "-m", "base"]) == 0
    base_commit = capsys.readouterr().out.strip()
    assert base_commit

    assert run_cli(["branch", "--repo", str(tmp_path), "feature"]) == 0
    capsys.readouterr()

    (tmp_path / "feature.txt").write_text("feature")
    assert (
        run_cli(
            [
                "add",
                "--repo",
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
                "--repo",
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

    assert run_cli(["merge", "--repo", str(tmp_path), "feature", "main"]) == 0
    merge_payload = json.loads(capsys.readouterr().out)
    assert merge_payload == {
        "source_ref": "feature",
        "target_ref": "main",
        "commit_id": feature_commit,
        "updated": True,
    }

    assert run_cli(["diff", "--repo", str(tmp_path), "main", "feature"]) == 0
    assert json.loads(capsys.readouterr().out) == []


def test_cli_merge_rejects_non_fast_forward(tmp_path: Path, capsys) -> None:
    (tmp_path / "shared.txt").write_text("base")
    assert run_cli(["commit", "--repo", str(tmp_path), "-m", "base"]) == 0
    capsys.readouterr()

    assert run_cli(["branch", "--repo", str(tmp_path), "feature"]) == 0
    capsys.readouterr()

    (tmp_path / "main.txt").write_text("main")
    assert run_cli(["commit", "--repo", str(tmp_path), "-m", "main commit"]) == 0
    main_commit = capsys.readouterr().out.strip()
    assert main_commit

    (tmp_path / "feature.txt").write_text("feature")
    assert (
        run_cli(
            [
                "add",
                "--repo",
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
                "--repo",
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

    assert run_cli(["merge", "--repo", str(tmp_path), "feature", "main"]) == 2
    stderr = capsys.readouterr().err
    assert "merge error: Cannot fast-forward" in stderr


def test_cli_import_s3_writes_manifest_and_blobs(
    tmp_path: Path, capsys, fake_s3_installer
) -> None:
    fake_s3_installer(
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
                "--repo",
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
    assert all(entry.source_uri is None for entry in entries)
    for entry in entries:
        blob_path = (
            tmp_path / ".fluxel" / "blobs" / entry.blob_hash[:2] / entry.blob_hash[2:]
        )
        assert blob_path.exists()


def test_cli_import_s3_preserves_existing_manifest_entries(
    tmp_path: Path, capsys, fake_s3_installer
) -> None:
    fake_s3_installer(
        {
            "bootstrap/a.txt": b"alpha",
            "incremental/b.txt": b"beta",
        },
    )

    assert (
        run_cli(
            [
                "import",
                "--repo",
                str(tmp_path),
                "s3://demo-bucket/bootstrap",
                "-m",
                "bootstrap",
            ]
        )
        == 0
    )
    first_commit = capsys.readouterr().out.strip()
    assert len(first_commit) == 64

    assert (
        run_cli(
            [
                "import",
                "--repo",
                str(tmp_path),
                "s3://demo-bucket/incremental",
                "-m",
                "incremental",
            ]
        )
        == 0
    )
    second_commit = capsys.readouterr().out.strip()
    assert len(second_commit) == 64
    assert second_commit != first_commit

    commit_payload = json.loads(
        (tmp_path / ".fluxel" / "commits" / f"{second_commit}.json").read_text()
    )
    manifest_path = (
        tmp_path / ".fluxel" / "manifests" / f"{commit_payload['manifest']}.jsonl"
    )
    entries = list(ManifestReader(manifest_path).iter_entries())

    assert [entry.path for entry in entries] == ["a.txt", "b.txt"]
    assert all(entry.source_uri is None for entry in entries)
    assert (
        sum(1 for path in (tmp_path / ".fluxel" / "blobs").rglob("*") if path.is_file())
        == 2
    )


def test_cli_import_s3_metadata_entries_can_be_read_and_verified(
    tmp_path: Path, capsys, fake_s3_installer
) -> None:
    fake_s3_installer(
        {
            "imports/a.txt": b"alpha",
            "imports/nested/b.txt": b"beta",
        },
    )

    assert (
        run_cli(
            [
                "import",
                "--repo",
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

    assert run_cli(["verify", "--repo", str(tmp_path), "--ref", "main"]) == 0
    verify_payload = json.loads(capsys.readouterr().out)
    assert verify_payload["created_commit"] is True
    assert verify_payload["verified_entries"] == 2
    assert verify_payload["commit_id"] != first_commit


def test_cli_import_s3_supports_repeated_path_filters(
    tmp_path: Path, capsys, fake_s3_installer
) -> None:
    fake_s3_installer(
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
                "--repo",
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
    tmp_path: Path, capsys, fake_s3_installer
) -> None:
    fake_s3_installer(
        {
            "all/a.txt": b"a",
            "all/nested/b.jpg": b"b",
        },
    )

    assert (
        run_cli(
            [
                "import",
                "--repo",
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
    tmp_path: Path, capsys, fake_s3_installer
) -> None:
    fake_s3_installer(
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
                "--repo",
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
                "--repo",
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
                "--repo",
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
                "--repo",
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

    assert run_cli(["index", "build", "--repo", str(tmp_path)]) == 0
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
    tmp_path: Path, capsys, fake_s3_installer
) -> None:
    fake_s3_installer(
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
                "--repo",
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

    assert run_cli(["branch", "--repo", str(tmp_path), "feature"]) == 0
    capsys.readouterr()

    assert (
        run_cli(
            [
                "rm",
                "--repo",
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
                "--repo",
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

    assert run_cli(["diff", "--repo", str(tmp_path), "main", "feature"]) == 0
    diff_payload = json.loads(capsys.readouterr().out)
    assert diff_payload == []
