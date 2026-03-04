from __future__ import annotations

import json
from pathlib import Path

from fluxel.core import FluxelFileSystem
from fluxel import run_cli
from fluxel.core import ManifestReader


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
