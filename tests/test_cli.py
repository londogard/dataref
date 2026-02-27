from __future__ import annotations

import json
from pathlib import Path

from fluxel import run_cli


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
