from __future__ import annotations

import json
from pathlib import Path

import pytest

from dataref import run_cli
from dataref.core.config import (
    CURRENT_FORMAT_VERSION,
    BaseConfig,
    DatarefConfig,
    LocalConfig,
    S3Config,
    init_config,
)
from dataref.core.repository import DatarefRepository


def test_config_init_via_cli_creates_config_file(tmp_path: Path, capsys) -> None:
    (tmp_path / "test.txt").write_text("data")
    assert run_cli(["commit", "--repo", str(tmp_path), "-m", "init"]) == 0
    capsys.readouterr()

    config = BaseConfig.load(tmp_path)
    assert isinstance(config, LocalConfig)
    assert config.dataset_root == str(tmp_path)
    assert config.default_branch == "main"


def test_config_init_s3_via_cli(tmp_path: Path, capsys) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    (repo / "test.txt").write_text("data")

    assert (
        run_cli(
            [
                "config",
                "init",
                "--repo",
                str(repo),
                "--backend",
                "s3",
                "--s3-bucket",
                "my-bucket",
                "--s3-prefix",
                "datasets/my-repo",
            ]
        )
        == 0
    )
    out = capsys.readouterr().out.strip()
    assert "Config initialized" in out

    config = BaseConfig.load(repo)
    assert isinstance(config, S3Config)
    assert config.bucket == "my-bucket"
    assert config.prefix == "datasets/my-repo"


def test_config_list_via_cli(tmp_path: Path, capsys) -> None:
    (tmp_path / "f.txt").write_text("x")
    assert run_cli(["commit", "--repo", str(tmp_path), "-m", "init"]) == 0
    capsys.readouterr()

    assert run_cli(["config", "list", "--repo", str(tmp_path)]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["backend"] == "local"
    assert payload["dataset_root"] == str(tmp_path)
    assert payload["default_branch"] == "main"
    assert "s3" not in payload


def test_config_get_subcommand_is_removed(tmp_path: Path) -> None:
    (tmp_path / "f.txt").write_text("x")
    assert run_cli(["commit", "--repo", str(tmp_path), "-m", "init"]) == 0

    assert run_cli(["config", "get", "--repo", str(tmp_path), "backend"]) == 2


def test_config_set_via_cli(tmp_path: Path, capsys) -> None:
    (tmp_path / "f.txt").write_text("x")
    assert run_cli(["commit", "--repo", str(tmp_path), "-m", "init"]) == 0
    capsys.readouterr()

    assert (
        run_cli(["config", "set", "--repo", str(tmp_path), "default_branch", "develop"])
        == 0
    )
    out = capsys.readouterr().out.strip()
    assert "default_branch=develop" in out

    config = BaseConfig.load(tmp_path)
    assert config is not None
    assert config.default_branch == "develop"


def test_config_no_config_error(tmp_path: Path, capsys) -> None:
    empty = tmp_path / "empty"
    empty.mkdir()

    assert run_cli(["config", "list", "--repo", str(empty)]) == 1
    stderr = capsys.readouterr().err
    assert "No config found" in stderr


def test_validate_config_rejects_missing_dataset_root(tmp_path: Path) -> None:
    try:
        LocalConfig(dataset_root="", default_branch="main")
        assert False, "Should have raised"
    except ValueError as e:
        assert "dataset_root" in str(e)


def test_validate_config_rejects_empty_bucket_for_s3(tmp_path: Path) -> None:
    try:
        S3Config(
            dataset_root=str(tmp_path),
            default_branch="main",
            bucket="",
            prefix="",
        )
        assert False, "Should have raised"
    except ValueError as e:
        assert "bucket" in str(e)


def test_config_init_idempotent(tmp_path: Path, capsys) -> None:
    (tmp_path / "f.txt").write_text("x")
    assert run_cli(["commit", "--repo", str(tmp_path), "-m", "init"]) == 0
    capsys.readouterr()

    config_path = tmp_path / ".dataref" / "config.json"
    assert config_path.exists()

    init_config(tmp_path, backend="local")
    assert config_path.exists()
    config = BaseConfig.load(tmp_path)
    assert isinstance(config, LocalConfig)


def test_config_set_s3_bucket_and_list(tmp_path: Path, capsys) -> None:
    (tmp_path / "f.txt").write_text("x")
    assert run_cli(["commit", "--repo", str(tmp_path), "-m", "init"]) == 0
    capsys.readouterr()

    assert (
        run_cli(
            [
                "config",
                "set",
                "--repo",
                str(tmp_path),
                "s3.bucket",
                "my-bucket",
            ]
        )
        == 0
    )
    capsys.readouterr()

    assert run_cli(["config", "list", "--repo", str(tmp_path)]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["bucket"] == "my-bucket"


def test_config_default_format_version(tmp_path: Path) -> None:
    config = LocalConfig(dataset_root=str(tmp_path))
    assert config.format_version == 1


def test_config_serialize_includes_format_version(tmp_path: Path) -> None:
    config = LocalConfig(dataset_root=str(tmp_path))
    config.save(tmp_path)
    raw = json.loads((tmp_path / ".dataref" / "config.json").read_text("utf-8"))
    assert raw["format_version"] == 1


def test_repo_rejects_unsupported_format_version(tmp_path: Path) -> None:
    DatarefRepository(tmp_path)
    config_path = tmp_path / ".dataref" / "config.json"
    config = json.loads(config_path.read_text("utf-8"))
    config["format_version"] = CURRENT_FORMAT_VERSION + 1
    config_path.write_text(json.dumps(config, indent=2) + "\n", "utf-8")
    try:
        DatarefRepository(tmp_path)
        assert False, "Should have raised"
    except ValueError as e:
        assert "newer" in str(e)


def test_config_missing_format_version_defaults_to_current(tmp_path: Path) -> None:
    (tmp_path / ".dataref").mkdir()
    (tmp_path / ".dataref" / "config.json").write_text(
        json.dumps(
            {
                "backend": "local",
                "dataset_root": str(tmp_path),
                "default_branch": "main",
            }
        )
    )
    config = BaseConfig.load(tmp_path)
    assert config is not None
    assert config.format_version == 1


def test_validate_config_rejects_future_format() -> None:
    try:
        LocalConfig(format_version=2, dataset_root="/tmp")
        assert False, "Should have raised"
    except ValueError as e:
        msg = str(e)
        assert "newer" in msg and "upgrade dataref" in msg


def test_validate_config_rejects_unsupported_old_format() -> None:
    try:
        LocalConfig(format_version=0, dataset_root="/tmp")
        assert False, "Should have raised"
    except ValueError as e:
        msg = str(e)
        assert "no longer supported" in msg and "dataref migrate" in msg


def test_config_save_and_load_roundtrip(tmp_path: Path) -> None:
    original: DatarefConfig = S3Config(
        dataset_root=str(tmp_path),
        default_branch="develop",
        bucket="b",
        prefix="p",
        endpoint_url="https://minio.example.com",
    )
    original.save(tmp_path)

    loaded = BaseConfig.load(tmp_path)
    assert isinstance(loaded, S3Config)
    assert loaded.dataset_root == str(tmp_path)
    assert loaded.default_branch == "develop"
    assert isinstance(loaded, S3Config)
    assert loaded.bucket == "b"
    assert loaded.prefix == "p"
    assert loaded.endpoint_url == "https://minio.example.com"
