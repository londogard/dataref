from __future__ import annotations

import json
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Literal


CURRENT_FORMAT_VERSION = 1
SUPPORTED_FORMAT_VERSIONS: frozenset[int] = frozenset({1})

BackendType = Literal["local", "s3"]


@dataclass
class S3Config:
    bucket: str = ""
    prefix: str = ""
    endpoint_url: str | None = None


@dataclass
class FluxelConfig:
    format_version: int = CURRENT_FORMAT_VERSION
    backend: BackendType = "local"
    dataset_root: str = "."
    default_branch: str = "main"
    s3: S3Config | None = None


CONFIG_FILENAME = "config.json"


def config_path(root: str | Path) -> Path:
    return Path(root).resolve() / ".fluxel" / CONFIG_FILENAME


def load_config(root: str | Path) -> FluxelConfig | None:
    path = config_path(root)
    if not path.exists():
        return None
    raw = json.loads(path.read_text("utf-8"))
    return _deserialize(raw)


def save_config(root: str | Path, config: FluxelConfig) -> Path:
    path = config_path(root)
    path.parent.mkdir(parents=True, exist_ok=True)
    raw = _serialize(config)
    path.write_text(json.dumps(raw, indent=2) + "\n", "utf-8")
    return path


def init_config(
    root: str | Path,
    *,
    backend: BackendType = "local",
    default_branch: str = "main",
    s3_bucket: str | None = None,
    s3_prefix: str | None = None,
    s3_endpoint_url: str | None = None,
) -> FluxelConfig:
    s3_config: S3Config | None = None
    if backend == "s3":
        if not s3_bucket:
            raise ValueError("S3 backend requires a bucket")
        s3_config = S3Config(
            bucket=s3_bucket,
            prefix=s3_prefix or "",
            endpoint_url=s3_endpoint_url,
        )
    config = FluxelConfig(
        backend=backend,
        dataset_root=str(Path(root).resolve()),
        default_branch=default_branch,
        s3=s3_config,
    )
    validate_config(config)
    return config


def validate_config(config: FluxelConfig) -> None:
    if config.format_version not in SUPPORTED_FORMAT_VERSIONS:
        if config.format_version > CURRENT_FORMAT_VERSION:
            raise ValueError(
                f"Repository format version {config.format_version} is newer than "
                f"this version of fluxel (supports {CURRENT_FORMAT_VERSION}). "
                f"Please upgrade fluxel to access this repository."
            )
        raise ValueError(
            f"Repository format version {config.format_version} is no longer supported. "
            f"Run 'fluxel migrate' to upgrade the repository."
        )
    if config.backend not in ("local", "s3"):
        raise ValueError(f"Unsupported backend: {config.backend}")
    if not config.dataset_root:
        raise ValueError("dataset_root must not be empty")
    if not config.default_branch:
        raise ValueError("default_branch must not be empty")
    if config.backend == "s3":
        if config.s3 is None:
            raise ValueError("S3 backend requires s3 config section")
        if not config.s3.bucket:
            raise ValueError("S3 backend requires a bucket")
    if config.backend == "local":
        root_path = Path(config.dataset_root)
        if not root_path.exists():
            raise ValueError(f"Local dataset root does not exist: {config.dataset_root}")
        if not (root_path / ".fluxel").is_dir():
            raise ValueError(
                f"Not a fluxel repository (no .fluxel directory): {config.dataset_root}"
            )


def _serialize(config: FluxelConfig) -> dict[str, object]:
    result: dict[str, object] = {
        "format_version": config.format_version,
        "backend": config.backend,
        "dataset_root": config.dataset_root,
        "default_branch": config.default_branch,
    }
    if config.s3 is not None:
        result["s3"] = asdict(config.s3)
    return result


def _deserialize(raw: dict[str, object]) -> FluxelConfig:
    s3_raw = raw.get("s3")
    s3_config: S3Config | None = None
    if isinstance(s3_raw, dict):
        s3_config = S3Config(
            bucket=str(s3_raw.get("bucket", "")),
            prefix=str(s3_raw.get("prefix", "")),
            endpoint_url=(
                str(s3_raw["endpoint_url"]) if s3_raw.get("endpoint_url") else None
            ),
        )
    return FluxelConfig(
        format_version=int(raw.get("format_version", CURRENT_FORMAT_VERSION)),
        backend=str(raw.get("backend", "local")),  # type: ignore[assignment]
        dataset_root=str(raw.get("dataset_root", ".")),
        default_branch=str(raw.get("default_branch", "main")),
        s3=s3_config,
    )
