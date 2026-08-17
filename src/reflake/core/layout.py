from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from .config import LocalConfig


@dataclass(frozen=True)
class ReflakeLayout:
    root: Path
    reflake_dir: Path
    blobs_dir: Path
    commits_dir: Path
    trees_dir: Path
    footers_dir: Path
    manifests_dir: Path
    staging_dir: Path
    refs_dir: Path
    heads_dir: Path

    @classmethod
    def initialize(cls, root: str | Path) -> "ReflakeLayout":
        root_path = Path(root).resolve()
        reflake_dir = root_path / ".reflake"
        blobs_dir = reflake_dir / "blobs"
        commits_dir = reflake_dir / "commits"
        trees_dir = reflake_dir / "trees"
        footers_dir = reflake_dir / "footers"
        manifests_dir = reflake_dir / "manifests"
        staging_dir = reflake_dir / "staging"
        refs_dir = reflake_dir / "refs"
        heads_dir = refs_dir / "heads"

        for path in (
            blobs_dir,
            commits_dir,
            trees_dir,
            footers_dir,
            manifests_dir,
            staging_dir,
            refs_dir,
            heads_dir,
        ):
            path.mkdir(parents=True, exist_ok=True)

        return cls(
            root=root_path,
            reflake_dir=reflake_dir,
            blobs_dir=blobs_dir,
            commits_dir=commits_dir,
            trees_dir=trees_dir,
            footers_dir=footers_dir,
            manifests_dir=manifests_dir,
            staging_dir=staging_dir,
            refs_dir=refs_dir,
            heads_dir=heads_dir,
        )


def initialize_reflake_layout(root: str | Path) -> ReflakeLayout:
    layout = ReflakeLayout.initialize(root)
    config_path = layout.reflake_dir / "config.json"
    if not config_path.exists():
        default_config = LocalConfig(dataset_root=str(layout.root))
        default_config.save(layout.root)
    return layout


def blob_relpath(content_hash: str) -> Path:
    return Path(content_hash[:2]) / content_hash[2:]
