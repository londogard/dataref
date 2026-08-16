from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from .config import LocalConfig


@dataclass(frozen=True)
class DatarefLayout:
    root: Path
    dataref_dir: Path
    blobs_dir: Path
    commits_dir: Path
    trees_dir: Path
    footers_dir: Path
    manifests_dir: Path
    staging_dir: Path
    refs_dir: Path
    heads_dir: Path

    @classmethod
    def initialize(cls, root: str | Path) -> "DatarefLayout":
        root_path = Path(root).resolve()
        dataref_dir = root_path / ".dataref"
        blobs_dir = dataref_dir / "blobs"
        commits_dir = dataref_dir / "commits"
        trees_dir = dataref_dir / "trees"
        footers_dir = dataref_dir / "footers"
        manifests_dir = dataref_dir / "manifests"
        staging_dir = dataref_dir / "staging"
        refs_dir = dataref_dir / "refs"
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
            dataref_dir=dataref_dir,
            blobs_dir=blobs_dir,
            commits_dir=commits_dir,
            trees_dir=trees_dir,
            footers_dir=footers_dir,
            manifests_dir=manifests_dir,
            staging_dir=staging_dir,
            refs_dir=refs_dir,
            heads_dir=heads_dir,
        )


def initialize_dataref_layout(root: str | Path) -> DatarefLayout:
    layout = DatarefLayout.initialize(root)
    config_path = layout.dataref_dir / "config.json"
    if not config_path.exists():
        default_config = LocalConfig(dataset_root=str(layout.root))
        default_config.save(layout.root)
    return layout


def blob_relpath(content_hash: str) -> Path:
    return Path(content_hash[:2]) / content_hash[2:]
