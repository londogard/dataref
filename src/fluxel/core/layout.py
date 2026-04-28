# Fluxel Guardrails (Permanent):
# - DO NOT optimize for ML throughput in canonical storage (`blobs/`): no sharding, tarballs, or parquet in the blob layer.
# - DO NOT read blob data for metadata-only operations (diff, list, log, status).
# - DO NOT introduce a server, daemon, or central database; Fluxel is 100% client-side/serverless.
# - DO NOT use SHA-1/SHA-256; use Blake3 for all content hashing.
# - PREFER JSONL manifests to preserve streaming and O(1) memory usage.

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class FluxelLayout:
    root: Path
    fluxel_dir: Path
    blobs_dir: Path
    commits_dir: Path
    manifests_dir: Path
    staging_dir: Path
    refs_dir: Path
    heads_dir: Path

    @classmethod
    def initialize(cls, root: str | Path) -> "FluxelLayout":
        root_path = Path(root).resolve()
        fluxel_dir = root_path / ".fluxel"
        blobs_dir = fluxel_dir / "blobs"
        commits_dir = fluxel_dir / "commits"
        manifests_dir = fluxel_dir / "manifests"
        staging_dir = fluxel_dir / "staging"
        refs_dir = fluxel_dir / "refs"
        heads_dir = refs_dir / "heads"

        for path in (
            blobs_dir,
            commits_dir,
            manifests_dir,
            staging_dir,
            refs_dir,
            heads_dir,
        ):
            path.mkdir(parents=True, exist_ok=True)

        return cls(
            root=root_path,
            fluxel_dir=fluxel_dir,
            blobs_dir=blobs_dir,
            commits_dir=commits_dir,
            manifests_dir=manifests_dir,
            staging_dir=staging_dir,
            refs_dir=refs_dir,
            heads_dir=heads_dir,
        )


def initialize_fluxel_layout(root: str | Path) -> FluxelLayout:
    return FluxelLayout.initialize(root)


def blob_relpath(content_hash: str) -> Path:
    return Path(content_hash[:2]) / content_hash[2:]
