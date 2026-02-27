# Fluxel Guardrails (Permanent):
# - DO NOT optimize for ML throughput in canonical storage (`blobs/`): no sharding, tarballs, or parquet in the blob layer.
# - DO NOT read blob data for metadata-only operations (diff, list, log, status).
# - DO NOT introduce a server, daemon, or central database; Fluxel is 100% client-side/serverless.
# - DO NOT use SHA-1/SHA-256; use Blake3 for all content hashing.
# - PREFER JSONL manifests to preserve streaming and O(1) memory usage.

from __future__ import annotations

import json
import os
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Callable, Iterable, Iterator

from .hashing import blake3_digest_file


@dataclass(frozen=True)
class ManifestEntry:
    path: str
    hash: str
    size: int
    mtime_ns: int

    @staticmethod
    def from_dict(data: dict[str, object]) -> "ManifestEntry":
        return ManifestEntry(
            path=str(data["path"]),
            hash=str(data["hash"]),
            size=int(data["size"]),
            mtime_ns=int(data["mtime_ns"]),
        )


class ManifestWriter:
    def __init__(self, manifest_path: str | Path) -> None:
        self.manifest_path = Path(manifest_path)

    def write_entries(self, entries: Iterable[ManifestEntry]) -> int:
        self.manifest_path.parent.mkdir(parents=True, exist_ok=True)
        written = 0
        with self.manifest_path.open("w", encoding="utf-8") as handle:
            for entry in entries:
                handle.write(json.dumps(asdict(entry), separators=(",", ":")))
                handle.write("\n")
                written += 1
        return written

    def write_files(
        self,
        files: Iterable[str | Path],
        *,
        root: str | Path,
        hash_file: Callable[[str | Path], str] = blake3_digest_file,
    ) -> int:
        root_path = Path(root).resolve()
        return self.write_entries(
            build_manifest_entries(files=files, root=root_path, hash_file=hash_file)
        )


class ManifestReader:
    def __init__(self, manifest_path: str | Path) -> None:
        self.manifest_path = Path(manifest_path)

    def iter_entries(self) -> Iterator[ManifestEntry]:
        if not self.manifest_path.exists():
            return
        with self.manifest_path.open("r", encoding="utf-8") as handle:
            for line in handle:
                line = line.strip()
                if not line:
                    continue
                yield ManifestEntry.from_dict(json.loads(line))

    def get_entry(self, logical_path: str) -> ManifestEntry | None:
        match: ManifestEntry | None = None
        for entry in self.iter_entries():
            if entry.path == logical_path:
                match = entry
        return match


def build_manifest_entries(
    files: Iterable[str | Path],
    *,
    root: str | Path,
    hash_file: Callable[[str | Path], str] = blake3_digest_file,
) -> Iterator[ManifestEntry]:
    root_path = Path(root).resolve()
    for file_path_raw in files:
        file_path = Path(file_path_raw).resolve()
        stat = file_path.stat()
        relative_path = file_path.relative_to(root_path).as_posix()
        yield ManifestEntry(
            path=relative_path,
            hash=hash_file(file_path),
            size=stat.st_size,
            mtime_ns=stat.st_mtime_ns,
        )


def walk_files(root: str | Path) -> Iterator[Path]:
    root_path = Path(root).resolve()
    for dirpath, dirnames, filenames in os.walk(root_path):
        dirnames[:] = [name for name in dirnames if name != ".fluxel"]
        for filename in filenames:
            yield Path(dirpath) / filename
