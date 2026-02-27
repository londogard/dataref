# Fluxel Guardrails (Permanent):
# - DO NOT optimize for ML throughput in canonical storage (`blobs/`): no sharding, tarballs, or parquet in the blob layer.
# - DO NOT read blob data for metadata-only operations (diff, list, log, status).
# - DO NOT introduce a server, daemon, or central database; Fluxel is 100% client-side/serverless.
# - DO NOT use SHA-1/SHA-256; use Blake3 for all content hashing.
# - PREFER JSONL manifests to preserve streaming and O(1) memory usage.

from __future__ import annotations

import io
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator

import fsspec
from fsspec.spec import AbstractFileSystem

from .layout import blob_relpath
from .manifest import ManifestEntry, ManifestReader
from .repository import FluxelRepository


@dataclass(frozen=True)
class FluxelURI:
    dataset: str
    ref: str
    logical_path: str


class FluxelFileSystem(AbstractFileSystem):
    protocol = "fluxel"

    def __init__(
        self,
        *,
        dataset_roots: dict[str, str | Path] | None = None,
        **kwargs: object,
    ) -> None:
        super().__init__(**kwargs)
        self.dataset_roots = {
            name: Path(root).resolve() for name, root in (dataset_roots or {}).items()
        }

    @classmethod
    def _strip_protocol(cls, path: str) -> str:
        return path[len("fluxel://") :] if path.startswith("fluxel://") else path

    def _open(
        self,
        path: str,
        mode: str = "rb",
        block_size: int | None = None,
        **kwargs: object,
    ) -> io.BytesIO:
        if mode != "rb":
            raise NotImplementedError("FluxelFileSystem currently supports read-only binary mode")
        resolved = self._resolve_entry(path)
        blob_path = self._blob_path(resolved.root, resolved.entry.hash)
        return io.BytesIO(blob_path.read_bytes())

    def exists(self, path: str, **kwargs: object) -> bool:
        try:
            self._resolve_entry(path)
            return True
        except (FileNotFoundError, ValueError):
            return False

    def info(self, path: str, **kwargs: object) -> dict[str, object]:
        resolved = self._resolve_entry(path)
        return {
            "name": path,
            "type": "file",
            "size": resolved.entry.size,
            "hash": resolved.entry.hash,
            "mtime_ns": resolved.entry.mtime_ns,
            "commit": resolved.commit_id,
            "dataset": resolved.uri.dataset,
        }

    def ls(self, path: str, detail: bool = True, **kwargs: object) -> list[dict[str, object]] | list[str]:
        uri = self._parse_uri(path)
        root = self._dataset_root(uri.dataset)
        repo = FluxelRepository(root)
        commit_id = repo.resolve_ref(uri.ref)
        commit_obj = repo.read_commit(commit_id)
        manifest_path = root / ".fluxel" / "manifests" / f"{commit_obj.manifest}.jsonl"
        reader = ManifestReader(manifest_path)

        normalized_prefix = uri.logical_path.strip("/")
        results: list[dict[str, object] | str] = []
        for entry in reader.iter_entries():
            if normalized_prefix and not (
                entry.path == normalized_prefix
                or entry.path.startswith(f"{normalized_prefix}/")
            ):
                continue
            as_uri = f"fluxel://{uri.dataset}@{uri.ref}/{entry.path}"
            if detail:
                results.append(
                    {
                        "name": as_uri,
                        "type": "file",
                        "size": entry.size,
                        "hash": entry.hash,
                    }
                )
            else:
                results.append(as_uri)
        return results

    def _resolve_entry(self, path: str) -> "ResolvedEntry":
        uri = self._parse_uri(path)
        root = self._dataset_root(uri.dataset)
        repo = FluxelRepository(root)
        commit_id = repo.resolve_ref(uri.ref)
        commit_obj = repo.read_commit(commit_id)
        manifest_path = root / ".fluxel" / "manifests" / f"{commit_obj.manifest}.jsonl"
        entry = ManifestReader(manifest_path).get_entry(uri.logical_path)
        if entry is None:
            raise FileNotFoundError(path)
        return ResolvedEntry(uri=uri, root=root, commit_id=commit_id, entry=entry)

    def _dataset_root(self, dataset: str) -> Path:
        if dataset in self.dataset_roots:
            return self.dataset_roots[dataset]
        candidate = Path.cwd() / dataset
        if candidate.exists():
            return candidate.resolve()
        return Path.cwd().resolve()

    def _parse_uri(self, path: str) -> FluxelURI:
        stripped = self._strip_protocol(path)
        if "@" not in stripped:
            raise ValueError("Fluxel URI must include a ref: fluxel://<dataset>@<ref>/<path>")
        dataset, remainder = stripped.split("@", maxsplit=1)
        if not dataset:
            raise ValueError("Fluxel URI dataset cannot be empty")
        if "/" in remainder:
            ref, logical_path = remainder.split("/", maxsplit=1)
        else:
            ref, logical_path = remainder, ""
        if not ref:
            raise ValueError("Fluxel URI ref cannot be empty")
        logical_path = logical_path.strip("/")
        if not logical_path:
            raise ValueError("Fluxel URI must include a logical file path")
        return FluxelURI(dataset=dataset, ref=ref, logical_path=logical_path)

    def _blob_path(self, dataset_root: Path, content_hash: str) -> Path:
        return dataset_root / ".fluxel" / "blobs" / blob_relpath(content_hash)


@dataclass(frozen=True)
class ResolvedEntry:
    uri: FluxelURI
    root: Path
    commit_id: str
    entry: ManifestEntry


fsspec.register_implementation("fluxel", FluxelFileSystem)
