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
from typing import BinaryIO, Iterator

import fsspec
from fsspec.spec import AbstractFileSystem

from .layout import blob_relpath
from .manifest import ManifestEntry
from .repository import FluxelRepository


@dataclass(frozen=True)
class FluxelURI:
    dataset: str
    ref: str
    logical_path: str
    include_staging: bool = False


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
    ) -> BinaryIO:
        if mode != "rb":
            raise NotImplementedError(
                "FluxelFileSystem currently supports read-only binary mode"
            )
        resolved = self._resolve_entry(path)
        if resolved.entry.blob_hash:
            blob_path = self._blob_path(resolved.root, resolved.entry.blob_hash)
            return io.BytesIO(blob_path.read_bytes())
        if resolved.entry.source_uri:
            return fsspec.open(resolved.entry.source_uri, mode="rb").open()
        raise FileNotFoundError(
            "Entry has no canonical blob hash and no readable source URI"
        )

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
            "identity_mode": resolved.entry.identity_mode,
            "mtime_ns": resolved.entry.mtime_ns,
            "commit": resolved.commit_id,
            "staged": resolved.uri.include_staging,
            "dataset": resolved.uri.dataset,
        }

    def ls(
        self, path: str, detail: bool = True, **kwargs: object
    ) -> list[dict[str, object]] | list[str]:
        uri = self._parse_uri(path)
        root = self._dataset_root(uri.dataset)
        repo = FluxelRepository(root)
        entries = repo.resolve_entries(uri.ref, include_staging=uri.include_staging)

        normalized_prefix = uri.logical_path.strip("/")
        results: list[dict[str, object] | str] = []
        for entry in entries.values():
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
                        "identity_mode": entry.identity_mode,
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
        entries = repo.resolve_entries(uri.ref, include_staging=uri.include_staging)
        entry = entries.get(uri.logical_path)
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
            raise ValueError(
                "Fluxel URI must include a ref: fluxel://<dataset>@<ref>/<path>"
            )
        dataset, remainder = stripped.split("@", maxsplit=1)
        if not dataset:
            raise ValueError("Fluxel URI dataset cannot be empty")
        if "/" in remainder:
            ref_raw, logical_path = remainder.split("/", maxsplit=1)
        else:
            ref_raw, logical_path = remainder, ""
        include_staging = False
        ref = ref_raw
        if ref_raw.endswith("+staged"):
            include_staging = True
            ref = ref_raw[: -len("+staged")]
        if not ref:
            raise ValueError("Fluxel URI ref cannot be empty")
        logical_path = logical_path.strip("/")
        if not logical_path:
            raise ValueError("Fluxel URI must include a logical file path")
        return FluxelURI(
            dataset=dataset,
            ref=ref,
            logical_path=logical_path,
            include_staging=include_staging,
        )

    def _blob_path(self, dataset_root: Path, content_hash: str) -> Path:
        return dataset_root / ".fluxel" / "blobs" / blob_relpath(content_hash)


@dataclass(frozen=True)
class ResolvedEntry:
    uri: FluxelURI
    root: Path
    commit_id: str
    entry: ManifestEntry


fsspec.register_implementation("fluxel", FluxelFileSystem)
