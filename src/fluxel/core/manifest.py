# Fluxel Guardrails (Permanent):
# - DO NOT optimize for ML throughput in canonical storage (`blobs/`): no sharding, tarballs, or parquet in the blob layer.
# - DO NOT read blob data for metadata-only operations (diff, list, log, status).
# - DO NOT introduce a server, daemon, or central database; Fluxel is 100% client-side/serverless.
# - DO NOT use SHA-1/SHA-256; use Blake3 for all content hashing.
# - PREFER JSONL manifests to preserve streaming and O(1) memory usage.

from __future__ import annotations

import json
from dataclasses import dataclass
from json import JSONDecodeError
from pathlib import Path
from pathlib import PurePosixPath
from typing import Callable, Iterable, Iterator

from .hashing import blake3_digest_file


SUPPORTED_IDENTITY_MODES = frozenset({"blake3", "meta"})
_BLAKE3_HEX_LENGTH = 64
_HEX_DIGITS = frozenset("0123456789abcdef")
_BLOB_BACKED_MANIFEST_TAG = "b"
_META_ONLY_MANIFEST_TAG = "m"


def _is_hex_digest(value: str) -> bool:
    normalized = value.lower()
    return len(normalized) == _BLAKE3_HEX_LENGTH and all(
        character in _HEX_DIGITS for character in normalized
    )


def _validate_manifest_path(path: str) -> None:
    if not path:
        raise ValueError("Manifest entry path cannot be empty")
    if path.startswith("/") or path.endswith("/"):
        raise ValueError("Manifest entry path must be a normalized relative path")
    if "\\" in path or "//" in path:
        raise ValueError("Manifest entry path must use normalized POSIX separators")

    parts = PurePosixPath(path).parts
    if not parts or any(part in {"", ".", ".."} for part in parts):
        raise ValueError("Manifest entry path must be a normalized relative path")


def _validate_hex_digest(value: str, *, field_name: str) -> None:
    if not _is_hex_digest(value):
        raise ValueError(
            f"Manifest entry {field_name} must be a 64-character hex digest"
        )


@dataclass(frozen=True)
class ManifestEntry:
    path: str
    hash: str
    size: int
    mtime_ns: int
    identity_mode: str = "blake3"
    identity_value: str | None = None
    blob_hash: str | None = None
    source_uri: str | None = None

    def __post_init__(self) -> None:
        if self.identity_value is None:
            object.__setattr__(self, "identity_value", self.hash)

        _validate_manifest_path(self.path)
        _validate_hex_digest(self.hash, field_name="hash")

        identity_value = self.identity_value
        if identity_value is None:
            raise ValueError("Manifest entry identity_value cannot be empty")
        _validate_hex_digest(identity_value, field_name="identity_value")

        if self.identity_mode not in SUPPORTED_IDENTITY_MODES:
            supported_modes = ", ".join(sorted(SUPPORTED_IDENTITY_MODES))
            raise ValueError(
                f"Manifest entry identity_mode must be one of: {supported_modes}"
            )
        if self.size < 0:
            raise ValueError("Manifest entry size cannot be negative")
        if self.mtime_ns < 0:
            raise ValueError("Manifest entry mtime_ns cannot be negative")
        if identity_value != self.hash:
            raise ValueError("Manifest entry identity_value must match hash")

        if self.blob_hash is not None:
            _validate_hex_digest(self.blob_hash, field_name="blob_hash")

        if self.source_uri is not None and not self.source_uri.strip():
            raise ValueError("Manifest entry source_uri cannot be empty")

        if self.identity_mode == "meta":
            if self.blob_hash is not None:
                raise ValueError(
                    "Metadata-only manifest entries cannot include blob_hash"
                )
            if self.source_uri is None:
                raise ValueError(
                    "Metadata-only manifest entries must include source_uri"
                )
        elif self.blob_hash is not None and self.blob_hash != self.hash:
            raise ValueError("Blob-backed manifest entries must keep blob_hash aligned")

    @staticmethod
    def from_dict(data: dict[str, object]) -> "ManifestEntry":
        if not isinstance(data, dict):
            raise ValueError("Manifest entry payload must be an object")
        hash_value = str(data.get("hash") or data.get("identity_value") or "")
        if not hash_value:
            raise ValueError("Manifest entry must include hash or identity_value")
        identity_mode = str(data.get("identity_mode") or "blake3")
        identity_value = data.get("identity_value")
        blob_hash = data.get("blob_hash")
        if blob_hash is None and "blob_hash" not in data and identity_mode == "blake3":
            blob_hash = hash_value
        source_uri = data.get("source_uri")

        try:
            path = str(data["path"])
            size = int(data["size"])
            mtime_ns = int(data["mtime_ns"])
        except KeyError as error:
            raise ValueError(
                f"Manifest entry is missing required field: {error.args[0]}"
            ) from error
        except (TypeError, ValueError) as error:
            raise ValueError(
                "Manifest entry size and mtime_ns must be integers"
            ) from error

        return ManifestEntry(
            path=path,
            hash=hash_value,
            size=size,
            mtime_ns=mtime_ns,
            identity_mode=identity_mode,
            identity_value=(
                str(identity_value) if identity_value is not None else hash_value
            ),
            blob_hash=str(blob_hash) if blob_hash is not None else None,
            source_uri=str(source_uri) if source_uri is not None else None,
        )


def serialize_manifest_entry(entry: ManifestEntry) -> str:
    if entry.identity_mode == "blake3":
        payload: list[object] = [
            _BLOB_BACKED_MANIFEST_TAG,
            entry.path,
            entry.hash,
            entry.size,
            entry.mtime_ns,
        ]
    elif entry.identity_mode == "meta":
        payload = [
            _META_ONLY_MANIFEST_TAG,
            entry.path,
            entry.hash,
            entry.size,
            entry.mtime_ns,
            entry.source_uri,
        ]
    else:
        supported_modes = ", ".join(sorted(SUPPORTED_IDENTITY_MODES))
        raise ValueError(
            f"Manifest entry identity_mode must be one of: {supported_modes}"
        )
    return json.dumps(payload, separators=(",", ":"))


def deserialize_manifest_entry(payload_text: str) -> ManifestEntry:
    try:
        payload = _load_manifest_payload(payload_text)
    except JSONDecodeError as error:
        raise ValueError("Corrupt manifest entry payload") from error

    return _manifest_entry_from_payload(payload)


def manifest_entry_path(payload_text: str) -> str:
    try:
        payload = _load_manifest_payload(payload_text)
    except JSONDecodeError as error:
        raise ValueError("Corrupt manifest entry payload") from error
    if not isinstance(payload, list) or len(payload) < 2:
        raise ValueError("Manifest entry payload must be a JSON array")
    return str(payload[1])


def _load_manifest_payload(payload_text: str) -> object:
    return json.loads(payload_text)


def _manifest_entry_from_payload(payload: object) -> ManifestEntry:

    if not isinstance(payload, list):
        raise ValueError("Manifest entry payload must be a JSON array")
    if len(payload) == 5 and payload[0] == _BLOB_BACKED_MANIFEST_TAG:
        _, path, hash_value, size, mtime_ns = payload
        return ManifestEntry(
            path=str(path),
            hash=str(hash_value),
            size=int(size),
            mtime_ns=int(mtime_ns),
            identity_mode="blake3",
            identity_value=str(hash_value),
            blob_hash=str(hash_value),
            source_uri=None,
        )
    if len(payload) == 6 and payload[0] == _META_ONLY_MANIFEST_TAG:
        _, path, hash_value, size, mtime_ns, source_uri = payload
        return ManifestEntry(
            path=str(path),
            hash=str(hash_value),
            size=int(size),
            mtime_ns=int(mtime_ns),
            identity_mode="meta",
            identity_value=str(hash_value),
            blob_hash=None,
            source_uri=str(source_uri),
        )
    raise ValueError("Manifest entry payload has an unsupported shape")


class ManifestWriter:
    def __init__(self, manifest_path: str | Path) -> None:
        self.manifest_path = Path(manifest_path)

    def write_entries(self, entries: Iterable[ManifestEntry]) -> int:
        self.manifest_path.parent.mkdir(parents=True, exist_ok=True)
        written = 0
        with self.manifest_path.open("w", encoding="utf-8") as handle:
            for entry in entries:
                handle.write(serialize_manifest_entry(entry))
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
            for line_number, line in enumerate(handle, start=1):
                line = line.strip()
                if not line:
                    continue
                try:
                    payload = _load_manifest_payload(line)
                except JSONDecodeError as error:
                    raise ValueError(
                        f"Corrupt manifest JSON at line {line_number} in {self.manifest_path}"
                    ) from error
                try:
                    yield _manifest_entry_from_payload(payload)
                except ValueError as error:
                    raise ValueError(
                        f"Invalid manifest entry at line {line_number} in {self.manifest_path}: {error}"
                    ) from error

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
        digest = hash_file(file_path)
        yield ManifestEntry(
            path=relative_path,
            hash=digest,
            size=stat.st_size,
            mtime_ns=stat.st_mtime_ns,
            identity_mode="blake3",
            identity_value=digest,
            blob_hash=digest,
            source_uri=file_path.as_uri(),
        )


def walk_files(root: str | Path) -> Iterator[Path]:
    root_path = Path(root).resolve()

    def iter_dir(path: Path) -> Iterator[Path]:
        for child in sorted(path.iterdir(), key=lambda item: item.name):
            if child.name == ".fluxel":
                continue
            if child.is_dir():
                yield from iter_dir(child)
                continue
            if child.is_file():
                yield child

    yield from iter_dir(root_path)
