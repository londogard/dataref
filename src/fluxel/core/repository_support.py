from __future__ import annotations

from dataclasses import replace
from pathlib import PurePosixPath

from blake3 import blake3

from .manifest import ManifestEntry


def metadata_identity(relative_path: str, size: int) -> str:
    payload = f"{relative_path}\n{size}".encode("utf-8")
    return blake3(payload).hexdigest()


def normalize_repository_path(path: str) -> str:
    _validate_no_binary(path)
    stripped = path.strip()
    if not stripped:
        raise ValueError("Path cannot be empty")
    if stripped.startswith("/"):
        raise ValueError("Path cannot be absolute")
    normalized = stripped.strip("/")
    if not normalized:
        raise ValueError("Path cannot be empty")
    if (
        normalized in (".", "..")
        or normalized.startswith("../")
        or "/../" in normalized
        or normalized.endswith("/..")
    ):
        raise ValueError("Path cannot traverse outside repository root")
    if "//" in normalized:
        raise ValueError("Path contains empty components")
    return normalized


def _validate_no_binary(token: str) -> None:
    if "\x00" in token:
        raise ValueError("Path contains null bytes")
    for ch in token:
        if 0 < ord(ch) < 32:
            raise ValueError("Path contains control characters")


def normalize_logical_paths(paths: list[str]) -> list[str]:
    normalized: list[str] = []
    seen: set[str] = set()
    for path in paths:
        value = normalize_repository_path(path)
        if value in seen:
            continue
        seen.add(value)
        normalized.append(value)
    return normalized


def matches_logical_path(entry_path: str, logical_path: str) -> bool:
    return entry_path == logical_path or entry_path.startswith(f"{logical_path}/")


def matches_any_logical_path(entry_path: str, logical_paths: list[str]) -> bool:
    return any(
        matches_logical_path(entry_path, logical_path) for logical_path in logical_paths
    )


def move_logical_path(
    entry_path: str,
    *,
    source_path: str,
    destination_path: str,
) -> str:
    if entry_path == source_path:
        return destination_path
    suffix = entry_path[len(source_path) :]
    return f"{destination_path}{suffix}"


def relocate_manifest_entry(
    entry: ManifestEntry,
    destination_path: str,
) -> ManifestEntry:
    if entry.identity_mode == "meta":
        identity_value = metadata_identity(destination_path, entry.size)
        return replace(
            entry,
            path=destination_path,
            hash=identity_value,
            identity_value=identity_value,
        )
    return replace(entry, path=destination_path)


def normalize_s3_import_path(
    *,
    key: str,
    prefix: str,
    size: int,
) -> str | None:
    if key.endswith("/") and size == 0:
        return None
    normalized_key = key.strip("/")
    if not normalized_key:
        return None
    normalized_prefix = prefix.strip("/")
    if normalized_prefix:
        if normalized_key == normalized_prefix:
            relative_path = normalized_key.rsplit("/", maxsplit=1)[-1]
        elif normalized_key.startswith(f"{normalized_prefix}/"):
            relative_path = normalized_key[len(normalized_prefix) + 1 :]
        else:
            raise ValueError(f"S3 key '{key}' is outside import prefix '{prefix}'")
    else:
        relative_path = normalized_key
    return normalize_repository_path(relative_path)


def normalize_import_patterns(path_patterns: list[str] | None) -> list[str]:
    patterns: list[str] = []
    for pattern in path_patterns or []:
        _validate_no_binary(pattern)
        stripped = pattern.strip()
        if not stripped:
            raise ValueError("Import path filter cannot be empty")
        if stripped.startswith("/"):
            raise ValueError("Import path filter cannot be absolute")
        normalized = stripped.strip("/")
        if not normalized:
            raise ValueError("Import path filter cannot be empty")
        if (
            normalized in (".", "..")
            or normalized.startswith("../")
            or "/../" in normalized
            or normalized.endswith("/..")
        ):
            raise ValueError(
                "Import path filter cannot traverse outside repository root"
            )
        if "//" in normalized:
            raise ValueError("Import path filter contains empty components")
        patterns.append(normalized)
    return patterns


def matches_import_patterns(relative_path: str, path_patterns: list[str]) -> bool:
    if not path_patterns:
        return True
    path = PurePosixPath(relative_path)
    return any(match_import_pattern(path, pattern) for pattern in path_patterns)


def match_import_pattern(path: PurePosixPath, pattern: str) -> bool:
    if path.match(pattern):
        return True
    if pattern.startswith("**/"):
        pattern_suffix = pattern[len("**/") :]
        return len(path.parts) == 1 and path.match(pattern_suffix)
    return False
