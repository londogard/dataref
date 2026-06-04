from __future__ import annotations

from bisect import bisect_right
from dataclasses import dataclass
import json
from pathlib import Path
from typing import Callable, Iterator

from .manifest import ManifestEntry


DEFAULT_INDEX_BLOCK_ENTRY_COUNT = 4096


@dataclass(frozen=True)
class ManifestIndexBlock:
    first_path: str
    offset: int


@dataclass(frozen=True)
class ManifestIndex:
    manifest_size: int
    block_entry_count: int
    blocks: tuple[ManifestIndexBlock, ...]


def build_manifest_index(
    manifest_path: str | Path,
    index_path: str | Path,
    *,
    block_entry_count: int = DEFAULT_INDEX_BLOCK_ENTRY_COUNT,
) -> None:
    manifest = Path(manifest_path)
    index = Path(index_path)
    index.parent.mkdir(parents=True, exist_ok=True)
    index.unlink(missing_ok=True)
    if block_entry_count <= 0:
        raise ValueError("block_entry_count must be positive")

    blocks: list[ManifestIndexBlock] = []
    previous_path: str | None = None
    entry_count = 0
    current_offset = 0
    with manifest.open("rb") as handle:
        for raw_line in handle:
            stripped = raw_line.strip()
            if stripped:
                entry_json = stripped.decode("utf-8")
                path = ManifestEntry.path_from_payload(entry_json)
                if previous_path is not None and path <= previous_path:
                    raise ValueError(
                        "Manifest entries must be sorted by path to build an index"
                    )
                if entry_count % block_entry_count == 0:
                    blocks.append(
                        ManifestIndexBlock(first_path=path, offset=current_offset)
                    )
                previous_path = path
                entry_count += 1
            current_offset += len(raw_line)

    payload = {
        "manifest_size": current_offset,
        "block_entry_count": block_entry_count,
        "blocks": [[block.first_path, block.offset] for block in blocks],
    }
    index.write_text(json.dumps(payload, separators=(",", ":")), encoding="utf-8")


def load_manifest_index_from_data(data: dict) -> ManifestIndex:
    blocks_list = data.get("blocks", [])
    blocks: tuple[ManifestIndexBlock, ...]
    if blocks_list:
        if isinstance(blocks_list[0], dict):
            blocks = tuple(
                ManifestIndexBlock(first_path=str(b["first_path"]), offset=int(b["offset"]))
                for b in blocks_list
            )
        else:
            blocks = tuple(
                ManifestIndexBlock(first_path=str(fp), offset=int(off))
                for fp, off in blocks_list
            )
    else:
        blocks = ()
    return ManifestIndex(
        manifest_size=int(data.get("manifest_size", 0)),
        block_entry_count=int(
            data.get("block_entry_count", DEFAULT_INDEX_BLOCK_ENTRY_COUNT)
        ),
        blocks=blocks,
    )


def load_manifest_index(index_path: str | Path) -> ManifestIndex:
    return load_manifest_index_from_data(
        json.loads(Path(index_path).read_text(encoding="utf-8"))
    )


def lookup_manifest_index_entry_json(
    logical_path: str,
    *,
    read_range: Callable[[int, int], bytes],
    index_path: str | Path | None = None,
    index: ManifestIndex | None = None,
) -> str | None:
    if index is None:
        if index_path is None:
            raise ValueError("Must provide either index or index_path")
        index = load_manifest_index(index_path)
    block_index = _lookup_block_index(index, logical_path)
    if block_index is None:
        return None

    for entry_json, path in _iter_block_entries(index, block_index, read_range):
        if path == logical_path:
            return entry_json
        if path > logical_path:
            return None
    return None


def iter_manifest_index_entry_jsons(
    logical_prefix: str | None = None,
    *,
    read_range: Callable[[int, int], bytes],
    index_path: str | Path | None = None,
    index: ManifestIndex | None = None,
) -> Iterator[str]:
    if index is None:
        if index_path is None:
            raise ValueError("Must provide either index or index_path")
        index = load_manifest_index(index_path)
    if not index.blocks:
        return

    if not logical_prefix:
        for block_index in range(len(index.blocks)):
            for entry_json, _ in _iter_block_entries(index, block_index, read_range):
                yield entry_json
        return

    normalized_prefix = logical_prefix.strip("/")
    descendant_prefix = f"{normalized_prefix.rstrip('/')}/"
    prefix_upper_bound = _prefix_upper_bound(descendant_prefix)
    block_index = _lookup_block_index(index, normalized_prefix)
    if block_index is None:
        block_index = 0

    for current_index in range(block_index, len(index.blocks)):
        first_path = index.blocks[current_index].first_path
        if first_path >= prefix_upper_bound and first_path != normalized_prefix:
            return

        for entry_json, path in _iter_block_entries(index, current_index, read_range):
            if path == normalized_prefix or (
                descendant_prefix <= path < prefix_upper_bound
            ):
                yield entry_json
                continue
            if path >= prefix_upper_bound and path != normalized_prefix:
                return


def _prefix_upper_bound(prefix: str) -> str:
    return f"{prefix}\U0010ffff"


def _lookup_block_index(index: ManifestIndex, logical_path: str) -> int | None:
    if not index.blocks:
        return None
    first_paths = [block.first_path for block in index.blocks]
    block_index = bisect_right(first_paths, logical_path) - 1
    if block_index < 0:
        return 0
    return block_index


def _iter_block_entries(
    index: ManifestIndex,
    block_index: int,
    read_range: Callable[[int, int], bytes],
) -> Iterator[tuple[str, str]]:
    start_offset = index.blocks[block_index].offset
    end_offset = (
        index.blocks[block_index + 1].offset
        if block_index + 1 < len(index.blocks)
        else index.manifest_size
    )
    payload = read_range(start_offset, end_offset)
    for raw_line in payload.splitlines():
        stripped = raw_line.strip()
        if not stripped:
            continue
        entry_json = stripped.decode("utf-8")
        yield entry_json, ManifestEntry.path_from_payload(entry_json)
