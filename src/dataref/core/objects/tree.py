"""Tree objects: the Merkle-tree node format for Dataref v2.

A tree object is a sorted JSONL file of entries.  Each entry addresses either a
child tree (real directory ``t`` or name-range shard ``s``) or a leaf file
(``b``/``m``/``bp``/``mp``).  Trees are content-addressed by the blake3 of their
serialized bytes, so unchanged directories reuse the same hash and are never
rewritten.

Line shapes (name is a single path component):

    ["t", name, hash]                              # real subdirectory
    ["s", name, hash]                              # name-range shard of this directory
    ["b", name, hash, size, mtime_ns]              # blob-backed file
    ["m", name, hash, size, mtime_ns, source_uri]  # source pointer (unverifiable)
    ["bp", name, hash, size, mtime_ns, footer]     # parquet, blob-backed, footer stats
    ["mp", name, hash, size, mtime_ns, source_uri, footer]  # parquet, source, footer stats
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Iterable

from blake3 import blake3

from ..manifest import ManifestEntry

KIND_TREE = "t"
KIND_SHARD = "s"
KIND_BLOB = "b"
KIND_META = "m"
KIND_BP = "bp"
KIND_MP = "mp"

SUBTREE_KINDS = frozenset({KIND_TREE, KIND_SHARD})
LEAF_KINDS = frozenset({KIND_BLOB, KIND_META, KIND_BP, KIND_MP})
SUPPORTED_KINDS = frozenset({KIND_TREE, KIND_SHARD, *LEAF_KINDS})

#: A tree object holds at most this many direct entries before it is split
#: into name-range shard subtrees (≈1 MB at ~100 bytes/line).
MAX_TREE_ENTRIES = 10_000

_HEX_DIGITS = frozenset("0123456789abcdef")
_TREE_HEX_LENGTH = 64


def _is_hex_digest(value: str) -> bool:
    return len(value) == _TREE_HEX_LENGTH and all(c in _HEX_DIGITS for c in value)


def _validate_entry_name(name: str) -> None:
    if not name:
        raise ValueError("Tree entry name cannot be empty")
    if "/" in name or "\\" in name:
        raise ValueError("Tree entry name must be a single path component")
    if name in {".", ".."}:
        raise ValueError("Tree entry name cannot be '.' or '..'")


@dataclass(frozen=True)
class TreeEntry:
    """One line of a tree object."""

    name: str
    kind: str
    hash: str
    size: int = 0
    mtime_ns: int = 0
    source_uri: str | None = None
    footer: str | None = None

    def __post_init__(self) -> None:
        _validate_entry_name(self.name)
        if self.kind not in SUPPORTED_KINDS:
            raise ValueError(f"Unsupported tree entry kind: {self.kind}")
        if not _is_hex_digest(self.hash):
            raise ValueError(f"Tree entry hash must be a 64-character hex digest")
        if self.size < 0:
            raise ValueError("Tree entry size cannot be negative")
        if self.mtime_ns < 0:
            raise ValueError("Tree entry mtime_ns cannot be negative")
        if self.kind in SUBTREE_KINDS:
            if self.size or self.mtime_ns or self.source_uri or self.footer:
                raise ValueError("Subtree entries only carry a name and hash")
        else:
            if self.kind in {KIND_BLOB, KIND_BP}:
                if self.source_uri is not None:
                    raise ValueError("Blob-backed entries cannot carry source_uri")
            if self.kind in {KIND_META, KIND_MP}:
                if not self.source_uri:
                    raise ValueError(
                        "Source-pointer entries must carry a non-empty source_uri"
                    )
            if self.kind in {KIND_BP, KIND_MP}:
                if not self.footer or not _is_hex_digest(self.footer):
                    raise ValueError(
                        "Parquet entries must carry a footer hash (64 hex chars)"
                    )
            else:
                if self.footer is not None:
                    raise ValueError("Non-parquet entries cannot carry a footer")

    @property
    def is_subtree(self) -> bool:
        return self.kind in SUBTREE_KINDS

    @property
    def is_leaf(self) -> bool:
        return not self.is_subtree

    def serialize(self) -> str:
        if self.kind in SUBTREE_KINDS:
            payload: list[object] = [self.kind, self.name, self.hash]
        elif self.kind == KIND_BLOB:
            payload = [self.kind, self.name, self.hash, self.size, self.mtime_ns]
        elif self.kind == KIND_META:
            payload = [
                self.kind,
                self.name,
                self.hash,
                self.size,
                self.mtime_ns,
                self.source_uri,
            ]
        elif self.kind == KIND_BP:
            payload = [
                self.kind,
                self.name,
                self.hash,
                self.size,
                self.mtime_ns,
                self.footer,
            ]
        else:  # KIND_MP
            payload = [
                self.kind,
                self.name,
                self.hash,
                self.size,
                self.mtime_ns,
                self.source_uri,
                self.footer,
            ]
        return json.dumps(payload, separators=(",", ":"))

    @staticmethod
    def parse(line: str) -> "TreeEntry":
        try:
            payload = json.loads(line)
        except json.JSONDecodeError as error:
            raise ValueError("Corrupt tree entry payload") from error
        if not isinstance(payload, list) or not payload:
            raise ValueError("Tree entry payload must be a JSON array")
        kind = str(payload[0])
        if kind not in SUPPORTED_KINDS:
            raise ValueError(f"Tree entry payload has an unsupported shape: {kind}")
        try:
            name = str(payload[1])
            hash_value = str(payload[2])
        except IndexError as error:
            raise ValueError("Tree entry payload is missing required fields") from error

        if kind in SUBTREE_KINDS:
            size, mtime_ns, source_uri, footer = 0, 0, None, None
        elif kind == KIND_BLOB:
            size, mtime_ns = int(payload[3]), int(payload[4])
            source_uri, footer = None, None
        elif kind == KIND_META:
            size, mtime_ns = int(payload[3]), int(payload[4])
            source_uri, footer = str(payload[5]), None
        elif kind == KIND_BP:
            size, mtime_ns = int(payload[3]), int(payload[4])
            source_uri, footer = None, str(payload[5])
        else:  # KIND_MP
            size, mtime_ns = int(payload[3]), int(payload[4])
            source_uri, footer = str(payload[5]), str(payload[6])
        return TreeEntry(
            name=name,
            kind=kind,
            hash=hash_value,
            size=size,
            mtime_ns=mtime_ns,
            source_uri=source_uri,
            footer=footer,
        )

    @staticmethod
    def name_from_payload(line: str) -> str:
        """Extract the entry name without a full JSON parse (for sort checks)."""
        return line.split('"', 4)[3]


def serialize_tree_object(entries: Iterable[TreeEntry]) -> bytes:
    """Serialize entries (sorted by name) into tree object bytes."""
    lines = [entry.serialize() for entry in entries]
    return ("\n".join(lines) + "\n").encode("utf-8")


def parse_tree_object(payload: bytes) -> list[TreeEntry]:
    entries: list[TreeEntry] = []
    previous_name: str | None = None
    for raw_line in payload.decode("utf-8").splitlines():
        line = raw_line.strip()
        if not line:
            continue
        entry = TreeEntry.parse(line)
        if previous_name is not None and entry.name <= previous_name:
            raise ValueError(
                f"Tree entries must be sorted by name; {entry.name!r} after "
                f"{previous_name!r}"
            )
        previous_name = entry.name
        entries.append(entry)
    return entries


def leaf_to_tree_entry(entry: ManifestEntry) -> TreeEntry:
    """Convert a leaf manifest entry (full path) into a tree entry (name only)."""
    name = entry.path.rsplit("/", 1)[-1]
    if entry.identity_mode == "blake3":
        kind = KIND_BP if entry.footer is not None else KIND_BLOB
        source_uri = None
    else:
        kind = KIND_MP if entry.footer is not None else KIND_META
        source_uri = entry.source_uri
    return TreeEntry(
        name=name,
        kind=kind,
        hash=entry.hash,
        size=entry.size,
        mtime_ns=entry.mtime_ns,
        source_uri=source_uri,
        footer=entry.footer,
    )


def tree_object_hash(payload: bytes) -> str:
    return blake3(payload).hexdigest()
