# Fluxel Guardrails (Permanent):
# - DO NOT optimize for ML throughput in canonical storage (`blobs/`): no sharding, tarballs, or parquet in the blob layer.
# - DO NOT read blob data for metadata-only operations (diff, list, log, status).
# - DO NOT introduce a server, daemon, or central database; Fluxel is 100% client-side/serverless.
# - DO NOT use SHA-1/SHA-256; use Blake3 for all content hashing.
# - PREFER JSONL manifests to preserve streaming and O(1) memory usage.

from __future__ import annotations

from pathlib import Path
from typing import BinaryIO

from blake3 import blake3

DEFAULT_CHUNK_SIZE = 64 * 1024


def blake3_digest_stream(stream: BinaryIO, chunk_size: int = DEFAULT_CHUNK_SIZE) -> str:
    hasher = blake3()
    while True:
        chunk = stream.read(chunk_size)
        if not chunk:
            break
        hasher.update(chunk)
    return hasher.hexdigest()


def blake3_digest_file(file_path: str | Path, chunk_size: int = DEFAULT_CHUNK_SIZE) -> str:
    with Path(file_path).open("rb") as handle:
        return blake3_digest_stream(handle, chunk_size=chunk_size)
