from __future__ import annotations

from pathlib import Path
from tempfile import NamedTemporaryFile

from ..manifest_index import build_manifest_index


def build_manifest_index_file(
    manifest_path: str | Path,
    *,
    suffix: str = ".idx",
) -> Path:
    with NamedTemporaryFile(mode="wb", suffix=suffix, delete=False) as temp:
        index_path = Path(temp.name)
    build_manifest_index(manifest_path, index_path)
    return index_path
