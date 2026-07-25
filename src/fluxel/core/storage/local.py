from __future__ import annotations

from pathlib import Path
from typing import Iterator

from ..domain import OptimisticLockError


class LocalStorageBackend:
    def __init__(self, root: str | Path) -> None:
        self.root = Path(root).resolve()

    def _full_path(self, relative_path: str) -> Path:
        return self.root / relative_path

    def read_bytes(self, relative_path: str) -> bytes:
        return self._full_path(relative_path).read_bytes()

    def write_bytes(
        self,
        relative_path: str,
        data: bytes,
        *,
        if_none_match: bool = False,
    ) -> None:
        file_path = self._full_path(relative_path)
        file_path.parent.mkdir(parents=True, exist_ok=True)
        if if_none_match and file_path.exists():
            raise OptimisticLockError(f"Path already exists: {relative_path}")
        file_path.write_bytes(data)

    def exists(self, relative_path: str) -> bool:
        return self._full_path(relative_path).exists()

    def ensure_dir(self, relative_path: str) -> None:
        self._full_path(relative_path).mkdir(parents=True, exist_ok=True)

    def iter_keys(self, prefix: str = "") -> Iterator[str]:
        base = self._full_path(prefix)
        if not base.exists():
            return
        for file_path in base.rglob("*"):
            if file_path.is_file():
                yield str(file_path.relative_to(self.root))

    def etag(self, relative_path: str) -> str | None:
        if not self.exists(relative_path):
            return None
        stat = self._full_path(relative_path).stat()
        return f"{stat.st_mtime_ns}-{stat.st_size}"
