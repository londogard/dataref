from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Optional

import fsspec


@dataclass(frozen=True)
class Storage:
    """Thin wrapper around fsspec for local/S3 paths."""

    root: str

    def fs(self):
        # s3://... -> s3fs, otherwise local
        return (
            fsspec.filesystem("s3")
            if self.root.startswith("s3://")
            else fsspec.filesystem("file")
        )

    def join(self, *parts: str) -> str:
        base = self.root.rstrip("/")
        suffix = "/".join(p.strip("/") for p in parts if p)
        return f"{base}/{suffix}" if suffix else base

    def exists(self, path: str) -> bool:
        return self.fs().exists(path)

    def mkdirs(self, path: str) -> None:
        fs = self.fs()
        if not fs.exists(path):
            fs.makedirs(path, exist_ok=True)

    def read_text(self, path: str) -> str:
        with self.fs().open(path, "r", encoding="utf-8") as f:
            return f.read()

    def write_text(self, path: str, text: str) -> None:
        parent = path.rsplit("/", 1)[0]
        self.mkdirs(parent)
        with self.fs().open(path, "w", encoding="utf-8") as f:
            f.write(text)

    def read_bytes(self, path: str) -> bytes:
        with self.fs().open(path, "rb") as f:
            return f.read()

    def write_bytes(self, path: str, data: bytes) -> None:
        parent = path.rsplit("/", 1)[0]
        self.mkdirs(parent)
        with self.fs().open(path, "wb") as f:
            f.write(data)

    def listdir(self, path: str) -> list[str]:
        fs = self.fs()
        if not fs.exists(path):
            return []
        # normalize to names
        entries = fs.ls(path, detail=False)
        return [e.split("/")[-1] for e in entries]

    def walk_files(self, path: str) -> list[dict[str, Any]]:
        """Return a list of file info dicts (detail=True) under path.

        Uses fsspec's find where available.
        """

        fs = self.fs()
        if not fs.exists(path):
            return []

        try:
            # Most implementations return a dict: {"path": {detail...}}
            details = fs.find(path, withdirs=False, detail=True)
            if isinstance(details, dict):
                return [
                    dict({"name": p}, **(info or {})) for p, info in details.items()
                ]
            # Some may return a list already
            return list(details)
        except Exception:
            # fallback: glob recursive
            matches = fs.glob(path.rstrip("/") + "/**")
            out: list[dict[str, Any]] = []
            for m in matches:
                try:
                    info = fs.info(m)
                except Exception:
                    continue
                if info.get("type") == "directory":
                    continue
                out.append(dict({"name": m}, **info))
            return out

    @staticmethod
    def parse_mtime(value: Any) -> Optional[datetime]:
        if value is None:
            return None
        if isinstance(value, datetime):
            return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
        # fsspec often returns float seconds
        if isinstance(value, (int, float)):
            return datetime.fromtimestamp(float(value), tz=timezone.utc)
        # Sometimes ISO strings
        if isinstance(value, str):
            try:
                dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
                return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
            except Exception:
                return None
        return None

    def rm(self, path: str, recursive: bool = False) -> None:
        self.fs().rm(path, recursive=recursive)

    def atomic_write_text(self, path: str, text: str, tmp_suffix: str = ".tmp") -> None:
        """Best-effort atomic write.

        Note: S3 doesn't offer true atomic rename; this uses write-then-move.
        We'll later upgrade this to use conditional PUT (If-Match) when available.
        """

        tmp_path = f"{path}{tmp_suffix}"
        self.write_text(tmp_path, text)
        fs = self.fs()
        # move/rename if supported
        try:
            fs.mv(tmp_path, path)
        except Exception:
            # fallback: copy + delete
            fs.cp(tmp_path, path)
            fs.rm(tmp_path)
