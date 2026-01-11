from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional

import fsspec
from fsspec.spec import AbstractFileSystem

from .repo import Repo


@dataclass
class _Parsed:
    repo_root: str
    ref: str
    name: str


def _parse_strand_path(path: str) -> _Parsed:
    """Parse a strand path.

    Supported form (fsspec):
      strand://s3://bucket/strand-repo@main/path/to/file

    - repo_root: s3://bucket/strand-repo
    - ref: main (or HEAD, or a commit id)
    - name: path/to/file (logical name, relative to dataset_root at snapshot time)
    """

    p = path
    # fsspec may call with /-prefixed paths
    p = p.lstrip("/")

    at = p.find("@")
    if at < 0:
        raise ValueError(
            "Invalid strand path. Expected: strand://<repo_root>@<ref>/<name>"
        )

    repo_root = p[:at]
    rest = p[at + 1 :]
    ref, sep, name = rest.partition("/")
    if not sep or not ref or not name:
        raise ValueError(
            "Invalid strand path. Expected: strand://<repo_root>@<ref>/<name>"
        )

    return _Parsed(repo_root=repo_root, ref=ref, name=name)


class StrandFileSystem(AbstractFileSystem):
    """Virtual FS resolving logical names via strand manifests.

    Read-only MVP.
    """

    protocol = "strand"

    def __init__(self, **storage_options: Any):
        super().__init__(**storage_options)
        self._storage_options = storage_options

    @classmethod
    def _strip_protocol(cls, path: str) -> str:
        # fsspec calls this internally; keep the inner path intact.
        if path.startswith("strand://"):
            return path[len("strand://") :]
        return super()._strip_protocol(path)

    def _resolve(self, path: str) -> tuple[str, str, str]:
        parsed = _parse_strand_path(self._strip_protocol(path))
        repo = Repo.open(parsed.repo_root)
        commit_id = repo.resolve_ref(parsed.ref)
        physical = repo.resolve_file_path(commit_id, parsed.name)
        return commit_id, parsed.name, physical

    def open(
        self,
        path: str,
        mode: str = "rb",
        block_size: Optional[int] = None,
        **kwargs: Any,
    ):
        if mode not in {"rb", "r"}:
            raise NotImplementedError("StrandFileSystem is read-only (rb/r)")

        _commit_id, _name, physical = self._resolve(path)
        return fsspec.open(physical, mode=mode, block_size=block_size, **kwargs).open()

    def exists(self, path: str) -> bool:
        try:
            _commit_id, _name, physical = self._resolve(path)
        except Exception:
            return False
        return fsspec.filesystem("s3").exists(physical) if physical.startswith("s3://") else fsspec.filesystem("file").exists(physical)

    def info(self, path: str, **kwargs: Any) -> dict[str, Any]:
        commit_id, name, physical = self._resolve(path)
        fs = fsspec.filesystem("s3") if physical.startswith("s3://") else fsspec.filesystem("file")
        info = fs.info(physical)
        return {
            **info,
            "name": name,
            "physical": physical,
            "commit": commit_id,
        }


# Register for fsspec.open("strand://...")
fsspec.register_implementation("strand", StrandFileSystem, clobber=True)
