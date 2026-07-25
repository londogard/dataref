from __future__ import annotations

import json
from pathlib import Path
from tempfile import NamedTemporaryFile

from .domain import BranchRefState


HEAD_FILE = "HEAD"


class LocalClientState:
    def __init__(self, root: str | Path) -> None:
        self.root = Path(root).resolve()
        self.fluxel_dir = self.root / ".fluxel"
        self.refs_dir = self.fluxel_dir / "refs"
        self.branch_state_dir = self.refs_dir / "heads"
        self.staging_dir = self.fluxel_dir / "staging"
        self.head_path = self.refs_dir / HEAD_FILE

        for path in (
            self.fluxel_dir,
            self.refs_dir,
            self.branch_state_dir,
            self.staging_dir,
        ):
            path.mkdir(parents=True, exist_ok=True)

    def ensure_current_branch(self, default_branch: str) -> None:
        if not self.head_path.exists():
            self.set_current_branch(default_branch)

    def current_branch(self) -> str:
        content = self.head_path.read_text(encoding="utf-8").strip()
        if not content.startswith("refs/heads/"):
            raise ValueError("HEAD must be a symbolic ref under refs/heads/")
        return content.split("refs/heads/", maxsplit=1)[1]

    def set_current_branch(self, branch: str) -> None:
        self._atomic_write_text(self.head_path, f"refs/heads/{branch}\n")

    def read_staging_payload(self, branch: str) -> str | None:
        stage_path = self.stage_path(branch)
        if not stage_path.exists():
            return None
        return stage_path.read_text(encoding="utf-8")

    def write_staging_payload(self, branch: str, payload: str | None) -> None:
        stage_path = self.stage_path(branch)
        if payload is None:
            stage_path.unlink(missing_ok=True)
            return
        self._atomic_write_text(stage_path, payload)

    def read_branch_snapshot(self, branch: str) -> BranchRefState | None:
        snapshot_path = self.branch_snapshot_path(branch)
        if not snapshot_path.exists():
            return None
        payload = json.loads(snapshot_path.read_text(encoding="utf-8"))
        return BranchRefState(
            branch=branch,
            commit_id=payload.get("commit_id"),
            version_token=payload.get("version_token"),
        )

    def write_branch_snapshot(
        self,
        branch: str,
        *,
        commit_id: str | None,
        version_token: str | None,
    ) -> None:
        payload = json.dumps(
            {
                "commit_id": commit_id,
                "version_token": version_token,
            },
            sort_keys=True,
        )
        self._atomic_write_text(self.branch_snapshot_path(branch), f"{payload}\n")

    def branch_snapshot_path(self, branch: str) -> Path:
        return self.branch_state_dir / f"{branch}.json"

    def stage_path(self, branch: str) -> Path:
        return self.staging_dir / f"{branch}.json"

    def _atomic_write_text(self, path: Path, payload: str) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        with NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            dir=path.parent,
            prefix=f".{path.name}.",
            suffix=".tmp",
            delete=False,
        ) as temp:
            temp_path = Path(temp.name)
            temp.write(payload)

        try:
            temp_path.replace(path)
        finally:
            temp_path.unlink(missing_ok=True)
