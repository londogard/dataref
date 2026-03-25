from __future__ import annotations

from pathlib import Path


HEAD_FILE = "HEAD"


class LocalClientState:
    def __init__(self, root: str | Path) -> None:
        self.root = Path(root).resolve()
        self.fluxel_dir = self.root / ".fluxel"
        self.refs_dir = self.fluxel_dir / "refs"
        self.staging_dir = self.fluxel_dir / "staging"

        for path in (self.fluxel_dir, self.refs_dir, self.staging_dir):
            path.mkdir(parents=True, exist_ok=True)

    def ensure_current_branch(self, default_branch: str) -> None:
        head_path = self.head_path()
        if not head_path.exists():
            self.set_current_branch(default_branch)

    def current_branch(self) -> str:
        content = self.head_path().read_text(encoding="utf-8").strip()
        if not content.startswith("refs/heads/"):
            raise ValueError("HEAD must be a symbolic ref under refs/heads/")
        return content.split("refs/heads/", maxsplit=1)[1]

    def set_current_branch(self, branch: str) -> None:
        self.head_path().write_text(f"refs/heads/{branch}\n", encoding="utf-8")

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
        stage_path.parent.mkdir(parents=True, exist_ok=True)
        stage_path.write_text(payload, encoding="utf-8")

    def head_path(self) -> Path:
        return self.refs_dir / HEAD_FILE

    def stage_path(self, branch: str) -> Path:
        return self.staging_dir / f"{branch}.json"
