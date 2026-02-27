# Fluxel Guardrails (Permanent):
# - DO NOT optimize for ML throughput in canonical storage (`blobs/`): no sharding, tarballs, or parquet in the blob layer.
# - DO NOT read blob data for metadata-only operations (diff, list, log, status).
# - DO NOT introduce a server, daemon, or central database; Fluxel is 100% client-side/serverless.
# - DO NOT use SHA-1/SHA-256; use Blake3 for all content hashing.
# - PREFER JSONL manifests to preserve streaming and O(1) memory usage.

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Iterator

from blake3 import blake3

from .hashing import DEFAULT_CHUNK_SIZE, blake3_digest_file
from .layout import FluxelLayout, blob_relpath, initialize_fluxel_layout
from .manifest import ManifestEntry, ManifestReader, ManifestWriter, walk_files


HEAD_FILE = "HEAD"


@dataclass(frozen=True)
class CommitObject:
    id: str
    message: str
    manifest: str
    parent: str | None
    created_at: str
    branch: str


@dataclass(frozen=True)
class DiffEntry:
    path: str
    change: str
    before_hash: str | None
    after_hash: str | None
    before_size: int | None
    after_size: int | None


class FluxelRepository:
    def __init__(self, root: str | Path) -> None:
        self.layout = initialize_fluxel_layout(root)
        self._ensure_head(default_branch="main")

    @property
    def root(self) -> Path:
        return self.layout.root

    def _head_path(self) -> Path:
        return self.layout.refs_dir / HEAD_FILE

    def _branch_path(self, branch_name: str) -> Path:
        return self.layout.heads_dir / branch_name

    def _ensure_head(self, default_branch: str) -> None:
        head_path = self._head_path()
        if not head_path.exists():
            head_path.write_text(f"refs/heads/{default_branch}\n", encoding="utf-8")
        default_ref = self._branch_path(default_branch)
        if not default_ref.exists():
            default_ref.parent.mkdir(parents=True, exist_ok=True)
            default_ref.write_text("", encoding="utf-8")

    def _read_head_ref(self) -> str:
        content = self._head_path().read_text(encoding="utf-8").strip()
        if not content.startswith("refs/heads/"):
            raise ValueError("HEAD must be a symbolic ref under refs/heads/")
        return content

    def current_branch(self) -> str:
        return self._read_head_ref().split("refs/heads/", maxsplit=1)[1]

    def head_commit(self) -> str | None:
        branch_path = self.layout.fluxel_dir / self._read_head_ref()
        if not branch_path.exists():
            return None
        commit_id = branch_path.read_text(encoding="utf-8").strip()
        return commit_id or None

    def resolve_ref(self, branch_or_commit: str) -> str:
        maybe_branch = self._branch_path(branch_or_commit)
        if maybe_branch.exists():
            commit_id = maybe_branch.read_text(encoding="utf-8").strip()
            if not commit_id:
                raise ValueError(f"Branch has no commits: {branch_or_commit}")
            return commit_id
        commit_path = self.layout.commits_dir / f"{branch_or_commit}.json"
        if commit_path.exists():
            return branch_or_commit
        raise ValueError(f"Unknown branch or commit: {branch_or_commit}")

    def branch(self, name: str) -> Path:
        if not name or "/" in name or name.startswith("."):
            raise ValueError("Invalid branch name")
        target = self._branch_path(name)
        if target.exists():
            raise ValueError(f"Branch already exists: {name}")
        head_commit = self.head_commit() or ""
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(f"{head_commit}\n" if head_commit else "", encoding="utf-8")
        return target

    def commit(self, message: str) -> str:
        if not message.strip():
            raise ValueError("Commit message cannot be empty")

        temp_manifest = self._write_temp_manifest(self._materialize_blobs_and_entries())
        manifest_hash = blake3_digest_file(temp_manifest)
        manifest_target = self.layout.manifests_dir / f"{manifest_hash}.jsonl"
        manifest_target.parent.mkdir(parents=True, exist_ok=True)
        if not manifest_target.exists():
            temp_manifest.replace(manifest_target)
        else:
            temp_manifest.unlink(missing_ok=True)

        parent_commit = self.head_commit()
        branch = self.current_branch()
        commit_body = {
            "message": message,
            "manifest": manifest_hash,
            "parent": parent_commit,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "branch": branch,
        }
        canonical = json.dumps(commit_body, sort_keys=True, separators=(",", ":")).encode("utf-8")
        commit_id = blake3(canonical).hexdigest()
        commit_object = CommitObject(id=commit_id, **commit_body)

        commit_path = self.layout.commits_dir / f"{commit_id}.json"
        commit_path.write_text(
            json.dumps(asdict(commit_object), indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        self._branch_path(branch).write_text(f"{commit_id}\n", encoding="utf-8")
        return commit_id

    def read_commit(self, commit_id: str) -> CommitObject:
        commit_path = self.layout.commits_dir / f"{commit_id}.json"
        if not commit_path.exists():
            raise ValueError(f"Unknown commit: {commit_id}")
        data = json.loads(commit_path.read_text(encoding="utf-8"))
        return CommitObject(
            id=str(data["id"]),
            message=str(data["message"]),
            manifest=str(data["manifest"]),
            parent=str(data["parent"]) if data["parent"] else None,
            created_at=str(data["created_at"]),
            branch=str(data["branch"]),
        )

    def diff(self, from_ref: str, to_ref: str) -> list[DiffEntry]:
        from_commit = self.read_commit(self.resolve_ref(from_ref))
        to_commit = self.read_commit(self.resolve_ref(to_ref))
        from_entries = self._manifest_index(from_commit.manifest)
        to_entries = self._manifest_index(to_commit.manifest)

        changes: list[DiffEntry] = []

        from_only = sorted(set(from_entries) - set(to_entries))
        for path in from_only:
            before = from_entries[path]
            changes.append(
                DiffEntry(
                    path=path,
                    change="removed",
                    before_hash=before.hash,
                    after_hash=None,
                    before_size=before.size,
                    after_size=None,
                )
            )

        to_only = sorted(set(to_entries) - set(from_entries))
        for path in to_only:
            after = to_entries[path]
            changes.append(
                DiffEntry(
                    path=path,
                    change="added",
                    before_hash=None,
                    after_hash=after.hash,
                    before_size=None,
                    after_size=after.size,
                )
            )

        shared = sorted(set(from_entries) & set(to_entries))
        for path in shared:
            before = from_entries[path]
            after = to_entries[path]
            if before.hash == after.hash and before.size == after.size:
                continue
            changes.append(
                DiffEntry(
                    path=path,
                    change="modified",
                    before_hash=before.hash,
                    after_hash=after.hash,
                    before_size=before.size,
                    after_size=after.size,
                )
            )

        return changes

    def _manifest_index(self, manifest_hash: str) -> dict[str, ManifestEntry]:
        manifest_path = self.layout.manifests_dir / f"{manifest_hash}.jsonl"
        index: dict[str, ManifestEntry] = {}
        for entry in ManifestReader(manifest_path).iter_entries():
            index[entry.path] = entry
        return index

    def _materialize_blobs_and_entries(self) -> Iterator[ManifestEntry]:
        for file_path in walk_files(self.root):
            digest = blake3_digest_file(file_path)
            self._store_blob(file_path, digest)
            stat = file_path.stat()
            yield ManifestEntry(
                path=file_path.relative_to(self.root).as_posix(),
                hash=digest,
                size=stat.st_size,
                mtime_ns=stat.st_mtime_ns,
            )

    def _store_blob(self, source_file: Path, content_hash: str) -> None:
        rel = blob_relpath(content_hash)
        target = self.layout.blobs_dir / rel
        if target.exists():
            return
        target.parent.mkdir(parents=True, exist_ok=True)
        with source_file.open("rb") as src, target.open("wb") as dst:
            while True:
                chunk = src.read(DEFAULT_CHUNK_SIZE)
                if not chunk:
                    break
                dst.write(chunk)

    def _write_temp_manifest(self, entries: Iterator[ManifestEntry]) -> Path:
        with NamedTemporaryFile(mode="w", suffix=".jsonl", delete=False, encoding="utf-8") as temp:
            temp_path = Path(temp.name)
        writer = ManifestWriter(temp_path)
        writer.write_entries(entries)
        return temp_path


def commit(root: str | Path, message: str) -> str:
    return FluxelRepository(root).commit(message)


def branch(root: str | Path, name: str) -> Path:
    return FluxelRepository(root).branch(name)


def diff(root: str | Path, from_ref: str, to_ref: str) -> list[DiffEntry]:
    return FluxelRepository(root).diff(from_ref=from_ref, to_ref=to_ref)
