# Fluxel Guardrails (Permanent):
# - DO NOT optimize for ML throughput in canonical storage (`blobs/`): no sharding, tarballs, or parquet in the blob layer.
# - DO NOT read blob data for metadata-only operations (diff, list, log, status).
# - DO NOT introduce a server, daemon, or central database; Fluxel is 100% client-side/serverless.
# - DO NOT use SHA-1/SHA-256; use Blake3 for all content hashing.
# - PREFER JSONL manifests to preserve streaming and O(1) memory usage.

from __future__ import annotations

import json
from pathlib import PurePosixPath
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Iterator, Literal

from blake3 import blake3

from .hashing import DEFAULT_CHUNK_SIZE, blake3_digest_file
from .layout import FluxelLayout, blob_relpath, initialize_fluxel_layout
from .manifest import ManifestEntry, ManifestReader, ManifestWriter, walk_files
from .storage import iter_s3_objects, open_source_uri, parse_s3_uri


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


@dataclass(frozen=True)
class VerifyResult:
    commit_id: str
    verified_entries: int
    candidate_entries: int
    total_entries: int
    created_commit: bool
    dry_run: bool


@dataclass(frozen=True)
class StageChange:
    path: str
    action: str
    identity_mode: str | None = None

    @staticmethod
    def from_dict(data: dict[str, object]) -> "StageChange":
        action = str(data["action"])
        identity_mode_raw = data.get("identity_mode")
        return StageChange(
            path=str(data["path"]),
            action=action,
            identity_mode=(
                str(identity_mode_raw) if identity_mode_raw is not None else None
            ),
        )


@dataclass(frozen=True)
class StageStatus:
    ref: str
    added: list[str]
    removed: list[str]


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

    def commit(
        self,
        message: str,
        identity_mode: Literal["blake3", "meta"] = "blake3",
        *,
        staged: bool = False,
        ref: str | None = None,
    ) -> str:
        if not message.strip():
            raise ValueError("Commit message cannot be empty")
        if identity_mode not in {"blake3", "meta"}:
            raise ValueError("identity_mode must be one of: blake3, meta")
        branch = ref or self.current_branch()
        self._ensure_branch_exists(branch)

        if staged:
            return self._commit_staged(message=message, branch=branch)

        temp_manifest = self._write_temp_manifest(
            self._materialize_blobs_and_entries(identity_mode=identity_mode)
        )
        manifest_hash = blake3_digest_file(temp_manifest)
        manifest_target = self.layout.manifests_dir / f"{manifest_hash}.jsonl"
        manifest_target.parent.mkdir(parents=True, exist_ok=True)
        if not manifest_target.exists():
            temp_manifest.replace(manifest_target)
        else:
            temp_manifest.unlink(missing_ok=True)

        parent_commit = self._branch_head_commit(branch)
        return self._write_commit_object(
            branch=branch,
            message=message,
            parent_commit=parent_commit,
            manifest_hash=manifest_hash,
        )

    def import_s3(
        self,
        source_uri: str,
        message: str,
        identity_mode: Literal["blake3", "meta"] = "blake3",
        *,
        path_patterns: list[str] | None = None,
        ref: str | None = None,
    ) -> str:
        if not message.strip():
            raise ValueError("Commit message cannot be empty")
        if identity_mode not in {"blake3", "meta"}:
            raise ValueError("identity_mode must be one of: blake3, meta")
        branch = ref or self.current_branch()
        self._ensure_branch_exists(branch)

        temp_manifest = self._write_temp_manifest(
            self._materialize_s3_entries(
                source_uri=source_uri,
                identity_mode=identity_mode,
                path_patterns=path_patterns,
            )
        )
        manifest_hash = blake3_digest_file(temp_manifest)
        manifest_target = self.layout.manifests_dir / f"{manifest_hash}.jsonl"
        manifest_target.parent.mkdir(parents=True, exist_ok=True)
        if not manifest_target.exists():
            temp_manifest.replace(manifest_target)
        else:
            temp_manifest.unlink(missing_ok=True)

        parent_commit = self._branch_head_commit(branch)
        return self._write_commit_object(
            branch=branch,
            message=message,
            parent_commit=parent_commit,
            manifest_hash=manifest_hash,
        )

    def add(
        self,
        paths: list[str],
        *,
        ref: str | None = None,
        identity_mode: str = "blake3",
    ) -> StageStatus:
        if identity_mode not in {"blake3", "meta"}:
            raise ValueError("identity_mode must be one of: blake3, meta")
        branch = ref or self.current_branch()
        self._ensure_branch_exists(branch)
        staged = self._load_stage(branch)
        for path in paths:
            normalized = self._normalize_stage_path(path)
            source = self.root / normalized
            if not source.exists() or not source.is_file():
                raise FileNotFoundError(f"Cannot stage missing file: {normalized}")
            staged[normalized] = StageChange(
                path=normalized,
                action="add",
                identity_mode=identity_mode,
            )
        self._save_stage(branch, staged)
        return self.status(ref=branch)

    def rm(self, paths: list[str], *, ref: str | None = None) -> StageStatus:
        branch = ref or self.current_branch()
        self._ensure_branch_exists(branch)
        staged = self._load_stage(branch)
        for path in paths:
            normalized = self._normalize_stage_path(path)
            staged[normalized] = StageChange(path=normalized, action="remove")
        self._save_stage(branch, staged)
        return self.status(ref=branch)

    def status(self, *, ref: str | None = None) -> StageStatus:
        branch = ref or self.current_branch()
        self._ensure_branch_exists(branch)
        staged = self._load_stage(branch)
        added = sorted(
            change.path for change in staged.values() if change.action == "add"
        )
        removed = sorted(
            change.path for change in staged.values() if change.action == "remove"
        )
        return StageStatus(ref=branch, added=added, removed=removed)

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

    def verify(
        self,
        ref: str = "main",
        path_prefixes: list[str] | None = None,
        *,
        dry_run: bool = False,
    ) -> VerifyResult:
        branch_path = self._branch_path(ref)
        if not branch_path.exists():
            raise ValueError("verify currently supports branch refs only")

        base_commit_id = self.resolve_ref(ref)
        base_commit = self.read_commit(base_commit_id)
        manifest_path = self.layout.manifests_dir / f"{base_commit.manifest}.jsonl"
        normalized_prefixes = [
            prefix.strip("/") for prefix in (path_prefixes or []) if prefix.strip("/")
        ]

        verified_entries = 0
        candidate_entries = 0
        total_entries = 0

        def should_verify(entry_path: str) -> bool:
            if not normalized_prefixes:
                return True
            return any(
                entry_path == prefix or entry_path.startswith(f"{prefix}/")
                for prefix in normalized_prefixes
            )

        def iter_verified_entries() -> Iterator[ManifestEntry]:
            nonlocal verified_entries, candidate_entries, total_entries
            reader = ManifestReader(manifest_path)
            for entry in reader.iter_entries():
                total_entries += 1
                if not should_verify(entry.path):
                    yield entry
                    continue
                if entry.blob_hash:
                    yield entry
                    continue
                candidate_entries += 1
                if dry_run:
                    yield entry
                    continue
                if not entry.source_uri:
                    raise FileNotFoundError(
                        f"Cannot verify '{entry.path}' because source_uri is missing"
                    )
                digest = self._store_blob_from_source_uri(entry.source_uri)
                verified_entries += 1
                yield ManifestEntry(
                    path=entry.path,
                    hash=digest,
                    size=entry.size,
                    mtime_ns=entry.mtime_ns,
                    identity_mode="blake3",
                    identity_value=digest,
                    blob_hash=digest,
                    source_uri=entry.source_uri,
                )

        temp_manifest = self._write_temp_manifest(iter_verified_entries())
        if verified_entries == 0:
            temp_manifest.unlink(missing_ok=True)
            return VerifyResult(
                commit_id=base_commit_id,
                verified_entries=0,
                candidate_entries=candidate_entries,
                total_entries=total_entries,
                created_commit=False,
                dry_run=dry_run,
            )

        manifest_hash = blake3_digest_file(temp_manifest)
        manifest_target = self.layout.manifests_dir / f"{manifest_hash}.jsonl"
        manifest_target.parent.mkdir(parents=True, exist_ok=True)
        if not manifest_target.exists():
            temp_manifest.replace(manifest_target)
        else:
            temp_manifest.unlink(missing_ok=True)

        commit_body = {
            "message": f"verify {ref}",
            "manifest": manifest_hash,
            "parent": base_commit_id,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "branch": ref,
        }
        canonical = json.dumps(
            commit_body, sort_keys=True, separators=(",", ":")
        ).encode("utf-8")
        commit_id = blake3(canonical).hexdigest()
        commit_object = CommitObject(id=commit_id, **commit_body)

        commit_path = self.layout.commits_dir / f"{commit_id}.json"
        commit_path.write_text(
            json.dumps(asdict(commit_object), indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        self._branch_path(ref).write_text(f"{commit_id}\n", encoding="utf-8")
        return VerifyResult(
            commit_id=commit_id,
            verified_entries=verified_entries,
            candidate_entries=candidate_entries,
            total_entries=total_entries,
            created_commit=True,
            dry_run=dry_run,
        )

    def _manifest_index(self, manifest_hash: str) -> dict[str, ManifestEntry]:
        manifest_path = self.layout.manifests_dir / f"{manifest_hash}.jsonl"
        index: dict[str, ManifestEntry] = {}
        for entry in ManifestReader(manifest_path).iter_entries():
            index[entry.path] = entry
        return index

    def resolve_entries(
        self, ref: str, *, include_staging: bool = False
    ) -> dict[str, ManifestEntry]:
        commit = self.read_commit(self.resolve_ref(ref))
        index = self._manifest_index(commit.manifest)
        if include_staging:
            for change in self._load_stage(ref).values():
                if change.action == "remove":
                    index.pop(change.path, None)
                    continue
                if change.action == "add":
                    index[change.path] = self._entry_from_working_path(
                        change.path,
                        change.identity_mode or "blake3",
                        store_blob=False,
                    )
        return index

    def _materialize_blobs_and_entries(
        self, *, identity_mode: str
    ) -> Iterator[ManifestEntry]:
        for file_path in walk_files(self.root):
            stat = file_path.stat()
            relative_path = file_path.relative_to(self.root).as_posix()
            source_uri = file_path.as_uri()
            if identity_mode == "blake3":
                identity_value = blake3_digest_file(file_path)
                blob_hash = identity_value
                self._store_blob(file_path, blob_hash)
            else:
                identity_value = self._metadata_identity(relative_path, stat.st_size)
                blob_hash = None
            yield ManifestEntry(
                path=relative_path,
                hash=identity_value,
                size=stat.st_size,
                mtime_ns=stat.st_mtime_ns,
                identity_mode=identity_mode,
                identity_value=identity_value,
                blob_hash=blob_hash,
                source_uri=source_uri,
            )

    def _materialize_s3_entries(
        self,
        *,
        source_uri: str,
        identity_mode: str,
        path_patterns: list[str] | None = None,
    ) -> Iterator[ManifestEntry]:
        _, prefix = parse_s3_uri(source_uri)
        normalized_prefix = prefix.strip("/")
        normalized_patterns = self._normalize_import_patterns(path_patterns)
        for obj in iter_s3_objects(source_uri):
            relative_path = self._normalize_s3_import_path(
                key=obj.key,
                prefix=normalized_prefix,
                size=obj.size,
            )
            if relative_path is None:
                continue
            if not self._matches_import_patterns(relative_path, normalized_patterns):
                continue
            if identity_mode == "blake3":
                identity_value = self._store_blob_from_source_uri(obj.source_uri)
                blob_hash = identity_value
            elif identity_mode == "meta":
                identity_value = self._metadata_identity(relative_path, obj.size)
                blob_hash = None
            else:
                raise ValueError("identity_mode must be one of: blake3, meta")
            yield ManifestEntry(
                path=relative_path,
                hash=identity_value,
                size=obj.size,
                mtime_ns=obj.mtime_ns,
                identity_mode=identity_mode,
                identity_value=identity_value,
                blob_hash=blob_hash,
                source_uri=obj.source_uri,
            )

    def _entry_from_working_path(
        self,
        relative_path: str,
        identity_mode: str,
        *,
        store_blob: bool,
    ) -> ManifestEntry:
        source_path = self.root / relative_path
        if not source_path.exists() or not source_path.is_file():
            raise FileNotFoundError(f"Cannot stage missing file: {relative_path}")
        stat = source_path.stat()
        source_uri = source_path.as_uri()
        if identity_mode == "blake3":
            identity_value = blake3_digest_file(source_path)
            blob_hash = identity_value
            if store_blob:
                self._store_blob(source_path, blob_hash)
            else:
                blob_hash = None
        elif identity_mode == "meta":
            identity_value = self._metadata_identity(relative_path, stat.st_size)
            blob_hash = None
        else:
            raise ValueError("identity_mode must be one of: blake3, meta")
        return ManifestEntry(
            path=relative_path,
            hash=identity_value,
            size=stat.st_size,
            mtime_ns=stat.st_mtime_ns,
            identity_mode=identity_mode,
            identity_value=identity_value,
            blob_hash=blob_hash,
            source_uri=source_uri,
        )

    def _metadata_identity(self, relative_path: str, size: int) -> str:
        payload = f"{relative_path}\n{size}".encode("utf-8")
        return blake3(payload).hexdigest()

    def _normalize_s3_import_path(
        self,
        *,
        key: str,
        prefix: str,
        size: int,
    ) -> str | None:
        if key.endswith("/") and size == 0:
            return None
        normalized_key = key.strip("/")
        if not normalized_key:
            return None
        normalized_prefix = prefix.strip("/")
        if normalized_prefix:
            if normalized_key == normalized_prefix:
                relative_path = normalized_key.rsplit("/", maxsplit=1)[-1]
            elif normalized_key.startswith(f"{normalized_prefix}/"):
                relative_path = normalized_key[len(normalized_prefix) + 1 :]
            else:
                raise ValueError(f"S3 key '{key}' is outside import prefix '{prefix}'")
        else:
            relative_path = normalized_key
        return self._normalize_stage_path(relative_path)

    def _normalize_import_patterns(
        self,
        path_patterns: list[str] | None,
    ) -> list[str]:
        patterns: list[str] = []
        for pattern in path_patterns or []:
            normalized = pattern.strip().strip("/")
            if not normalized:
                raise ValueError("Import path filter cannot be empty")
            if normalized.startswith("../") or "/../" in normalized or normalized == "..":
                raise ValueError("Import path filter cannot traverse outside repository root")
            patterns.append(normalized)
        return patterns

    def _matches_import_patterns(
        self,
        relative_path: str,
        path_patterns: list[str],
    ) -> bool:
        if not path_patterns:
            return True
        path = PurePosixPath(relative_path)
        return any(self._match_import_pattern(path, pattern) for pattern in path_patterns)

    def _match_import_pattern(
        self,
        path: PurePosixPath,
        pattern: str,
    ) -> bool:
        if path.match(pattern):
            return True
        if pattern.startswith("**/"):
            pattern_suffix = pattern[len("**/") :]
            return len(path.parts) == 1 and path.match(pattern_suffix)
        return False

    def _stage_path(self, branch: str) -> Path:
        return self.layout.staging_dir / f"{branch}.json"

    def _normalize_stage_path(self, path: str) -> str:
        normalized = path.strip().strip("/")
        if not normalized:
            raise ValueError("Path cannot be empty")
        if normalized.startswith("../") or "/../" in normalized or normalized == "..":
            raise ValueError("Path cannot traverse outside repository root")
        return normalized

    def _load_stage(self, branch: str) -> dict[str, StageChange]:
        stage_path = self._stage_path(branch)
        if not stage_path.exists():
            return {}
        raw = json.loads(stage_path.read_text(encoding="utf-8"))
        if not isinstance(raw, list):
            raise ValueError("Stage file must contain a list")
        staged: dict[str, StageChange] = {}
        for item in raw:
            if not isinstance(item, dict):
                continue
            change = StageChange.from_dict(item)
            staged[change.path] = change
        return staged

    def _save_stage(self, branch: str, staged: dict[str, StageChange]) -> None:
        stage_path = self._stage_path(branch)
        if not staged:
            stage_path.unlink(missing_ok=True)
            return
        payload = [
            {
                "path": change.path,
                "action": change.action,
                "identity_mode": change.identity_mode,
            }
            for change in sorted(staged.values(), key=lambda item: item.path)
        ]
        stage_path.parent.mkdir(parents=True, exist_ok=True)
        stage_path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")

    def _ensure_branch_exists(self, branch: str) -> None:
        branch_path = self._branch_path(branch)
        if not branch_path.exists():
            raise ValueError(f"Unknown branch: {branch}")

    def _branch_head_commit(self, branch: str) -> str | None:
        branch_path = self._branch_path(branch)
        if not branch_path.exists():
            return None
        value = branch_path.read_text(encoding="utf-8").strip()
        return value or None

    def _write_commit_object(
        self,
        *,
        branch: str,
        message: str,
        parent_commit: str | None,
        manifest_hash: str,
    ) -> str:
        commit_body = {
            "message": message,
            "manifest": manifest_hash,
            "parent": parent_commit,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "branch": branch,
        }
        canonical = json.dumps(
            commit_body, sort_keys=True, separators=(",", ":")
        ).encode("utf-8")
        commit_id = blake3(canonical).hexdigest()
        commit_object = CommitObject(id=commit_id, **commit_body)
        commit_path = self.layout.commits_dir / f"{commit_id}.json"
        commit_path.write_text(
            json.dumps(asdict(commit_object), indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        self._branch_path(branch).write_text(f"{commit_id}\n", encoding="utf-8")
        return commit_id

    def _commit_staged(self, *, message: str, branch: str) -> str:
        staged = self._load_stage(branch)
        if not staged:
            raise ValueError(f"No staged changes for branch: {branch}")

        parent_commit = self._branch_head_commit(branch)
        index: dict[str, ManifestEntry]
        if parent_commit:
            parent = self.read_commit(parent_commit)
            index = self._manifest_index(parent.manifest)
        else:
            index = {}

        for change in staged.values():
            if change.action == "remove":
                index.pop(change.path, None)
                continue
            if change.action == "add":
                index[change.path] = self._entry_from_working_path(
                    change.path,
                    change.identity_mode or "blake3",
                    store_blob=True,
                )
                continue
            raise ValueError(f"Unknown stage action: {change.action}")

        def iter_entries() -> Iterator[ManifestEntry]:
            for path in sorted(index):
                yield index[path]

        temp_manifest = self._write_temp_manifest(iter_entries())
        manifest_hash = blake3_digest_file(temp_manifest)
        manifest_target = self.layout.manifests_dir / f"{manifest_hash}.jsonl"
        manifest_target.parent.mkdir(parents=True, exist_ok=True)
        if not manifest_target.exists():
            temp_manifest.replace(manifest_target)
        else:
            temp_manifest.unlink(missing_ok=True)

        commit_id = self._write_commit_object(
            branch=branch,
            message=message,
            parent_commit=parent_commit,
            manifest_hash=manifest_hash,
        )
        self._save_stage(branch, {})
        return commit_id

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

    def _store_blob_from_source_uri(self, source_uri: str) -> str:
        with NamedTemporaryFile(mode="wb", delete=False) as temp:
            temp_path = Path(temp.name)
            hasher = blake3()
            with open_source_uri(source_uri) as source:
                while True:
                    chunk = source.read(DEFAULT_CHUNK_SIZE)
                    if not chunk:
                        break
                    hasher.update(chunk)
                    temp.write(chunk)
        digest = hasher.hexdigest()
        target = self.layout.blobs_dir / blob_relpath(digest)
        if target.exists():
            temp_path.unlink(missing_ok=True)
            return digest
        target.parent.mkdir(parents=True, exist_ok=True)
        temp_path.replace(target)
        return digest

    def _write_temp_manifest(self, entries: Iterator[ManifestEntry]) -> Path:
        with NamedTemporaryFile(
            mode="w", suffix=".jsonl", delete=False, encoding="utf-8"
        ) as temp:
            temp_path = Path(temp.name)
        writer = ManifestWriter(temp_path)
        writer.write_entries(entries)
        return temp_path


def commit(
    root: str | Path,
    message: str,
    identity_mode: str = "blake3",
    *,
    staged: bool = False,
    ref: str | None = None,
) -> str:
    return FluxelRepository(root).commit(
        message,
        identity_mode=identity_mode,
        staged=staged,
        ref=ref,
    )


def import_s3(
    root: str | Path,
    source_uri: str,
    message: str,
    identity_mode: str = "blake3",
    *,
    path_patterns: list[str] | None = None,
    ref: str | None = None,
) -> str:
    return FluxelRepository(root).import_s3(
        source_uri,
        message,
        identity_mode=identity_mode,
        path_patterns=path_patterns,
        ref=ref,
    )


def branch(root: str | Path, name: str) -> Path:
    return FluxelRepository(root).branch(name)


def add(
    root: str | Path,
    paths: list[str],
    *,
    ref: str | None = None,
    identity_mode: str = "blake3",
) -> StageStatus:
    return FluxelRepository(root).add(paths, ref=ref, identity_mode=identity_mode)


def rm(root: str | Path, paths: list[str], *, ref: str | None = None) -> StageStatus:
    return FluxelRepository(root).rm(paths, ref=ref)


def status(root: str | Path, *, ref: str | None = None) -> StageStatus:
    return FluxelRepository(root).status(ref=ref)


def diff(root: str | Path, from_ref: str, to_ref: str) -> list[DiffEntry]:
    return FluxelRepository(root).diff(from_ref=from_ref, to_ref=to_ref)


def verify(
    root: str | Path,
    ref: str = "main",
    path_prefixes: list[str] | None = None,
    *,
    dry_run: bool = False,
) -> VerifyResult:
    return FluxelRepository(root).verify(
        ref=ref, path_prefixes=path_prefixes, dry_run=dry_run
    )
