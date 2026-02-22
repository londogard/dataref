from __future__ import annotations

import io
import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

import polars as pl

from .hash import sha256_hex, sha256_json
from .manifest import ManifestPartRef, SnapshotManifestRoot
from .models import Commit, RepoConfig
from .storage import Storage


@dataclass
class Repo:
    config: RepoConfig
    storage: Storage

    @classmethod
    def open(cls, root: str) -> "Repo":
        storage = Storage(root=root)
        config_path = storage.join(".strand", "config.json")
        if not storage.exists(config_path):
            raise FileNotFoundError(
                f"No strand repo found at {root}. Run `strand init {root}` first."
            )
        cfg = RepoConfig.model_validate_json(storage.read_text(config_path))
        return cls(config=cfg, storage=storage)

    @classmethod
    def init(cls, root: str) -> "Repo":
        storage = Storage(root=root)
        strand_dir = storage.join(".strand")
        storage.mkdirs(strand_dir)

        cfg = RepoConfig(root=root)
        storage.write_text(
            storage.join(".strand", "config.json"), cfg.model_dump_json(indent=2)
        )

        # layout
        storage.mkdirs(storage.join(".strand", "objects"))
        storage.mkdirs(storage.join(".strand", "refs", "heads"))
        storage.mkdirs(storage.join(".strand", "datasets"))
        storage.write_text(storage.join(".strand", "HEAD"), "refs/heads/main\n")
        # create main ref (empty)
        storage.write_text(storage.join(".strand", "refs", "heads", "main"), "")

        return cls(config=cfg, storage=storage)

    def _head_ref_path(self) -> str:
        head = self.storage.read_text(self.storage.join(".strand", "HEAD")).strip()
        return self.storage.join(".strand", head)

    def head_commit(self) -> Optional[str]:
        ref_path = self._head_ref_path()
        if not self.storage.exists(ref_path):
            return None
        value = self.storage.read_text(ref_path).strip()
        return value or None

    def write_object_json(self, obj: dict) -> str:
        obj_id = sha256_json(obj)
        path = self.storage.join(".strand", "objects", f"{obj_id}.json")
        if not self.storage.exists(path):
            self.storage.write_text(path, json.dumps(obj, indent=2, ensure_ascii=False))
        return obj_id

    def write_object_bytes(self, data: bytes, ext: str) -> str:
        if not ext.startswith("."):
            ext = "." + ext
        obj_id = sha256_hex(data)
        path = self.storage.join(".strand", "objects", f"{obj_id}{ext}")
        if not self.storage.exists(path):
            self.storage.write_bytes(path, data)
        return obj_id

    def read_object_bytes(self, obj_id: str, ext: str) -> bytes:
        if not ext.startswith("."):
            ext = "." + ext
        path = self.storage.join(".strand", "objects", f"{obj_id}{ext}")
        return self.storage.read_bytes(path)

    def read_object_json(self, obj_id: str) -> dict:
        path = self.storage.join(".strand", "objects", f"{obj_id}.json")
        return json.loads(self.storage.read_text(path))

    def commit(self, message: str, author: Optional[str] = None) -> str:
        parent = self.head_commit()
        commit = Commit(message=message, author=author, parent=parent, tree={})
        commit_id = self.write_object_json(commit.model_dump(mode="json"))

        # update HEAD ref
        ref_path = self._head_ref_path()
        self.storage.atomic_write_text(ref_path, f"{commit_id}\n")
        return commit_id

    def get_commit(self, commit_id: str) -> Commit:
        return Commit.model_validate(self.read_object_json(commit_id))

    def resolve_ref(self, ref: str) -> str:
        """Resolve a ref name to a commit id.

        Supports:
        - full commit id (64 hex)
        - branch name (refs/heads/<name>)
        - HEAD
        """

        if not ref or ref == "HEAD":
            head = self.head_commit()
            if not head:
                raise ValueError("No commits on current branch")
            return head

        if re.fullmatch(r"[0-9a-f]{64}", ref):
            return ref

        ref_path = self.storage.join(".strand", "refs", "heads", ref)
        if not self.storage.exists(ref_path):
            raise FileNotFoundError(f"Unknown ref: {ref}")
        commit_id = self.storage.read_text(ref_path).strip()
        if not commit_id:
            raise ValueError(f"Ref has no commits: {ref}")
        return commit_id

    @staticmethod
    def _bucket_for_name(name: str, buckets: int) -> int:
        # Deterministic and dependency-free.
        h = sha256_hex(name.encode("utf-8"))
        return int(h[:8], 16) % buckets

    def _write_manifest_parts(
        self,
        dataset_root: str,
        entries: list[dict],
        buckets: int,
    ) -> list[ManifestPartRef]:
        by_bucket: dict[int, list[dict]] = {}
        for e in entries:
            b = self._bucket_for_name(e["name"], buckets)
            by_bucket.setdefault(b, []).append(e)

        parts: list[ManifestPartRef] = []
        for bucket, rows in sorted(by_bucket.items(), key=lambda kv: kv[0]):
            df = pl.DataFrame(rows)
            # keep deterministic order within a part
            df = df.sort("name")
            buf = io.BytesIO()
            df.write_parquet(buf)
            obj_id = self.write_object_bytes(buf.getvalue(), ext=".parquet")
            parts.append(ManifestPartRef(bucket=bucket, object_id=obj_id, rows=df.height))

        return parts

    def _load_manifest_part_map(self, part_obj_id: str) -> dict[str, tuple[int, Optional[str]]]:
        data = self.read_object_bytes(part_obj_id, ext=".parquet")
        df = pl.read_parquet(io.BytesIO(data))
        # name -> (size, etag)
        name_col = df["name"].to_list()
        size_col = df["size"].to_list()
        etag_col = df["etag"].to_list() if "etag" in df.columns else [None] * len(name_col)
        return {n: (int(s), (e if e != "" else None)) for n, s, e in zip(name_col, size_col, etag_col)}

    def _load_manifest_part_paths(self, part_obj_id: str) -> dict[str, str]:
        data = self.read_object_bytes(part_obj_id, ext=".parquet")
        df = pl.read_parquet(io.BytesIO(data), columns=["name", "s3_path"])  # type: ignore[arg-type]
        names = df["name"].to_list()
        paths = df["s3_path"].to_list()
        return {n: p for n, p in zip(names, paths)}

    def _load_manifest_part_names(self, part_obj_id: str) -> list[str]:
        data = self.read_object_bytes(part_obj_id, ext=".parquet")
        df = pl.read_parquet(io.BytesIO(data), columns=["name"])  # type: ignore[arg-type]
        return [str(n) for n in df["name"].to_list()]

    def snapshot(
        self,
        dataset_root: str,
        message: Optional[str] = None,
        author: Optional[str] = None,
    ) -> str:
        """Create a snapshot commit for a dataset root.

        Stores a partitioned Parquet manifest to avoid a single huge JSON blob.
        Logical name = path relative to dataset_root.
        """

        dataset_storage = Storage(root=dataset_root)
        infos = dataset_storage.walk_files(dataset_root)

        prefix = dataset_root.rstrip("/") + "/"
        rows: list[dict] = []
        for info in infos:
            full = info.get("name") or info.get("path")
            if not full or not isinstance(full, str):
                continue

            logical = full[len(prefix) :] if full.startswith(prefix) else full.split("/")[-1]

            size = int(info.get("size") or 0)
            mtime = Storage.parse_mtime(info.get("mtime") or info.get("last_modified"))
            mtime_ms = int(mtime.timestamp() * 1000) if mtime else None
            etag = info.get("ETag") or info.get("etag")
            if isinstance(etag, str):
                etag = etag.strip('"')
            else:
                etag = None

            rows.append(
                {
                    "name": logical,
                    "s3_path": full,
                    "size": size,
                    "mtime_ms": mtime_ms,
                    "etag": etag,
                }
            )

        created_at = datetime.now(timezone.utc)
        buckets = 256
        parts = self._write_manifest_parts(dataset_root=dataset_root, entries=rows, buckets=buckets)

        manifest_root = SnapshotManifestRoot(
            dataset_root=dataset_root,
            created_at=created_at,
            partitioning={"algorithm": "sha256_mod", "buckets": buckets},
            parts=parts,
        )

        manifest_id = self.write_object_json(manifest_root.model_dump(mode="json"))

        parent = self.head_commit()
        commit = Commit(
            message=message or f"snapshot {dataset_root}",
            author=author,
            parent=parent,
            tree={
                "kind": "snapshot",
                "dataset": dataset_root,
                "manifest": manifest_id,
            },
        )
        commit_id = self.write_object_json(commit.model_dump(mode="json"))

        ref_path = self._head_ref_path()
        self.storage.atomic_write_text(ref_path, f"{commit_id}\n")
        return commit_id

    def diff_snapshots(
        self,
        from_commit_id: str,
        to_commit_id: str,
    ) -> dict:
        """Compute a file-level diff between two snapshot commits."""

        a = self.get_commit(from_commit_id)
        b = self.get_commit(to_commit_id)

        if a.tree.get("kind") != "snapshot" or b.tree.get("kind") != "snapshot":
            raise ValueError("Both commits must be snapshot commits")

        a_manifest_id = a.tree.get("manifest")
        b_manifest_id = b.tree.get("manifest")
        if not a_manifest_id or not b_manifest_id:
            raise ValueError("Missing manifest ids in commit trees")

        a_root = SnapshotManifestRoot.model_validate(self.read_object_json(a_manifest_id))
        b_root = SnapshotManifestRoot.model_validate(self.read_object_json(b_manifest_id))

        a_parts = {p.bucket: p for p in a_root.parts}
        b_parts = {p.bucket: p for p in b_root.parts}

        added: list[str] = []
        removed: list[str] = []
        modified: list[str] = []

        for bucket in sorted(set(a_parts) | set(b_parts)):
            a_map = (
                self._load_manifest_part_map(a_parts[bucket].object_id)
                if bucket in a_parts
                else {}
            )
            b_map = (
                self._load_manifest_part_map(b_parts[bucket].object_id)
                if bucket in b_parts
                else {}
            )

            a_keys = set(a_map)
            b_keys = set(b_map)
            added.extend(sorted(b_keys - a_keys))
            removed.extend(sorted(a_keys - b_keys))

            for name in sorted(a_keys & b_keys):
                if a_map[name] != b_map[name]:
                    modified.append(name)

        return {
            "from": from_commit_id,
            "to": to_commit_id,
            "dataset_from": a.tree.get("dataset"),
            "dataset_to": b.tree.get("dataset"),
            "added": added,
            "removed": removed,
            "modified": modified,
        }

    def resolve_file_path(self, commit_id: str, name: str) -> str:
        """Resolve a logical name to a physical path for a snapshot commit."""

        commit = self.get_commit(commit_id)
        if commit.tree.get("kind") != "snapshot":
            raise ValueError("Commit is not a snapshot commit")
        manifest_id = commit.tree.get("manifest")
        if not manifest_id:
            raise ValueError("Missing manifest id")

        root = SnapshotManifestRoot.model_validate(self.read_object_json(manifest_id))
        buckets = root.partitioning.buckets
        bucket = self._bucket_for_name(name, buckets)
        part = next((p for p in root.parts if p.bucket == bucket), None)
        if not part:
            raise FileNotFoundError(name)

        mapping = self._load_manifest_part_paths(part.object_id)
        if name not in mapping:
            raise FileNotFoundError(name)
        return mapping[name]

    def log(self, limit: int = 50) -> list[Commit]:
        out: list[Commit] = []
        cur = self.head_commit()
        while cur and len(out) < limit:
            obj = self.read_object_json(cur)
            c = Commit.model_validate(obj)
            out.append(c)
            cur = c.parent
        return out

    def current_branch(self) -> str:
        head = self.storage.read_text(self.storage.join(".strand", "HEAD")).strip()
        if head.startswith("refs/heads/"):
            return head.split("/", 2)[-1]
        return head

    def create_branch(self, name: str, from_commit: Optional[str] = None) -> None:
        if not name or "/" in name or ".." in name:
            raise ValueError("Invalid branch name")
        commit = from_commit or self.head_commit() or ""
        self.storage.write_text(
            self.storage.join(".strand", "refs", "heads", name),
            f"{commit}\n" if commit else "",
        )

    def checkout_branch(self, name: str) -> None:
        ref = self.storage.join(".strand", "refs", "heads", name)
        if not self.storage.exists(ref):
            raise FileNotFoundError(f"Branch not found: {name}")
        self.storage.write_text(
            self.storage.join(".strand", "HEAD"), f"refs/heads/{name}\n"
        )

    def _dataset_ref_path(self, dataset: str, ref: str) -> str:
        if not dataset or "/" in dataset or ".." in dataset:
            raise ValueError("Dataset name cannot be empty or contain / or ..")
        if not ref or "/" in ref or ".." in ref:
            raise ValueError("Ref name cannot be empty or contain / or ..")
        return self.storage.join(".strand", "datasets", dataset, "refs", ref)

    def _ensure_dataset_ref(self, dataset: str, ref: str) -> None:
        path = self._dataset_ref_path(dataset, ref)
        parent = path.rsplit("/", 1)[0]
        self.storage.mkdirs(parent)
        if not self.storage.exists(path):
            self.storage.write_text(path, "")

    def dataset_head(self, dataset: str, ref: str = "main") -> Optional[str]:
        path = self._dataset_ref_path(dataset, ref)
        if not self.storage.exists(path):
            return None
        value = self.storage.read_text(path).strip()
        return value or None

    def set_dataset_ref(self, dataset: str, snapshot_id: str, ref: str = "main") -> None:
        self._ensure_dataset_ref(dataset, ref)
        self.storage.atomic_write_text(
            self._dataset_ref_path(dataset, ref), f"{snapshot_id}\n"
        )

    def snapshot_dataset(
        self,
        dataset: str,
        dataset_root: str,
        ref: str = "main",
        message: Optional[str] = None,
        author: Optional[str] = None,
    ) -> str:
        snapshot_id = self.snapshot(dataset_root=dataset_root, message=message, author=author)
        self.set_dataset_ref(dataset=dataset, ref=ref, snapshot_id=snapshot_id)
        return snapshot_id

    def clone_dataset_ref(self, dataset: str, source_ref: str, target_ref: str) -> None:
        source_snapshot = self.dataset_head(dataset=dataset, ref=source_ref)
        if not source_snapshot:
            raise ValueError(
                f"Source ref is empty or does not exist: {dataset}@{source_ref}"
            )
        self.set_dataset_ref(dataset=dataset, ref=target_ref, snapshot_id=source_snapshot)

    def list_dataset_files(self, dataset: str, ref: str = "main") -> list[str]:
        snapshot_id = self.dataset_head(dataset=dataset, ref=ref)
        if not snapshot_id:
            return []

        commit = self.get_commit(snapshot_id)
        if commit.tree.get("kind") != "snapshot":
            raise ValueError(
                f"Dataset ref does not point to a snapshot commit (found: {commit.tree.get('kind')})"
            )
        manifest_id = commit.tree.get("manifest")
        if not manifest_id:
            raise ValueError("Missing manifest id")

        root = SnapshotManifestRoot.model_validate(self.read_object_json(manifest_id))
        files: list[str] = []
        for part in root.parts:
            files.extend(self._load_manifest_part_names(part.object_id))
        return sorted(files)
