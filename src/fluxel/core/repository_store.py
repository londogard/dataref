from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from uuid import uuid4
from typing import BinaryIO, Iterator, Literal, Protocol

import boto3
from botocore.exceptions import ClientError

from .layout import blob_relpath, initialize_fluxel_layout
from .manifest import ManifestEntry, ManifestReader
from .storage import OptimisticLockError


RepositoryObjectKind = Literal["blob", "commit", "manifest", "ref"]


@dataclass(frozen=True)
class BranchRefState:
    branch: str
    commit_id: str | None
    version_token: str | None


class RepositoryStore(Protocol):
    def read_commit_bytes(self, commit_id: str) -> bytes | None: ...

    def write_commit_bytes(
        self,
        commit_id: str,
        payload: bytes,
        *,
        if_missing: bool = False,
    ) -> None: ...

    def iter_manifest_entries(self, manifest_hash: str) -> Iterator[ManifestEntry]: ...

    def write_manifest_file(
        self,
        manifest_hash: str,
        source_path: str | Path,
        *,
        if_missing: bool = False,
    ) -> None: ...

    def read_branch_ref(self, branch: str) -> BranchRefState | None: ...

    def write_branch_ref(self, branch: str, commit_id: str | None) -> None: ...

    def compare_and_set_branch_ref(
        self,
        branch: str,
        commit_id: str | None,
        *,
        expected_version_token: str | None,
    ) -> bool: ...

    def read_blob_bytes(self, blob_hash: str) -> bytes: ...

    def write_blob_file(
        self,
        blob_hash: str,
        source_path: str | Path,
        *,
        if_missing: bool = True,
    ) -> None: ...

    def object_exists(self, kind: RepositoryObjectKind, object_id: str) -> bool: ...

    def version_token(
        self, kind: RepositoryObjectKind, object_id: str
    ) -> str | None: ...


class LocalRepositoryStore:
    def __init__(self, root: str | Path) -> None:
        self.layout = initialize_fluxel_layout(root)

    def read_commit_bytes(self, commit_id: str) -> bytes | None:
        commit_path = self.commit_path(commit_id)
        if not commit_path.exists():
            return None
        return commit_path.read_bytes()

    def write_commit_bytes(
        self,
        commit_id: str,
        payload: bytes,
        *,
        if_missing: bool = False,
    ) -> None:
        commit_path = self.commit_path(commit_id)
        commit_path.parent.mkdir(parents=True, exist_ok=True)
        if if_missing and commit_path.exists():
            raise OptimisticLockError(f"Commit already exists: {commit_id}")
        commit_path.write_bytes(payload)

    def iter_manifest_entries(self, manifest_hash: str) -> Iterator[ManifestEntry]:
        yield from ManifestReader(self.manifest_path(manifest_hash)).iter_entries()

    def write_manifest_file(
        self,
        manifest_hash: str,
        source_path: str | Path,
        *,
        if_missing: bool = False,
    ) -> None:
        manifest_path = self.manifest_path(manifest_hash)
        source = Path(source_path)
        manifest_path.parent.mkdir(parents=True, exist_ok=True)
        if if_missing and manifest_path.exists():
            raise OptimisticLockError(f"Manifest already exists: {manifest_hash}")
        source.replace(manifest_path)

    def read_branch_ref(self, branch: str) -> BranchRefState | None:
        branch_path = self.branch_path(branch)
        if not branch_path.exists():
            return None
        commit_id = branch_path.read_text(encoding="utf-8").strip() or None
        return BranchRefState(
            branch=branch,
            commit_id=commit_id,
            version_token=self.version_token("ref", branch),
        )

    def write_branch_ref(self, branch: str, commit_id: str | None) -> None:
        branch_path = self.branch_path(branch)
        branch_path.parent.mkdir(parents=True, exist_ok=True)
        payload = f"{commit_id}\n" if commit_id else ""
        branch_path.write_text(payload, encoding="utf-8")

    def compare_and_set_branch_ref(
        self,
        branch: str,
        commit_id: str | None,
        *,
        expected_version_token: str | None,
    ) -> bool:
        current_token = self.version_token("ref", branch)
        if current_token != expected_version_token:
            return False
        self.write_branch_ref(branch, commit_id)
        return True

    def read_blob_bytes(self, blob_hash: str) -> bytes:
        return self.blob_path(blob_hash).read_bytes()

    def write_blob_file(
        self,
        blob_hash: str,
        source_path: str | Path,
        *,
        if_missing: bool = True,
    ) -> None:
        blob_path = self.blob_path(blob_hash)
        if if_missing and blob_path.exists():
            return
        blob_path.parent.mkdir(parents=True, exist_ok=True)
        with Path(source_path).open("rb") as src, blob_path.open("wb") as dst:
            while chunk := src.read(1024 * 1024):
                dst.write(chunk)

    def object_exists(self, kind: RepositoryObjectKind, object_id: str) -> bool:
        return self._path_for(kind, object_id).exists()

    def version_token(self, kind: RepositoryObjectKind, object_id: str) -> str | None:
        path = self._path_for(kind, object_id)
        if not path.exists():
            return None
        stat = path.stat()
        return f"{stat.st_mtime_ns}-{stat.st_size}"

    def blob_path(self, blob_hash: str) -> Path:
        return self.layout.blobs_dir / blob_relpath(blob_hash)

    def commit_path(self, commit_id: str) -> Path:
        return self.layout.commits_dir / f"{commit_id}.json"

    def manifest_path(self, manifest_hash: str) -> Path:
        return self.layout.manifests_dir / f"{manifest_hash}.jsonl"

    def branch_path(self, branch: str) -> Path:
        return self.layout.heads_dir / branch

    def _path_for(self, kind: RepositoryObjectKind, object_id: str) -> Path:
        if kind == "blob":
            return self.blob_path(object_id)
        if kind == "commit":
            return self.commit_path(object_id)
        if kind == "manifest":
            return self.manifest_path(object_id)
        return self.branch_path(object_id)


class S3RepositoryStore:
    def __init__(
        self,
        bucket: str,
        prefix: str = "",
        *,
        client: object | None = None,
    ) -> None:
        self.bucket = bucket
        self.prefix = prefix.strip("/")
        self.client = client or boto3.client("s3")

    def read_commit_bytes(self, commit_id: str) -> bytes | None:
        try:
            response = self.client.get_object(
                Bucket=self.bucket,
                Key=self._key("commit", commit_id),
            )
        except ClientError as error:
            if self._missing(error):
                return None
            raise
        return response["Body"].read()

    def write_commit_bytes(
        self,
        commit_id: str,
        payload: bytes,
        *,
        if_missing: bool = False,
    ) -> None:
        kwargs = {
            "Bucket": self.bucket,
            "Key": self._key("commit", commit_id),
            "Body": payload,
        }
        if if_missing:
            kwargs["IfNoneMatch"] = "*"
        try:
            self.client.put_object(**kwargs)
        except ClientError as error:
            if if_missing and self._precondition_failed(error):
                raise OptimisticLockError(
                    f"Commit already exists: {commit_id}"
                ) from error
            raise

    def iter_manifest_entries(self, manifest_hash: str) -> Iterator[ManifestEntry]:
        try:
            response = self.client.get_object(
                Bucket=self.bucket,
                Key=self._key("manifest", manifest_hash),
            )
        except ClientError as error:
            if self._missing(error):
                return
            raise
        body = response["Body"]
        try:
            for raw_line in body.iter_lines():
                line = raw_line.decode("utf-8").strip()
                if not line:
                    continue
                yield ManifestEntry.from_dict(json.loads(line))
        finally:
            body.close()

    def write_manifest_file(
        self,
        manifest_hash: str,
        source_path: str | Path,
        *,
        if_missing: bool = False,
    ) -> None:
        with Path(source_path).open("rb") as handle:
            self._put_stream(
                key=self._key("manifest", manifest_hash),
                body=handle,
                if_missing=if_missing,
                error_message=f"Manifest already exists: {manifest_hash}",
            )

    def read_branch_ref(self, branch: str) -> BranchRefState | None:
        key = self._key("ref", branch)
        try:
            response = self.client.get_object(Bucket=self.bucket, Key=key)
        except ClientError as error:
            if self._missing(error):
                return None
            raise
        commit_id = response["Body"].read().decode("utf-8").strip() or None
        return BranchRefState(
            branch=branch,
            commit_id=commit_id,
            version_token=response.get("ETag", "").strip('"') or None,
        )

    def write_branch_ref(self, branch: str, commit_id: str | None) -> None:
        payload = f"{commit_id}\n".encode("utf-8") if commit_id else b""
        self.client.put_object(
            Bucket=self.bucket,
            Key=self._key("ref", branch),
            Body=payload,
        )

    def compare_and_set_branch_ref(
        self,
        branch: str,
        commit_id: str | None,
        *,
        expected_version_token: str | None,
    ) -> bool:
        lock_token = str(uuid4())
        if not self._acquire_branch_lock(branch, lock_token):
            return False
        try:
            current = self.read_branch_ref(branch)
            current_version = current.version_token if current else None
            if current_version != expected_version_token:
                return False
            self.write_branch_ref(branch, commit_id)
            return True
        finally:
            self._release_branch_lock(branch, lock_token)

    def read_blob_bytes(self, blob_hash: str) -> bytes:
        response = self.client.get_object(
            Bucket=self.bucket,
            Key=self._key("blob", blob_hash),
        )
        return response["Body"].read()

    def write_blob_file(
        self,
        blob_hash: str,
        source_path: str | Path,
        *,
        if_missing: bool = True,
    ) -> None:
        with Path(source_path).open("rb") as handle:
            self._put_stream(
                key=self._key("blob", blob_hash),
                body=handle,
                if_missing=if_missing,
                error_message=f"Blob already exists: {blob_hash}",
            )

    def object_exists(self, kind: RepositoryObjectKind, object_id: str) -> bool:
        try:
            self.client.head_object(Bucket=self.bucket, Key=self._key(kind, object_id))
            return True
        except ClientError as error:
            if self._missing(error):
                return False
            raise

    def version_token(self, kind: RepositoryObjectKind, object_id: str) -> str | None:
        try:
            response = self.client.head_object(
                Bucket=self.bucket,
                Key=self._key(kind, object_id),
            )
        except ClientError as error:
            if self._missing(error):
                return None
            raise
        return response.get("ETag", "").strip('"') or None

    def _key(self, kind: RepositoryObjectKind, object_id: str) -> str:
        relative_path = self._relative_path(kind, object_id)
        if self.prefix:
            return f"{self.prefix}/{relative_path}"
        return relative_path

    def _relative_path(self, kind: RepositoryObjectKind, object_id: str) -> str:
        if kind == "blob":
            return f"blobs/{blob_relpath(object_id).as_posix()}"
        if kind == "commit":
            return f"commits/{object_id}.json"
        if kind == "manifest":
            return f"manifests/{object_id}.jsonl"
        return f"refs/heads/{object_id}"

    def _lock_key(self, branch: str) -> str:
        suffix = f"locks/refs/heads/{branch}.lock"
        if self.prefix:
            return f"{self.prefix}/{suffix}"
        return suffix

    def _acquire_branch_lock(self, branch: str, token: str) -> bool:
        try:
            self.client.put_object(
                Bucket=self.bucket,
                Key=self._lock_key(branch),
                Body=token.encode("utf-8"),
                IfNoneMatch="*",
            )
        except ClientError as error:
            if self._precondition_failed(error):
                return False
            raise
        return True

    def _release_branch_lock(self, branch: str, token: str) -> None:
        try:
            response = self.client.get_object(
                Bucket=self.bucket, Key=self._lock_key(branch)
            )
        except ClientError as error:
            if self._missing(error):
                return
            raise
        body = response["Body"]
        try:
            current_token = body.read().decode("utf-8")
        finally:
            body.close()
        if current_token != token:
            return
        self.client.delete_object(Bucket=self.bucket, Key=self._lock_key(branch))

    def _put_stream(
        self,
        *,
        key: str,
        body: BinaryIO,
        if_missing: bool,
        error_message: str,
    ) -> None:
        kwargs = {
            "Bucket": self.bucket,
            "Key": key,
            "Body": body,
        }
        if if_missing:
            kwargs["IfNoneMatch"] = "*"
        try:
            self.client.put_object(**kwargs)
        except ClientError as error:
            if if_missing and self._precondition_failed(error):
                raise OptimisticLockError(error_message) from error
            raise

    def _missing(self, error: ClientError) -> bool:
        code = error.response.get("Error", {}).get("Code", "")
        return code in {"404", "NoSuchKey", "NotFound"}

    def _precondition_failed(self, error: ClientError) -> bool:
        code = error.response.get("Error", {}).get("Code", "")
        return code in {"PreconditionFailed", "412"}
