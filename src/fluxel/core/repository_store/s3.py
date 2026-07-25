from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any, BinaryIO, Iterator
from uuid import uuid4

import boto3
from botocore.exceptions import ClientError

from ..domain import (
    DEFAULT_BRANCH_LOCK_TIMEOUT_SECONDS,
    BranchLockState,
    BranchRefState,
    OptimisticLockError,
    RepositoryObjectKind,
)
from ..layout import blob_relpath
from ..manifest import ManifestEntry
from ..manifest_index import (
    ManifestIndex,
    iter_manifest_index_entry_jsons,
    lookup_manifest_index_entry_json,
)
from ..storage import BlobTransferBackend, _s3_is_404, _s3_is_precondition_failed


class S3RepositoryStore:
    def __init__(
        self,
        bucket: str,
        prefix: str = "",
        *,
        client: Any | None = None,
        branch_root: str | Path | None = None,
        lock_timeout_seconds: int = DEFAULT_BRANCH_LOCK_TIMEOUT_SECONDS,
        blob_transfer: BlobTransferBackend | None = None,
    ) -> None:
        self.bucket = bucket
        self.prefix = prefix.strip("/")
        self.client = client or boto3.client("s3")
        self.branch_root = Path(branch_root).resolve() if branch_root else None
        self.lock_timeout_seconds = max(1, lock_timeout_seconds)
        self._manifest_index_cache: dict[str, Path] = {}
        self._blob_transfer = blob_transfer

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
            manifest_uri = f"s3://{self.bucket}/{self._key('manifest', manifest_hash)}"
            for line_number, raw_line in enumerate(body.iter_lines(), start=1):
                line = raw_line.decode("utf-8").strip()
                if not line:
                    continue
                try:
                    yield ManifestEntry.deserialize(line)
                except ValueError as error:
                    raise ValueError(
                        f"Invalid manifest entry at line {line_number} in {manifest_uri}: {error}"
                    ) from error
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

    def write_manifest_index_file(
        self,
        manifest_hash: str,
        source_path: str | Path,
        *,
        if_missing: bool = False,
    ) -> None:
        with Path(source_path).open("rb") as handle:
            self._put_stream(
                key=self._key("manifest-index", manifest_hash),
                body=handle,
                if_missing=if_missing,
                error_message=f"Manifest index already exists: {manifest_hash}",
            )

    def read_manifest_index_bytes(self, manifest_hash: str) -> bytes | None:
        cached = self._cached_manifest_index_path(manifest_hash)
        if cached is not None:
            return cached.read_bytes()
        try:
            response = self.client.get_object(
                Bucket=self.bucket,
                Key=self._key("manifest-index", manifest_hash),
            )
        except ClientError as error:
            if self._missing(error):
                return None
            raise
        return response["Body"].read()

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

    def branch_path(self, branch: str) -> Path:
        if self.branch_root is not None:
            return self.branch_root / branch
        return Path(".fluxel") / "refs" / "heads" / branch

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
        expected_commit_id: str | None = None,
    ) -> bool:
        lock_token = str(uuid4())
        if not self._acquire_branch_lock(branch, lock_token):
            return False
        try:
            current = self.read_branch_ref(branch)
            current_version = current.version_token if current else None
            if current_version != expected_version_token:
                return False
            current_commit_id = current.commit_id if current else None
            if current_commit_id != expected_commit_id:
                return False
            self.write_branch_ref(branch, commit_id)
            return True
        finally:
            self._release_branch_lock(branch, lock_token)

    def _blob_uri(self, blob_hash: str) -> str:
        return f"s3://{self.bucket}/{self._key('blob', blob_hash)}"

    # ── Branch lock inspection / cleanup ─────────────────────────────────

    def list_branch_locks(self) -> dict[str, BranchLockState]:
        """Return a dict mapping branch name → lock state for every active lock.

        Only available on S3-backed stores (locks are S3 objects under the
        ``locks/refs/heads/`` prefix).  Local stores always return an empty
        dict.
        """
        lock_prefix = self._lock_prefix()
        locks: dict[str, BranchLockState] = {}
        paginator = self.client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self.bucket, Prefix=lock_prefix):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                branch = self._branch_from_lock_key(key)
                if branch is None:
                    continue
                lock_state = self._read_branch_lock(branch)
                if lock_state is not None:
                    locks[branch] = lock_state
        return locks

    def branch_lock_info(self, branch: str) -> BranchLockState | None:
        """Return lock state for *branch*, or ``None`` if it is not locked."""
        return self._read_branch_lock(branch)

    def force_release_branch_lock(self, branch: str) -> bool:
        """Delete the lock object for *branch* regardless of token ownership.

        Returns ``True`` if a lock existed and was released, ``False`` if no
        lock was present.
        """
        key = self._lock_key(branch)
        try:
            self.client.head_object(Bucket=self.bucket, Key=key)
        except ClientError as error:
            if self._missing(error):
                return False
            raise
        self.client.delete_object(Bucket=self.bucket, Key=key)
        return True

    # ── Blob operations ───────────────────────────────────────────────────

    def read_blob_bytes(self, blob_hash: str) -> bytes:
        if self._blob_transfer is not None:
            with NamedTemporaryFile(delete=False) as tmp:
                tmp_path = tmp.name
            try:
                self._blob_transfer.download(self._blob_uri(blob_hash), tmp_path)
                return Path(tmp_path).read_bytes()
            finally:
                Path(tmp_path).unlink(missing_ok=True)
        response = self.client.get_object(
            Bucket=self.bucket,
            Key=self._key("blob", blob_hash),
        )
        return response["Body"].read()

    def open_blob(self, blob_hash: str) -> BinaryIO:
        response = self.client.get_object(
            Bucket=self.bucket,
            Key=self._key("blob", blob_hash),
        )
        return response["Body"]

    def lookup_manifest_entry(
        self,
        manifest_hash: str,
        logical_path: str,
        *,
        manifest_index: ManifestIndex | None = None,
    ) -> ManifestEntry | None:
        if manifest_index is not None:
            entry_json = lookup_manifest_index_entry_json(
                logical_path,
                read_range=lambda start, end: self._read_remote_manifest_range(
                    manifest_hash, start, end
                ),
                index=manifest_index,
            )
        else:
            index_path = self._cached_manifest_index_path(manifest_hash)
            if index_path is None:
                raise FileNotFoundError(f"Missing manifest index for: {manifest_hash}")
            entry_json = lookup_manifest_index_entry_json(
                logical_path,
                read_range=lambda start, end: self._read_remote_manifest_range(
                    manifest_hash, start, end
                ),
                index_path=index_path,
            )
        if entry_json is None:
            return None
        return ManifestEntry.deserialize(entry_json)

    def iter_manifest_entries_for_prefix(
        self,
        manifest_hash: str,
        logical_prefix: str,
        *,
        manifest_index: ManifestIndex | None = None,
    ) -> Iterator[ManifestEntry]:
        normalized_prefix = logical_prefix.strip("/")
        if manifest_index is not None:
            iter_jsons = iter_manifest_index_entry_jsons(
                logical_prefix=normalized_prefix or None,
                read_range=lambda start, end: self._read_remote_manifest_range(
                    manifest_hash, start, end
                ),
                index=manifest_index,
            )
        else:
            index_path = self._cached_manifest_index_path(manifest_hash)
            if index_path is None:
                raise FileNotFoundError(f"Missing manifest index for: {manifest_hash}")
            iter_jsons = iter_manifest_index_entry_jsons(
                logical_prefix=normalized_prefix or None,
                read_range=lambda start, end: self._read_remote_manifest_range(
                    manifest_hash, start, end
                ),
                index_path=index_path,
            )
        for entry_json in iter_jsons:
            yield ManifestEntry.deserialize(entry_json)

    def write_blob_file(
        self,
        blob_hash: str,
        source_path: str | Path,
        *,
        if_missing: bool = True,
    ) -> None:
        if self._blob_transfer is not None:
            if if_missing and self.object_exists("blob", blob_hash):
                return
            self._blob_transfer.upload(
                str(source_path),
                self._blob_uri(blob_hash),
                if_not_exists=if_missing,
            )
            return
        with Path(source_path).open("rb") as handle:
            try:
                self._put_stream(
                    key=self._key("blob", blob_hash),
                    body=handle,
                    if_missing=if_missing,
                    error_message=f"Blob already exists: {blob_hash}",
                )
            except OptimisticLockError:
                if if_missing:
                    return
                raise

    def write_blob_stream(
        self,
        blob_hash: str,
        source: BinaryIO,
        *,
        if_missing: bool = True,
    ) -> None:
        try:
            self._put_stream(
                key=self._key("blob", blob_hash),
                body=source,
                if_missing=if_missing,
                error_message=f"Blob already exists: {blob_hash}",
            )
        except OptimisticLockError:
            if if_missing:
                return
            raise

    def object_exists(self, kind: RepositoryObjectKind, object_id: str) -> bool:
        if kind == "blob" and self._blob_transfer is not None:
            return self._blob_transfer.exists(self._blob_uri(object_id))
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
        if kind == "manifest-index":
            return f"manifests/{object_id}.idx"
        return f"refs/heads/{object_id}"

    def _cached_manifest_index_path(self, manifest_hash: str) -> Path | None:
        cached_path = self._manifest_index_cache.get(manifest_hash)
        if cached_path is not None and cached_path.exists():
            return cached_path

        try:
            response = self.client.get_object(
                Bucket=self.bucket,
                Key=self._key("manifest-index", manifest_hash),
            )
        except ClientError as error:
            if self._missing(error):
                return None
            raise

        with NamedTemporaryFile(mode="wb", suffix=".idx", delete=False) as temp:
            temp_path = Path(temp.name)
            temp.write(response["Body"].read())
        self._manifest_index_cache[manifest_hash] = temp_path
        return temp_path

    def _read_remote_manifest_range(
        self, manifest_hash: str, start: int, end: int
    ) -> bytes:
        if end <= start:
            return b""
        response = self.client.get_object(
            Bucket=self.bucket,
            Key=self._key("manifest", manifest_hash),
            Range=f"bytes={start}-{end - 1}",
        )
        body = response["Body"]
        try:
            return body.read()
        finally:
            body.close()

    def _lock_prefix(self) -> str:
        suffix = "locks/refs/heads/"
        if self.prefix:
            return f"{self.prefix}/{suffix}"
        return suffix

    def _branch_from_lock_key(self, key: str) -> str | None:
        lock_prefix = self._lock_prefix()
        if not key.startswith(lock_prefix) or not key.endswith(".lock"):
            return None
        relative = key[len(lock_prefix):]
        if relative.endswith(".lock"):
            relative = relative[: -len(".lock")]
        return relative or None

    def _lock_key(self, branch: str) -> str:
        suffix = f"locks/refs/heads/{branch}.lock"
        if self.prefix:
            return f"{self.prefix}/{suffix}"
        return suffix

    def _acquire_branch_lock(self, branch: str, token: str) -> bool:
        if self._try_acquire_branch_lock(branch, token):
            return True

        current_lock = self._read_branch_lock(branch)
        if current_lock is None or not self._is_stale_branch_lock(current_lock):
            return False

        self._release_branch_lock(branch, current_lock.token)
        return self._try_acquire_branch_lock(branch, token)

    def _try_acquire_branch_lock(self, branch: str, token: str) -> bool:
        expires_at = datetime.now(timezone.utc) + timedelta(
            seconds=self.lock_timeout_seconds
        )
        payload = json.dumps(
            {
                "token": token,
                "expires_at": expires_at.isoformat(),
            },
            sort_keys=True,
        ).encode("utf-8")
        try:
            self.client.put_object(
                Bucket=self.bucket,
                Key=self._lock_key(branch),
                Body=payload,
                IfNoneMatch="*",
            )
        except ClientError as error:
            if self._precondition_failed(error):
                return False
            raise
        return True

    def _read_branch_lock(self, branch: str) -> BranchLockState | None:
        try:
            response = self.client.get_object(
                Bucket=self.bucket,
                Key=self._lock_key(branch),
            )
        except ClientError as error:
            if self._missing(error):
                return None
            raise

        body = response["Body"]
        try:
            raw_payload = body.read().decode("utf-8")
        finally:
            body.close()

        last_modified_raw = response.get("LastModified")
        last_modified: datetime | None = None
        if isinstance(last_modified_raw, datetime):
            if last_modified_raw.tzinfo is None:
                last_modified = last_modified_raw.replace(tzinfo=timezone.utc)
            else:
                last_modified = last_modified_raw.astimezone(timezone.utc)

        try:
            payload = json.loads(raw_payload)
        except json.JSONDecodeError:
            payload = None

        if isinstance(payload, dict):
            token = str(payload.get("token") or "").strip()
            expires_at_raw = payload.get("expires_at")
            expires_at: datetime | None = None
            if isinstance(expires_at_raw, str) and expires_at_raw:
                expires_at = datetime.fromisoformat(expires_at_raw)
                if expires_at.tzinfo is None:
                    expires_at = expires_at.replace(tzinfo=timezone.utc)
                else:
                    expires_at = expires_at.astimezone(timezone.utc)
            if token:
                return BranchLockState(
                    token=token,
                    expires_at=expires_at,
                    last_modified=last_modified,
                )

        token = raw_payload.strip()
        if not token:
            return None
        return BranchLockState(
            token=token,
            expires_at=None,
            last_modified=last_modified,
        )

    def _is_stale_branch_lock(self, lock_state: BranchLockState) -> bool:
        now = datetime.now(timezone.utc)
        if lock_state.expires_at is not None:
            return lock_state.expires_at <= now
        if lock_state.last_modified is None:
            return False
        return (
            lock_state.last_modified + timedelta(seconds=self.lock_timeout_seconds)
            <= now
        )

    def _release_branch_lock(self, branch: str, token: str) -> None:
        current_lock = self._read_branch_lock(branch)
        if current_lock is None or current_lock.token != token:
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
        return _s3_is_404(error)

    def _precondition_failed(self, error: ClientError) -> bool:
        return _s3_is_precondition_failed(error)
