"""Storage backends and utilities for fluxel.core.

This package replaces the former ``storage.py`` module.  All public names are
re-exported here so that existing import paths remain fully backwards-compatible.

The ``boto3`` name is explicitly imported and re-exposed so that test fixtures
that monkeypatch ``fluxel.core.storage.boto3.client`` continue to work.
"""

from __future__ import annotations

import boto3  # noqa: F401  – kept for monkeypatching in tests

from ..domain import OptimisticLockError  # noqa: F401  – re-exported for callers

from .base import (
    BlobTransferBackend,
    S3ObjectMetadata,
    SourceObjectMetadata,
    StorageBackend,
)
from .local import LocalStorageBackend
from .s3 import (
    S3StorageBackend,
    _s3_is_404,
    _s3_is_precondition_failed,
    describe_source_uri,
    iter_s3_objects,
    open_source_uri,
    parse_s3_uri,
)
from .transfer import (
    S3BlobTransferBackend,
    S5CmdBlobTransferBackend,
    build_blob_transfer_backend,
)

__all__ = [
    "BlobTransferBackend",
    "LocalStorageBackend",
    "OptimisticLockError",
    "S3BlobTransferBackend",
    "S3ObjectMetadata",
    "S3StorageBackend",
    "S5CmdBlobTransferBackend",
    "SourceObjectMetadata",
    "StorageBackend",
    "build_blob_transfer_backend",
    "describe_source_uri",
    "iter_s3_objects",
    "open_source_uri",
    "parse_s3_uri",
    # Private helpers re-exported for repository_store compatibility
    "_s3_is_404",
    "_s3_is_precondition_failed",
]
