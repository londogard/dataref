"""Repository store backends for fluxel.core.

This package replaces the former ``repository_store.py`` module.  All public
names are re-exported here so that existing import paths remain fully
backwards-compatible.

The ``boto3`` name is explicitly imported and re-exposed so that test fixtures
that monkeypatch ``fluxel.core.repository_store.boto3.client`` continue to
work.
"""

from __future__ import annotations

import boto3  # noqa: F401  – kept for monkeypatching in tests

from ..domain import BranchLockState, BranchRefState  # noqa: F401 – re-exported for callers

from .base import RepositoryStore
from .local import LocalRepositoryStore
from .s3 import S3RepositoryStore
from .utils import build_manifest_index_file

__all__ = [
    "BranchLockState",
    "BranchRefState",
    "LocalRepositoryStore",
    "RepositoryStore",
    "S3RepositoryStore",
    "build_manifest_index_file",
]

