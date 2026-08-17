"""Collaborator services for ReflakeRepository.

These objects own cohesive slices of repository behavior:

- ``RefManager`` — branch refs, commit reads, and their caches.
- ``TreeWriter`` — bottom-up tree building, overlays, and commit writing.
- ``StagingArea`` — branch-scoped staging state, source expansion, and status.
- ``EntryFactory`` — entry materialization and canonical blob storage.
"""

from __future__ import annotations

from .entries import EntryFactory
from .refs import RefManager, _BoundedCache
from .staging import StagingArea
from .tree import TreeWriter

__all__ = [
    "EntryFactory",
    "RefManager",
    "StagingArea",
    "TreeWriter",
    "_BoundedCache",
]
