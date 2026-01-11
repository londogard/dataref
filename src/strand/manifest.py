from __future__ import annotations

from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field


class ManifestEntry(BaseModel):
    name: str = Field(description="Logical name (path relative to dataset root)")
    s3_path: str = Field(description="Physical path to open (e.g. s3://bucket/key)")
    size: int
    mtime_ms: Optional[int] = Field(default=None, description="mtime as epoch milliseconds")
    etag: Optional[str] = None


class ManifestPartitioning(BaseModel):
    algorithm: str = Field(default="sha256_mod", description="Deterministic partitioning algorithm")
    buckets: int = Field(default=256, description="Number of hash buckets")


class ManifestPartRef(BaseModel):
    bucket: int
    object_id: str = Field(description="Object id of Parquet part in .strand/objects")
    rows: int


class SnapshotManifestRoot(BaseModel):
    version: int = 2
    dataset_root: str
    created_at: datetime
    partitioning: ManifestPartitioning = Field(default_factory=ManifestPartitioning)
    parts: list[ManifestPartRef]
