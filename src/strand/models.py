from __future__ import annotations

from datetime import datetime, timezone
from typing import Literal, Optional

from pydantic import BaseModel, Field


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


class RepoConfig(BaseModel):
    root: str = Field(
        description="Repository root URI. Supports s3://bucket/prefix or local paths via fsspec."
    )
    strand_dir: str = Field(
        default=".strand", description="Metadata directory under root"
    )


class Commit(BaseModel):
    version: int = 1
    message: str
    author: Optional[str] = None
    created_at: datetime = Field(default_factory=utc_now)

    parent: Optional[str] = None
    tree: dict = Field(
        default_factory=dict,
        description="A content-addressed snapshot (implementation-specific).",
    )


class Ref(BaseModel):
    kind: Literal["heads"] = "heads"
    name: str
    commit: Optional[str] = None
