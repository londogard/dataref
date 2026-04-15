# Fluxel Guardrails (Permanent):
# - DO NOT optimize for ML throughput in canonical storage (`blobs/`): no sharding, tarballs, or parquet in the blob layer.
# - DO NOT read blob data for metadata-only operations (diff, list, log, status).
# - DO NOT introduce a server, daemon, or central database; Fluxel is 100% client-side/serverless.
# - DO NOT use SHA-1/SHA-256; use Blake3 for all content hashing.
# - PREFER JSONL manifests to preserve streaming and O(1) memory usage.

from __future__ import annotations

import csv
from dataclasses import dataclass
from pathlib import Path
from tempfile import NamedTemporaryFile

import duckdb

from .repository import open_repository


@dataclass(frozen=True)
class AnalyticalIndexPaths:
    database_path: Path
    parquet_path: Path | None


def build_analytical_index(
    root: str | Path,
    ref: str = "main",
    *,
    output_dir: str | Path | None = None,
    export_parquet: bool = False,
) -> AnalyticalIndexPaths:
    repo = open_repository(root)
    commit_id = repo.resolve_ref(ref)
    commit = repo.read_commit(commit_id)

    index_root = (
        Path(output_dir).resolve()
        if output_dir
        else (repo.client_state.fluxel_dir / "index")
    )
    index_root.mkdir(parents=True, exist_ok=True)
    db_path = index_root / f"{commit_id}.duckdb"

    with NamedTemporaryFile(
        mode="w", suffix=".csv", delete=False, encoding="utf-8", newline=""
    ) as temp:
        manifest_path = Path(temp.name)
    try:
        with manifest_path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.writer(handle)
            writer.writerow(["path", "hash", "size", "mtime_ns", "commit_id", "branch"])
            for entry in repo.store.iter_manifest_entries(commit.manifest):
                writer.writerow(
                    [
                        entry.path,
                        entry.hash,
                        entry.size,
                        entry.mtime_ns,
                        commit_id,
                        commit.branch,
                    ]
                )

        conn = duckdb.connect(str(db_path))
        try:
            conn.execute(
                """
                CREATE OR REPLACE TABLE files AS
                SELECT
                    path::VARCHAR AS path,
                    hash::VARCHAR AS hash,
                    size::BIGINT AS size,
                    mtime_ns::BIGINT AS mtime_ns,
                    commit_id::VARCHAR AS commit_id,
                    branch::VARCHAR AS branch
                FROM read_csv_auto(?, header=true)
                """,
                [str(manifest_path)],
            )
            conn.execute("CREATE INDEX IF NOT EXISTS idx_files_path ON files(path)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_files_size ON files(size)")
        finally:
            conn.close()
    finally:
        manifest_path.unlink(missing_ok=True)

    parquet_path: Path | None = None
    if export_parquet:
        parquet_path = index_root / f"{commit_id}.parquet"
        conn = duckdb.connect(str(db_path))
        try:
            conn.execute("COPY files TO ? (FORMAT PARQUET)", [str(parquet_path)])
        finally:
            conn.close()

    return AnalyticalIndexPaths(database_path=db_path, parquet_path=parquet_path)


def query_analytical_index(
    database_path: str | Path, query: str
) -> list[tuple[object, ...]]:
    db_path = Path(database_path).resolve()
    if not db_path.exists():
        raise FileNotFoundError(f"Index database not found: {db_path}")
    conn = duckdb.connect(str(db_path), read_only=True)
    try:
        return conn.execute(query).fetchall()
    finally:
        conn.close()


def drop_analytical_index(database_path: str | Path) -> None:
    db_path = Path(database_path).resolve()
    db_path.unlink(missing_ok=True)
