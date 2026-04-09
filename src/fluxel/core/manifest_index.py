from __future__ import annotations

import json
import sqlite3
from json import JSONDecodeError
from pathlib import Path
from typing import Iterator


def build_manifest_index(manifest_path: str | Path, index_path: str | Path) -> None:
    manifest = Path(manifest_path)
    index = Path(index_path)
    index.parent.mkdir(parents=True, exist_ok=True)
    index.unlink(missing_ok=True)

    connection = sqlite3.connect(index)
    try:
        connection.execute("PRAGMA journal_mode=OFF")
        connection.execute("PRAGMA synchronous=OFF")
        connection.execute("PRAGMA temp_store=MEMORY")
        connection.execute(
            """
            CREATE TABLE entries (
                path TEXT PRIMARY KEY,
                entry_json TEXT NOT NULL
            ) WITHOUT ROWID
            """
        )

        batch: list[tuple[str, str]] = []
        with manifest.open("rb") as handle:
            for raw_line in handle:
                stripped = raw_line.strip()
                if stripped:
                    entry_json = stripped.decode("utf-8")
                    entry = json.loads(entry_json)
                    batch.append((str(entry["path"]), entry_json))
                    if len(batch) >= 1_000:
                        connection.executemany(
                            "INSERT INTO entries(path, entry_json) VALUES (?, ?)",
                            batch,
                        )
                        batch.clear()

        if batch:
            connection.executemany(
                "INSERT INTO entries(path, entry_json) VALUES (?, ?)",
                batch,
            )
        connection.commit()
    finally:
        connection.close()


def lookup_manifest_index_entry_json(
    index_path: str | Path, logical_path: str
) -> str | None:
    connection = sqlite3.connect(Path(index_path))
    try:
        row = connection.execute(
            "SELECT entry_json FROM entries WHERE path = ?",
            [logical_path],
        ).fetchone()
    finally:
        connection.close()

    if row is None:
        return None
    return str(row[0])


def iter_manifest_index_entry_jsons(
    index_path: str | Path, logical_prefix: str | None = None
) -> Iterator[str]:
    connection = sqlite3.connect(Path(index_path))
    try:
        if not logical_prefix:
            cursor = connection.execute("SELECT entry_json FROM entries ORDER BY path")
        else:
            descendant_prefix = f"{logical_prefix.rstrip('/')}/"
            cursor = connection.execute(
                """
                SELECT entry_json
                FROM entries
                WHERE path = ? OR (path >= ? AND path < ?)
                ORDER BY path
                """,
                [
                    logical_prefix,
                    descendant_prefix,
                    _prefix_upper_bound(descendant_prefix),
                ],
            )
        for row in cursor:
            yield str(row[0])
    finally:
        connection.close()


def parse_manifest_index_entry_json(entry_json: str) -> dict[str, object]:
    try:
        payload = json.loads(entry_json)
    except JSONDecodeError as error:
        raise ValueError("Corrupt manifest index entry payload") from error
    if not isinstance(payload, dict):
        raise ValueError("Manifest index entry payload must be an object")
    return payload


def _prefix_upper_bound(prefix: str) -> str:
    return f"{prefix}\U0010ffff"
