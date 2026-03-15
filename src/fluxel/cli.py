from __future__ import annotations

import json
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

from simple_parsing import ArgumentParser
from simple_parsing.helpers import field, flag, subparsers

from .core import (
    FluxelRepository,
    add,
    build_analytical_index,
    commit,
    drop_analytical_index,
    import_s3,
    query_analytical_index,
    rm,
    status,
    verify,
)


IdentityMode = Literal["blake3", "meta"]


@dataclass
class CommitArgs:
    message: str = field(alias=["-m", "--message"], help="Commit message")
    identity: IdentityMode = "blake3"  # Identity strategy: full-content blake3 or metadata hash(path+size)"
    root: str = "."  # "Dataset root path"
    staged: bool = flag(
        False,
        help="Commit only staged changes for a branch",
    )
    ref: str | None = None  # Branch ref to update (defaults to current branch)


@dataclass
class ImportArgs:
    source: str = field(positional=True, help="S3 URI to import, e.g. s3://bucket/prefix")
    message: str = field(alias=["-m", "--message"], help="Commit message")
    identity: IdentityMode = "blake3"  # Identity strategy: full-content blake3 or metadata hash(path+size)"
    path: list[str] = field(
        default_factory=list,
        alias="--path",
        action="append",
        help="Optional relative path/glob filter (repeatable)",
    )
    root: str = "."  # Dataset root path
    ref: str | None = None  # Branch ref to update (defaults to current branch)


@dataclass
class AddArgs:
    paths: list[str] = field(positional=True, nargs="+", help="Paths to stage")
    identity: IdentityMode = "blake3"  # Identity strategy for staged additions
    root: str = field(default=".", help="Dataset root path")
    ref: str | None = field(
        default=None,
        help="Branch ref for staging (defaults to current branch)",
    )


@dataclass
class RmArgs:
    paths: list[str] = field(positional=True, nargs="+", help="Paths to remove")
    root: str = field(default=".", help="Dataset root path")
    ref: str | None = field(
        default=None,
        help="Branch ref for staging (defaults to current branch)",
    )


@dataclass
class StatusArgs:
    root: str = field(default=".", help="Dataset root path")
    ref: str | None = field(
        default=None,
        help="Branch ref for staging (defaults to current branch)",
    )


@dataclass
class BranchArgs:
    name: str = field(positional=True, help="Branch name")
    root: str = field(default=".", help="Dataset root path")


@dataclass
class DiffArgs:
    from_ref: str = field(positional=True, help="Source ref (branch or commit)")
    to_ref: str = field(positional=True, help="Target ref (branch or commit)")
    root: str = "."  # Dataset root path


@dataclass
class VerifyArgs:
    root: str = "."  # Dataset root path
    ref: str = "main"  # Branch ref to verify
    path: list[str] = field(
        default_factory=list,
        alias="--path",
        action="append",
        help="Optional path/prefix filter (repeatable)",
    )
    dry_run: bool = flag(
        False,
        alias="--dry-run",
        help="Report entries that would be verified without writing blobs or commits",
    )


@dataclass
class IndexBuildArgs:
    root: str = "."  # Dataset root path
    ref: str = "main"  # Ref to index
    output_dir: str | None = field(
        default=None,
        alias="--output-dir",
        help="Output directory for index",
    )
    parquet: bool = flag(
        False,
        help="Also export Parquet from the index table",
    )


@dataclass
class IndexQueryArgs:
    db: str  # Path to DuckDB index file
    sql: str  # SQL query to execute


@dataclass
class IndexDropArgs:
    db: str  # Path to DuckDB index file


@dataclass
class IndexArgs:
    command: IndexBuildArgs | IndexQueryArgs | IndexDropArgs = subparsers(
        {
            "build": IndexBuildArgs,
            "query": IndexQueryArgs,
            "drop": IndexDropArgs,
        }
    )


@dataclass
class FluxelCLI:
    command: (
        CommitArgs
        | ImportArgs
        | AddArgs
        | RmArgs
        | StatusArgs
        | BranchArgs
        | DiffArgs
        | VerifyArgs
        | IndexArgs
    ) = subparsers(
        {
            "commit": CommitArgs,
            "import": ImportArgs,
            "add": AddArgs,
            "rm": RmArgs,
            "status": StatusArgs,
            "branch": BranchArgs,
            "diff": DiffArgs,
            "verify": VerifyArgs,
            "index": IndexArgs,
        }
    )


def build_parser() -> ArgumentParser:
    parser = ArgumentParser(prog="fluxel", description="Fluxel CLI")
    parser.add_arguments(FluxelCLI, dest="cli")
    return parser


def _stage_payload(stage: object) -> dict[str, object]:
    return {
        "ref": stage.ref,
        "added": stage.added,
        "removed": stage.removed,
    }


def _flatten_option_values(values: list[object]) -> list[str]:
    flattened: list[str] = []
    for value in values:
        if isinstance(value, list):
            flattened.extend(str(item) for item in value)
            continue
        flattened.append(str(value))
    return flattened


def run_cli(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv).cli
    command = args.command

    if isinstance(command, CommitArgs):
        commit_id = commit(
            Path(command.root),
            command.message,
            identity_mode=command.identity,
            staged=command.staged,
            ref=command.ref,
        )
        print(commit_id)
        return 0

    if isinstance(command, ImportArgs):
        commit_id = import_s3(
            Path(command.root),
            command.source,
            command.message,
            identity_mode=command.identity,
            path_patterns=_flatten_option_values(command.path),
            ref=command.ref,
        )
        print(commit_id)
        return 0

    if isinstance(command, AddArgs):
        stage = add(
            root=Path(command.root),
            paths=command.paths,
            ref=command.ref,
            identity_mode=command.identity,
        )
        print(json.dumps(_stage_payload(stage), indent=2))
        return 0

    if isinstance(command, RmArgs):
        stage = rm(
            root=Path(command.root),
            paths=command.paths,
            ref=command.ref,
        )
        print(json.dumps(_stage_payload(stage), indent=2))
        return 0

    if isinstance(command, StatusArgs):
        stage = status(
            root=Path(command.root),
            ref=command.ref,
        )
        print(json.dumps(_stage_payload(stage), indent=2))
        return 0

    if isinstance(command, BranchArgs):
        branch_path = FluxelRepository(Path(command.root)).branch(command.name)
        print(str(branch_path))
        return 0

    if isinstance(command, DiffArgs):
        changes = FluxelRepository(Path(command.root)).diff(
            command.from_ref,
            command.to_ref,
        )
        payload = [
            {
                "path": change.path,
                "change": change.change,
                "before_hash": change.before_hash,
                "after_hash": change.after_hash,
                "before_size": change.before_size,
                "after_size": change.after_size,
            }
            for change in changes
        ]
        print(json.dumps(payload, indent=2))
        return 0

    if isinstance(command, VerifyArgs):
        result = verify(
            root=Path(command.root),
            ref=command.ref,
            path_prefixes=command.path,
            dry_run=command.dry_run,
        )
        payload = {
            "commit_id": result.commit_id,
            "verified_entries": result.verified_entries,
            "candidate_entries": result.candidate_entries,
            "total_entries": result.total_entries,
            "created_commit": result.created_commit,
            "dry_run": result.dry_run,
        }
        print(json.dumps(payload, indent=2))
        return 0

    if isinstance(command, IndexArgs):
        index_command = command.command
        if isinstance(index_command, IndexBuildArgs):
            paths = build_analytical_index(
                root=Path(index_command.root),
                ref=index_command.ref,
                output_dir=index_command.output_dir,
                export_parquet=index_command.parquet,
            )
            result = {
                "database_path": str(paths.database_path),
                "parquet_path": str(paths.parquet_path) if paths.parquet_path else None,
            }
            print(json.dumps(result, indent=2))
            return 0

        if isinstance(index_command, IndexQueryArgs):
            rows = query_analytical_index(index_command.db, index_command.sql)
            print(json.dumps(rows))
            return 0

        if isinstance(index_command, IndexDropArgs):
            drop_analytical_index(index_command.db)
            print("ok")
            return 0

    parser.error("Unsupported command")
    return 2


def main() -> None:
    raise SystemExit(run_cli(sys.argv[1:]))
