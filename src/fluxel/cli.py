from __future__ import annotations

import json
import sys
from dataclasses import dataclass
from typing import Literal

from botocore.exceptions import BotoCoreError, ClientError
from simple_parsing import ArgumentParser
from simple_parsing.helpers import field, flag, subparsers

from .core import (
    RefConflictError,
    add,
    branch,
    build_analytical_index,
    commit,
    diff,
    drop_analytical_index,
    import_s3,
    merge,
    move,
    query_analytical_index,
    remove,
    rm,
    status,
    verify,
)


IdentityMode = Literal["blake3", "meta"]
HANDLED_CLI_ERRORS = (
    BotoCoreError,
    ClientError,
    FileNotFoundError,
    OSError,
    PermissionError,
    RefConflictError,
    ValueError,
)


@dataclass
class CommitArgs:
    message: str = field(alias=["-m", "--message"], help="Commit message")
    identity: IdentityMode = (
        "blake3"  # Identity strategy: full-content blake3 or metadata hash(path+size)"
    )
    root: str = field(
        default=".",
        alias="--repo",
        help="Repository path or URI",
    )
    staged: bool = flag(
        False,
        help="Commit only staged changes for a branch",
    )
    ref: str | None = None  # Branch ref to update (defaults to current branch)


@dataclass
class ImportArgs:
    source: str = field(
        positional=True, help="S3 URI to import, e.g. s3://bucket/prefix"
    )
    message: str = field(alias=["-m", "--message"], help="Commit message")
    identity: IdentityMode = (
        "blake3"  # Identity strategy: full-content blake3 or metadata hash(path+size)"
    )
    path_patterns: list[str] = field(
        default_factory=list,
        alias="--path",
        action="append",
        help="Optional relative path/glob filter (repeatable)",
    )
    root: str = field(
        default=".",
        alias="--repo",
        help="Repository path or URI",
    )
    ref: str | None = None  # Branch ref to update (defaults to current branch)


@dataclass
class AddArgs:
    paths: list[str] = field(
        positional=True, nargs="+", help="Files, directories, or S3 paths to stage"
    )
    identity: IdentityMode = "blake3"  # Identity strategy for staged additions
    destination_path: str | None = field(
        default=None,
        alias="--as",
        help="Logical destination path for a single staged source",
    )
    root: str = field(
        default=".",
        alias="--repo",
        help="Repository path or URI",
    )
    ref: str | None = field(
        default=None,
        help="Branch ref for staging (defaults to current branch)",
    )


@dataclass
class RmArgs:
    paths: list[str] = field(positional=True, nargs="+", help="Paths to remove")
    message: str | None = field(
        default=None,
        alias=["-m", "--message"],
        help="Commit message for a metadata-only removal",
    )
    root: str = field(
        default=".",
        alias="--repo",
        help="Repository path or URI",
    )
    ref: str | None = field(
        default=None,
        help="Branch ref for staging (defaults to current branch)",
    )
    staged: bool = flag(
        False,
        help="Stage removals instead of writing a metadata-only commit",
    )


@dataclass
class MoveArgs:
    source_path: str = field(positional=True, help="Path or prefix to rename")
    destination_path: str = field(
        positional=True,
        help="Destination path or prefix",
    )
    message: str = field(alias=["-m", "--message"], help="Commit message")
    root: str = field(
        default=".",
        alias="--repo",
        help="Repository path or URI",
    )
    ref: str | None = field(
        default=None,
        help="Branch ref to update (defaults to current branch)",
    )


@dataclass
class StatusArgs:
    root: str = field(
        default=".",
        alias="--repo",
        help="Repository path or URI",
    )
    ref: str | None = field(
        default=None,
        help="Branch ref for staging (defaults to current branch)",
    )


@dataclass
class BranchArgs:
    name: str = field(positional=True, help="Branch name")
    root: str = field(
        default=".",
        alias="--repo",
        help="Repository path or URI",
    )


@dataclass
class DiffArgs:
    from_ref: str = field(positional=True, help="Source ref (branch or commit)")
    to_ref: str = field(positional=True, help="Target ref (branch or commit)")
    root: str = field(
        default=".",
        alias="--repo",
        help="Repository path or URI",
    )


@dataclass
class MergeArgs:
    source_ref: str = field(positional=True, help="Ref to merge from")
    target_ref: str = field(positional=True, help="Branch ref to fast-forward")
    root: str = field(
        default=".",
        alias="--repo",
        help="Repository path or URI",
    )


@dataclass
class VerifyArgs:
    root: str = field(
        default=".",
        alias="--repo",
        help="Repository path or URI",
    )
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
    root: str = field(
        default=".",
        alias="--repo",
        help="Repository path or URI",
    )
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
        | MoveArgs
        | StatusArgs
        | BranchArgs
        | DiffArgs
        | MergeArgs
        | VerifyArgs
        | IndexArgs
    ) = subparsers(
        {
            "commit": CommitArgs,
            "import": ImportArgs,
            "add": AddArgs,
            "rm": RmArgs,
            "mv": MoveArgs,
            "status": StatusArgs,
            "branch": BranchArgs,
            "diff": DiffArgs,
            "merge": MergeArgs,
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
        # `simple_parsing` may surface repeated list-valued flags as nested lists
        # (e.g. [['**/*.jpg'], ['root.txt']]) for this dataclass field shape.
        if isinstance(value, list):
            flattened.extend(str(item) for item in value)
            continue
        flattened.append(str(value))
    return flattened


def _command_name(command: object) -> str:
    if isinstance(command, CommitArgs):
        return "commit"
    if isinstance(command, ImportArgs):
        return "import"
    if isinstance(command, AddArgs):
        return "add"
    if isinstance(command, RmArgs):
        return "rm"
    if isinstance(command, MoveArgs):
        return "mv"
    if isinstance(command, StatusArgs):
        return "status"
    if isinstance(command, BranchArgs):
        return "branch"
    if isinstance(command, DiffArgs):
        return "diff"
    if isinstance(command, MergeArgs):
        return "merge"
    if isinstance(command, VerifyArgs):
        return "verify"
    if isinstance(command, IndexArgs):
        if isinstance(command.command, IndexBuildArgs):
            return "index build"
        if isinstance(command.command, IndexQueryArgs):
            return "index query"
        if isinstance(command.command, IndexDropArgs):
            return "index drop"
        return "index"
    return "fluxel"


def run_cli(argv: list[str] | None = None) -> int:
    argv = argv or sys.argv[1:]
    parser = build_parser()
    args = parser.parse_args(argv).cli
    command = args.command
    command_name = _command_name(command)

    try:
        if isinstance(command, CommitArgs):
            commit_id = commit(
                command.root,
                command.message,
                identity_mode=command.identity,
                staged=command.staged,
                ref=command.ref,
            )
            print(commit_id)
            return 0

        if isinstance(command, ImportArgs):
            commit_id = import_s3(
                command.root,
                command.source,
                command.message,
                identity_mode=command.identity,
                path_patterns=_flatten_option_values(command.path_patterns),
                ref=command.ref,
            )
            print(commit_id)
            return 0

        if isinstance(command, AddArgs):
            stage = add(
                root=command.root,
                paths=command.paths,
                ref=command.ref,
                identity_mode=command.identity,
                destination_path=command.destination_path,
            )
            print(json.dumps(_stage_payload(stage), indent=2))
            return 0

        if isinstance(command, RmArgs):
            if command.message is not None and command.staged:
                print(
                    "rm error: cannot combine --message with --staged",
                    file=sys.stderr,
                )
                return 2
            if command.message is not None:
                result = remove(
                    root=command.root,
                    paths=command.paths,
                    message=command.message,
                    ref=command.ref,
                )
                print(
                    json.dumps(
                        {
                            "ref": result.ref,
                            "commit_id": result.commit_id,
                            "removed_paths": result.removed_paths,
                        },
                        indent=2,
                    )
                )
                return 0

            stage = rm(root=command.root, paths=command.paths, ref=command.ref)
            print(json.dumps(_stage_payload(stage), indent=2))
            return 0

        if isinstance(command, MoveArgs):
            result = move(
                root=command.root,
                source_path=command.source_path,
                destination_path=command.destination_path,
                message=command.message,
                ref=command.ref,
            )
            print(
                json.dumps(
                    {
                        "ref": result.ref,
                        "commit_id": result.commit_id,
                        "source_path": result.source_path,
                        "destination_path": result.destination_path,
                        "moved_paths": result.moved_paths,
                    },
                    indent=2,
                )
            )
            return 0

        if isinstance(command, StatusArgs):
            stage = status(
                root=command.root,
                ref=command.ref,
            )
            print(json.dumps(_stage_payload(stage), indent=2))
            return 0

        if isinstance(command, BranchArgs):
            branch_path = branch(command.root, command.name)
            print(str(branch_path))
            return 0

        if isinstance(command, DiffArgs):
            changes = diff(
                command.root,
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

        if isinstance(command, MergeArgs):
            result = merge(
                root=command.root,
                source_ref=command.source_ref,
                target_ref=command.target_ref,
            )
            print(
                json.dumps(
                    {
                        "source_ref": result.source_ref,
                        "target_ref": result.target_ref,
                        "commit_id": result.commit_id,
                        "updated": result.updated,
                    },
                    indent=2,
                )
            )
            return 0

        if isinstance(command, VerifyArgs):
            result = verify(
                root=command.root,
                ref=command.ref,
                path_prefixes=_flatten_option_values(command.path),
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
                    root=index_command.root,
                    ref=index_command.ref,
                    output_dir=index_command.output_dir,
                    export_parquet=index_command.parquet,
                )
                print(
                    json.dumps(
                        {
                            "database_path": str(paths.database_path),
                            "parquet_path": (
                                str(paths.parquet_path)
                                if paths.parquet_path is not None
                                else None
                            ),
                        },
                        indent=2,
                    )
                )
                return 0
            if isinstance(index_command, IndexQueryArgs):
                rows = query_analytical_index(index_command.db, index_command.sql)
                print(json.dumps(rows, indent=2))
                return 0
            if isinstance(index_command, IndexDropArgs):
                drop_analytical_index(index_command.db)
                print("ok")
                return 0
    except HANDLED_CLI_ERRORS as error:
        print(f"{command_name} error: {error}", file=sys.stderr)
        return 2

    raise AssertionError(f"Unsupported command type: {type(command).__name__}")


def main(argv: list[str] | None = None) -> int:
    return run_cli(argv)


if __name__ == "__main__":
    raise SystemExit(main())
