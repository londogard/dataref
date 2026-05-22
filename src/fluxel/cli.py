from __future__ import annotations

import json
import sys
from dataclasses import dataclass
from pathlib import Path
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
    generate_transfer_commands,
    import_s3,
    merge,
    move_staged,
    move as move_committed,
    query_analytical_index,
    remove,
    rm,
    status,
    verify,
    log,
    checkout,
    restore_files,
    open_repository,
)
from .core.config import (
    BackendType,
    FluxelConfig,
    S3Config,
    _serialize as serialize_config,
    init_config,
    load_config,
    save_config,
    validate_config,
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
    transfer_backend: str | None = field(
        default=None,
        alias="--transfer-backend",
        help="Blob transfer backend (boto3 or s5cmd)",
    )


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
    transfer_backend: str | None = field(
        default=None,
        alias="--transfer-backend",
        help="Blob transfer backend (boto3 or s5cmd)",
    )


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
    transfer_backend: str | None = field(
        default=None,
        alias="--transfer-backend",
        help="Blob transfer backend (boto3 or s5cmd)",
    )


@dataclass
class RmArgs:
    paths: list[str] = field(positional=True, nargs="+", help="Paths to remove")
    message: str | None = field(
        default=None,
        alias=["-m", "--message"],
        help="Commit message (omit to stage removals instead of committing)",
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
class MoveArgs:
    source_path: str = field(positional=True, help="Path or prefix to rename")
    destination_path: str = field(
        positional=True,
        help="Destination path or prefix",
    )
    message: str | None = field(
        default=None,
        alias=["-m", "--message"],
        help="Commit message (omit to stage the move instead of committing)",
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
    json: bool = flag(
        False,
        help="Print status as structured JSON",
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
class LogArgs:
    ref: str | None = field(
        default=None,
        positional=True,
        nargs="?",
        help="Ref (branch or commit) to show log for (defaults to current branch)",
    )
    root: str = field(
        default=".",
        alias="--repo",
        help="Repository path or URI",
    )
    json: bool = flag(
        False,
        help="Print history as structured JSON",
    )


@dataclass
class ListArgs:
    root: str = field(
        default=".",
        alias="--repo",
        help="Repository path or URI",
    )
    ref: str | None = field(
        default=None,
        help="Ref (branch/commit) to list (defaults to current branch)",
    )
    path: str = field(
        default="",
        positional=True,
        nargs="?",
        help="Logical path or prefix to list (default: root)",
    )
    json: bool = flag(
        False,
        alias="--json",
        help="Print listing as structured JSON",
    )


@dataclass
class CheckoutArgs:
    name: str | None = field(
        positional=True,
        default=None,
        help="Branch name to switch to (omit with --ref to restore files)",
    )
    ref: str | None = field(
        default=None,
        alias="--ref",
        help="Ref (branch/commit) to restore files from",
    )
    path: list[str] = field(
        default_factory=list,
        alias="--path",
        action="append",
        help="File paths to restore (repeatable, default: all files in ref)",
    )
    force: bool = field(
        default=False,
        alias="--force",
        help="Allow overwriting existing files",
    )
    root: str = field(
        default=".",
        alias="--repo",
        help="Repository path or URI",
    )


@dataclass
class TransferArgs:
    root: str = field(
        default=".",
        alias="--repo",
        help="Repository path",
    )
    ref: str | None = field(
        default=None,
        help="Ref (branch/commit) to transfer blobs for (default: current branch)",
    )
    output: str | None = field(
        default=None,
        alias=["-o", "--output"],
        help="Output file path (default: stdout)",
    )
    mode: str = field(
        default="upload",
        alias="--mode",
        help="Transfer direction: upload (local->S3) or download (S3->local)",
    )
    include_metadata: bool = flag(
        False,
        alias="--include-metadata",
        help="Also include manifest and commit metadata files in the command list",
    )


@dataclass
class ConfigInitArgs:
    root: str = field(
        default=".",
        alias="--repo",
        help="Repository path",
    )
    backend: str = field(
        default="local",
        alias="--backend",
        help="Backend type (local or s3)",
    )
    s3_bucket: str | None = field(
        default=None,
        alias="--s3-bucket",
        help="S3 bucket name (required for s3 backend)",
    )
    s3_prefix: str | None = field(
        default=None,
        alias="--s3-prefix",
        help="S3 key prefix",
    )
    s3_endpoint: str | None = field(
        default=None,
        alias="--s3-endpoint",
        help="S3 endpoint URL",
    )
    default_branch: str = field(
        default="main",
        alias="--default-branch",
        help="Default branch name",
    )


@dataclass
class ConfigGetArgs:
    key: str = field(positional=True, help="Config key (e.g. backend, dataset_root, default_branch, s3.bucket)")
    root: str = field(
        default=".",
        alias="--repo",
        help="Repository path",
    )


@dataclass
class ConfigSetArgs:
    key: str = field(positional=True, help="Config key (e.g. backend, dataset_root, s3.bucket)")
    value: str = field(positional=True, help="Config value")
    root: str = field(
        default=".",
        alias="--repo",
        help="Repository path",
    )


@dataclass
class ConfigListArgs:
    root: str = field(
        default=".",
        alias="--repo",
        help="Repository path",
    )


@dataclass
class ConfigArgs:
    command: ConfigInitArgs | ConfigGetArgs | ConfigSetArgs | ConfigListArgs = subparsers(
        {
            "init": ConfigInitArgs,
            "get": ConfigGetArgs,
            "set": ConfigSetArgs,
            "list": ConfigListArgs,
        }
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
    json: bool = flag(False, alias="--json", help="Print diff as structured JSON")


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
    transfer_backend: str | None = field(
        default=None,
        alias="--transfer-backend",
        help="Blob transfer backend (boto3 or s5cmd)",
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
        | LogArgs
        | ListArgs
        | CheckoutArgs
        | TransferArgs
        | ConfigArgs
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
            "log": LogArgs,
            "list": ListArgs,
            "checkout": CheckoutArgs,
            "transfer": TransferArgs,
            "config": ConfigArgs,
        }
    )


def build_parser() -> ArgumentParser:
    parser = ArgumentParser(prog="fluxel", description="Fluxel CLI")
    parser.add_argument(
        "--version",
        action="version",
        version="fluxel 0.1.0",
        help="Show version and exit",
    )
    parser.add_arguments(FluxelCLI, dest="cli")
    return parser


def _print_status(stage: object) -> None:
    if stage.working_tree_added or stage.working_tree_removed or stage.working_tree_modified:
        if stage.working_tree_added:
            print("Working tree changes:")
            for path in stage.working_tree_added:
                print(f"  added:    {path}")
        if stage.working_tree_modified:
            for path in stage.working_tree_modified:
                print(f"  modified: {path}")
        if stage.working_tree_removed:
            for path in stage.working_tree_removed:
                print(f"  removed:  {path}")
        print()

    if stage.added or stage.removed:
        print("Staged changes:")
        for path in stage.added:
            print(f"  added:    {path}")
        for path in stage.removed:
            print(f"  removed:  {path}")
    elif not stage.working_tree_added and not stage.working_tree_removed and not stage.working_tree_modified:
        print("Nothing to commit, working tree clean")


def _stage_payload(stage: object) -> dict[str, object]:
    return {
        "ref": stage.ref,
        "added": stage.added,
        "removed": stage.removed,
        "modified": stage.modified,
        "working_tree_added": stage.working_tree_added,
        "working_tree_removed": stage.working_tree_removed,
        "working_tree_modified": stage.working_tree_modified,
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


def _config_get_value(config: FluxelConfig, key: str) -> object | None:
    if key == "backend":
        return config.backend
    if key == "dataset_root":
        return config.dataset_root
    if key == "default_branch":
        return config.default_branch
    if key == "s3.bucket":
        return config.s3.bucket if config.s3 else None
    if key == "s3.prefix":
        return config.s3.prefix if config.s3 else None
    if key == "s3.endpoint_url":
        return config.s3.endpoint_url if config.s3 else None
    return None


def _config_set_value(config: FluxelConfig, key: str, value: str) -> None:
    if key == "backend":
        if value not in ("local", "s3"):
            raise ValueError(f"Backend must be 'local' or 's3', got: {value}")
        config.backend = value  # type: ignore[assignment]
        if value == "s3" and config.s3 is None:
            config.s3 = S3Config()
        if value == "local":
            config.s3 = None
    elif key == "dataset_root":
        config.dataset_root = value
    elif key == "default_branch":
        config.default_branch = value
    elif key == "s3.bucket":
        if config.s3 is None:
            config.s3 = S3Config()
        config.s3.bucket = value
    elif key == "s3.prefix":
        if config.s3 is None:
            config.s3 = S3Config()
        config.s3.prefix = value
    elif key == "s3.endpoint_url":
        if config.s3 is None:
            config.s3 = S3Config()
        config.s3.endpoint_url = value
    else:
        raise ValueError(f"Unknown config key: {key}")


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
    if isinstance(command, LogArgs):
        return "log"
    if isinstance(command, ListArgs):
        return "list"
    if isinstance(command, CheckoutArgs):
        return "checkout"
    if isinstance(command, IndexArgs):
        if isinstance(command.command, IndexBuildArgs):
            return "index build"
        if isinstance(command.command, IndexQueryArgs):
            return "index query"
        if isinstance(command.command, IndexDropArgs):
            return "index drop"
        return "index"
    if isinstance(command, ConfigArgs):
        if isinstance(command.command, ConfigInitArgs):
            return "config init"
        if isinstance(command.command, ConfigGetArgs):
            return "config get"
        if isinstance(command.command, ConfigSetArgs):
            return "config set"
        if isinstance(command.command, ConfigListArgs):
            return "config list"
        return "config"
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
                blob_transfer=command.transfer_backend,
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
                blob_transfer=command.transfer_backend,
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
                blob_transfer=command.transfer_backend,
            )
            print(json.dumps(_stage_payload(stage), indent=2))
            return 0

        if isinstance(command, RmArgs):
            if command.message:
                result = remove(root=command.root, paths=command.paths, message=command.message, ref=command.ref)
                print(json.dumps({"ref": result.ref, "commit_id": result.commit_id, "removed_paths": result.removed_paths}, indent=2))
                return 0
            stage = rm(root=command.root, paths=command.paths, ref=command.ref)
            print(json.dumps(_stage_payload(stage), indent=2))
            return 0

        if isinstance(command, MoveArgs):
            if command.message:
                result = move_committed(
                    root=command.root,
                    source_path=command.source_path,
                    destination_path=command.destination_path,
                    message=command.message,
                    ref=command.ref,
                )
                print(json.dumps({"ref": result.ref, "commit_id": result.commit_id, "source_path": result.source_path, "destination_path": result.destination_path, "moved_paths": result.moved_paths}, indent=2))
                return 0
            stage = move_staged(
                root=command.root,
                source_path=command.source_path,
                destination_path=command.destination_path,
                ref=command.ref,
            )
            print(json.dumps(_stage_payload(stage), indent=2))
            return 0

        if isinstance(command, StatusArgs):
            stage = status(
                root=command.root,
                ref=command.ref,
            )
            if command.json:
                print(json.dumps(_stage_payload(stage), indent=2))
            else:
                _print_status(stage)
            return 0

        if isinstance(command, BranchArgs):
            branch(command.root, command.name)
            print(f"Created branch '{command.name}'")
            return 0

        if isinstance(command, DiffArgs):
            changes = diff(
                command.root,
                command.from_ref,
                command.to_ref,
            )
            if command.json:
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
            else:
                for c in changes:
                    print(f"  {c.change}: {c.path}")
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
                blob_transfer=command.transfer_backend,
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

        if isinstance(command, LogArgs):
            repo = open_repository(command.root)
            ref = command.ref or repo.current_branch()
            commits = list(repo.log(ref))
            if command.json:
                payload = [
                    {
                        "id": c.id,
                        "message": c.message,
                        "manifest": c.manifest,
                        "parent": c.parent,
                        "created_at": c.created_at,
                        "branch": c.branch,
                    }
                    for c in commits
                ]
                print(json.dumps(payload, indent=2))
            else:
                for i, c in enumerate(commits):
                    if i > 0:
                        print()
                    print(f"commit {c.id}")
                    print(f"Date:   {c.created_at}")
                    if c.parent:
                        print(f"Parent: {c.parent}")
                    print(f"Branch: {c.branch}")
                    print()
                    msg_indented = "\n".join(f"    {line}" for line in c.message.splitlines())
                    print(msg_indented)
            return 0

        if isinstance(command, ListArgs):
            repo = open_repository(command.root)
            ref = command.ref or repo.current_branch()
            entries = repo.resolve_entries_for_prefix(
                ref,
                command.path,
            )
            if command.json:
                payload = [
                    {
                        "path": entry.path,
                        "hash": entry.hash,
                        "size": entry.size,
                        "identity_mode": entry.identity_mode,
                        "blob_hash": entry.blob_hash,
                        "source_uri": entry.source_uri,
                    }
                    for entry in entries.values()
                ]
                print(json.dumps(payload, indent=2))
            else:
                for path in sorted(entries):
                    print(path)
            return 0

        if isinstance(command, ConfigArgs):
            config_command = command.command
            if isinstance(config_command, ConfigInitArgs):
                config = init_config(
                    config_command.root,
                    backend=config_command.backend,  # type: ignore[arg-type]
                    default_branch=config_command.default_branch,
                    s3_bucket=config_command.s3_bucket,
                    s3_prefix=config_command.s3_prefix,
                    s3_endpoint_url=config_command.s3_endpoint,
                )
                path = save_config(config_command.root, config)
                print(f"Config initialized: {path}")
                return 0
            if isinstance(config_command, ConfigGetArgs):
                config = load_config(config_command.root)
                if config is None:
                    print(
                        "No config found. Run 'fluxel config init' first.",
                        file=sys.stderr,
                    )
                    return 2
                value = _config_get_value(config, config_command.key)
                if value is None:
                    print(
                        f"Unknown config key: {config_command.key}",
                        file=sys.stderr,
                    )
                    return 2
                if isinstance(value, str):
                    print(value)
                else:
                    print(json.dumps(value))
                return 0
            if isinstance(config_command, ConfigSetArgs):
                config = load_config(config_command.root)
                if config is None:
                    print(
                        "No config found. Run 'fluxel config init' first.",
                        file=sys.stderr,
                    )
                    return 2
                _config_set_value(config, config_command.key, config_command.value)
                path = save_config(config_command.root, config)
                print(f"Updated: {config_command.key}={config_command.value}")
                return 0
            if isinstance(config_command, ConfigListArgs):
                config = load_config(config_command.root)
                if config is None:
                    print(
                        "No config found. Run 'fluxel config init' first.",
                        file=sys.stderr,
                    )
                    return 2
                raw = serialize_config(config)
                print(json.dumps(raw, indent=2))
                return 0

        if isinstance(command, CheckoutArgs):
            if command.ref is not None:
                restored = restore_files(
                    command.root,
                    command.ref,
                    paths=_flatten_option_values(command.path) or None,
                    force=command.force,
                )
                if not restored:
                    print("Nothing to restore")
                else:
                    rel_paths = "\n".join(f"  restored: {p}" for p in restored)
                    print(f"Restored {len(restored)} file(s) from '{command.ref}':\n{rel_paths}")
                return 0
            if command.name is None:
                print("Either specify --ref <ref> to restore files or a branch name to switch to", file=sys.stderr)
                return 2
            checkout(command.root, command.name)
            print(f"Switched to branch '{command.name}'")
            return 0

        if isinstance(command, TransferArgs):
            commands = generate_transfer_commands(
                command.root,
                ref=command.ref,
                mode=command.mode,
                include_metadata=command.include_metadata,
            )
            text = "\n".join(commands)
            if command.output:
                Path(command.output).write_text(text + "\n")
                print(f"Wrote {len(commands)} s5cmd command(s) to {command.output}")
            else:
                print(text)
            return 0
    except HANDLED_CLI_ERRORS as error:
        print(f"{command_name} error: {error}", file=sys.stderr)
        return 2

    raise AssertionError(f"Unsupported command type: {type(command).__name__}")


def main(argv: list[str] | None = None) -> int:
    return run_cli(argv)


if __name__ == "__main__":
    raise SystemExit(main())
