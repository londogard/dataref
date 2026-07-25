from __future__ import annotations

import json
import sys
from dataclasses import asdict, dataclass
from typing import Literal, Any

from botocore.exceptions import BotoCoreError, ClientError
import msgspec
from simple_parsing import ArgumentParser
from simple_parsing.helpers import field, flag, subparsers

from .core import (
    BranchLockState,
    RefConflictError,
    StageStatus,
    add,
    branch,
    build_analytical_index,
    commit,
    diff,
    merge,
    move_staged,
    rm,
    status,
    verify,
    checkout,
    restore_files,
    open_repository,
)
from .core.config import (
    BaseConfig,
    FluxelConfig,
    LocalConfig,
    S3Config,
    init_config,
)

IdentityMode = Literal["blake3", "meta"]
HANDLED_CLI_ERRORS = (
    BotoCoreError,
    ClientError,
    FileNotFoundError,
    PermissionError,
    RefConflictError,
    ValueError,
)


def _lock_is_stale(lock_state: BranchLockState, timeout_seconds: int) -> bool:
    """Return True if *lock_state* has exceeded *timeout_seconds*."""
    from datetime import datetime, timedelta, timezone

    now = datetime.now(timezone.utc)
    if lock_state.expires_at is not None:
        return lock_state.expires_at <= now
    if lock_state.last_modified is None:
        return False
    return (
        lock_state.last_modified + timedelta(seconds=timeout_seconds) <= now
    )


@dataclass
class CommitArgs:
    message: str = field(alias=["-m", "--message"], help="Commit message")
    staged_only: bool = flag(
        False,
        alias=["--staged", "--staged-only"],
        help="Commit only staged changes, ignoring the working tree",
    )
    root: str = field(
        default=".",
        alias="--repo",
        help="Repository path or URI",
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
    json: bool = flag(
        False,
        alias="--json",
        help="Print result as structured JSON",
    )
    root: str = field(
        default=".",
        alias="--repo",
        help="Repository path or URI",
    )


@dataclass
class RmArgs:
    paths: list[str] = field(positional=True, nargs="+", help="Paths to remove")
    json: bool = flag(
        False,
        alias="--json",
        help="Print result as structured JSON",
    )
    root: str = field(
        default=".",
        alias="--repo",
        help="Repository path or URI",
    )


@dataclass
class MoveArgs:
    source_path: str = field(positional=True, help="Path or prefix to rename")
    destination_path: str = field(
        positional=True,
        help="Destination path or prefix",
    )
    json: bool = flag(
        False,
        alias="--json",
        help="Print result as structured JSON",
    )
    root: str = field(
        default=".",
        alias="--repo",
        help="Repository path or URI",
    )


@dataclass
class StatusArgs:
    root: str = field(
        default=".",
        alias="--repo",
        help="Repository path or URI",
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
    name: str = field(
        positional=True,
        help="Branch name to switch to",
    )
    root: str = field(
        default=".",
        alias="--repo",
        help="Repository path or URI",
    )


@dataclass
class RestoreArgs:
    ref: str = field(
        positional=True,
        help="Ref (branch or commit) to restore files from",
    )
    path: list[str] = field(
        default_factory=list,
        alias="--path",
        action="append",
        help="File paths to restore (repeatable, default: all files in ref)",
    )
    force: bool = flag(
        False,
        alias="--force",
        help="Allow overwriting existing files",
    )
    root: str = field(
        default=".",
        alias="--repo",
        help="Repository path or URI",
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
class ConfigSetArgs:
    key: str = field(
        positional=True, help="Config key (e.g. backend, dataset_root, s3.bucket)"
    )
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
class InitArgs:
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
class ConfigArgs:
    command: ConfigInitArgs | ConfigSetArgs | ConfigListArgs = subparsers(
        {
            "init": ConfigInitArgs,
            "set": ConfigSetArgs,
            "list": ConfigListArgs,
        }
    )


@dataclass
class PushArgs:
    remote: str = field(positional=True, help="Remote S3 URI (s3://bucket/prefix)")
    ref: str | None = field(
        default=None,
        alias="--ref",
        help="Branch to push (default: current branch)",
    )
    root: str = field(
        default=".",
        alias="--repo",
        help="Repository path or URI",
    )
    transfer_backend: str = field(
        default="boto3",
        alias="--transfer-backend",
        help="Blob transfer backend: boto3 (default) or s5cmd",
    )
    json: bool = flag(
        False,
        alias="--json",
        help="Print result as structured JSON",
    )


@dataclass
class PullArgs:
    remote: str = field(positional=True, help="Remote S3 URI (s3://bucket/prefix)")
    ref: str | None = field(
        default=None,
        alias="--ref",
        help="Branch to pull (default: current branch)",
    )
    root: str = field(
        default=".",
        alias="--repo",
        help="Repository path or URI",
    )
    transfer_backend: str = field(
        default="boto3",
        alias="--transfer-backend",
        help="Blob transfer backend: boto3 (default) or s5cmd",
    )
    json: bool = flag(
        False,
        alias="--json",
        help="Print result as structured JSON",
    )


@dataclass
class FetchArgs:
    remote: str = field(positional=True, help="Remote S3 URI (s3://bucket/prefix)")
    ref: str | None = field(
        default=None,
        alias="--ref",
        help="Branch to fetch (default: current branch)",
    )
    root: str = field(
        default=".",
        alias="--repo",
        help="Repository path or URI",
    )
    transfer_backend: str = field(
        default="boto3",
        alias="--transfer-backend",
        help="Blob transfer backend: boto3 (default) or s5cmd",
    )
    json: bool = flag(
        False,
        alias="--json",
        help="Print result as structured JSON",
    )


@dataclass
class TransferArgs:
    direction: str = field(
        default="upload",
        positional=True,
        help="Transfer direction: upload (local->S3) or download (S3->local)",
    )
    execute: bool = flag(
        False, alias="--execute", help="Execute generated s5cmd/aws commands"
    )
    root: str = field(default=".", alias="--repo", help="Repository path or URI")
    ref: str | None = field(
        default=None, alias="--ref", help="Ref to transfer (default: current branch)"
    )
    json: bool = flag(False, alias="--json", help="Print commands as JSON array")


@dataclass
class LockListArgs:
    root: str = field(
        default=".",
        alias="--repo",
        help="Repository path or URI (S3-backed only)",
    )
    json: bool = flag(
        False,
        alias="--json",
        help="Print locks as structured JSON",
    )


@dataclass
class LockCleanupArgs:
    root: str = field(
        default=".",
        alias="--repo",
        help="Repository path or URI (S3-backed only)",
    )
    branch: str | None = field(
        default=None,
        positional=True,
        nargs="?",
        help="Branch to unlock (omit for all stale locks)",
    )
    force: bool = flag(
        False,
        alias="--force",
        help="Release locks even if they are not stale",
    )
    json: bool = flag(
        False,
        alias="--json",
        help="Print result as structured JSON",
    )


@dataclass
class LockArgs:
    command: LockListArgs | LockCleanupArgs = subparsers(
        {
            "list": LockListArgs,
            "cleanup": LockCleanupArgs,
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
    json: bool = flag(
        False,
        alias="--json",
        help="Print result as structured JSON",
    )
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
    json: bool = flag(
        False,
        alias="--json",
        help="Print result as structured JSON",
    )


@dataclass
class IndexBuildArgs:
    root: str = field(
        default=".",
        alias="--repo",
        help="Repository path or URI",
    )
    output_dir: str | None = field(
        default=None,
        alias="--output-dir",
        help="Output directory for index",
    )
    parquet: bool = flag(
        False,
        help="Also export Parquet from the index table",
    )
    json: bool = flag(
        False,
        alias="--json",
        help="Print result as structured JSON",
    )


@dataclass
class IndexArgs:
    command: IndexBuildArgs = subparsers(
        {
            "build": IndexBuildArgs,
        }
    )


@dataclass
class FluxelCLI:
    command: (
        CommitArgs
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
        | RestoreArgs
        | InitArgs
        | ConfigArgs
        | PushArgs
        | PullArgs
        | FetchArgs
        | TransferArgs
        | LockArgs
    ) = subparsers(
        {
            "commit": CommitArgs,
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
            "restore": RestoreArgs,
            "init": InitArgs,
            "config": ConfigArgs,
            "push": PushArgs,
            "pull": PullArgs,
            "fetch": FetchArgs,
            "transfer": TransferArgs,
            "lock": LockArgs,
        }
    )


def build_parser() -> _CleanParser:
    parser = _CleanParser(
        prog="fluxel",
        description="Serverless object-storage-first data versioning engine.",
    )
    parser.add_argument(
        "--version",
        action="version",
        version="fluxel 0.1.0",
        help="Show version and exit",
    )
    parser.add_arguments(FluxelCLI, dest="cli")
    return parser


class _CleanParser(ArgumentParser):
    """ArgumentParser that suppresses simple-parsing internal group headers."""

    def format_help(self):
        import re

        text = super().format_help()
        text = re.sub(
            r"FluxelCLI \['cli'\]:\n  FluxelCLI\(command: '[^)]+'\)\n?",
            "",
            text,
        )
        return text


def _print_status(stage: StageStatus) -> None:
    if (
        stage.working_tree_added
        or stage.working_tree_removed
        or stage.working_tree_modified
    ):
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
    elif (
        not stage.working_tree_added
        and not stage.working_tree_removed
        and not stage.working_tree_modified
    ):
        print("Nothing to commit, working tree clean")


def _stage_payload(stage: StageStatus) -> dict[str, object]:
    return {
        "ref": stage.ref,
        "added": stage.added,
        "removed": stage.removed,
        "modified": stage.modified,
        "working_tree_added": stage.working_tree_added,
        "working_tree_removed": stage.working_tree_removed,
        "working_tree_modified": stage.working_tree_modified,
    }


def _flatten_option_values(values: list[Any]) -> list[str]:
    flattened: list[str] = []
    for value in values:
        if isinstance(value, list):
            flattened.extend(str(item) for item in value)
            continue
        flattened.append(str(value))
    return flattened


def _config_set_value(config: FluxelConfig, key: str, value: str) -> FluxelConfig:
    if key == "backend":
        if value not in ("local", "s3"):
            raise ValueError(f"Backend must be 'local' or 's3', got: {value}")
        if value == "s3":
            bucket = config.bucket if isinstance(config, S3Config) else ""
            try:
                return S3Config(
                    dataset_root=config.dataset_root,
                    default_branch=config.default_branch,
                    bucket=bucket,
                )
            except ValueError:
                raise ValueError(
                    "Cannot switch to S3 backend without a bucket. Set s3.bucket first."
                )
        return LocalConfig(
            dataset_root=config.dataset_root,
            default_branch=config.default_branch,
        )
    elif key == "dataset_root":
        config.dataset_root = value
    elif key == "default_branch":
        config.default_branch = value
    elif key == "identity":
        if value not in ("blake3", "meta"):
            raise ValueError(f"identity must be 'blake3' or 'meta', got: {value}")
        config.identity = value
    elif key == "transfer_backend":
        config.transfer_backend = value if value else None
    elif key == "s3.bucket":
        if isinstance(config, LocalConfig):
            return S3Config(
                dataset_root=config.dataset_root,
                default_branch=config.default_branch,
                bucket=value,
            )
        config.bucket = value
    elif key == "s3.prefix":
        if not isinstance(config, S3Config):
            raise ValueError("Cannot set s3.prefix on a local config")
        config.prefix = value
    elif key == "s3.endpoint_url":
        if not isinstance(config, S3Config):
            raise ValueError("Cannot set s3.endpoint_url on a local config")
        config.endpoint_url = value
    else:
        raise ValueError(f"Unknown config key: {key}")
    return config


def _command_name(command: object) -> str:
    if isinstance(command, CommitArgs):
        return "commit"
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
    if isinstance(command, RestoreArgs):
        return "restore"
    if isinstance(command, InitArgs):
        return "init"
    if isinstance(command, IndexArgs):
        return "index build"
    if isinstance(command, PushArgs):
        return "push"
    if isinstance(command, PullArgs):
        return "pull"
    if isinstance(command, FetchArgs):
        return "fetch"
    if isinstance(command, TransferArgs):
        return "transfer"
    if isinstance(command, LockArgs):
        if isinstance(command.command, LockListArgs):
            return "lock list"
        if isinstance(command.command, LockCleanupArgs):
            return "lock cleanup"
        return "lock"
    if isinstance(command, ConfigArgs):
        if isinstance(command.command, ConfigInitArgs):
            return "config init"
        if isinstance(command.command, ConfigSetArgs):
            return "config set"
        if isinstance(command.command, ConfigListArgs):
            return "config list"
        return "config"
    return "fluxel"


def run_cli(argv: list[str] | None = None) -> int:
    argv = argv or sys.argv[1:]
    parser = build_parser()
    try:
        args = parser.parse_args(argv).cli
    except SystemExit as exc:
        return exc.code if isinstance(exc.code, int) else 2
    command = args.command
    command_name = _command_name(command)

    try:
        if isinstance(command, CommitArgs):
            commit_id = commit(
                command.root,
                command.message,
                staged_only=command.staged_only,
            )
            print(commit_id)
            config = BaseConfig.load(command.root)
            if config is not None and config.identity == "meta":
                print(
                    "⚠  Metadata-only identity: this revision is unverifiable "
                    "until `fluxel verify` is run. "
                    "You must retain source objects for future verification.",
                    file=sys.stderr,
                )
            return 0

        if isinstance(command, AddArgs):
            stage = add(
                root=command.root,
                paths=command.paths,
                identity_mode=command.identity,
                destination_path=command.destination_path,
            )
            if command.json:
                print(json.dumps(_stage_payload(stage), indent=2))
            else:
                _print_status(stage)
            if command.identity == "meta":
                print(
                    "⚠  Metadata-only identity: revisions are unverifiable "
                    "until `fluxel verify` is run. "
                    "You must retain source objects for future verification.",
                    file=sys.stderr,
                )
            return 0

        if isinstance(command, RmArgs):
            stage = rm(root=command.root, paths=command.paths)
            if command.json:
                print(json.dumps(_stage_payload(stage), indent=2))
            else:
                _print_status(stage)
            return 0

        if isinstance(command, MoveArgs):
            stage = move_staged(
                root=command.root,
                source_path=command.source_path,
                destination_path=command.destination_path,
            )
            if command.json:
                print(json.dumps(_stage_payload(stage), indent=2))
            else:
                _print_status(stage)
            return 0

        if isinstance(command, StatusArgs):
            stage = status(
                root=command.root,
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
            if command.json:
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
            else:
                if result.updated:
                    print(
                        f"Merged '{result.source_ref}' into '{result.target_ref}' ({result.commit_id})"
                    )
                else:
                    print(
                        f"'{result.target_ref}' is already up to date with '{result.source_ref}'"
                    )
            return 0

        if isinstance(command, VerifyArgs):
            result = verify(
                root=command.root,
                path_prefixes=_flatten_option_values(command.path),
                dry_run=command.dry_run,
            )
            if command.json:
                payload = {
                    "commit_id": result.commit_id,
                    "verified_entries": result.verified_entries,
                    "candidate_entries": result.candidate_entries,
                    "total_entries": result.total_entries,
                    "created_commit": result.created_commit,
                    "dry_run": result.dry_run,
                }
                print(json.dumps(payload, indent=2))
            else:
                verb = "Would verify" if result.dry_run else "Verified"
                print(
                    f"{verb} {result.verified_entries}/{result.candidate_entries} entries "
                    f"(total: {result.total_entries})"
                )
                if result.created_commit:
                    print(f"Created commit: {result.commit_id}")
                remaining = result.candidate_entries - result.verified_entries
                if remaining > 0 and not result.dry_run:
                    print(
                        f"⚠  {remaining} unverifiable entries remain "
                        f"(source objects must be retained for future verification).",
                        file=sys.stderr,
                    )
            return 0

        if isinstance(command, IndexArgs):
            cmd = command.command
            paths = build_analytical_index(
                root=cmd.root,
                output_dir=cmd.output_dir,
                export_parquet=cmd.parquet,
            )
            if cmd.json:
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
            else:
                print(f"Database: {paths.database_path}")
                if paths.parquet_path:
                    print(f"Parquet:  {paths.parquet_path}")
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
                    msg_indented = "\n".join(
                        f"    {line}" for line in c.message.splitlines()
                    )
                    print(msg_indented)
            return 0

        if isinstance(command, ListArgs):
            repo = open_repository(command.root)
            entries = repo.resolve_entries_for_prefix(
                repo.current_branch(),
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
                path = config.save(config_command.root)
                print(f"Config initialized: {path}")
                return 0
            if isinstance(config_command, ConfigSetArgs):
                config = BaseConfig.load(config_command.root)
                if config is None:
                    print(
                        "No config found. Run 'fluxel config init' first.",
                        file=sys.stderr,
                    )
                    return 2
                config = _config_set_value(
                    config, config_command.key, config_command.value
                )
                path = config.save(config_command.root)
                print(f"Updated: {config_command.key}={config_command.value}")
                return 0
            if isinstance(config_command, ConfigListArgs):
                config = BaseConfig.load(config_command.root)
                if config is None:
                    print(
                        "No config found. Run 'fluxel config init' first.",
                        file=sys.stderr,
                    )
                    return 2
                raw = msgspec.json.format(msgspec.json.encode(config).decode("utf-8"))
                print(raw)
                return 0

        if isinstance(command, CheckoutArgs):
            checkout(command.root, command.name)
            print(f"Switched to branch '{command.name}'")
            return 0

        if isinstance(command, RestoreArgs):
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
                print(
                    f"Restored {len(restored)} file(s) from '{command.ref}':\n{rel_paths}"
                )
            return 0

        if isinstance(command, InitArgs):
            config = init_config(
                command.root,
                backend=command.backend,  # type: ignore[arg-type]
                default_branch=command.default_branch,
                s3_bucket=command.s3_bucket,
                s3_prefix=command.s3_prefix,
                s3_endpoint_url=command.s3_endpoint,
            )
            path = config.save(command.root)
            print(f"Repository initialized: {path}")
            return 0

        if isinstance(command, PushArgs):
            from .core.repository_sync import push

            repo = open_repository(command.root, blob_transfer=command.transfer_backend)
            result = push(
                repo,
                command.remote,
                ref=command.ref,
                blob_transfer=command.transfer_backend,
            )
            if command.json:
                print(json.dumps(asdict(result), indent=2))
            else:
                if result.updated:
                    print(
                        f"Pushed {result.pushed_commits} commit(s), "
                        f"{result.pushed_blobs} blob(s) to {command.remote}"
                    )
                else:
                    print("Everything up-to-date")
            return 0

        if isinstance(command, PullArgs):
            from .core.repository_sync import pull

            repo = open_repository(command.root, blob_transfer=command.transfer_backend)
            result = pull(
                repo,
                command.remote,
                ref=command.ref,
                blob_transfer=command.transfer_backend,
            )
            if command.json:
                print(json.dumps(asdict(result), indent=2))
            else:
                if result.updated:
                    print(
                        f"Pulled {result.pulled_commits} commit(s), "
                        f"{result.pulled_blobs} blob(s) from {command.remote}"
                    )
                else:
                    print("Already up-to-date")
            return 0

        if isinstance(command, FetchArgs):
            from .core.repository_sync import fetch

            repo = open_repository(command.root, blob_transfer=command.transfer_backend)
            result = fetch(
                repo,
                command.remote,
                ref=command.ref,
                blob_transfer=command.transfer_backend,
            )
            if command.json:
                print(json.dumps(asdict(result), indent=2))
            else:
                print(
                    f"Fetched {result.fetched_commits} commit(s), "
                    f"{result.fetched_blobs} blob(s) from {command.remote}"
                )
            return 0

        if isinstance(command, TransferArgs):
            repo = open_repository(command.root)
            cmds = repo.generate_transfer_commands(
                ref=command.ref,
                mode=command.direction,
                include_metadata=True,
            )
            if command.json:
                print(json.dumps(cmds, indent=2))
            elif command.execute:
                import subprocess

                for cmd in cmds:
                    subprocess.run(cmd, shell=True, check=True)
                print(f"Executed {len(cmds)} transfer commands")
            else:
                for cmd in cmds:
                    print(cmd)
            return 0

        if isinstance(command, LockArgs):
            lock_command = command.command
            if isinstance(lock_command, LockListArgs):
                repo = open_repository(lock_command.root)
                locks = repo.list_locks()
                if lock_command.json:
                    payload = {
                        branch: {
                            "token": ls.token,
                            "expires_at": (
                                ls.expires_at.isoformat()
                                if ls.expires_at
                                else None
                            ),
                            "last_modified": (
                                ls.last_modified.isoformat()
                                if ls.last_modified
                                else None
                            ),
                            "stale": _lock_is_stale(ls, repo.lock_timeout_seconds),
                        }
                        for branch, ls in locks.items()
                    }
                    print(json.dumps(payload, indent=2))
                elif not locks:
                    print("No active locks")
                else:
                    timeout = repo.lock_timeout_seconds
                    print(f"{'Branch':<30} {'Status':<10} {'Expires'}")
                    print("-" * 60)
                    for branch_name, ls in sorted(locks.items()):
                        stale = _lock_is_stale(ls, timeout)
                        lock_status = "STALE" if stale else "active"
                        expires = (
                            ls.expires_at.strftime("%Y-%m-%d %H:%M:%S UTC")
                            if ls.expires_at
                            else "unknown"
                        )
                        print(
                            f"{branch_name:<30} {lock_status:<10} {expires}"
                        )
                return 0

            if isinstance(lock_command, LockCleanupArgs):
                repo = open_repository(lock_command.root)
                locks = repo.list_locks()
                timeout = repo.lock_timeout_seconds

                released: dict[str, bool] = {}
                if lock_command.branch is not None:
                    candidates = {lock_command.branch} & locks.keys()
                else:
                    candidates = {
                        b
                        for b, ls in locks.items()
                        if lock_command.force or _lock_is_stale(ls, timeout)
                    }

                for branch_name in sorted(candidates):
                    released[branch_name] = repo.force_release_lock(branch_name)

                if lock_command.json:
                    print(json.dumps({"released": released}, indent=2))
                elif not released:
                    print("No locks to release")
                else:
                    for branch_name, ok in released.items():
                        release_status = "Released" if ok else "Not found"
                        print(f"  {release_status}: {branch_name}")
                return 0
    except HANDLED_CLI_ERRORS as error:
        print(f"{command_name} error: {error}", file=sys.stderr)
        return 2

    raise AssertionError(f"Unsupported command type: {type(command).__name__}")


def main(argv: list[str] | None = None) -> int:
    return run_cli(argv)


if __name__ == "__main__":
    raise SystemExit(main())
