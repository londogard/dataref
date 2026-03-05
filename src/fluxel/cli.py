from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from .core import (
    FluxelRepository,
    add,
    build_analytical_index,
    commit,
    drop_analytical_index,
    query_analytical_index,
    rm,
    status,
    verify,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="fluxel", description="Fluxel CLI")
    subparsers = parser.add_subparsers(dest="command", required=True)

    commit_parser = subparsers.add_parser("commit", help="Create a commit")
    commit_parser.add_argument("-m", "--message", required=True, help="Commit message")
    commit_parser.add_argument("--root", default=".", help="Dataset root path")
    commit_parser.add_argument(
        "--identity",
        choices=["blake3", "meta"],
        default="blake3",
        help="Identity strategy: full-content blake3 or metadata hash(path+size)",
    )
    commit_parser.add_argument(
        "--staged",
        action="store_true",
        help="Commit only staged changes for a branch",
    )
    commit_parser.add_argument(
        "--ref",
        default=None,
        help="Branch ref to update (defaults to current branch)",
    )

    add_parser = subparsers.add_parser("add", help="Stage files for commit")
    add_parser.add_argument("paths", nargs="+", help="Paths to stage")
    add_parser.add_argument("--root", default=".", help="Dataset root path")
    add_parser.add_argument(
        "--ref",
        default=None,
        help="Branch ref for staging (defaults to current branch)",
    )
    add_parser.add_argument(
        "--identity",
        choices=["blake3", "meta"],
        default="blake3",
        help="Identity strategy for staged additions",
    )

    rm_parser = subparsers.add_parser("rm", help="Stage file removals")
    rm_parser.add_argument("paths", nargs="+", help="Paths to remove")
    rm_parser.add_argument("--root", default=".", help="Dataset root path")
    rm_parser.add_argument(
        "--ref",
        default=None,
        help="Branch ref for staging (defaults to current branch)",
    )

    status_parser = subparsers.add_parser("status", help="Show staged status")
    status_parser.add_argument("--root", default=".", help="Dataset root path")
    status_parser.add_argument(
        "--ref",
        default=None,
        help="Branch ref for staging (defaults to current branch)",
    )

    branch_parser = subparsers.add_parser("branch", help="Create a branch pointer")
    branch_parser.add_argument("name", help="Branch name")
    branch_parser.add_argument("--root", default=".", help="Dataset root path")

    diff_parser = subparsers.add_parser(
        "diff", help="Diff two refs using metadata only"
    )
    diff_parser.add_argument("from_ref", help="Source ref (branch or commit)")
    diff_parser.add_argument("to_ref", help="Target ref (branch or commit)")
    diff_parser.add_argument("--root", default=".", help="Dataset root path")

    verify_parser = subparsers.add_parser(
        "verify",
        help="Promote metadata-only entries to canonical blake3 blobs",
    )
    verify_parser.add_argument("--root", default=".", help="Dataset root path")
    verify_parser.add_argument("--ref", default="main", help="Branch ref to verify")
    verify_parser.add_argument(
        "--path",
        action="append",
        default=None,
        help="Optional path/prefix filter (repeatable)",
    )
    verify_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Report entries that would be verified without writing blobs or commits",
    )

    index_parser = subparsers.add_parser("index", help="Manage analytical index")
    index_sub = index_parser.add_subparsers(dest="index_command", required=True)

    index_build = index_sub.add_parser(
        "build", help="Build disposable analytical index"
    )
    index_build.add_argument("--root", default=".", help="Dataset root path")
    index_build.add_argument("--ref", default="main", help="Ref to index")
    index_build.add_argument(
        "--output-dir", default=None, help="Output directory for index"
    )
    index_build.add_argument(
        "--parquet",
        action="store_true",
        help="Also export Parquet from the index table",
    )

    index_query = index_sub.add_parser("query", help="Run SQL query against index")
    index_query.add_argument("--db", required=True, help="Path to DuckDB index file")
    index_query.add_argument("--sql", required=True, help="SQL query to execute")

    index_drop = index_sub.add_parser("drop", help="Delete disposable analytical index")
    index_drop.add_argument("--db", required=True, help="Path to DuckDB index file")

    return parser


def run_cli(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.command == "commit":
        commit_id = commit(
            Path(args.root),
            args.message,
            identity_mode=args.identity,
            staged=args.staged,
            ref=args.ref,
        )
        print(commit_id)
        return 0

    if args.command == "add":
        stage = add(
            root=Path(args.root),
            paths=args.paths,
            ref=args.ref,
            identity_mode=args.identity,
        )
        print(
            json.dumps(
                {
                    "ref": stage.ref,
                    "added": stage.added,
                    "removed": stage.removed,
                },
                indent=2,
            )
        )
        return 0

    if args.command == "rm":
        stage = rm(
            root=Path(args.root),
            paths=args.paths,
            ref=args.ref,
        )
        print(
            json.dumps(
                {
                    "ref": stage.ref,
                    "added": stage.added,
                    "removed": stage.removed,
                },
                indent=2,
            )
        )
        return 0

    if args.command == "status":
        stage = status(
            root=Path(args.root),
            ref=args.ref,
        )
        print(
            json.dumps(
                {
                    "ref": stage.ref,
                    "added": stage.added,
                    "removed": stage.removed,
                },
                indent=2,
            )
        )
        return 0

    if args.command == "branch":
        branch_path = FluxelRepository(Path(args.root)).branch(args.name)
        print(str(branch_path))
        return 0

    if args.command == "diff":
        changes = FluxelRepository(Path(args.root)).diff(args.from_ref, args.to_ref)
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

    if args.command == "verify":
        result = verify(
            root=Path(args.root),
            ref=args.ref,
            path_prefixes=args.path,
            dry_run=args.dry_run,
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

    if args.command == "index":
        if args.index_command == "build":
            paths = build_analytical_index(
                root=Path(args.root),
                ref=args.ref,
                output_dir=args.output_dir,
                export_parquet=args.parquet,
            )
            result = {
                "database_path": str(paths.database_path),
                "parquet_path": str(paths.parquet_path) if paths.parquet_path else None,
            }
            print(json.dumps(result, indent=2))
            return 0

        if args.index_command == "query":
            rows = query_analytical_index(args.db, args.sql)
            print(json.dumps(rows))
            return 0

        if args.index_command == "drop":
            drop_analytical_index(args.db)
            print("ok")
            return 0

    parser.error("Unsupported command")
    return 2


def main() -> None:
    raise SystemExit(run_cli(sys.argv[1:]))
