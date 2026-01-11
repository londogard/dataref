# strand

Client-driven data versioning for S3 — think “Git for data, but actually good”.

## Why Python first?
Python is a great starting point for a client-first tool:
- Fast iteration + great UX for a Git-like CLI.
- Performance is still strong because key engines are native:
	- `deltalake` uses delta-rs (Rust) under the hood.
	- `polars` is Rust-backed and very fast for scans/metadata work.

So yes: Python + polars + delta-rs is a pragmatic way to get started, and you can always move hot paths to Rust later.

## Current status (MVP)
This repo currently implements a minimal metadata-only repository format:
- `strand init <root>` creates a `.strand/` directory under `<root>` (local or `s3://...`)
- `strand commit -m "msg" <root>` writes an immutable commit object under `.strand/objects/`
- `strand log <root>` prints history
- `strand branch <root> <name>` and `strand checkout <root> <name>` manage refs

This is the foundation for real data snapshots (manifests) and later diff/merge.

## Repo format (draft)
All metadata lives under `<root>/.strand/`:
- `.strand/config.json` repo config
- `.strand/objects/<sha256>.json` content-addressed objects
- `.strand/refs/heads/<branch>` branch refs
- `.strand/HEAD` current ref

## Install (dev)

Using `uv`:
- `uv venv && source .venv/bin/activate`
- `uv pip install -e '.[dev]'`
- `pytest`

Optionally (lockfile-driven):
- `uv lock`
- `uv sync --extra dev`

## CLI usage
- `strand init /tmp/mydata`
- `strand commit /tmp/mydata -m "initial"`
- `strand log /tmp/mydata`

For S3:
- `strand init s3://my-bucket/my-prefix`

## Next milestones
- Snapshot manifests for Parquet/Delta/Iceberg roots
- `diff` (between commits/branches)
- `merge` (conflict handling for dataset-level operations)
- Stronger ACID semantics using S3 conditional writes (ETag / If-Match)
