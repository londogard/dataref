# Fluxel

Fluxel is a serverless (client-first), object-storage-first data versioning engine.

The design is deliberately opinionated: keep canonical data storage boring and immutable, and put intelligence in metadata and access layers.

## Guardrails (Strict)

- Do not optimize canonical blob storage for ML throughput (no tarball/parquet/sharded blob layer).
- Do not read blob payloads for metadata-only operations (`diff`, `list`, `log`, `status`).
- Do not introduce a server/daemon/central database.
- Use Blake3 for all content hashing.
- Prefer JSONL manifests for stream-safe, O(1)-memory behavior.

## Core Philosophy

Fluxel separates the platform into three layers:

1. **Canonical Layer (`blobs/`)**
	- Content-addressed objects keyed by Blake3 digest.
	- Physical layout is simple and deterministic (`<hash[:2]>/<hash[2:]>`).
2. **Metadata Layer (`manifests/`, `commits/`, `refs/`)**
	- JSONL manifests map logical path -> identity + metadata.
	- Commit objects (JSON) and branch refs provide Git-like lineage semantics.
3. **Access Layer (`fsspec`)**
	- `fluxel://<dataset>@<branch_or_commit>/<path>` resolves metadata, then reads either canonical blob bytes or source URI bytes for metadata-only entries.

## MVP Status (Current)

Fluxel is intentionally in MVP mode.

### Implemented

- Commit snapshots over a dataset root (`fluxel commit`).
- Streaming S3 imports (`fluxel import`) with `blake3` or metadata identity modes.
- Branch-scoped staging workflow (`fluxel add`, `fluxel rm`, `fluxel status`, `fluxel commit --staged`).
- Commit identity modes: `blake3` (default) and `meta` (`hash(path+size)`).
- Verify command to promote metadata-only entries to canonical blobs (`fluxel verify`).
- Zero-copy branch pointers (`fluxel branch`).
- Fast-forward-only branch merge (`fluxel merge`).
- Metadata-only diff between refs (`fluxel diff`).
- Disposable analytical index from manifest (`fluxel index build/query/drop`, DuckDB + optional Parquet export).
- `fsspec` provider for `fluxel://` URI reads.
- Local + S3 storage backend abstractions available in code.

### Not Fully Wired Yet

- Repository commands still use local-path flow as primary execution path.
- No remote sync CLI (`push/pull/fetch`) yet.
- No `log/status/list/checkout` CLI surface yet.
- No `s5cmd` command-list generation path for bulk transfer yet.

## Technical Stack

- Python 3.11+
- `blake3` for hashing
- JSONL manifests + JSON commit objects
- `fsspec` for URI access abstraction
- `duckdb` for disposable analytical indexing

## Install

```bash
uv sync
uv run fluxel --help
```

## Quickstart

```bash
mkdir -p /tmp/fluxel-demo
echo "hello" > /tmp/fluxel-demo/a.txt

uv run fluxel commit --root /tmp/fluxel-demo -m "initial"
uv run fluxel commit --root /tmp/fluxel-demo -m "fast metadata snapshot" --identity meta
uv run fluxel import --root /tmp/fluxel-demo s3://my-bucket/bootstrap -m "bootstrap import"
uv run fluxel import --root /tmp/fluxel-demo s3://my-bucket/bootstrap -m "metadata import" --identity meta
uv run fluxel import --root /tmp/fluxel-demo s3://my-bucket/bootstrap -m "jpg subset" --path "**/*.jpg" --path root.csv
uv run fluxel verify --root /tmp/fluxel-demo --ref main

# branch-scoped staged flow
uv run fluxel branch --root /tmp/fluxel-demo feature
uv run fluxel add --root /tmp/fluxel-demo --ref feature data/new.csv
uv run fluxel status --root /tmp/fluxel-demo --ref feature
uv run fluxel commit --root /tmp/fluxel-demo --ref feature --staged -m "feature updates"
uv run fluxel merge --root /tmp/fluxel-demo feature main

echo "hello v2" > /tmp/fluxel-demo/a.txt
uv run fluxel commit --root /tmp/fluxel-demo -m "update"

uv run fluxel branch --root /tmp/fluxel-demo experiment
uv run fluxel diff --root /tmp/fluxel-demo <from_ref> <to_ref>
```

## Analytical Index (Derived, Disposable)

```bash
uv run fluxel index build --root /tmp/fluxel-demo --ref main --parquet
uv run fluxel index query --db /path/to/<commit>.duckdb --sql "SELECT COUNT(*) FROM files"
uv run fluxel index drop --db /path/to/<commit>.duckdb
```

If the index is deleted, Fluxel remains fully functional from manifests and commits.

## `fsspec` URI Example

```python
from fluxel.core import FluxelFileSystem

fs = FluxelFileSystem(dataset_roots={"my_data": "/tmp/fluxel-demo"})
with fs.open("fluxel://my_data@main/a.txt", "rb") as handle:
	 data = handle.read()

# include branch staged (not-yet-committed) changes
with fs.open("fluxel://my_data@feature+staged/a.txt", "rb") as handle:
	 staged_data = handle.read()
```

In `--identity meta` snapshots, Fluxel reads from `source_uri` when no canonical `blobs/` object exists.

## Identity Modes

`fluxel commit` supports two identity modes:

- `--identity blake3` (default)
	- Reads file bytes.
	- Stores canonical blob in `.fluxel/blobs/`.
	- Manifest entry includes `identity_mode=blake3`, `identity_value`, and `blob_hash`.

- `--identity meta`
	- Does not read file bytes.
	- Computes identity as `blake3("<relative_path>\n<size>")`.
	- Stores no canonical blob (`blob_hash=null`) and keeps `source_uri` for reads.

This is useful for large bootstrap imports where strong content verification can be deferred.

## Verify Command

`fluxel verify` promotes metadata-only (`--identity meta`) manifest entries into canonical `blake3` blob-backed entries:

```bash
uv run fluxel verify --root /tmp/fluxel-demo --ref main
uv run fluxel verify --root /tmp/fluxel-demo --ref main --path images --path logs/2026
uv run fluxel verify --root /tmp/fluxel-demo --ref main --dry-run
```

- Verifies all entries by default (or selected path prefixes with `--path`).
- `--dry-run` reports how many entries would be promoted without changing blobs/commits.
- Reads bytes from each entry's `source_uri`, computes Blake3, and stores canonical blob content.
- Writes a new commit only when at least one entry is promoted.

## Merge Command

`fluxel merge` updates a target branch by fast-forward only:

```bash
uv run fluxel merge --root /tmp/fluxel-demo feature main
```

- The source ref can be a branch or commit.
- The target ref must be a branch.
- The merge succeeds only when the target branch head is an ancestor of the source ref.
- Non-fast-forward merges are rejected.

## Repository Layout

Fluxel creates `.fluxel/` under each dataset root:

- `blobs/` - canonical content-addressed object store
- `manifests/` - JSONL path->hash snapshots
- `commits/` - commit metadata objects
- `refs/heads/` - branch pointers
- `refs/HEAD` - symbolic active branch reference (default `main`)

## Mandatory Validation Coverage

Current tests cover required invariants:

- Metadata-only diff reads no blob payloads.
- Manifest generation for 100k entries stays under RAM cap.
- `fluxel://my_data@main/test.csv` resolves and returns expected bytes.

Run test suite:

```bash
uv run pytest tests
```
