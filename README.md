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
	- JSONL manifests map logical path -> hash + metadata.
	- Commit objects (JSON) and branch refs provide Git-like lineage semantics.
3. **Access Layer (`fsspec`)**
	- `fluxel://<dataset>@<branch_or_commit>/<path>` resolves metadata, then reads blob bytes.

## MVP Status (Current)

Fluxel is intentionally in MVP mode.

### Implemented

- Commit snapshots over a dataset root (`fluxel commit`).
- Zero-copy branch pointers (`fluxel branch`).
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
```

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