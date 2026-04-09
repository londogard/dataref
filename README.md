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
- Repository URI support via `--repo <path|s3://bucket/prefix>` and `open_repository(...)`.
- Streaming S3 imports (`fluxel import`) with `blake3` or metadata identity modes.
- Branch-scoped staging workflow (`fluxel add`, `fluxel rm`, `fluxel status`, `fluxel commit --staged`).
- Incremental ingress paths that preserve existing manifest entries while adding only new content metadata/blobs.
- Commit identity modes: `blake3` (default) and `meta` (`hash(path+size)`).
- Verify command to promote metadata-only entries to canonical blobs (`fluxel verify`).
- Zero-copy branch pointers (`fluxel branch`).
- Fast-forward-only branch merge (`fluxel merge`).
- Metadata-only diff between refs (`fluxel diff`).
- Metadata-only manifest mutations for committed refs (`fluxel rm -m ...`, `fluxel mv -m ... ...`).
- Disposable analytical index from manifest (`fluxel index build/query/drop`, DuckDB + optional Parquet export).
- `fsspec` provider for `fluxel://` URI reads.
- Local + S3 storage backend abstractions available in code.

### Not Fully Wired Yet

- No remote sync CLI (`push/pull/fetch`) yet.
- No `log/status/list/checkout` CLI surface yet.
- No `s5cmd` command-list generation path for bulk transfer yet.
- S3 branch locking now recovers expired stale lock objects automatically, but there is still no operator-facing lock inspection or cleanup command.

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

## License And Support

Fluxel is licensed under the GNU Affero General Public License v3.0 or later.

- The license keeps copyright and license notices attached to redistributed copies.
- Modified networked deployments must make their corresponding source available under the AGPL terms.
- That gives companies a practical reason to fund maintenance if they depend on Fluxel while keeping the project genuinely open source.

If your company uses Fluxel, sponsor ongoing maintenance at <https://github.com/sponsors/londogard>.

## Quickstart

```bash
mkdir -p /tmp/fluxel-demo
echo "hello" > /tmp/fluxel-demo/a.txt

uv run fluxel commit --repo /tmp/fluxel-demo -m "initial"
uv run fluxel commit --repo /tmp/fluxel-demo -m "fast metadata snapshot" --identity meta
uv run fluxel import --repo /tmp/fluxel-demo s3://my-bucket/bootstrap -m "bootstrap import"
uv run fluxel import --repo /tmp/fluxel-demo s3://my-bucket/bootstrap -m "metadata import" --identity meta
uv run fluxel import --repo /tmp/fluxel-demo s3://my-bucket/bootstrap -m "jpg subset" --path "**/*.jpg" --path root.csv
uv run fluxel import --repo /tmp/fluxel-demo s3://my-bucket/incremental -m "add new import batch"
uv run fluxel verify --repo /tmp/fluxel-demo --ref main

# branch-scoped staged flow
uv run fluxel branch --repo /tmp/fluxel-demo feature
uv run fluxel add --repo /tmp/fluxel-demo --ref feature data/new.csv
uv run fluxel add --repo /tmp/fluxel-demo --ref feature --as imports/raw.csv /tmp/outside-repo/raw.csv
uv run fluxel add --repo /tmp/fluxel-demo --ref feature --as imports/bundle /tmp/outside-repo/bundle
uv run fluxel add --repo /tmp/fluxel-demo --ref feature --identity meta --as imports/bootstrap.csv s3://my-bucket/bootstrap.csv
uv run fluxel add --repo /tmp/fluxel-demo --ref feature --identity meta --as imports/bootstrap s3://my-bucket/bootstrap
uv run fluxel status --repo /tmp/fluxel-demo --ref feature
uv run fluxel commit --repo /tmp/fluxel-demo --ref feature --staged -m "feature updates"
uv run fluxel merge --repo /tmp/fluxel-demo feature main

echo "hello v2" > /tmp/fluxel-demo/a.txt
uv run fluxel commit --repo /tmp/fluxel-demo -m "update"

uv run fluxel branch --repo /tmp/fluxel-demo experiment
uv run fluxel diff --repo /tmp/fluxel-demo <from_ref> <to_ref>
uv run fluxel rm --repo /tmp/fluxel-demo old-prefix -m "remove old files"
uv run fluxel mv --repo /tmp/fluxel-demo raw/images curated/images -m "rename image prefix"

# remote repo metadata operations from the current working tree
uv run fluxel branch --repo s3://my-bucket/datasets/demo feature
uv run fluxel commit --repo s3://my-bucket/datasets/demo -m "snapshot current working tree"
uv run fluxel rm --repo s3://my-bucket/datasets/demo obsolete -m "drop obsolete paths"
uv run fluxel mv --repo s3://my-bucket/datasets/demo bootstrap final -m "rename imported prefix"
```

## Analytical Index (Derived, Disposable)

```bash
uv run fluxel index build --repo /tmp/fluxel-demo --ref main --parquet
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
uv run fluxel verify --repo /tmp/fluxel-demo --ref main
uv run fluxel verify --repo /tmp/fluxel-demo --ref main --path images --path logs/2026
uv run fluxel verify --repo /tmp/fluxel-demo --ref main --dry-run
```

- Verifies all entries by default (or selected path prefixes with `--path`).
- `--dry-run` reports how many entries would be promoted without changing blobs/commits.
- Reads bytes from each entry's `source_uri`, computes Blake3, and stores canonical blob content.
- Writes a new commit only when at least one entry is promoted.

## Incremental Ingress

Fluxel's efficient content-ingress paths are:

```bash
uv run fluxel add --repo /tmp/fluxel-demo local/new.csv
uv run fluxel add --repo /tmp/fluxel-demo --as imports/new.csv /tmp/random/new.csv
uv run fluxel add --repo /tmp/fluxel-demo --as imports/new-batch /tmp/random/new-batch
uv run fluxel add --repo /tmp/fluxel-demo --identity meta --as imports/bootstrap.csv s3://my-bucket/bootstrap.csv
uv run fluxel add --repo /tmp/fluxel-demo --identity meta --as imports/bootstrap s3://my-bucket/bootstrap
uv run fluxel commit --repo /tmp/fluxel-demo --staged -m "add one file"
uv run fluxel import --repo /tmp/fluxel-demo s3://my-bucket/incremental -m "merge imported batch"
uv run fluxel verify --repo /tmp/fluxel-demo --ref main --path images --path root.txt
```

- `add` + `commit --staged` preserves the current branch manifest and reads bytes only for staged additions.
- `add` accepts repo-relative files, arbitrary local files, local directories, single S3 objects, and S3 prefixes; `--as` maps a single file/object to one logical path or remaps a directory/prefix under a destination prefix.
- `import` merges imported S3 entries into the current branch manifest instead of replacing the snapshot.
- `verify` reads bytes only for selected metadata-only entries that still need canonical blobs.
- Existing manifest entries are preserved without re-uploading unchanged blob content.

## S3 Integration Tests

Fluxel includes `integration`-marked tests for real S3-compatible behavior. The preferred target is Ministack.

Set these environment variables before running the suite:

```bash
export FLUXEL_MINISTACK_ENDPOINT=http://127.0.0.1:9000
export FLUXEL_MINISTACK_ACCESS_KEY=ministack
export FLUXEL_MINISTACK_SECRET_KEY=ministack123
export FLUXEL_MINISTACK_REGION=us-east-1
```

Then run:

```bash
uv run pytest tests/test_s3_integration.py -m integration
```

If `FLUXEL_MINISTACK_ENDPOINT` is unset or the endpoint is unreachable, the integration tests skip automatically.

## Merge Command

`fluxel merge` updates a target branch by fast-forward only:

```bash
uv run fluxel merge --repo /tmp/fluxel-demo feature main
```

- The source ref can be a branch or commit.
- The target ref must be a branch.
- The merge succeeds only when the target branch head is an ancestor of the source ref.
- Non-fast-forward merges are rejected.

## Metadata-Only Remove And Move

`fluxel rm` and `fluxel mv` can mutate committed refs directly by writing a new manifest and commit:

```bash
uv run fluxel rm --repo /tmp/fluxel-demo logs/2025 -m "remove old logs"
uv run fluxel mv --repo /tmp/fluxel-demo incoming/images curated/images -m "rename prefix"
uv run fluxel rm --repo s3://my-bucket/datasets/demo temp -m "drop temp data"
```

- These operations read manifest metadata only; they do not download unchanged blob payloads.
- `rm` accepts file paths or path prefixes and removes all matching logical entries.
- `mv` accepts a file path or prefix and rewrites matching logical paths in the manifest.
- Existing staged behavior remains available: `fluxel rm` without `-m/--message` still stages removals for `fluxel commit --staged`.

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
