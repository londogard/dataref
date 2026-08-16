# Dataref

Dataref is a serverless (client-first), object-storage-first data versioning engine.

The design is deliberately opinionated: keep canonical data storage boring and immutable, and put intelligence in metadata and access layers.

## Guardrails (Strict)

- Do not optimize canonical blob storage for ML throughput (no tarball/parquet/sharded blob layer).
- Do not read blob payloads for metadata-only operations (`diff`, `list`, `log`, `status`).
- Do not introduce a server/daemon/central database.
- Use Blake3 for all content hashing.
- Prefer JSONL manifests for stream-safe, O(1)-memory behavior.

## Core Philosophy

Dataref separates the platform into three layers:

1. **Canonical Layer (`blobs/`)**
	- Content-addressed objects keyed by Blake3 digest.
	- Physical layout is simple and deterministic (`<hash[:2]>/<hash[2:]>`).
2. **Metadata Layer (`manifests/`, `commits/`, `refs/`)**
	- JSONL manifests map logical path -> identity + metadata.
	- Commit objects (JSON) and branch refs provide Git-like lineage semantics.
3. **Access Layer (`fsspec`)**
	- `dataref://<dataset>@<branch_or_commit>/<path>` resolves metadata, then reads either canonical blob bytes or source URI bytes for metadata-only entries.

## MVP Status (Current)

Dataref is intentionally in MVP mode.

### Implemented

- Commit snapshots over a dataset root (`dataref commit`).
- Repository URI support via `--repo <path|s3://bucket/prefix>` and `open_repository(...)`.
- Repository initialization (`dataref init`) with local or S3 backend.
- Remote sync workflow (`dataref fetch`, `dataref pull`, `dataref push`).
- Operator-facing S3 lock inspection and cleanup (`dataref lock list`, `dataref lock cleanup`).
- Streaming S3 ingress from `s3://` objects and prefixes via staged `dataref add ... --identity meta --as ...` followed by `dataref commit --staged`.
- Branch-scoped staging workflow (`dataref add`, `dataref rm`, `dataref status`, `dataref commit --staged`).
- Incremental ingress paths that preserve existing manifest entries while adding only new content metadata/blobs.
- Commit identity modes: `blake3` (default) and `meta` (`hash(path+size)`).
- Verify command to promote metadata-only entries to canonical blobs (`dataref verify`).
- Zero-copy branch pointers (`dataref branch`).
- Fast-forward-only branch merge (`dataref merge`).
- Metadata-only diff between refs (`dataref diff`).
- Metadata-only manifest mutations for committed refs via the staged flow (`dataref rm ...`, `dataref mv ... ...`, then `dataref commit --staged`).
- File restoration from refs (`dataref restore <ref> [--path ...] [--force]`).
- Disposable analytical index from manifest (`dataref index build`, DuckDB + optional Parquet export).
- `fsspec` provider for `dataref://` URI reads.
- Local + S3 storage backend abstractions available in code.
- Human-readable output by default; `--json` flag on all commands for programmatic use.

## Technical Stack

- Python 3.11+
- `blake3` for hashing
- JSONL manifests + JSON commit objects
- `fsspec` for URI access abstraction
- `duckdb` for disposable analytical indexing

## Install
```bash
uv pip install dataref
```

### Developer mode in repo
```bash
uv sync
uv run dataref --help
```

## License And Support

Dataref is licensed under the GNU Affero General Public License v3.0 or later.

- The license keeps copyright and license notices attached to redistributed copies.
- Modified networked deployments must make their corresponding source available under the AGPL terms.
- That gives companies a practical reason to fund maintenance if they depend on Dataref while keeping the project genuinely open source.

If your company uses Dataref, sponsor ongoing maintenance at <https://github.com/sponsors/londogard>.

## Quickstart

```bash
# Initialize a new repository
mkdir -p /tmp/dataref-demo
uv run dataref init --repo /tmp/dataref-demo
# Or with S3 backend: uv run dataref init --repo /tmp/dataref-demo --backend s3 --s3-bucket my-bucket

echo "hello" > /tmp/dataref-demo/a.txt

uv run dataref commit --repo /tmp/dataref-demo -m "initial"

# Stage an S3 prefix as metadata-only entries, then commit the staged additions
uv run dataref add --repo /tmp/dataref-demo --identity meta --as imports/bootstrap s3://my-bucket/bootstrap
uv run dataref commit --repo /tmp/dataref-demo --staged -m "metadata import"
uv run dataref verify --repo /tmp/dataref-demo

# branch-scoped staged flow
uv run dataref branch --repo /tmp/dataref-demo feature
uv run dataref checkout --repo /tmp/dataref-demo feature
uv run dataref add --repo /tmp/dataref-demo data/new.csv
uv run dataref add --repo /tmp/dataref-demo --as imports/raw.csv /tmp/outside-repo/raw.csv
uv run dataref add --repo /tmp/dataref-demo --as imports/bundle /tmp/outside-repo/bundle
uv run dataref add --repo /tmp/dataref-demo --identity meta --as imports/bootstrap.csv s3://my-bucket/bootstrap.csv
uv run dataref add --repo /tmp/dataref-demo --identity meta --as imports/bootstrap s3://my-bucket/bootstrap
uv run dataref status --repo /tmp/dataref-demo
uv run dataref commit --repo /tmp/dataref-demo --staged -m "feature updates"
uv run dataref checkout --repo /tmp/dataref-demo main
uv run dataref merge --repo /tmp/dataref-demo feature main

# restore files from a ref
uv run dataref restore --repo /tmp/dataref-demo main
uv run dataref restore --repo /tmp/dataref-demo main --path data/new.csv
uv run dataref restore --repo /tmp/dataref-demo main --force

echo "hello v2" > /tmp/dataref-demo/a.txt
uv run dataref commit --repo /tmp/dataref-demo -m "update"

uv run dataref branch --repo /tmp/dataref-demo experiment
uv run dataref diff --repo /tmp/dataref-demo <from_ref> <to_ref>

# Stage and commit metadata mutations
uv run dataref rm --repo /tmp/dataref-demo old-prefix
uv run dataref mv --repo /tmp/dataref-demo raw/images curated/images
uv run dataref status --repo /tmp/dataref-demo
uv run dataref commit --repo /tmp/dataref-demo -m "clean up old files and rename image prefix"

# Or stage the mutation and commit it with a single message
uv run dataref rm --repo /tmp/dataref-demo logs/2025
uv run dataref mv --repo /tmp/dataref-demo incoming/images curated/images
uv run dataref commit --repo /tmp/dataref-demo --staged -m "remove old logs and rename prefix"

# remote repo metadata operations from the current working tree
uv run dataref branch --repo s3://my-bucket/datasets/demo feature
uv run dataref commit --repo s3://my-bucket/datasets/demo -m "snapshot current working tree"
uv run dataref rm --repo s3://my-bucket/datasets/demo obsolete
uv run dataref mv --repo s3://my-bucket/datasets/demo bootstrap final
uv run dataref commit --repo s3://my-bucket/datasets/demo --staged -m "drop obsolete paths and rename imported prefix"

# JSON output for programmatic use (all commands support --json)
uv run dataref status --repo /tmp/dataref-demo --json
uv run dataref add --repo /tmp/dataref-demo data/new.csv --json
uv run dataref diff --repo /tmp/dataref-demo main feature --json
```

## Analytical Index (Derived, Disposable)

```bash
uv run dataref index build --repo /tmp/dataref-demo --parquet
```

`dataref index build` writes a DuckDB database (and optional Parquet export) for the current branch's manifest to `.dataref/index/<commit_id>.duckdb`. Query it with the DuckDB CLI:

```bash
duckdb /path/to/<commit>.duckdb "SELECT COUNT(*) FROM files"
```

If the index is deleted, Dataref remains fully functional from manifests and commits.

## `fsspec` URI Example

```python
from dataref.core import DatarefFileSystem

fs = DatarefFileSystem(dataset_roots={"my_data": "/tmp/dataref-demo"})
with fs.open("dataref://my_data@main/a.txt", "rb") as handle:
	 data = handle.read()

# include branch staged (not-yet-committed) changes
with fs.open("dataref://my_data@feature+staged/a.txt", "rb") as handle:
	 staged_data = handle.read()
```

In `meta` snapshots, Dataref reads from `source_uri` when no canonical `blobs/` object exists.

## Identity Modes

Dataref supports two identity modes for manifest entries:

- `blake3` (default)
	- Reads file bytes.
	- Stores canonical blob in `.dataref/blobs/`.
	- Manifest entry includes `identity_mode=blake3`, `identity_value`, and `blob_hash`.

- `meta`
	- Does not read file bytes.
	- Computes identity as `blake3("<relative_path>\n<size>")`.
	- Stores no canonical blob (`blob_hash=null`) and keeps `source_uri` for reads.

Set the mode per staged addition with `dataref add --identity meta`, or set the
repository-wide default for `dataref commit` with `dataref config set identity meta`.

This is useful for large bootstrap imports where strong content verification can be deferred.

### Durability contract for `meta`

Metadata-only (`meta`) revisions are **unverifiable**: the entry's
identity is derived from path and size, not from content bytes. Until you run
`dataref verify`, Dataref cannot prove that the content at `source_uri` matches
what was originally imported.

**Warnings.** The CLI emits a warning to stderr whenever you stage with
`--identity meta` or commit a repository whose identity is configured to `meta`,
and after `verify` reports how many unverifiable entries remain.

**Source-retention policy.** Because metadata-only entries have no canonical
blob, you **must** retain the source objects at their original `source_uri`
until the entry has been promoted via `dataref verify`. If a source object is
deleted, overwritten, or moved before verification, the corresponding manifest
entry becomes irrecoverable — no content can be read and no hash can be
validated.

**Promotion to verifiable.** Run `dataref verify` to read every metadata-only
entry's source blob, compute a Blake3 content hash, store the canonical blob,
and rewrite the manifest entry in `blake3` mode. After promotion the source
retention requirement is lifted for those entries.

**Lifecycle summary:**

| State | `identity_mode` | `blob_hash` | Can read? | Can prove integrity? | Source required? |
|---|---|---|---|---|---|
| Metadata-only | `meta` | `null` | ✅ (from `source_uri`) | ❌ | ✅ |
| Verified | `blake3` | hash | ✅ (from `blobs/`) | ✅ | ❌ |

## Verify Command

`dataref verify` promotes metadata-only (`meta`) manifest entries of the current branch into canonical `blake3` blob-backed entries:

```bash
uv run dataref verify --repo /tmp/dataref-demo
uv run dataref verify --repo /tmp/dataref-demo --path images --path logs/2026
uv run dataref verify --repo /tmp/dataref-demo --dry-run
```

- Verifies all entries by default (or selected path prefixes with `--path`).
- `--dry-run` reports how many entries would be promoted without changing blobs/commits.
- Reads bytes from each entry's `source_uri`, computes Blake3, and stores canonical blob content.
- Writes a new commit only when at least one entry is promoted.

## Incremental Ingress

Dataref's efficient content-ingress paths are:

```bash
uv run dataref add --repo /tmp/dataref-demo local/new.csv
uv run dataref add --repo /tmp/dataref-demo --as imports/new.csv /tmp/random/new.csv
uv run dataref add --repo /tmp/dataref-demo --as imports/new-batch /tmp/random/new-batch
uv run dataref add --repo /tmp/dataref-demo --identity meta --as imports/bootstrap.csv s3://my-bucket/bootstrap.csv
uv run dataref add --repo /tmp/dataref-demo --identity meta --as imports/bootstrap s3://my-bucket/bootstrap
uv run dataref commit --repo /tmp/dataref-demo --staged -m "add one file"
uv run dataref verify --repo /tmp/dataref-demo --path images --path root.txt
```

- `add` + `commit --staged` preserves the current branch manifest and reads bytes only for staged additions.
- `add` accepts repo-relative files, arbitrary local files, local directories, single S3 objects, and S3 prefixes; `--as` maps a single file/object to one logical path or remaps a directory/prefix under a destination prefix.
- `verify` reads bytes only for selected metadata-only entries that still need canonical blobs.
- Existing manifest entries are preserved without re-uploading unchanged blob content.

## S3 Integration Tests

Dataref includes `integration`-marked tests for real S3-compatible behavior. The preferred target is Ministack.

For the standard local workflow, run a single command from the repository root:

```bash
bash scripts/run_s3_integration.sh
```

That script starts a temporary Ministack container on `127.0.0.1:4566`, waits for the health endpoint, resets emulator state, runs `tests/test_s3_integration.py`, and cleans up the container when the test run finishes.

If you prefer task-runner aliases, the repo also provides:

```bash
make test-s3-integration
```

GitHub Actions runs the same script in the dedicated S3 integration job.

Start Ministack locally:

```bash
docker run --rm -p 4566:4566 nahuelnucera/ministack
```

If you also want MiniStack features that launch real sidecar containers such as RDS, ECS, or Docker-backed Lambda runtimes, mount the Docker socket:

```bash
docker run --rm -p 4566:4566 -v /var/run/docker.sock:/var/run/docker.sock nahuelnucera/ministack
```

Verify the emulator is ready:

```bash
curl http://127.0.0.1:4566/_ministack/health
```

Then set these environment variables before running the suite:

```bash
export DATAREF_MINISTACK_ENDPOINT=http://127.0.0.1:4566
export DATAREF_MINISTACK_ACCESS_KEY=test
export DATAREF_MINISTACK_SECRET_KEY=test
export DATAREF_MINISTACK_REGION=us-east-1
```

Dataref's integration fixture already uses path-style boto3 S3 addressing, so no extra S3 client flags are needed.

Then run:

```bash
uv run pytest tests/test_s3_integration.py -m integration
```

If `DATAREF_MINISTACK_ENDPOINT` is unset or the endpoint is unreachable, the integration tests skip automatically.

To wipe the local emulator state between runs without restarting the container:

```bash
curl -X POST http://127.0.0.1:4566/_ministack/reset
```

## Merge Command

`dataref merge` updates a target branch by fast-forward only:

```bash
uv run dataref merge --repo /tmp/dataref-demo feature main
```

- The source ref can be a branch or commit.
- The target ref must be a branch.
- The merge succeeds only when the target branch head is an ancestor of the source ref.
- Non-fast-forward merges are rejected.

## Metadata-Only Remove And Move

`dataref rm` and `dataref mv` stage metadata-only mutations; `dataref commit --staged` writes a new manifest and commit:

```bash
uv run dataref rm --repo /tmp/dataref-demo logs/2025
uv run dataref mv --repo /tmp/dataref-demo incoming/images curated/images
uv run dataref commit --repo /tmp/dataref-demo --staged -m "remove old logs and rename prefix"
```

- These operations read manifest metadata only; they do not download unchanged blob payloads.
- `rm` accepts file paths or path prefixes and removes all matching logical entries.
- `mv` accepts a file path or prefix and rewrites matching logical paths in the manifest.
- `dataref status` shows staged removals and renames before `dataref commit --staged`.

## Repository Layout

Dataref creates `.dataref/` under each dataset root:

- `blobs/` - canonical content-addressed object store
- `manifests/` - JSONL path->hash snapshots
- `commits/` - commit metadata objects
- `refs/heads/` - branch pointers
- `refs/HEAD` - symbolic active branch reference (default `main`)

## Mandatory Validation Coverage

Current tests cover required invariants:

- Metadata-only diff reads no blob payloads.
- Manifest generation for 100k entries stays under RAM cap.
- `dataref://my_data@main/test.csv` resolves and returns expected bytes.

Run test suite:

```bash
uv run pytest tests
```
