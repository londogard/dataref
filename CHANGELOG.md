# Changelog

All notable changes to this project will be documented in this file.

The format is based on Keep a Changelog, and Dataref currently tracks changes before its first public alpha release.

## Unreleased

## [0.1.0] - 2026-08-16

### Added

- **Footer-stats pruning engine (rev. 3, docs/architecture.md §4)**: `dataref query
  prune <ref> <path> --where "col >= x"` selects the parquet row groups that may
  match an AND-composed predicate (`= != < <= > >= IS NULL IS NOT NULL`) using only
  the compact `footers/<hash>` stats objects — no data bytes are read. Exposed as
  `parse_where_clause` / `prune_row_groups` / `plan_pruned_scan` in
  `core/query/pruning.py`; unknown columns and type mismatches keep row groups
  conservatively.
- **Store unification (rev. 3, §12)**: `core/repository_store/` and `core/storage/`
  are merged into `core/objects/` — one `ObjectStore` protocol
  (`objects/base.py`), two adapters (`LocalObjectStore`, `S3ObjectStore`), plus
  `backends.py` (storage/transfer protocols), `source.py` (source-URI access,
  `S3StorageBackend`), and `transfer.py` (boto3/s5cmd blob transfer).
- **Parquet footer capture (P1)**: with `config set parquet_footer true`, parquet
  files get a compact footer-stats object (schema hash + per-row-group column
  min/max/nulls, hand-decoded from the thrift-compact `FileMetaData`) stored
  under `footers/<hash>` and referenced from new `bp`/`mp` tree entries —
  enabling row-group pruning without reading object bytes. Unchanged files are
  backfilled on the next commit; `verify`-style re-scan can backfill later.
- `dataref cat <ref> <path>` prints a file's bytes from a ref; `list`, `cat`,
  `diff`, `log`, and `query` work on virtual refs (S3 URIs) without a local
  worktree.
- Streaming S3 import with metadata identity mode and repeatable path filters.
- Staged add support for arbitrary local files, directories, S3 objects, and S3 prefixes.
- Real S3 integration coverage for remote repository flows.
- AGPL-3.0-or-later licensing, attribution notice, and funding metadata for the first public release.
- **Tree-based object model (v2, P0 of `docs/architecture.md`)**: commits now
  build Merkle tree DAGs (`trees/`) instead of full JSONL manifests. Exact
  lookups descend the path chain through a client-side content-addressed
  tree cache (0–1 GETs warm); prefix/bulk listings stream from pruned
  subtree walks (O(matches + depth)); the `.idx` sidecar and embedded
  `commit.manifest_index` are gone.
- Derived manifests: a tree can be flattened to a JSONL manifest with block
  offsets, cached per-client keyed by the root-tree hash, for
  latency-critical point lookups (`export_derived_manifest`,
  `lookup_derived_entry`).
- Directory fanout is bounded (10k entries); oversized directories split
  into name-range shard subtrees, keeping tree depth ≤ ~4–5 at 100M entries.
- `commit --staged` re-applies the staged overlay onto a new parent and
  retries the CAS when a peer advanced the branch (P0 conflict-retry
  contract, `docs/architecture.md` §6).

### Changed

- **3-way metadata merge (P3, §9)**: diverged branches merge instead of
  erroring — `merge` computes the merge base (LCA), streams base/ours/theirs
  trees with standard merge rules (auto-keep identical additions, take the
  changed side, drop double-removals), and creates a merge commit with two
  parents when histories diverge; conflicting paths raise
  `MergeConflictError` with the offending paths.
- `dataref reflog` (P3): every successful ref update is recorded per-branch in
  client state (old → new commit, operation, timestamp); `dataref catalog`
  lists branches with their heads and messages.
- **Plan-then-batch sync (P2, §7)**: `push`/`pull`/`fetch` now compute the exact
  missing-object set (commits, trees, footers, blobs) as a transfer plan and
  execute it in one batch through the s5cmd backend (manifest file) or
  per-object through boto3, with an optional progress callback. Parquet
  footer stats objects now sync too.
- **Adapter error translation (P2, §8)**: the S3 adapters raise domain errors
  (`ObjectMissingError`, `PreconditionFailedError`, `StorageUnavailableError`,
  all `DatarefError` subclasses) instead of raw botocore exceptions; the CLI no
  longer special-cases `BotoCoreError`/`ClientError`.
- `dataref gc` (P2, §11): audit-only by default — computes the reachable set
  from all refs (commit DAG → trees → blobs/footers) and reports orphans;
  `--prune` deletes them.
- `filesystem.py` moved to the `core/vfs/` package; `index.py` moved to the
  `core/query/` package (DuckDB catalog now carries a `footer` column); the
  CLI subcommand `index build` is now `query build`.
- `CommitObject` is now `{id, message, tree, parents: [...], created_at,
  branch, generation}`; the `manifest`/`parent`/`manifest_index` fields are
  gone. `SnapshotWriter` is replaced by `TreeWriter`
  (`services/tree.py`).
- Stores are `ObjectStore + RefStore + TreeQuerier`; S3 branch-lock
  machinery (locks, `lock list`, `lock cleanup`) is removed — version-token
  CAS is the only safety primitive.
- Full commits preserve parent-only entries and staged additions (v1 merge
  semantics); deletions require explicit staging, as before.
- Commit creation is faster than v1 in metadata mode (no per-file entry
  objects; ~48k files/sec on the 200k-file meta benchmark).
- CLI examples and tests now prefer `--repo` repository selection semantics.
- Client-local state writes now use atomic replace semantics for HEAD and staging payloads.
- Manifest index prefix iteration now streams rows instead of materializing full result sets.
- Manifest exact-path and prefix lookups now fall back to a full manifest scan when no index is available, so legacy or index-less repositories remain fully readable.
- `status(ref=...)` working-tree comparison now honors the requested branch instead of always comparing against the currently checked-out branch.
- The `RepositoryStore` contract is now split into focused `ObjectStore`, `RefStore`, `ManifestIndexStore`, and `ManifestQuerier` protocols, with `RepositoryStore` kept as the composed facade for compatibility.
- Manifest index lookups are deduplicated into a single shared implementation (`repository_store/manifest_query.py`) used by both local and S3 stores.
- Commit creation no longer relies on `_last_manifest_index` instance state; the manifest index is threaded explicitly through manifest writing and commit creation.
- S3 blob writes via streams (`push`/`pull`/`fetch`) now honor the configured blob transfer backend, so `s5cmd` accelerates sync transfers.
- The in-process commit cache is now bounded (LRU-style) instead of growing without limit.
- `DatarefRepository` is now a facade over four focused collaborator services (`RefManager`, `SnapshotWriter`, `StagingArea`, `EntryFactory` in `core/services/`); the god-object class was split from ~1650 to ~790 lines and `repository_ops.py` no longer reaches into repository internals.

### Removed

- Removed `manifest_index.py`, `manifest_query.py`, `.idx` sidecars, the embedded manifest index, and the index/fallback decision tree.
- Removed the `dataref import` command; S3 ingress is now staged via `dataref add ... s3://... [--identity meta --as ...]` followed by `dataref commit --staged`.
- Removed `--identity` from `dataref commit`; the commit identity mode is now configured with `dataref config set identity meta`, or set per stage with `dataref add --identity ...`.
- Removed the direct-commit `-m/--message` flag from `dataref rm` and `dataref mv`; metadata-only mutations are staged and committed with `dataref commit --staged`.
- Removed `--ref` from `dataref verify` and `dataref index build`; both now target the current branch.
- Removed `dataref index query` and `dataref index drop`; `dataref index build` remains and the produced DuckDB database can be queried with the DuckDB CLI.

### Fixed

- Manifest parsing now validates entry shape, digests, and metadata-only invariants with line-aware errors.
- Commit creation is back to near-pre-refactor throughput: the commit hot loop now streams serialized manifest lines from the worktree instead of constructing and validating a `ManifestEntry` object per file (~2× faster for metadata-only commits; validated again on read).
- CLI commands now return clean `... error:` messages for common validation, filesystem, and object-storage failures instead of raw tracebacks.
- Corrupt S3-hosted manifest lines now surface actionable validation errors.
