# Spec

## Overview

**Fluxel** is a serverless, object-storage-first data versioning engine. It uses Git-like semantics (commits, branches, merges, diffs, staging) optimised for data, not code: manifests over tree objects, content-addressed blob storage, streamable JSONL formats, and DuckDB-powered analytical indexes.

Key design choices:
- **No server** — 100% client-side.
- **Object-storage-native** — S3 is a first-class backend.
- **Metadata-first** — `diff`, `list`, `log`, `status`, `rm`, `mv` never read blob payloads.
- **Blake3** for all hashing (64 KB streaming chunks).
- **JSONL manifests** for O(1)-memory streaming through sorted entry iterators.

---

## Operations

| Command | Approx. Big-O | Path (files traversed) | Description |
|---------|--------------|------------------------|-------------|
| `flx add <path>` | O(1) per path | `cli.py` → `repository.add()` → `client_state.add_staging()` | Stages a file for commit. Two modes: `hash` (blake3 of content) or `meta` (blake3 of `"path\nsize"`). Appends to an in-memory staging dict; no disk I/O. |
| `flx rm <path>` | O(1) per path | `cli.py` → `repository.rm()` → `client_state.remove_staging()` | Stages a file for removal. Same staging mechanism as `add`. |
| `flx mv <src> <dst>` | O(1) per path | `cli.py` → `repository.move()` → `client_state.add_staging()` + `client_state.remove_staging()` | Adds a staged `add` for dst and staged `rm` for src. |
| `flx commit` | O(N) — N = files to commit | `cli.py` → `repository.commit()` → `manifest.walk_files()` / `manifest.ManifestWriter` + `manifest_index.build_manifest_index()` + `repository_store.write_manifest()` + `repository_store.write_commit()` + `client_state.clear_staging()` | Walks files (or reads staged changes), hashes content (or derives meta identity), writes blobs to `hash[:2]/hash[2:]`, writes JSONL manifest + `.idx` sidecar + commit object. Streaming merge sort for `--staged`. |
| `flx branch` (create) | O(1) | `cli.py` → `repository.branch()` → `repository_store.compare_and_set_branch_ref()` | Creates branch ref via CAS (S3: optimistic lock with Etag / IfNoneMatch:*). |
| `flx branch` (delete) | O(1) | `cli.py` → `repository.delete_branch()` → `repository_store.delete_branch_ref()` | Deletes branch ref file. |
| `flx checkout <branch>` | O(1) | `cli.py` → `repository.checkout()` → `client_state.write_head()` | Rewrites HEAD file (atomic rename). No data movement. |
| `flx merge <branch>` | O(M + N) — M,N = manifest entries | `cli.py` → `repository.merge()` → `_merge_sorted_entry_streams()` | Streaming merge of two sorted manifest iterators. Fast-forward if possible (O(1) ref write). Otherwise 3-way merge. |
| `flx log` | O(C) — C = commits in chain | `cli.py` → `repository.log()` → `repository_store.read_commit()` (walk parent chain) | Follows parent pointers from HEAD, reading commit objects. |
| `flx status` | O(W + S) — W = working tree, S = staging | `cli.py` → `repository.status()` → `manifest.walk_files()` + `client_state.read_staging()` | Compares working tree against HEAD manifest and staging. |
| `flx diff <a> <b>` | O(M + N) — M,N = manifest entries | `cli.py` → `repository.diff()` → `manifest.ManifestReader` (JSONL streaming) | Streaming merge of two manifest iterators (like `diff -u`). No blob reads. O(1) memory. |
| `flx list [prefix]` | O(K + log N) — K = matched entries | `cli.py` → `repository.list_files()` → `manifest_index.lookup_manifest_index_entry_json()` (sparse index) | Binary search on blocked sidecar index + byte-range block scan. S3: Range GET requests. |
| `flx lookup <path>` | O(log N) | `cli.py` → `repository.lookup_entry()` → `manifest_index.lookup_manifest_index_entry_json()` | Sparse index binary search (bisect_right over block first-paths), then linear scan of one block (~4096 entries). |
| `flx verify` (dry-run) | O(N) — N = manifest entries | `cli.py` → `repository.verify()` → manifest reader | Reads manifest entries, checks blob existence (no hash recompute). |
| `flx verify` (full) | O(N × F) — F = file size | `cli.py` → `repository.verify()` → `hashing.blake3_digest_file()` | Streams each blob through Blake3 and compares against stored hash. |
| `flx import <s3://...>` | O(N × F) | `cli.py` → `repository.import_s3()` → `storage.S3BlobTransferBackend` + streaming merge sort | Downloads objects from S3, hashes, stores blobs, creates manifest + sidecar index + commit. |
| `flx index build` | O(N) + DuckDB time | `cli.py` → `index.build_index()` → DuckDB CSV import | Exports manifest to CSV, imports into DuckDB, builds analytical parquet index. |
| `flx index query <sql>` | DuckDB-optimised | `cli.py` → `index.query_index()` → DuckDB | Runs SQL against DuckDB-backed parquet index. |
| `flx transfer <dest>` (S3→S3) | O(N × F) | `cli.py` → `repository.generate_transfer_commands()` → `storage.S3BlobTransferBackend` or `S5CmdBlobTransferBackend` | Generates bulk transfer commands (aws s3 cp / s5cmd). |

---

## Key Data Structures

| Structure | File | Format | Purpose |
|-----------|------|--------|---------|
| Manifest | `manifest.py` | JSONL (compact array) | Sorted list of file entries for a commit. One file per line. |
| Sidecar Index | `manifest_index.py` | Binary sidecar (`.idx`) | Sparse index over manifest: ~4096 entries per block, binary search over first-paths. Enables O(log N) lookups. |
| Commit Object | `repository.py:CommitObject` | JSON | `{id, message, manifest, parent, created_at, branch}`. |
| Stage Changes | `client_state.py` | In-memory dict + JSONL staging file | Pending `add`/`remove` operations before commit. |
| Blob Storage | `layout.py` | `hash[:2]/hash[2:]` sharded files | Content-addressed blob storage. Deduplication via `if_missing` write. |
| Branch Ref | `repository_store.py` | JSON file | `{branch, commit_id, version_token}`. |

---

## Storage Layout (`.fluxel/`)

```
.fluxel/
  blobs/             # Content-addressed blobs (hash[:2]/hash[2:])
  commits/           # Commit objects (JSON)
  manifests/         # JSONL manifests + .idx sidecar indexes
  staging/           # Staging area for pending changes
  refs/
    heads/           # Branch references
    locks/           # S3 optimistic lock objects (S3 only)
```

---

## Key Design Decisions

- **Identity modes**: `hash` (blake3 of content, deduplicating) vs `meta` (blake3 of `"path\nsize"`, metadata-only, no blob storage).
- **Sharding**: Blobs stored as `hash[:2]/hash[2:]` — first 2 hex chars = directory prefix for filesystem-friendly hierarchy (max 256 dirs).
- **Atomicity**: Local writes use tempfile + os.replace (atomic on POSIX). S3 writes use `IfNoneMatch:*` for conditional puts.
- **Concurrency**: S3 branch refs protected by custom lock objects with stale-lock detection (30s timeout).
- **Hashing**: Blake3 streamed in 64 KB chunks — no full file loaded in memory.
- **Directory traversal**: `os.scandir` (not `os.listdir`) for single-syscall stat+readdir. Entries sorted per directory.
