# Pre-launch TODOs

This list is deliberately launch-oriented rather than a feature roadmap.

## P0 — complete before users can share repositories

- [X] Make `push` and `pull` reject divergent history and check the CAS result.
  They currently copy objects and then move the destination branch without
  proving that the destination head is an ancestor; they also report success
  when `compare_and_set_branch_ref` returns `False`. CAS prevents a race, not
  a destructive non-fast-forward update. Add two-client divergence and
  concurrent-update tests for both directions.
- [x] Add a release gate that runs: non-integration tests, real S3 integration,
  an isolated install of the built wheel, CLI smoke tests, and a clean-tree
  check. Implemented in `.github/workflows/ci.yml`:
  `test-and-build` (non-integration tests + build + clean-tree + artifact upload),
  `s3-integration` (real S3 against MiniStack),
  `isolated-install-smoke` (wheel install in fresh venv + CLI smoke),
  and `release-gate` (unified gate depending on all three).
  The `release-gate` job can be set as the single required status check
  in branch-protection rules.
- [x] Decide and document the durability contract for `--identity meta`.
  Source URIs can change after a metadata-only import; label such revisions as
  unverifiable until `verify`, warn in CLI output, and document the required
  source-retention policy.
  Implemented:
  - `ManifestEntry.is_verified` property distinguishes blob-backed from
    metadata-only entries.
  - CLI warns to stderr on `add --identity meta`, `commit` with meta config,
    and `verify` when unverifiable entries remain.
  - Durability contract and source-retention policy documented in README
    under "Identity Modes → Durability contract for --identity meta".
- [x] Publish operator runbooks for S3 IAM, encryption, bucket versioning,
  lifecycle/retention, backups, lock recovery, and incident recovery. Add a
  supported lock inspection/cleanup command before relying on shared S3 repos.
  Implemented:
  - `docs/operator-runbook.md` covers IAM policy, SSE-S3/KMS encryption,
    bucket versioning, lifecycle rules, backup/restore strategies, lock
    recovery procedures, and five incident recovery scenarios.
  - `fluxel lock list` and `fluxel lock cleanup` CLI commands with `--json`,
    `--force`, and `--repo` flags.
  - `S3RepositoryStore.list_branch_locks()`, `.branch_lock_info()`, and
    `.force_release_branch_lock()` plus `FluxelRepository.list_locks()`,
    `.lock_info()`, `.force_release_lock()`, and `.lock_timeout_seconds`.

## P1 — strongly recommended for the first public release

- [ ] Make the configured type-check gate useful. `uv run pyrefly check`
  currently reports 89 errors (including exported names, protocol typing, and
  filesystem return types), while the config keys emit warnings. Either fix the
  errors or scope/configure the check deliberately, then add it to CI.
- [ ] Update the README's MVP status: it says remote sync CLI is not wired,
  but `push`, `pull`, and `fetch` now exist. Document their safety semantics,
  supported remotes, and recovery workflow.
- [ ] Replace whole-object sync copies with streamed transfers. The current
  sync helper reads each complete blob into memory and writes a temporary file,
  which makes large-object sync memory- and disk-heavy.
- [ ] Change fsspec dataset resolution to fail for an unknown dataset instead
  of falling back to the current directory. A typo can otherwise read from an
  unintended local repository.
- [ ] Test the supported-version matrix: Python 3.11 and 3.12, current fsspec,
  AWS S3, and the configured S3-compatible endpoint. Include interrupted
  transfer, stale-lock, access-denied, and corrupted-object scenarios.
- [ ] Add privacy/security release checks: dependency/vulnerability scan,
  license review of runtime dependencies, documentation on secrets/credential
  handling, and a statement of telemetry behavior.

## P2 — early operational follow-ups

- [ ] Add remote configuration (`remote add`) and clone; requiring a full S3
  URI on every sync is error-prone.
- [ ] Add garbage collection with a dry-run and retention policy for unreachable
  blobs/manifests.
- [ ] Add machine-readable release notes, versioning/migration policy, and a
  reproducible PyPI publishing workflow.


----

Architecture?

  CLI / fsspec
      ↓
  Application use cases
  (commit, add, verify, sync, restore, merge)
      ↓
  Domain
  (commits, manifests, paths, identity, commit graph, errors)
      ↓
  Ports
  (RepositoryStore, ClientState, Workspace, BlobTransfer)
      ↓
  Adapters
  (local filesystem, S3, fsspec, CLI formatting)

**Concretely:**
* Keep ManifestEntry, path validation, hashing rules, commit graph ancestry, and domain errors pure—no Path, boto3, or CLI imports.
* Make RepositoryStore the shared-object port, ClientState the local mutable-state port, and add a streaming BlobTransfer port.
* Move each command into a small use case with explicit input/result objects: CommitUseCase, SyncUseCase, RestoreUseCase, etc.
* Make ref updates go through one operation such as advance_ref(expected_head, new_head). The application layer must prove fast-forward ancestry and must treat a failed CAS as a conflict.
* Keep immutable-object publication separate from the final ref update: upload blobs → manifest → commit → CAS branch ref.
* Add one contract-test suite that every RepositoryStore implementation must pass; run it against local storage and S3-compatible storage.
* Keep fsspec as a thin read adapter, not part of repository business logic.