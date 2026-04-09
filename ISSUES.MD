# Fluxel Issue Backlog (Spec-Aligned)

Use these as copy-paste GitHub issues. Each issue is scoped for MVP progress toward object-storage-first execution.

---

## 1) Wire `FluxelRepository` to `StorageBackend` (object-storage-first path)

**Problem**
Repository operations are still primarily local-path driven. `LocalStorageBackend` and `S3StorageBackend` exist, but are not the primary execution path in commit/diff/ref workflows.

**Goal**
Make repository read/write operations backend-driven so local and object storage are first-class.

**Scope**
- Add backend injection to `FluxelRepository`.
- Route commit/manifest/ref object I/O through backend interface.
- Preserve current local behavior as default.

**Acceptance Criteria**
- `commit`, `branch`, `diff`, and ref resolution work with both local and S3 backends.
- No direct `Path` reads/writes remain in repository critical path (except bootstrap/config).
- Existing tests pass; add backend-focused tests.

**Labels**
`enhancement`, `architecture`, `object-storage`, `priority-high`

---

## 2) Add repository config + backend selection (`fluxel config`)

**Problem**
There is no stable way to configure dataset root, backend type, bucket/prefix, and defaults.

**Goal**
Introduce repo-level config to make backend selection explicit and reproducible.

**Scope**
- Define `.fluxel/config.json` schema.
- Add CLI command(s) to set/get config values.
- Resolve backend from config in CLI entrypoints.

**Acceptance Criteria**
- User can initialize and inspect backend config from CLI.
- Commands run without repeatedly passing backend parameters.
- Invalid config fails with clear error messages.

**Labels**
`enhancement`, `cli`, `config`, `priority-high`

---

## 3) Implement optimistic ref updates (conditional writes)

**Problem**
Branch/HEAD updates need robust concurrency semantics for multi-writer safety.

**Goal**
Use optimistic locking (ETag / conditional write semantics) for ref mutations.

**Scope**
- Add compare-and-set style ref update API.
- Use `if_none_match` / conditional checks where appropriate.
- Standardize conflict errors (`OptimisticLockError`).

**Acceptance Criteria**
- Concurrent branch updates are deterministic (fail-fast or explicit policy).
- Conflicts are surfaced as typed errors.
- Add concurrency tests for local + S3 backends.

**Labels**
`enhancement`, `concurrency`, `correctness`, `priority-high`

---

## 4) Add `log` command (metadata-only history traversal)

**Problem**
There is no CLI command to inspect commit lineage.

**Goal**
Implement `fluxel log` using commit parent links only.

**Scope**
- Support branch and commit refs.
- Output commit ID, timestamp, message, parent.
- Optional JSON output mode.

**Acceptance Criteria**
- `fluxel log --repo <repo> main` prints deterministic history.
- No blob reads occur during history traversal.
- Unit tests cover empty/single/multi-commit chains.

**Labels**
`enhancement`, `cli`, `metadata-only`, `priority-medium`

---

## 5) Add `status` command (metadata-only working tree scan)

**Problem**
No command exists to show modified/added/removed files before commit.

**Goal**
Implement metadata-only status against `HEAD` manifest.

**Scope**
- Compare current filesystem metadata/hash with `HEAD` manifest.
- Show added/modified/removed sets.
- JSON output mode for automation.

**Acceptance Criteria**
- `fluxel status` reports expected changes on sample datasets.
- Command avoids reading existing blob payloads.
- Tests cover large file sets and empty repo states.

**Labels**
`enhancement`, `cli`, `metadata-only`, `priority-medium`

---

## 6) Add `checkout` command (read path from ref)

**Problem**
No CLI mechanism to materialize files from a selected ref.

**Goal**
Provide a safe checkout flow that restores files from blobs via manifest mapping.

**Scope**
- Start with basic restore mode.
- Add overwrite safety flags.
- Respect branch/commit refs.

**Acceptance Criteria**
- Checkout restores correct bytes for all manifest entries.
- Safety behavior is explicit (default safe mode).
- Integration tests verify round-trip commit -> checkout.

**Labels**
`enhancement`, `cli`, `restore`, `priority-medium`

---

## 7) Generate `s5cmd` command lists for bulk transfer

**Problem**
Spec requires optional high-throughput transfer path bypassing Python HTTP bottlenecks.

**Goal**
Add command-list generation for upload/download using `s5cmd`.

**Scope**
- Produce deterministic command lists for blob and metadata sync.
- Support dry-run output.
- Keep transport execution optional (generation only in first phase).

**Acceptance Criteria**
- Command list generation covers full object graph for a ref/range.
- Generated commands are idempotent and documented.
- Tests validate list correctness for sample repos.

**Labels**
`enhancement`, `performance`, `object-storage`, `priority-medium`

---

## 8) Stream blob reads in `FluxelFileSystem._open`

**Problem**
Current `_open` path materializes full blob bytes in memory (`BytesIO`), which is not ideal for large files.

**Goal**
Return a streaming file handle backed by storage backend/file object.

**Scope**
- Replace eager `read_bytes` + `BytesIO` with stream-oriented implementation.
- Keep protocol behavior compatible with `fsspec` expectations.
- Preserve existing URI semantics.

**Acceptance Criteria**
- Large file reads do not scale linearly in memory.
- Existing URI-routing test remains valid.
- Add test for multi-GB-like streaming behavior (simulated).

**Labels**
`enhancement`, `performance`, `fsspec`, `priority-high`

---

## 9) Add S3 integration test suite (LocalStack or moto)

**Problem**
S3 backend behavior is not validated end-to-end in CI.

**Goal**
Create integration coverage for object-storage workflows.

**Scope**
- Test commit/ref/manifest/blob paths using S3-compatible backend.
- Include optimistic-lock conflict cases.
- Gate tests under integration marker.

**Acceptance Criteria**
- CI can run S3 integration suite reliably.
- Failures are actionable and deterministic.
- Document how to run tests locally.

**Labels**
`testing`, `integration`, `s3`, `priority-high`

---

## 10) Manifest scaling benchmark suite (1M+ metadata records)

**Problem**
Spec targets extreme scale; there is no dedicated benchmark harness yet.

**Goal**
Add reproducible benchmarks for manifest write/read/diff/index operations.

**Scope**
- Benchmark synthetic datasets at increasing scale tiers.
- Capture throughput, latency, and memory usage.
- Track results in CI artifacts or docs.

**Acceptance Criteria**
- Benchmarks run with fixed seeds and reproducible outputs.
- Memory behavior for streaming operations is visible and bounded.
- Document baseline numbers and regression thresholds.

**Labels**
`performance`, `benchmark`, `scalability`, `priority-medium`

---

## 11) Repo format versioning + migration safety

**Problem**
Future layout changes can break older repos without explicit version signaling.

**Goal**
Introduce format version metadata and migration strategy.

**Scope**
- Add format version file under `.fluxel/`.
- Enforce version checks at CLI startup.
- Provide migration command or explicit failure guidance.

**Acceptance Criteria**
- Unsupported formats fail fast with clear remediation message.
- Compatible versions proceed transparently.
- Tests cover downgrade/upgrade detection.

**Labels**
`enhancement`, `reliability`, `versioning`, `priority-medium`

---

## 12) Path and URI hardening audit (`fluxel://` and logical paths)

**Problem**
URI and logical path parsing must be robust against malformed or traversal-like inputs.

**Goal**
Harden parser and path normalization rules.

**Scope**
- Validate dataset/ref/path tokens strictly.
- Reject dangerous or invalid path forms.
- Add negative tests for malformed URIs.

**Acceptance Criteria**
- Parser rejects invalid URIs with clear errors.
- No path traversal escapes allowed.
- Security-focused tests cover edge-case matrix.

**Labels**
`security`, `hardening`, `fsspec`, `priority-high`
