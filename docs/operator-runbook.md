# Fluxel Operator Runbook

This document covers operational procedures for running Fluxel against
S3-compatible object storage.  It assumes you are already familiar with the
[Fluxel README](../README.md).

---

## S3 IAM

Fluxel requires **read and write access** to the configured S3 bucket and
prefix.  The following IAM policy is the **minimum** required for normal
operations:

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "s3:GetObject",
                "s3:PutObject",
                "s3:DeleteObject",
                "s3:ListBucket"
            ],
            "Resource": [
                "arn:aws:s3:::<BUCKET>",
                "arn:aws:s3:::<BUCKET>/<PREFIX>/*"
            ]
        }
    ]
}
```

**Additional permissions for lock recovery** (optional, for operators):
- `s3:ListBucket` on the `locks/` prefix is already covered above.
- `s3:DeleteObject` on `locks/refs/heads/*` is required for `fluxel lock cleanup`.

**Credentials** are supplied via the standard AWS credential chain:
environment variables (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`),
`~/.aws/credentials`, or IAM instance profiles.  Fluxel does not store
credentials itself.

---

## Encryption

### In transit
All S3 API calls use HTTPS (TLS).  Configure your S3 endpoint to enforce
TLS 1.2+.

### At rest

| Tier | Mechanism | Recommendation |
|------|-----------|----------------|
| S3 server-side | SSE-S3 (AES-256) | Enable as bucket default.  Zero Fluxel configuration needed. |
| S3 server-side | SSE-KMS | Supported via AWS KMS.  Set the default bucket encryption to KMS and ensure the Fluxel IAM role has `kms:Decrypt` and `kms:GenerateDataKey` on the KMS key. |
| Client-side | Not yet supported | File an issue if this is a blocker. |

**Bucket policy snippet (enforce SSE-S3):**

```json
{
    "Effect": "Deny",
    "Principal": "*",
    "Action": "s3:PutObject",
    "Resource": "arn:aws:s3:::<BUCKET>/*",
    "Condition": {
        "StringNotEquals": {
            "s3:x-amz-server-side-encryption": "AES256"
        }
    }
}
```

---

## Bucket Versioning

**Enable bucket versioning** on your Fluxel S3 bucket.  Fluxel objects
(blobs, manifests, commits, refs) are **immutable by key** — once written,
they are never updated in place.  Versioning provides:

- **Accidental-deletion protection**.  If an operator or automation
  deletes an object, the previous version is recoverable.
- **Audit trail**.  Every overwrite (including lock acquisition/release)
  is recorded as a new version.

Enable with:

```bash
aws s3api put-bucket-versioning \
    --bucket <BUCKET> \
    --versioning-configuration Status=Enabled
```

**Fluxel does not require versioning to function**, but it is strongly
recommended for production deployments.

---

## Lifecycle / Retention

S3 lifecycle rules let you manage storage costs without breaking Fluxel's
integrity model.

### Recommended rules

1. **Expire old object versions** (clean up after versioning roll-over):
   ```json
   {
       "Rules": [
           {
               "Id": "expire-old-versions",
               "Status": "Enabled",
               "Filter": {},
               "NoncurrentVersionExpiration": {
                   "NoncurrentDays": 90
               }
           }
       ]
   }
   ```

2. **Transition blobs to cheaper storage** (optional, cost optimization):
   ```json
   {
       "Id": "transition-blobs-to-IA",
       "Status": "Enabled",
       "Filter": {"Prefix": "<PREFIX>/blobs/"},
       "Transitions": [
           {
               "Days": 30,
               "StorageClass": "STANDARD_IA"
           }
       ]
   }
   ```

   Blobs are content-addressed and immutable; transitioning them to
   Infrequent Access or Glacier Instant Retrieval is safe.

**Do not** set lifecycle rules that delete the *current version* of any
object — Fluxel never overwrites objects in place, so the current version
is always the canonical one.

---

## Backups

Fluxel's S3 objects are the canonical store.  Your backup strategy should
protect against **bucket-level** loss (region failure, account compromise,
accidental bucket deletion).

### Recommended backup strategies

| Strategy | Coverage | Recovery time |
|----------|----------|---------------|
| **S3 Cross-Region Replication (CRR)** | All objects replicated to a second region | Minutes (promote replica) |
| **AWS Backup for S3** | Point-in-time restore of the entire bucket | Hours |
| **Periodic `s3 sync` to another bucket** | Blobs, manifests, commits, refs | Hours |

### What to back up

Everything under the Fluxel prefix:

```
<PREFIX>/
  blobs/          # Content-addressed blob objects
  commits/        # Commit metadata (JSON)
  manifests/      # Manifest snapshots (JSONL)
  manifests/*.idx # Manifest indexes (SQLite)
  refs/heads/     # Branch pointers
  locks/          # Active branch locks (ephemeral, optional)
```

**Locks are ephemeral** — they time out after the configured
`lock_timeout_seconds` (default: 30 s).  You do not need to back them up,
but including them is harmless.

### Restore procedure

1. Restore the S3 prefix from your backup to the target bucket.
2. Run `fluxel lock cleanup --force` to clear any stale locks that may
   have been restored from backup.
3. Clients can resume normal operations — they will pick up the restored
   branch refs on their next command.

---

## Lock Recovery

Fluxel uses **S3-based advisory locks** to serialise concurrent branch
ref updates in shared S3 repositories.  Every `compare_and_set_branch_ref`
call acquires a short-lived lock before reading and writing the ref.

### Lock lifecycle

1. **Acquire** — A client writes a lock object at
   `locks/refs/heads/<branch>.lock` with `IfNoneMatch: *` (atomic create).
2. **Hold** — The client reads the current ref, validates ancestry, and
   writes the new commit-id.
3. **Release** — The client deletes the lock object.

Locks include an `expires_at` timestamp (`lock_timeout_seconds` in the
future, default 30 s).  If a client crashes mid-operation, the lock becomes
**stale** after the timeout and can be broken by the next writer.

### Inspecting locks

```bash
# List all active locks
fluxel lock list --repo s3://my-bucket/my-prefix

# List as JSON
fluxel lock list --repo s3://my-bucket/my-prefix --json
```

Example output:

```
Branch                         Status     Expires
------------------------------------------------------------
feature                        STALE      2026-07-20 14:32:10 UTC
main                           active     2026-07-20 14:32:45 UTC
```

### Cleaning up stale locks

```bash
# Release all stale locks
fluxel lock cleanup --repo s3://my-bucket/my-prefix

# Release a specific branch lock (even if not stale)
fluxel lock cleanup --repo s3://my-bucket/my-prefix --force feature

# Dry-run with JSON
fluxel lock cleanup --repo s3://my-bucket/my-prefix --json
```

### When to use `--force`

- A client crashed and left a lock that hasn't expired yet.
- A restored backup contains lock objects from the backup window.
- You are certain no other client holds the lock legitimately.

**Warning:** Forcing a lock that is *actively* held by another client can
cause that client's CAS operation to fail with a `RefConflictError`.
The conflict is safe (no data loss), but the client must retry.

---

## Incident Recovery

### Symptom: `RefConflictError` on every commit/push/pull

**Cause:** A stale lock is preventing branch updates, or two clients are
racing.

**Resolution:**
1. Run `fluxel lock list --repo <URI>` to inspect locks.
2. If locks are stale: `fluxel lock cleanup --repo <URI>`
3. If locks are active: wait for the other client to finish, or
   `fluxel lock cleanup --force <branch>` if you are certain the other
   client is gone.

---

### Symptom: `push` / `pull` reports "Everything up-to-date" but refs differ

**Cause:** Another client updated the branch between your last fetch and
the push.  This is a **non-fast-forward** scenario.

**Resolution:**
1. `fluxel pull --repo <URI>` to fetch the latest branch state.
2. Resolve any conflicts manually.
3. Commit and push again.

---

### Symptom: `verify` fails with `FileNotFoundError` (source_uri missing)

**Cause:** A metadata-only (`meta`) entry's source object was
deleted or moved before verification.

**Resolution:**
1. Restore the source object at its original `source_uri`.
2. Re-run `fluxel verify`.
3. If the source object cannot be restored, the entry is irrecoverable.
   Remove it with `fluxel rm <path>` and commit with `fluxel commit --staged`.

---

### Symptom: Corrupted or missing blob

**Cause:** A blob object was deleted or truncated in S3 (e.g. by an
overly aggressive lifecycle rule).

**Resolution:**
1. If bucket versioning is enabled, restore the previous version of the
   blob from the S3 console or CLI.
2. If versioning is not enabled and no backup exists, the blob is lost.
   Entries referencing the lost blob will fail to read.  Re-import or
   re-add the data.

---

### Symptom: Bucket or prefix accidentally deleted

**Resolution:**
1. Restore from backup (see [Backups](#backups)).
2. Run `fluxel lock cleanup --force --repo <URI>` to clear stale locks.
3. Verify integrity: `fluxel verify --repo <URI>`.

---

## Quick Reference

| Task | Command |
|------|---------|
| Verify integrity | `fluxel verify --repo <URI>` |
| Audit orphaned objects | `fluxel gc --repo <URI>` (`--prune` deletes) |
| List branches with heads | `fluxel catalog --repo <URI>` |

Concurrency safety is CAS-only (docs/architecture.md §6): ref updates use
version-token compare-and-set, so there are no branch locks to list, time out,
or force-release.
