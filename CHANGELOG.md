# Changelog

All notable changes to this project will be documented in this file.

The format is based on Keep a Changelog, and Fluxel currently tracks changes before its first public beta release.

## Unreleased

### Added

- Streaming S3 import with metadata identity mode and repeatable path filters.
- Manifest sidecar indexes for exact-path and prefix-based lookups.
- Staged add support for arbitrary local files, directories, S3 objects, and S3 prefixes.
- Real S3 integration coverage for remote repository flows.
- AGPL-3.0-or-later licensing, attribution notice, and funding metadata for the first public beta.

### Changed

- CLI examples and tests now prefer `--repo` repository selection semantics.
- Client-local state writes now use atomic replace semantics for HEAD and staging payloads.
- Manifest index prefix iteration now streams rows instead of materializing full result sets.

### Fixed

- Manifest parsing now validates entry shape, digests, and metadata-only invariants with line-aware errors.
- CLI commands now return clean `... error:` messages for common validation, filesystem, and object-storage failures instead of raw tracebacks.
- Corrupt S3-hosted manifest lines now surface actionable validation errors.