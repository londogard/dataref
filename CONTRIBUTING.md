# Contributing

## Development Setup

Dataref uses `uv` for dependency management and local commands.

```bash
uv sync --group dev
uv run dataref --help
```

## Local Validation

Run the full local suite:

```bash
uv run pytest tests
```

Run only the real S3 integration tests against Ministack:

```bash
export DATAREF_MINISTACK_ENDPOINT=http://127.0.0.1:9000
export DATAREF_MINISTACK_ACCESS_KEY=ministack
export DATAREF_MINISTACK_SECRET_KEY=ministack123
export DATAREF_MINISTACK_REGION=us-east-1

uv run pytest tests/test_s3_integration.py -m integration
```

If `DATAREF_MINISTACK_ENDPOINT` is unset or unreachable, the integration tests skip automatically.

## Project Expectations

- Dataref is licensed under AGPL-3.0-or-later; preserve the license and notice files in redistributions.
- Prefer Python type hints by default.
- Keep Dataref client-first: no server, daemon, or central database.
- Keep canonical blob storage simple and immutable.
- Metadata-only operations must not read blob payloads.
- Prefer stream-safe, O(1)-memory patterns when working with manifests and large imports.
- Use Blake3 for Dataref content and identity hashing.

## Pull Requests

- Keep changes focused and explain user-facing behavior changes clearly.
- Add or update tests for every functional change.
- Update [README.md](README.md) and [CHANGELOG.md](CHANGELOG.md) when the CLI, packaging, or documented workflows change.
- Keep [NOTICE](NOTICE) aligned with project attribution and sponsorship guidance.
- Prefer `--repo` in docs and examples for repository selection.