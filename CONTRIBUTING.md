# Contributing

## Prerequisites

- Python 3.10+
- [uv](https://docs.astral.sh/uv/) >= 0.10

## Setup

```bash
uv sync
```

## Running tests

```bash
uv run poe test
```

## Type checking

```bash
uv run poe mypy
```

## Linting / formatting

```bash
uv run poe lint      # ruff check
uv run poe fmt       # ruff format
```

## Code style

- Follow existing conventions.
- No external dependencies beyond those in `pyproject.toml`.
- Keep the public API surface minimal — new public symbols need justification.
- Use `logging.DEBUG` for routine events, `logging.WARNING` for recoverable errors,
  `logging.INFO` for lifecycle events (initialized, shut down).
- All provider behavior must align with the
  [providers spec](https://github.com/Dynatrace/feature-management-knowledge-base)
  sections 1.1, 2.1, 2.3, 2.4, 3.0, 3.1, 4.1, 5.7, 6, and 8.

## Commits

This project uses [Conventional Commits](https://www.conventionalcommits.org/).
PR titles and single-commit messages are validated automatically.

## Release

Releases are managed by [release-please](https://github.com/googleapis/release-please).
Merging a release-please PR to `main` triggers a PyPI publish via trusted publishing.
