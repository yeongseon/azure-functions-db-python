# AGENTS.md

## Purpose
`azure-functions-db-python` provides pseudo DB trigger functionality for Azure Functions Python v2 applications using SQLAlchemy.

## Read First
- `README.md`
- `CONTRIBUTING.md`
- `docs/02-architecture.md`
- `docs/03-semantics.md`

## Working Rules
- Preserve the package's Python compatibility and public API behavior unless the change explicitly updates the contract.
- Keep documentation, examples, and tests synchronized.
- Semantics changes require ADR or design note.
- Never suppress type errors.
- Guarantee claims must be honest (pseudo trigger, at-least-once).

## Validation
- `make test`
- `make lint`
- `make typecheck`
- `make build`

## Release Process
- Version is managed via `hatch` (dynamic from `src/azure_functions_db/__init__.py`).
- **Do NOT manually edit version strings.** Use the Makefile targets below.
- When bumping version, update `tests/test_public_api.py` to match the new version string.

### Commands
- `make release-patch` — bump patch version, update changelog, tag, and push
- `make release-minor` — bump minor version, update changelog, tag, and push
- `make release-major` — bump major version, update changelog, tag, and push
- `make release VERSION=x.y.z` — set explicit version, update changelog, tag, and push
- `make tag-release VERSION=x.y.z` — create and push an annotated tag (used internally by release targets)

### Flow
1. `make release-patch` (or `-minor` / `-major`) on `main`
2. This runs: `hatch version` → `git commit` → `make changelog` → `git commit` → `git tag` → `git push`
3. Tag push triggers **Publish to PyPI** GitHub Actions workflow automatically.
