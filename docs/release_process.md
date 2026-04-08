# Release Process

This document outlines the steps to release a new version of **azure-functions-db** to PyPI using the automated GitHub Actions pipeline and the Makefile-based workflow.

---

## Overview

The release pipeline is **tag-driven**:

1. Bump version + generate changelog + tag on `main`
2. Tag push triggers **Publish to PyPI** and **Create GitHub Release** workflows automatically
3. No manual publishing required

---

## First Release (v0.1.0)

Since v0.1.0 is already set in `src/azure_functions_db/__init__.py`, use `make release` with the explicit version:

```bash
# Ensure main is up-to-date
git checkout main && git pull

# Validate before release
make check-all       # lint + typecheck + test + security

# Tag and release (version is already 0.1.0, so no bump needed)
make changelog       # Generate CHANGELOG.md from git history
make commit-changelog
make tag-release VERSION=0.1.0
```

This pushes the `v0.1.0` tag, which triggers:

- **Publish to PyPI** (`publish-pypi.yml`) — builds and publishes the package via trusted publishing (OIDC)
- **Create GitHub Release** (`create-release.yml`) — creates a GitHub Release from the tag

---

## Subsequent Releases

Use Makefile targets to bump the version, generate the changelog, and tag:

```bash
make release-patch     # Patch release (e.g., v0.1.0 → v0.1.1)
make release-minor     # Minor release (e.g., v0.1.1 → v0.2.0)
make release-major     # Major release (e.g., v0.2.0 → v1.0.0)
```

Each command will:

1. Update the version in `src/azure_functions_db/__init__.py` via `hatch version`
2. Commit the version bump
3. Generate or update `CHANGELOG.md` via `git-cliff`
4. Commit the changelog
5. Create a Git tag (e.g., `v0.2.0`) and push to `main`

Tag push automatically triggers the CI/CD workflows listed above.

> **Important**: After bumping the version, update `tests/test_public_api.py` to match the new version string. See [AGENTS.md](../AGENTS.md) for details.

> Make sure your `main` branch is up-to-date before running these commands.

---

## Pre-Release Validation

Before any release, run the full validation suite:

```bash
make lint          # Ruff linting
make typecheck     # mypy type checking
make test          # pytest test suite
make security      # Bandit security scan
make check-all     # All of the above in one command
make build         # Build distributions (wheel + sdist)
```

Optionally, test the built package locally:

```bash
pip install dist/azure_functions_db-<version>-py3-none-any.whl
```

---

## Changelog Generation

The changelog is generated automatically by [git-cliff](https://git-cliff.org/) from conventional commit messages.

### Configuration

- `cliff.toml` — defines commit grouping, categories, and output format
- `Makefile` — `make changelog` runs `git-cliff -o CHANGELOG.md`

### Commit Message Convention

Follow [Conventional Commits](https://www.conventionalcommits.org/) for proper changelog grouping:

| Prefix | Changelog Category |
|--------|--------------------|
| `feat:` | Features |
| `fix:` | Bug Fixes |
| `docs:` | Documentation |
| `refactor:` | Refactor |
| `style:` | Styling |
| `test:` | Testing |
| `perf:` | Performance |
| `ci:` / `chore:` | Miscellaneous Tasks |
| `build:` | Other |

Use scopes for more context: `fix(trigger): handle empty batch gracefully`

### Manual Changelog Regeneration

```bash
make changelog           # Regenerate CHANGELOG.md from all tags
make commit-changelog    # Stage and commit the updated changelog
```

---

## Manual Publishing (Fallback)

If the automated pipeline is unavailable, you can publish manually:

```bash
make publish-pypi       # Publish to PyPI via hatch (requires ~/.pypirc)
make publish-test       # Publish to TestPyPI
```

To install from TestPyPI:

```bash
pip install --index-url https://test.pypi.org/simple/ azure-functions-db
```

---

## Summary of Makefile Commands

| Task | Command |
|------|---------|
| Run all checks | `make check-all` |
| Version bump + changelog + tag | `make release-patch` / `release-minor` / `release-major` |
| Explicit version release | `make release VERSION=x.y.z` |
| Build distributions | `make build` |
| Publish to PyPI (fallback) | `make publish-pypi` |
| Publish to TestPyPI | `make publish-test` |
| Regenerate changelog only | `make changelog` |
| Show current version | `make version` |

---

## Related

- [CHANGELOG.md](https://github.com/yeongseon/azure-functions-db/blob/main/CHANGELOG.md)
- [AGENTS.md](../AGENTS.md) — release flow and version update rules
- [Contributing](../CONTRIBUTING.md)
