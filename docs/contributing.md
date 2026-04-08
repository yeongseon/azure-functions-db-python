# Contributing

Thank you for considering contributing to `azure-functions-db`.

## Development Setup

```bash
git clone https://github.com/yeongseon/azure-functions-db.git
cd azure-functions-db
make install
```

Requirements:

- Python 3.11+
- [hatch](https://hatch.pypa.io/) for build and version management
- [Ruff](https://docs.astral.sh/ruff/) for linting and formatting
- [mypy](https://mypy-lang.org/) for type checking

## Running Checks

```bash
make test        # Run all tests
make lint        # Run Ruff linter
make typecheck   # Run mypy
make build       # Verify package builds
```

Or run everything at once:

```bash
make check-all
```

## Branch Naming

Use the following prefixes:

- `feature/*` — new functionality
- `fix/*` — bug fixes
- `docs/*` — documentation changes
- `adr/*` — architecture decision records

## Pull Request Guidelines

Every PR should address:

1. **What** is being changed.
2. **Why** it is needed.
3. **Semantics impact** — does this change delivery guarantees or API contracts?
4. **Migration** — is a migration required for existing users?
5. **Tests** — are tests added or updated?

## Code Style

- Python type hints required on all public APIs.
- Public surface docstrings required (Google style).
- Use structured logging — no `print` statements (except in examples).
- No direct SQL string concatenation.
- Ruff configuration: `select = ["E", "F", "I"]`, `line-length = 100`.
- Never suppress type errors with `as any`, `@ts-ignore`, or `type: ignore`.

## Documentation

- Keep README, PRD, and semantics documents consistent with each other.
- New features must include example updates.
- Semantics changes require an ADR (Architecture Decision Record).
- User-facing docs must always preserve this statement:

> pseudo trigger / at-least-once / idempotent handler required

## Test Requirements

- Unit tests for all new code.
- Adapter changes must include integration tests.
- Lease and checkpoint changes must include race condition tests.
- Coverage threshold: 90%.

See [Testing](testing.md) for details on running and writing tests.

## Principles

- PRs that change semantics must include an ADR or design note.
- New adapters may not be merged without contract tests.
- Public API additions must be accompanied by docs and example updates.
- Do not use language that overstates guarantees.
