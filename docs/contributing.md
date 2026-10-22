# Contributing

Thank you for considering contributing to `azure-functions-db-python`.

## Development Setup

```bash
git clone https://github.com/yeongseon/azure-functions-db-python.git
cd azure-functions-db-python
make install
```

Requirements:

- Python 3.11+
- [hatch](https://hatch.pypa.io/) for build and version management
- [Ruff](https://docs.astral.sh/ruff/) for linting and formatting
- [mypy](https://mypy-lang.org/) for type checking

## Running Checks

```bash
make test        # Offline suite, coverage >=95%
make lint        # Run Ruff linter
make typecheck   # Run mypy
make build       # Verify package builds
```

Or run everything at once:

```bash
make check-all
```

`make check-all` is an exact alias of `make verify`: quality, offline tests,
all required run-owned databases/Azurite, separate real Functions-host execution,
installed-wheel SQLite smoke, package build and strict docs. Docker/Podman, Azurite,
Core Tools, database drivers and ODBC Driver 18 are required for the full gate;
`make check` and `make test` need no external services.

| Command | Purpose |
| --- | --- |
| `make install` / `make install-ci` | Local Hatch setup / CI-selected interpreter setup |
| `make doctor` | Read-only prerequisites, nonzero for required missing tools |
| `make format` / `make format-check` | Explicit mutation / non-mutating formatting |
| `make check` | Complete non-mutating quality/policy gate, strict types once |
| `make integration` | Private service lifecycle and strict per-backend execution |
| `make integration-local` | Existing dedicated test databases; never drop databases or external containers |
| `make verify` / `make check-all` | Full local gate / exact alias |
| `make e2e-azure` | Existing deployed candidate, URL and expected version required |
| `make report` | Saved evidence only, no execution |
| `make clean` / `make clean-all` | Generated artifacts only / historical deep cleanup including `.venv` |

`DB_BACKEND=<name>` selects PARTIAL integration evidence only; verify ignores that
focus and requires SQLite, PostgreSQL, MySQL, MSSQL and Azurite. Externally managed
lanes require `DB_TEST_DATABASE_CONFIRMED=1` because fixtures create/drop test
tables. Never point them at production/shared tables. Lint/types/security/build/docs
commands remain focused. See [Testing](testing.md) for evidence/status semantics.

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
- Coverage threshold: 95%.

See [Testing](testing.md) for details on running and writing tests.

## Principles

- PRs that change semantics must include an ADR or design note.
- New adapters may not be merged without contract tests.
- Public API additions must be accompanied by docs and example updates.
- Do not use language that overstates guarantees.
