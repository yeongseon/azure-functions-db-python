# Testing

How to run and write tests for `azure-functions-db-python`.

## Running Tests

```bash
make test        # Offline suite with coverage >=95%
make lint        # Run Ruff linter
make typecheck   # Run mypy type checker
make build       # Verify package builds cleanly
```

Run all checks at once:

```bash
make check-all
```

## Coverage

The project enforces a **95% coverage threshold**. Coverage is measured by
`pytest-cov` and configured in `pyproject.toml`:

```toml
[tool.coverage.report]
fail_under = 95
```

## Three-Layer Test Architecture

The project uses three test layers with increasing scope and external
dependency requirements.

### Layer 1 — Unit Tests (`tests/`)

Fast, in-process tests that run against SQLite `:memory:` with all external
services mocked. These form the CI quality gate for every push.

| File | Purpose |
| --- | --- |
| `tests/test_decorator.py` | Decorator API tests (input, output, trigger, inject_reader, inject_writer) |
| `tests/test_public_api.py` | Export surface verification (27 symbols, version string) |
| `tests/test_shared_core.py` | Cursor comparator, serialization, event ID, SQL builder |
| `tests/test_state_blob.py` | BlobCheckpointStore mock-based tests |
| `tests/test_state_errors.py` | Lease/checkpoint error handling |
| `tests/test_hardening.py` | Crash, stale-runner, and ambiguous-commit scenarios |
| `tests/test_toolkit_metadata.py` | `_azure_functions_metadata` convention |

**Run**: `pytest tests/ -m "not integration and not host_e2e and not azure_e2e and not azurite"`

### Layer 2 — Integration Tests (`tests/integration/`)

Live database tests that run against real database containers via GitHub
Actions service containers. Each database backend is gated by an environment
variable — when not set, the corresponding tests are skipped.

| Env var | Database | Example URL |
| --- | --- | --- |
| `TEST_SQLITE_URL` | SQLite (file-based) | `sqlite:///tmp/test.db` |
| `TEST_POSTGRES_URL` | PostgreSQL | `postgresql+psycopg://postgres:postgres@localhost:5432/testdb` |
| `TEST_MYSQL_URL` | MySQL | `mysql+pymysql://root:root@localhost:3306/testdb` |
| `TEST_MSSQL_URL` | SQL Server | `mssql+pyodbc://sa:Password1!@localhost:1433/testdb?driver=...` |
| `TEST_AZURITE_CONN_STR` | Azurite (blob storage) | `DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;...` |

#### Test files

| File | Scope | Markers |
| --- | --- | --- |
| `tests/integration/db/test_source_live.py` | `SqlAlchemySource` queries across all 4 backends | `integration`, `<db>` |
| `tests/integration/db/test_reader_live.py` | `DbReader` binding across all 4 backends | `integration`, `<db>` |
| `tests/integration/db/test_writer_live.py` | `DbWriter` insert/upsert, incl. MSSQL `ConfigurationError` | `integration`, `<db>` |
| `tests/integration/test_checkpoint_store_live.py` | `BlobCheckpointStore` against Azurite | `integration`, `azurite` |

#### Shared fixtures (`tests/integration/conftest.py`)

- `db_params()` / `upsert_db_params()` — pytest parametrize helpers per-backend
- `db_engine` — creates tables, seeds data, tears down per-test
- `users_table`, `orders_table`, `order_items_table` — schema fixtures
- `read_all_rows()` — helper to fetch all rows from a table

**Run locally** (SQLite only):

```bash
DB_BACKEND=sqlite make integration
```

**Run all** (requires running database containers):

```bash
export TEST_POSTGRES_URL="postgresql+psycopg://postgres:postgres@localhost:5432/testdb"
export TEST_MYSQL_URL="mysql+pymysql://root:root@localhost:3306/testdb"
export TEST_MSSQL_URL="mssql+pyodbc://sa:Password1!@localhost:1433/testdb?driver=ODBC+Driver+18+for+SQL+Server&TrustServerCertificate=yes"
export TEST_AZURITE_CONN_STR="DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;"
DB_TEST_DATABASE_CONFIRMED=1 make integration-local
```

`make integration` starts private SQLite, PostgreSQL 16, MySQL 8, SQL Server 2022
and Azurite with unique names, ports and storage; it cleans up only run-owned
resources. Connection preflight, nonzero test selection and no unexpected skips
are mandatory. Missing Docker/Podman, drivers, URLs or tools are never PASS.
Casual ad-hoc pytest retains missing-URL auto-skips, but required runners classify
the actual JUnit outcomes strictly. Reader/source parametrized `db_key` cases and
marked writer cases are both selected, without a broad-selector fallback.

`integration-local` never drops/deletes user databases or external containers.
It requires dedicated disposable test databases and explicit
`DB_TEST_DATABASE_CONFIRMED=1`: existing fixtures create/seed/drop test tables.
Azurite fixtures delete only their uniquely named test containers.

### Command and evidence contract

`make check` is non-mutating quality/policies; `make test` is entirely offline.
`make verify` always combines check, test, every backend, separate real-host,
installed-wheel SQLite smoke, build and strict docs. `make check-all` is an exact
alias. A focused `DB_BACKEND=<name>` run is PARTIAL; verify clears that focus.
`make doctor` checks required prerequisites without installing anything.
`make report` renders saved JSON without running tests. `make clean` removes
generated artifacts only; `clean-all` also performs historical deep environment
cleanup. See [Contributing](contributing.md) for the complete target table.

Generated evidence is gitignored under `test-results/`:

- `test/`: `junit.xml`, `coverage.xml`, `summary.json`, `environment.json`, logs.
- `integration/`: aggregate summary, backend summaries, copied `junit/` and `logs/`.
- `host-smoke/`, `wheel-smoke/`: separate execution and installed-artifact summaries.
- `verify/`: aggregate summary, `lanes.json`, environment and per-target logs.
- `azure-e2e/`: separate live JUnit, summary, identity-check logs and environment.

Schema-versioned summaries carry command/lane, required status, scope, counts,
exit code and artifact paths. Backend/version/driver/readiness/cleanup are retained
without credentials. URL passwords/userinfo, AccountKey, SAS, `sig` and tokens are
redacted even in JUnit failure messages and logs. CI uses the same runner on its
actual selected Python and uploads available artifacts with `if: always()`;
console and Actions Job Summary use the same counts/status.

PASS requires execution; test/process/readiness failures and timeouts are FAIL.
Missing prerequisites/evidence, zero/all-skipped tests and unexpected required
skips are INCOMPLETE/nonzero. NOT_APPLICABLE is only explicitly unsupported scope,
not a missing backend. Only verify has FULL_LOCAL scope; local success is never
Azure certification. `make e2e-azure` requires `E2E_BASE_URL` and `EXPECTED_VERSION`,
checks deployed identity, and never provisions resources. Real Azure/PostgreSQL
release certification retains its separate exact-artifact/OIDC/cleanup workflow.

### Layer 3 — End-to-End Tests (`tests/e2e/`)

Full-stack tests that exercise the Azure Functions host and Azure deployment.

#### Host smoke tests (`tests/e2e/host/`)

Run the e2e sample app (`examples/e2e_app/`) under the Azure Functions local
runtime and verify that all decorator/binding modes work end-to-end against a
real database.

| File | Scope | Markers |
| --- | --- | --- |
| `tests/e2e/host/test_bindings_e2e.py` | HTTP endpoints testing input/output/reader/writer bindings | `host_e2e` |

**Requires**: `func` CLI, `E2E_BASE_URL`, and a running database.

#### Azure smoke tests (`tests/e2e/azure/`)

Thin post-deployment tests that hit a live Azure Function App to verify the
package works in the real Azure environment.

| File | Scope | Markers |
| --- | --- | --- |
| `tests/e2e/azure/test_azure_smoke.py` | Health, read, write endpoints against Azure | `azure_e2e` |

**Requires**: `E2E_BASE_URL` pointing to a deployed Function App.

## Pytest Markers

All custom markers are registered in `pyproject.toml`:

| Marker | Purpose |
| --- | --- |
| `integration` | Live database integration tests |
| `host_e2e` | Tests against local Azure Functions host |
| `azure_e2e` | Tests against deployed Azure Function App |
| `postgres` | PostgreSQL-specific tests |
| `mysql` | MySQL-specific tests |
| `mssql` | SQL Server-specific tests |
| `sqlite` | SQLite-specific tests |
| `azurite` | Azurite blob storage tests |

## CI Workflows

### Unit + Lint (`ci.yml`)

Runs on every push. Matrix: Ubuntu latest, Python 3.11–3.14.

Stages:
1. Lint (Ruff)
2. Type check (mypy)
3. Unit tests
4. Build verification

### DB Integration (`db-integration.yml`)

Runs on push to `main`, PRs, and weekly schedule. Matrix by database:

| Database | Service container | Extra setup |
| --- | --- | --- |
| SQLite | (none) | — |
| PostgreSQL | `postgres:16` | — |
| MySQL | `mysql:8` | — |
| SQL Server | `mcr.microsoft.com/mssql/server:2022-latest` | ODBC Driver 18 install |
| Azurite | `mcr.microsoft.com/azure-storage/azurite` | — |

### Azure E2E (`e2e-azure.yml`)

Runs on manual dispatch, weekly schedule, and release tags. Deploys a Function
App via Bicep and runs `tests/e2e/azure/` against it.

## Writing Tests

- Use [pytest](https://docs.pytest.org/) as the test framework.
- Follow existing patterns in `tests/` for consistency.
- New features must include tests that cover both success and failure cases.

### Example test

```python
def test_input_decorator_injects_result():
    db = DbBindings()

    @db.input("user", url="sqlite:///:memory:", table="users", pk={"id": 1})
    def handler(user):
        return user

    # Assert decorator configuration is correct
    assert hasattr(handler, "__wrapped__") or callable(handler)
```

## Key Test Scenarios

| Scenario | Description |
| --- | --- |
| First run | No checkpoint exists, first batch processed successfully |
| Identical timestamps | Multiple rows with same cursor value processed without omission |
| Handler failure | Checkpoint does not advance |
| Commit failure | Duplicates possible, no data loss |
| Lease race | Two runners start simultaneously, only one commits |
| Schema drift | Additional columns do not break existing handlers |
| Empty tick | No changes detected, no-op succeeds |
