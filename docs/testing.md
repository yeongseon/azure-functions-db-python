# Testing

How to run and write tests for `azure-functions-db-python`.

## Running Tests

```bash
make test        # pytest -v tests (Layer 1; Layer 2 tests skip without TEST_*_URL; e2e markers deselected)
make cov         # same suite with an HTML/XML/terminal coverage report
make lint        # Ruff check + format check, then mypy
make typecheck   # mypy src tests
make build       # Verify package builds cleanly
```

`make test` is not a database or Azure check: without the `TEST_*_URL` /
`TEST_AZURITE_CONN_STR` variables the live tests in `tests/integration/` are skipped, and the
`host_e2e` / `azure_e2e` markers are deselected by the default `addopts` in `pyproject.toml`.
See [Which lane proves what](#which-lane-proves-what).

Run all checks at once:

```bash
make check-all
```

## Coverage

The coverage gate is `fail_under` in `pyproject.toml`; that file is authoritative, and
`AGENTS.md` states the same policy (**95%**). Coverage is measured by `pytest-cov` (branch
coverage, source `src/azure_functions_db`):

```toml
[tool.coverage.report]
fail_under = 95
```

`pytest` applies `--cov` through `addopts`, so `make test`, `make cov` and
`python -m pytest tests` all enforce it. Commands that pass `--no-cov` (the integration, e2e and
publish-gate runs below) do not measure coverage. `tests/test_docs_coverage_threshold.py` fails
if a number in these docs differs from `fail_under`.

## Which lane proves what

| Lane | Command / workflow | Runs in CI | What it proves | What it does not prove |
| --- | --- | --- | --- | --- |
| Unit | `make test`; `ci-test.yml` runs `python -m pytest tests` on Python 3.11-3.14 | Yes, with the coverage gate | Logic against SQLite `:memory:` and mocks | Real database dialect behavior, Azurite, the Functions host or Azure |
| Live DB | `db-integration.yml`: `pytest tests/integration -m "integration and <db>"` for `sqlite`, `postgres`, `mysql`, `mssql` | Yes, on pushes to `main` and pull requests that touch `src/`, `tests/`, `pyproject.toml` or the workflow, with `INTEGRATION_FAIL_ON_SKIP=1` | Source, reader and writer against real service containers | Azure hosting; Azure SQL |
| Live Azurite | same workflow, `pytest tests/integration -m azurite` (once, in the `sqlite` job) | Yes | `BlobCheckpointStore` against the Azurite emulator | A real Azure Storage account |
| Functions host smoke | `ci-test.yml` job *Functions host smoke*: `func start` on `tests/host_smoke/` with Azurite | Yes | The poll trigger executes inside the real Functions host | Real Azure, and it does not run `tests/e2e/host/` |
| Local host e2e | `tests/e2e/host/` (`host_e2e` marker) | **No workflow runs it**; opt-in | Binding endpoints of `examples/e2e_app/` against your own host and database | Anything in CI |
| Azure e2e | `e2e-azure.yml` (manual dispatch, weekly, and as the `azure-e2e` gate of `publish-pypi.yml`): deploys with Bicep and runs `tests/e2e -m azure_e2e` | Only with Azure OIDC secrets: without them the workflow skips the e2e job with a warning and fails closed for release certification | The package on a real deployed Function App | Nothing from the mock-based unit lane |

A mock-based or local test never proves a deployment; only the Azure e2e lane exercises real
Azure.

## Three-Layer Test Architecture

The project uses three test layers with increasing scope and external
dependency requirements.

### Layer 1 — Unit Tests (`tests/`)

Fast, in-process tests that run against SQLite `:memory:` with all external
services mocked. These carry the coverage gate.

| File | Purpose |
| --- | --- |
| `tests/test_decorator.py` | Decorator API tests (input, output, trigger, inject_reader, inject_writer) |
| `tests/test_public_api.py` | Export surface verification (`__all__`, version string) |
| `tests/test_shared_core.py` | Cursor comparator, serialization, event ID, SQL builder |
| `tests/test_state_blob.py` | BlobCheckpointStore mock-based tests |
| `tests/test_state_errors.py` | Lease/checkpoint error handling |
| `tests/test_hardening.py` | Crash, stale-runner, and ambiguous-commit scenarios |
| `tests/test_toolkit_metadata.py` | `_azure_functions_metadata` convention |

**Run**: `make test`, or the publish workflow's service-free selection:
`pytest tests/ -m "not integration and not host_e2e and not azure_e2e and not azurite"`

### Layer 2 — Integration Tests (`tests/integration/`)

Live database tests that run against real database containers via GitHub
Actions service containers (`db-integration.yml`). Each database backend is gated by
an environment variable — when not set, the corresponding tests are skipped locally. CI sets
`INTEGRATION_FAIL_ON_SKIP=1`, so a skip there is a failure.

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
pytest tests/integration -m "integration and sqlite" -v --no-cov
```

**Run all** (requires running database containers):

```bash
export TEST_POSTGRES_URL="postgresql+psycopg://postgres:postgres@localhost:5432/testdb"
export TEST_MYSQL_URL="mysql+pymysql://root:root@localhost:3306/testdb"
export TEST_MSSQL_URL="mssql+pyodbc://sa:Password1!@localhost:1433/testdb?driver=ODBC+Driver+18+for+SQL+Server&TrustServerCertificate=yes"
export TEST_AZURITE_CONN_STR="DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;"
pytest tests/integration -m integration -v --no-cov
```

### Layer 3 — End-to-End Tests (`tests/e2e/`)

Full-stack tests that exercise the Azure Functions host and Azure deployment. Both
markers are deselected by the default `addopts`, so select them with `-m`.

#### Host smoke tests (`tests/e2e/host/`)

Run the e2e sample app (`examples/e2e_app/`) under the Azure Functions local
runtime and verify that all decorator/binding modes work end-to-end against a
real database.

| File | Scope | Markers |
| --- | --- | --- |
| `tests/e2e/host/test_bindings_e2e.py` | HTTP endpoints testing input/output/reader/writer bindings | `host_e2e` |

**Run**: `E2E_BASE_URL=http://localhost:7071 pytest tests/e2e/host -m host_e2e -v --no-cov`
(without `-m host_e2e` every test is deselected).

**Requires**: `func` CLI, `E2E_BASE_URL`, and a running database. No CI workflow runs this
directory; CI's host coverage is the separate smoke job on `tests/host_smoke/`.

#### Azure smoke tests (`tests/e2e/azure/`)

Thin post-deployment tests that hit a live Azure Function App to verify the
package works in the real Azure environment.

| File | Scope | Markers |
| --- | --- | --- |
| `tests/e2e/azure/test_azure_smoke.py` | Health, read, write endpoints against Azure | `azure_e2e` |

**Run**: `E2E_BASE_URL=https://<app>.azurewebsites.net pytest tests/e2e/azure -m azure_e2e -v --no-cov`

**Requires**: `E2E_BASE_URL` pointing to a deployed Function App. `e2e-azure.yml` deploys one and runs
this suite.

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

### Unit + Lint (`ci-test.yml`, "Test and Coverage")

Runs on pull requests and pushes. Unit tests run on Ubuntu latest with Python 3.11–3.14 and
enforce the coverage gate. The workflow also lints and type-checks, builds the wheel and sdist,
runs the suite against minimum dependency versions, and runs the Functions host smoke job
(Python 3.11 and 3.12).

### DB Integration (`db-integration.yml`)

Runs on push to `main` and on pull requests that touch `src/**`, `tests/**`, `pyproject.toml`
or the workflow itself. Matrix by database (Azurite starts only in the `sqlite` job):

| Database | Service container | Extra setup |
| --- | --- | --- |
| SQLite | (none) | — |
| PostgreSQL | `postgres:16` | — |
| MySQL | `mysql:8` | — |
| SQL Server | `mcr.microsoft.com/mssql/server:2022-latest` | ODBC Driver 18 install |
| Azurite | `azurite` installed with npm, run on the runner (not a service container) | — |

### Azure E2E (`e2e-azure.yml`)

Runs on manual dispatch, a weekly schedule, and as the `azure-e2e` gate called by
`publish-pypi.yml` on release tags. Deploys a Function App via Bicep and runs
`tests/e2e -m azure_e2e` against it. It needs Azure OIDC secrets.

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
