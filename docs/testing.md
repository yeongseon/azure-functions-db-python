# Testing

How to run and write tests for `azure-functions-db`.

## Running Tests

```bash
make test        # Run all tests with pytest
make lint        # Run Ruff linter
make typecheck   # Run mypy type checker
make build       # Verify package builds cleanly
```

Run all checks at once:

```bash
make check-all
```

## Coverage

The project enforces a **90% coverage threshold**. Coverage is measured by
`pytest-cov` and configured in `pyproject.toml`:

```toml
[tool.coverage.report]
fail_under = 90
```

## Test Structure

| File | Purpose |
| --- | --- |
| `tests/test_decorator.py` | Decorator API tests (input, output, trigger, inject_reader, inject_writer) |
| `tests/test_public_api.py` | Export surface verification (27 symbols, version string) |

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

## Test Categories

### Unit Tests

Core logic that runs without external dependencies:

- Cursor comparator and serialization
- Event ID generation
- SQL builder
- Retry classifier
- Decorator configuration and validation
- Mutual exclusivity rules

### Contract Tests

Verify that all adapters satisfy the same input/output contract:

- Ordering guarantees
- Empty batch semantics
- Checkpoint generation
- Timezone normalization
- Duplicate-safe cursor handling

### Integration Tests

Run against real database containers (Docker recommended):

- PostgreSQL
- MySQL / MariaDB
- SQL Server

### End-to-End Tests

Full stack: Azure Functions local runtime + Azurite + database container.

## CI Matrix

The CI pipeline tests across:

- **OS**: Ubuntu latest
- **Python**: 3.11, 3.12, 3.13, 3.14

Stages:

1. Lint (Ruff)
2. Type check (mypy)
3. Unit tests
4. Build verification

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
