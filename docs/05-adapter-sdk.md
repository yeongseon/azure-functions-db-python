# Adapter SDK

> **Most users don't need this document.** If your database has a SQLAlchemy dialect, just install the driver and use the connection URL — bindings and `SqlAlchemySource` usually do not require a custom adapter. This document is for developers building adapters for databases that cannot be reached through SQLAlchemy.

This document defines the internal and external extension contract for developers adding DB adapters.

## 1. Purpose

When supporting a new database, it should be possible to plug in by adding only an adapter,
without modifying application-level code or PollRunner.

## 2. Adapter Classification

### 2.1 SQLAlchemyAdapter
Common RDBMS base adapter.
- PostgreSQL
- MySQL/MariaDB
- SQL Server
- Oracle
- SQLite

### 2.2 NativeAdapter
For databases that are difficult to handle via SQLAlchemy, or databases that provide native change streams.
- MongoDB
- Kafka-backed source
- Custom REST cursor source

## 3. Base Protocol

```python
class SourceAdapter(Protocol):
    name: str

    def validate(self) -> None: ...
    def open(self) -> None: ...
    def close(self) -> None: ...
    def fetch_changes(self, checkpoint: Checkpoint, limit: int) -> FetchResult: ...
    def capability(self) -> AdapterCapability: ...
    def source_descriptor(self) -> SourceDescriptor: ...
```

The adapter returns raw records; core normalizes them into `RowChange`.
`supports_deletes()` has been consolidated into `capability()`.

## 4. FetchResult

```python
@dataclass
class RawRecord:
    """Raw record returned by the adapter before normalization"""
    row: dict[str, object]        # Raw row read from the DB
    cursor_value: object           # Cursor column value
    pk_values: dict[str, object]   # PK dict

@dataclass
class FetchResult:
    records: list[RawRecord]
    next_checkpoint: Checkpoint | None
    stats: dict[str, object]
```

Rules:
- If `records` is empty, `next_checkpoint` is generally also `None`
- `next_checkpoint` must point to the last record
- The source adapter must maintain stable ordering
- Adapters do not create `RowChange` — that is handled by core's `EventNormalizer`

## 5. SQLAlchemy Adapter Details

### Required Inputs
- url
- table or query
- cursor_column
- pk_columns

### Required Responsibilities
- Dialect-specific quoting
- Stable ORDER BY generation
- Parameter binding
- row -> dict conversion
- datetime normalization (UTC recommended)

### Recommended Implementation
- Use SQLAlchemy Core
- Minimize ORM session dependency
- Schema reflection can be cached once at startup

## 6. Query Builder Rules

### Table Mode Generation Rules
```sql
SELECT <projected_columns>
FROM <schema.table>
WHERE (
  cursor > :cursor_value
  OR (cursor = :cursor_value AND pk > :pk_value)
)
ORDER BY cursor ASC, pk ASC
LIMIT :limit
```

For composite PKs, use tuple comparison or OR-expanded predicates.

### Query Mode Rules
Wrap the user query as a subquery.

```sql
SELECT *
FROM (
  <user_query>
) AS source
WHERE ...
ORDER BY ...
LIMIT ...
```

## 7. Delete Support Policy

Adapters declare delete support via capability.

```python
@dataclass
class AdapterCapability:
    supports_delete_detection: bool
    supports_before_image: bool
    supports_native_resume_token: bool
    supports_partitioning: bool
```

## 8. Adapter Implementation Checklist

- [ ] Provide stable ordering
- [ ] Handle empty results
- [ ] Normalize timezones
- [ ] Validate both string and integer PKs
- [ ] Generate duplicate-safe checkpoints
- [ ] Use SQL injection-safe parameterization
- [ ] Implement schema reflection fallback
- [ ] Include 3 or more integration tests

## 9. Per-DB Notes

### PostgreSQL
- Recommended driver: `psycopg`
- Prefer `timestamp with time zone`
- Works well with the outbox strategy

### MySQL / MariaDB
- Be mindful of `updated_at` precision
- Account for re-read potential depending on transaction isolation level

### SQL Server
- Can leverage `rowversion` or `datetime2`
- Note that the official Azure SQL trigger uses change tracking + polling loop;
  maintain the pseudo trigger documentation model honestly.

### SQLite
- Primarily for development/testing
- Not recommended as a production pseudo trigger source

## 10. Non-SQL Adapter Guidelines

Adapters like MongoDB should be maintained as separate packages without a SQLAlchemy dependency.

Examples:
- `azure-functions-db-python-mongo`
- `azure-functions-db-python-postgres-cdc`

The key requirement is to maintain only the common `RowChange` contract.
