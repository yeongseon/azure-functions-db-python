# Binding Semantics

## 1. Purpose
Bindings are imperative helpers for reading and writing DB data inside Azure Functions.
Unlike triggers, they do not manage state (checkpoint/lease) and operate on a per-invocation basis.

## 2. Product Boundary
- This library is a Python-level DB helper
- It is not an Azure Functions host-native binding extension
- Users call DbReader/DbWriter directly inside the function body

## 3. Input Binding (DbReader)

### 3.1 Basic Contract
- Invocation-scoped reads
- Uses the DB's default isolation level (can be overridden)
- No snapshot/repeatable-read guarantee across multiple reads
- None = "row not found" (not a DB access failure)

### 3.2 Failure Semantics
- DB connection failure → raises ConnectionError (function fails)
- Query failure → raises QueryError (function fails)
- Row not found → returns None (normal behavior)
- Fail-closed principle: operational failures are never silently hidden as None/empty list

### 3.3 Connection Lifecycle
- Engine is obtained from EngineProvider when DbReader is created (lazy singleton)
- Each get()/query() call uses a short-lived connection
- No long-lived sessions are maintained
- Calling close() releases reader resources (engine pool is retained)

## 4. Output Binding (DbWriter)

### 4.1 Basic Contract
- Invocation-scoped writes
- Each write call (insert/upsert/update/delete) is an independent transaction
- Batch methods (insert_many/upsert_many) are all-or-nothing

### 4.2 Transaction Boundaries
- Default: per-call autocommit
- Batch: wrapped in a single transaction for all-or-nothing semantics
- No transaction guarantee across multiple write calls (each is independent)
- Future: explicit multi-write transactions via a DbSession context manager, if needed

### 4.3 Failure Semantics
- DB connection failure → raises ConnectionError
- Write failure (constraint violation, etc.) → raises WriteError
- Failure mid-batch → entire batch is rolled back, raises WriteError
- Function failure handling: delegated to Azure Functions retry policy (no internal retry in the library)

### 4.4 Idempotency
- upsert is inherently idempotent
- insert raises WriteError on PK conflict
- Users who want retry-safe operations should use upsert

### 4.5 Connection Lifecycle
- Engine is obtained from EngineProvider when DbWriter is created (lazy singleton)
- Each write call uses a short-lived connection + transaction
- Calling close() releases writer resources

## 5. Trigger + Binding Combination

### 5.1 Independence
- Trigger checkpoint/lease and binding writes are separate transactions
- DbWriter can be used inside a trigger handler, but is independent of checkpoint commit
- A successful output write does not advance the checkpoint

### 5.2 Failure Scenarios
- DbWriter.insert() succeeds inside handler → handler succeeds → checkpoint commit fails
  → same batch is reprocessed on the next tick → DbWriter.insert() may run again (duplicate)
  → users must use upsert or an idempotent key

### 5.3 Engine Sharing
- If trigger and binding use the same DB URL, EngineProvider returns the same engine
- Connection pool is shared; sessions/transactions are independent

## 6. Shared Core Contracts

### 6.1 EngineProvider
- Manages lazy singleton engines keyed by DbConfig
- Same config = same engine/pool
- Thread-safe

### 6.2 DbConfig
- url (required)
- pool_size, max_overflow, pool_timeout (optional)
- connect_args (optional)
- echo (optional, for debugging)

### 6.3 Error Hierarchy
- DbError (base)
  - ConnectionError
  - QueryError
  - WriteError
  - NotFoundError
- Trigger-specific: PollerError hierarchy (kept as-is, independent of DbError)

## 7. Future Extensions

## 7. Decorator API (DbFunctionApp)

### 7.1 Data Injection Decorators

- `@db.db_input()` — injects query results directly into handler parameter
  - PK mode: `pk=dict | Callable` → injects `dict | None`
  - Query mode: `query=str, params=dict | Callable` → injects `list[dict]`
  - `on_not_found="none"` (default) or `"raise"` for strict mode
- `@db.db_output()` — auto-writes handler return value to DB
  - `action="insert"` (default) or `"upsert"` with `conflict_columns`
  - Return `dict` for single row, `list[dict]` for batch, `None` for no-op

### 7.2 Client Injection Decorators (imperative escape hatches)

- `@db.db_reader()` — injects `DbReader` instance per invocation
- `@db.db_writer()` — injects `DbWriter` instance per invocation
- Use for complex operations: multiple queries, transactions, update/delete

### 7.3 Trigger Decorator

- `@db.db_trigger()` — decorator that wraps `PollTrigger` for change detection
- Implemented via `DbFunctionApp` class with automatic lifecycle management
- See [Python API Spec](04-python-api-spec.md) for full usage

### 7.2 DbSession (if needed)
- Explicit multi-write transaction context manager
- No ambient transaction
- Users opt in explicitly

### 7.3 Pydantic Mapping
- DbReader.get() → option to return a Pydantic model
- DbWriter.insert() → option to accept a Pydantic model as input

## 8. Notice to Users

> The bindings in azure-functions-db are not Azure Functions host-native bindings;
> they are Python-level DB helpers called directly inside the function body.
> Each write is an invocation-scoped transaction and is independent of the trigger's checkpoint commit.
