# Architecture

## 1. Top-Level Structure

`azure-functions-db` is composed of three top-level modules:

1. **core**
   - `DbConfig`
   - `EngineProvider`
   - shared types
   - shared errors
   - serializers
2. **trigger**
   - polling orchestrator, source adapter, state management, Azure Functions integration
   - consumes `core` to handle pseudo trigger execution
3. **binding**
    - `DbReader` (input binding)
    - `DbWriter` (output binding)
    - per-invocation session/connection lifecycle management
4. **decorator**
    - `DbBindings` class providing Azure Functions-style decorators
    - `trigger` wraps `PollTrigger`, `input` injects query results, `output` auto-writes return values
    - `inject_reader` / `inject_writer` provide imperative `DbReader` / `DbWriter` injection
    - consumes both `trigger` and `binding` modules

### 1.1 Core Layer

Owns shared DB configuration, engine, types, errors, and serialization responsibilities.

- `DbConfig`: shared configuration representation including DB URL, schema, table, etc.
- `EngineProvider`: lazy singleton engine/pool management keyed by config
- shared types: `Row`, `RowDict`, common payload types
- shared errors: base exceptions inherited by both trigger and binding layers
- serializers: DB value normalization and row dict conversion

### 1.2 Trigger Layer

The existing trigger-related layers are preserved; DB connectivity, types, errors, and serialization use `core`.

### 1.3 Binding Layer

The binding layer provides an imperative API called directly from Azure Functions function bodies.

- `DbReader`: single-row lookup by PK, raw SQL queries
- `DbWriter`: insert/upsert/update/delete and batch writes
- per-invocation session management: sessions are opened and closed per function invocation by default

### 1.4 Dependency Rules

- `trigger -> core` dependency only
- `binding -> core` dependency only
- `decorator -> trigger, binding, core` (thin orchestration layer)
- Cross-import between `trigger` and `binding` is forbidden

### 1.5 Binding Execution Flow

```text
Azure Function Invocation
    -> user function
        -> DbReader / DbWriter instantiation
            -> DbConfig construction
            -> EngineProvider.get_or_create(config)
            -> invocation session open
            -> read / write execution
            -> success: return result or commit
            -> failure: surface exception
            -> close()
```

## Async Support

| Decorator | Async Handler | Mechanism |
|-----------|--------------|-----------|
| `trigger` | ❌ Not supported | `PollTrigger.run` is synchronous; async handlers are rejected at decoration time |
| `input` | ✅ Supported | DB I/O runs via `asyncio.to_thread()` |
| `output` | ✅ Supported | DB write runs via `asyncio.to_thread()` |
| `inject_reader` | ✅ Supported | Returns `_AsyncDbReaderProxy` with async methods |
| `inject_writer` | ✅ Supported | Returns `_AsyncDbWriterProxy` with async methods |

When an async handler is used with `input`, `output`, `inject_reader`, or `inject_writer`, all blocking database operations are automatically executed in a worker thread via `asyncio.to_thread()`, keeping the event loop free.

The `trigger` decorator explicitly rejects async handlers with a `ConfigurationError` because `PollTrigger.run()` is synchronous and calling an async handler without `await` would silently produce an unawaited coroutine.

## Decorator Composition

DbBindings decorators can be combined on a single handler. The following rules are enforced at decoration time:

### Valid Combinations
| Combination | Use Case |
|------------|----------|
| `trigger` + `output` | Process DB changes and write results |
| `trigger` + `inject_writer` | Process DB changes with imperative writes |
| `input` + `output` | Read data and write results |
| `input` + `inject_writer` | Read data with imperative writes |
| `inject_reader` + `inject_writer` | Full imperative control |
| `inject_reader` + `output` | Imperative read + auto-write |

### Invalid Combinations
| Combination | Reason |
|------------|--------|
| `input` + `inject_reader` | Redundant — both read data, use one |
| Any decorator applied twice | Not meaningful |

### Ordering
Azure Functions decorators (e.g., `@app.schedule`) must be outermost. DbBindings decorators should be closest to the function definition.

## 2. Execution Flow

```text
Azure Timer Trigger
    -> PollRunner.start()
        -> StateStore.load()
        -> StateStore.acquire_lease()    # CAS with ETag
        -> SourceAdapter.fetch_changes()
        -> core normalizer(raw records → RowChange)
        -> Handler.invoke(events)
        -> StateStore.commit()           # checkpoint + lease in one CAS write
        -> StateStore.heartbeat()        # if handler is long-running
```

## 3. Core Components

### 3.1 PollRunner
Overall lifecycle coordinator.

Responsibilities:
- per-tick execution
- overlap prevention
- batch-level commit
- handler exception surfacing
- retry-friendly exception classification

### 3.2 SourceAdapter
Abstraction for reading changes from each database.

Key methods:
- `validate()`
- `open()` / `close()`
- `fetch_changes(checkpoint, limit)` → `FetchResult` (raw records + next_checkpoint)
- `capability()` → `AdapterCapability`

The adapter returns raw records; core normalizes them into `RowChange`.

### 3.3 StateStore
Manages checkpoint and lease as a **single state blob**.

Contains:
- current watermark + tiebreaker PK
- last successful batch metadata
- lease owner / fencing_token / expires_at / heartbeat_at
- source fingerprint

All state changes are performed via **ETag-based CAS (conditional write)**,
making lease acquisition, heartbeat, and checkpoint commit atomic.

### 3.4 EventNormalizer (internal to core)
Normalizes raw records returned by the adapter into the common `RowChange` event.
This logic belongs to core; adapters only return raw records.

### 3.5 HandlerInvoker
Invokes the user handler in two modes:

- simple mode: `handler(events)`
- rich mode: `handler(events, context)`

## 4. MVP Strategy: Cursor Polling

Default algorithm:

```text
checkpoint = (cursor_value, tiebreaker_pk)

SELECT ...
FROM table
WHERE (cursor > :cursor_value)
   OR (cursor = :cursor_value AND pk > :tiebreaker_pk)
ORDER BY cursor ASC, pk ASC
LIMIT :batch_size
```

### Advantages of This Strategy
- High DB portability
- Integrates well with SQLAlchemy
- Reproducible locally and in production
- Easy to debug

### Limitations
- Cannot detect hard deletes (alternative strategies required)
- Sensitive to `updated_at` precision and clock skew
- Difficult to provide row-level before/after diffs
- Not a true event stream

## 5. Alternative Strategies

### 5.1 OutboxStrategy
The service writes to an outbox table; the library only consumes the outbox.

### 5.2 HashDiffStrategy
Periodically compares snapshots/hashes to infer changes.
High cost; reserved for special cases.

### 5.3 NativeChangeStreamStrategy
Used when the database provides a native change stream/CDC.

Examples:
- PostgreSQL logical decoding
- MySQL binlog-based
- MongoDB change stream
- SQL Server change tracking / CDC

## 6. Lease Design

Even with a timer trigger, there is a risk of overlap during scale-out or restart timing.
Therefore, the library manages **a lease embedded within a single state blob**.

Lease fields in the state blob:
- owner_id
- fencing_token
- acquired_at
- expires_at
- heartbeat_at

Principles:
- Other instances skip if a valid lease exists
- A heartbeat is required for long-running handlers
- Writers with a lower fencing token are forbidden from committing
- Lease acquisition, renewal, and commit are all performed atomically via **ETag CAS**
- No separate lease blob — prevents TOCTOU races

## 7. Commit Model

Default is **checkpoint advance after batch success**.

```text
fetch batch -> invoke handler -> success -> commit new checkpoint
                               -> failure -> no checkpoint advance
```

This model allows duplicates but minimizes data loss.

## 8. Partitioning

MVP is based on a single logical stream.
Partitioning will be introduced in subsequent versions.

Partition key candidates:
- schema/table
- tenant_id
- modulo hash(pk)
- native partition id (change stream-based)

## 9. Deployment Topology

### Simple Configuration
- Azure Function App
- Azure Storage account
- Target database
- Application Insights

### Extended Configuration
- All of the above +
- Service Bus/Event Hub relay
- Dedicated checkpoint store
- Multiple pollers per app

## 10. Architecture Principles

- Keep execution simple; keep semantics explicit
- Broad DB reach; conservative guarantees
- Handlers behave like pure functions; orchestration is the framework's job
- Operational concerns built-in by default; abstractions kept thin
