# Azure Functions DB

[![PyPI](https://img.shields.io/pypi/v/azure-functions-db.svg)](https://pypi.org/project/azure-functions-db/)
[![Python Version](https://img.shields.io/badge/python-3.10%20%7C%203.11%20%7C%203.12%20%7C%203.13%20%7C%203.14-blue)](https://pypi.org/project/azure-functions-db/)
[![CI](https://github.com/yeongseon/azure-functions-db-python/actions/workflows/ci-test.yml/badge.svg)](https://github.com/yeongseon/azure-functions-db-python/actions/workflows/ci-test.yml)
[![Release](https://github.com/yeongseon/azure-functions-db-python/actions/workflows/publish-pypi.yml/badge.svg)](https://github.com/yeongseon/azure-functions-db-python/actions/workflows/publish-pypi.yml)
[![codecov](https://codecov.io/gh/yeongseon/azure-functions-db-python/branch/main/graph/badge.svg)](https://codecov.io/gh/yeongseon/azure-functions-db-python)
[![pre-commit](https://img.shields.io/badge/pre--commit-enabled-brightgreen?logo=pre-commit)](https://pre-commit.com/)
[![Docs](https://img.shields.io/badge/docs-gh--pages-blue)](https://yeongseon.github.io/azure-functions-db-python/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

SQLAlchemy-powered database integration helpers for **Azure Functions Python v2** — binding-style decorators for input/output/client injection and a poll-based pseudo trigger that works with **any database that ships a SQLAlchemy dialect**.

> **Not a native Azure Functions binding extension.**
> This package does **not** register native Azure Functions bindings with the Functions host. The `@db.input` / `@db.output` / `@db.trigger` decorators are Python function wrappers that resolve data, inject writers, or poll for changes around your handler. For runtime-native Azure SQL bindings, use the official extension (see below).

---

Part of the **Azure Functions Python DX Toolkit**
→ Bring FastAPI-like developer experience to Azure Functions

## Why this exists

Microsoft already ships [official Azure SQL bindings](https://learn.microsoft.com/azure/azure-functions/functions-bindings-azure-sql) — including a [SQL trigger](https://learn.microsoft.com/azure/azure-functions/functions-bindings-azure-sql-trigger) backed by SQL Change Tracking — for **Azure SQL Database** and **SQL Server**. If those cover your scenario, prefer them.

This package fills a different gap. Python teams on Azure Functions still have to hand-roll integration when they need:

- **No generic SQLAlchemy trigger** — the official SQL bindings target Azure SQL / SQL Server only.
- **No unified multi-dialect binding layer** — PostgreSQL, MySQL, SQLite, Oracle, DuckDB, CockroachDB, and other SQLAlchemy dialects each require custom code.
- **No SQLAlchemy-native reader/writer injection pattern** for the Python v2 programming model.
- **No first-class polling primitive** — checkpoint, lease, batching, idempotency, and at-least-once delivery built on top of the existing timer trigger.

## When to use this

Use **`azure-functions-db`** when:

- You need PostgreSQL, MySQL, SQLite, Oracle, DuckDB, CockroachDB, or another SQLAlchemy dialect.
- You want Python decorator-based input / output / client injection that maps cleanly onto v2 handlers.
- You want timer-driven polling with checkpoint and lease control instead of a host-managed trigger extension.
- You want local-first, testable DB integration without depending on a native Functions extension.

Use the **official Azure SQL bindings** when:

- You target Azure SQL Database or SQL Server.
- You want a runtime-native SQL trigger / input binding / output binding registered with the Functions host.
- You rely on SQL Change Tracking and extension-managed scaling for your trigger source.

## What it does

- **Poll-based pseudo trigger** — change detection via cursor-column polling, with checkpoint, lease, batching, and at-least-once delivery on top of an Azure Functions timer trigger.
- **Any SQLAlchemy database** — PostgreSQL, MySQL, SQL Server out of the box; Oracle, CockroachDB, DuckDB, and [any other dialect](https://docs.sqlalchemy.org/en/20/dialects/) with one extra `pip install`.
- **Single `pip install`** — one package with optional extras for the common database drivers.
- **Input-style data injection** — `@db.input` resolves query results and passes them to your handler.
- **Output-style writer injection** — `@db.output` injects a `DbOut` writer; you call `.set(...)` to write explicitly.
- **Client injection** — `@db.inject_reader` / `@db.inject_writer` provide imperative `DbReader` / `DbWriter` clients for multi-statement reads or transactional writes.

## Compared with official Azure SQL bindings

| Capability                              | Official Azure SQL bindings                       | `azure-functions-db`                              |
| --------------------------------------- | ------------------------------------------------- | ------------------------------------------------- |
| Azure SQL / SQL Server                  | Native, runtime-managed                           | Supported via SQLAlchemy + pyodbc                 |
| PostgreSQL                              | Not supported                                     | Built-in extra (`[postgres]`)                     |
| MySQL                                   | Not supported                                     | Built-in extra (`[mysql]`)                        |
| SQLite                                  | Not supported                                     | Supported via SQLAlchemy                          |
| Oracle / DuckDB / CockroachDB           | Not supported                                     | BYOD — install dialect, pass SQLAlchemy URL       |
| Custom non-SQL source                   | Not supported                                     | Implement `SourceAdapter` for `db.trigger(...)`   |
| Runtime-native binding registration     | Yes (via Functions host extension)                | No — Python decorator wrapper                     |
| Trigger mechanism                       | SQL Change Tracking                               | Cursor-column polling on a timer trigger          |
| Scaling integration                     | Functions extension scale controller              | Driven by your timer trigger schedule             |
| Delivery guarantee                      | Per official docs                                 | At-least-once (handlers must be idempotent)       |
| Checkpoint storage                      | Managed by extension                              | Azure Blob Storage (`BlobCheckpointStore`)        |

## Choose your integration path

| Path | When to use | What to do |
|------|-------------|------------|
| **Built-in extras** | PostgreSQL, MySQL, or SQL Server | `pip install azure-functions-db[postgres]` and go |
| **Bring your own SQLAlchemy database** | Oracle, CockroachDB, DuckDB, or any other RDBMS with a SQLAlchemy dialect | Install the driver, use the SQLAlchemy connection URL |
| **Custom trigger source** *(triggers only)* | Non-SQL sources (MongoDB, Kafka, REST APIs) | Implement the `SourceAdapter` Protocol for `db.trigger()` |

### Bring your own database

The bindings and `SqlAlchemySource` are designed to work with **any database that has a SQLAlchemy dialect**. The built-in extras just bundle common drivers for convenience.

Three steps:

1. **Install the driver** — e.g. `pip install oracledb` for Oracle
2. **Use the SQLAlchemy URL** — e.g. `url="oracle+oracledb://user:pass@host/db"`
3. **Pass engine options if needed** — use `engine_kwargs` for driver-specific settings

```python
from azure_functions_db import DbBindings

db = DbBindings()

@db.input("rows", url="oracle+oracledb://user:pass@host:1521/mydb",
          query="SELECT * FROM orders WHERE status = :status",
          params={"status": "pending"})
def read_oracle_orders(rows: list[dict]) -> None:
    for row in rows:
        print(row)
```

The same applies to triggers — `SqlAlchemySource` accepts any SQLAlchemy URL:

```python
from azure_functions_db import SqlAlchemySource

source = SqlAlchemySource(
    url="oracle+oracledb://user:pass@host:1521/mydb",
    table="orders",
    cursor_column="updated_at",
    pk_columns=["id"],
)
```

> **Note:** The built-in extras (PostgreSQL, MySQL, SQL Server) are the tested path. Other dialects work through SQLAlchemy compatibility but are not explicitly tested by this project. Exact connection URL syntax varies by driver — check your driver's documentation.

> See [`examples/byod_oracle/`](examples/byod_oracle/) for a complete runnable Function App and [`examples/usage_byod.py`](examples/usage_byod.py) for a minimal standalone script.

### Custom trigger source

If your data source has no SQLAlchemy dialect, implement the [`SourceAdapter`](docs/05-adapter-sdk.md) protocol and pass it directly to `db.trigger(source=...)`. This applies only to the trigger feature. See the [Adapter SDK](docs/05-adapter-sdk.md) for the full contract.

## Shared Core

`azure-functions-db-python` now exposes shared infrastructure for upcoming bindings. Use `DbConfig` for normalized connection settings and `EngineProvider` when multiple components should reuse the same lazily created SQLAlchemy engine.

## Installation

```bash
# Core package (pick your database)
pip install azure-functions-db[postgres]
pip install azure-functions-db[mysql]
pip install azure-functions-db[mssql]

# Multiple databases
pip install azure-functions-db[postgres,mysql]

# All drivers
pip install azure-functions-db[all]
```

Your Function App dependencies should include:

```text
azure-functions
azure-functions-db[postgres]
```

## Quick Start

### Which decorator to use?

| Need | Decorator | Mode |
|------|-----------|------|
| Read data into handler | `input` | Input-style data injection |
| Write data to DB | `output` | Output-style writer injection |
| Complex reads (multiple queries) | `inject_reader` | Imperative client injection |
| Complex writes (transactions) | `inject_writer` | Imperative client injection |
| React to DB changes | `trigger` | Poll-based pseudo trigger |

> All decorators are Python function wrappers. They are **not** registered as native Azure Functions bindings with the host.

### Input-style data injection

`@db.input` injects the actual query result into your handler — no client needed.

**Row lookup mode** — fetch a single row by primary key:

```python
from azure_functions_db import DbBindings

db = DbBindings()

# Static primary key
@db.input("user", url="%DB_URL%", table="users", pk={"id": 42})
def load_user(user: dict | None) -> None:
    if user:
        print(user["name"])

# Dynamic primary key — resolved from handler kwargs
@db.input("user", url="%DB_URL%", table="users",
             pk=lambda req: {"id": req.params["id"]})
def get_user(req, user: dict | None) -> None:
    print(user)
```

**Query mode** — fetch multiple rows with SQL:

```python
# Multiple rows by SQL query
@db.input("users", url="%DB_URL%",
             query="SELECT * FROM users WHERE active = :active",
             params={"active": True})
def list_active_users(users: list[dict]) -> None:
    for user in users:
        print(user["email"])
```

### Output-style writer injection

`@db.output` injects a `DbOut` instance into your handler — call `.set()` to write explicitly.

```python
from azure_functions_db import DbBindings, DbOut

db = DbBindings()

# Insert — call .set() with a dict for single row, list[dict] for batch
@db.output("out", url="%DB_URL%", table="orders")
def create_order(out: DbOut) -> str:
    out.set({"id": 1, "status": "pending", "total": 99.99})
    return "Created"

# Upsert — set action and conflict_columns
@db.output("out", url="%DB_URL%", table="orders",
              action="upsert", conflict_columns=["id"])
def upsert_orders(out: DbOut) -> str:
    out.set([
        {"id": 1, "status": "shipped", "total": 99.99},
        {"id": 2, "status": "pending", "total": 49.99},
    ])
    return "Upserted"
```

The handler's return value is independent of the write — use it for HTTP responses or anything else:

```python
import azure.functions as func
from azure_functions_db import DbBindings, DbOut

db = DbBindings()

@db.output("out", url="%DB_URL%", table="orders")
def create_order(req: func.HttpRequest, out: DbOut) -> func.HttpResponse:
    out.set({"id": 1, "status": "pending"})
    return func.HttpResponse("Created", status_code=201)
```

Supported upsert dialects: PostgreSQL, SQLite, MySQL.

`DbOut.set([])` is an explicit no-op: it accepts an empty list (e.g. when your handler computed nothing to write) and emits no SQL. Calling `set` again with a non-empty payload after a no-op still writes normally.

### Client injection (imperative escape hatches)

For complex operations (multiple queries, transactions, update/delete), use `inject_reader` / `inject_writer` to receive a client instance:

```python
from azure_functions_db import DbBindings, DbReader, DbWriter

db = DbBindings()

@db.inject_reader("reader", url="%DB_URL%", table="users")
def complex_read(reader: DbReader) -> None:
    user = reader.get(pk={"id": 42})
    orders = reader.query("SELECT * FROM orders WHERE user_id = :uid", params={"uid": 42})

    total = reader.scalar("SELECT COUNT(*) FROM users")
    profile = reader.one("SELECT * FROM users WHERE id = :id", params={"id": 42})
    maybe_admin = reader.one_or_none(
        "SELECT * FROM admins WHERE user_id = :uid", params={"uid": 42}
    )

@db.inject_writer("writer", url="%DB_URL%", table="orders")
def complex_write(writer: DbWriter) -> None:
    writer.insert(data={"id": 1, "status": "pending"})
    writer.update(data={"status": "shipped"}, pk={"id": 1})
    writer.delete(pk={"id": 1})
```

#### Atomic multi-statement writes — `DbWriter.transaction()`

`transaction()` is a context manager that opens a single SQLAlchemy connection, begins a transaction, and shares it across every `insert` / `upsert` / `update` / `delete` call made on the same writer inside the `with` block. The transaction commits on successful exit and rolls back on any exception, including `WriteError` raised by individual write calls.

```python
@db.inject_writer("writer", url="%DB_URL%", table="orders")
def transfer(writer: DbWriter) -> None:
    with writer.transaction():
        writer.insert(data={"id": 1, "status": "pending", "total": 99.99})
        writer.update(data={"status": "shipped"}, pk={"id": 1})
```

Notes:

- Nested transactions are not supported; calling `transaction()` while one is active raises `WriteError`.
- After the `with` block exits the writer returns to its default per-call autocommit behavior.
- `transaction()` works only on the imperative `DbWriter` (`inject_writer`). The `@db.output` `DbOut` writer commits its single `.set(...)` call atomically and does not need a transaction wrapper.

#### `DbReader` row-shape helpers

| Method | Rows | Behavior |
|---|---|---|
| `reader.scalar(sql, params=...)` | 0 or 1 | First column of the row, or `None` for zero rows. Multiple rows raise `QueryError`. |
| `reader.one(sql, params=...)` | exactly 1 | Returns a `dict[str, object]`. Zero or multiple rows raise `QueryError`. |
| `reader.one_or_none(sql, params=...)` | 0 or 1 | Returns a `dict[str, object]` or `None`. Multiple rows raise `QueryError`. |

### Trigger (poll-based pseudo trigger)

> **This is a pseudo trigger, not a native Azure Functions trigger.**
> `@db.trigger` does not register a binding with the Functions host. It must be stacked **on top of a real Azure Functions trigger** (typically `@app.schedule` / timer) that fires the polling loop.
>
> Delivery is **at-least-once**. Duplicates may occur during process crashes, lease transitions, or checkpoint commit failures. **Handlers must be idempotent.** See [Semantics — Duplicate Windows](docs/03-semantics.md#13-duplicate-and-reprocessing-windows).

```python
import azure.functions as func
from azure.storage.blob import ContainerClient
from azure_functions_db import BlobCheckpointStore, DbBindings, RowChange, SqlAlchemySource

app = func.FunctionApp()
db = DbBindings()

source = SqlAlchemySource(
    url="%ORDERS_DB_URL%",
    table="orders",
    schema="public",
    cursor_column="updated_at",
    pk_columns=["id"],
)

checkpoint_store = BlobCheckpointStore(
    container_client=ContainerClient.from_connection_string(
        conn_str="%AzureWebJobsStorage%",
        container_name="db-state",
    ),
    source_fingerprint=source.source_descriptor.fingerprint,
)

@app.function_name(name="orders_poll")
@app.schedule(schedule="0 */1 * * * *", arg_name="timer", use_monitor=True)
@db.trigger(arg_name="events", source=source, checkpoint_store=checkpoint_store)
def orders_poll(timer: func.TimerRequest, events: list[RowChange]) -> None:
    for event in events:
        # Idempotent processing required: the same `event` may be delivered more than once.
        print(f"Order {event.pk}: {event.op}")
```

> See [Python API Spec](docs/04-python-api-spec.md) for the full API reference.

### Combined: Trigger + writer injection

Process database changes and write results to another table. Uses `EngineProvider` for shared connection pooling.

```python
import azure.functions as func
from azure.storage.blob import ContainerClient

from azure_functions_db import (
    BlobCheckpointStore,
    DbBindings,
    DbOut,
    EngineProvider,
    RowChange,
    SqlAlchemySource,
)

app = func.FunctionApp()
db = DbBindings()

engine_provider = EngineProvider()

source = SqlAlchemySource(
    url="%SOURCE_DB_URL%",
    table="orders",
    cursor_column="updated_at",
    pk_columns=["id"],
    engine_provider=engine_provider,
)

checkpoint_store = BlobCheckpointStore(
    container_client=ContainerClient.from_connection_string(
        conn_str="%AzureWebJobsStorage%",
        container_name="db-state",
    ),
    source_fingerprint=source.source_descriptor.fingerprint,
)

@app.function_name(name="orders_poll")
@app.schedule(schedule="0 */1 * * * *", arg_name="timer", use_monitor=True)
@db.trigger(arg_name="events", source=source, checkpoint_store=checkpoint_store)
@db.output(
    "out",
    url="%DEST_DB_URL%",
    table="processed_orders",
    action="upsert",
    conflict_columns=["order_id"],
    engine_provider=engine_provider,
)
def orders_poll(timer: func.TimerRequest, events: list[RowChange], out: DbOut) -> None:
    out.set([
        {
            "order_id": event.pk["id"],
            "customer": event.after["name"],
            "processed_at": str(event.cursor),
        }
        for event in events
        if event.after is not None
    ])
```

See [`examples/trigger_with_binding/`](examples/trigger_with_binding/) for a complete runnable sample.

## Built-in Extras

These databases have pre-packaged driver dependencies. Install the matching extra and you're ready to go.

| Database | Extra | Driver |
|----------|-------|--------|
| PostgreSQL | `azure-functions-db[postgres]` | [psycopg](https://www.psycopg.org/) |
| MySQL | `azure-functions-db[mysql]` | [PyMySQL](https://pymysql.readthedocs.io/) |
| SQL Server | `azure-functions-db[mssql]` | [pyodbc](https://github.com/mkleehammer/pyodbc) |

Any other database with a [SQLAlchemy dialect](https://docs.sqlalchemy.org/en/20/dialects/) works too — just install the driver yourself. See [Choose your integration path](#choose-your-integration-path).

## Scope

- Azure Functions Python **v2 programming model**
- Timer-triggered functions for poll-based change detection
- SQLAlchemy 2.0+ for database abstraction
- Checkpoint storage via Azure Blob Storage
- Read/write bindings via HTTP/Queue/Event triggers

This package does **not** implement a native Azure Functions trigger extension. It uses a poll-based approach on top of the existing timer trigger.

## Async handlers

`async def` handlers are supported. SQLAlchemy operations inside `azure-functions-db` run synchronously; when an `async` handler invokes a binding (input fetch, `DbOut.set(...)`, polling commit, etc.) the package offloads the blocking call to a worker thread via `asyncio.to_thread`, so the event loop is not blocked.

This package does **not** use SQLAlchemy `AsyncEngine` internally. If you need fully native asyncio drivers (e.g. `asyncpg`, `aiomysql`), drive them yourself outside the binding — `azure-functions-db` deliberately exposes a single sync engine path so behavior across dialects stays identical.

## `engine_kwargs` flow-through

Every binding decorator and `DbConfig` accept an `engine_kwargs` mapping that is forwarded to `sqlalchemy.create_engine`. Anything the underlying dialect supports — connection / query timeouts, pool sizing, isolation level, `connect_args`, custom event listeners — flows through unchanged. Use `EngineProvider` when several bindings should share a single engine instance with a consistent `engine_kwargs` configuration.

## Observability

`azure-functions-db-python` exposes structured log helpers plus a lightweight `MetricsCollector` protocol so you can connect your own metrics backend without adding hard dependencies.

```python
from collections.abc import Mapping

from azure_functions_db import MetricsCollector, PollTrigger


class PrintMetricsCollector:
    def increment(
        self, name: str, value: float = 1, *, labels: Mapping[str, str] | None = None
    ) -> None:
        print("increment", name, value, labels)

    def observe(
        self, name: str, value: float, *, labels: Mapping[str, str] | None = None
    ) -> None:
        print("observe", name, value, labels)

    def set_gauge(
        self, name: str, value: float, *, labels: Mapping[str, str] | None = None
    ) -> None:
        print("gauge", name, value, labels)


trigger = PollTrigger(
    name="orders",
    source=source,
    checkpoint_store=checkpoint_store,
    metrics=PrintMetricsCollector(),
)
```

## Key Design Decisions

- **Pseudo trigger** — timer-based polling instead of native C# extension ([ADR-001](docs/16-ADR-001-pseudo-trigger-over-native.md))
- **SQLAlchemy-centric** — single ORM layer for all databases ([ADR-002](docs/17-ADR-002-sqlalchemy-centric-adapter.md))
- **Blob checkpoint** — Azure Blob Storage for checkpoint persistence ([ADR-003](docs/18-ADR-003-blob-checkpoint-mvp.md))
- **At-least-once** — default delivery guarantee with idempotency support ([ADR-004](docs/19-ADR-004-at-least-once-default.md))
- **Unified package** — trigger + binding in one package ([ADR-005](docs/23-ADR-005-unified-package-design.md))

## Duplicate Handling

This package provides **at-least-once** delivery for the polling trigger. Duplicates may occur during process crashes, lease transitions, or checkpoint commit failures. Handlers must be idempotent. Recommended patterns:

- Use the row primary key (`event.pk`) plus `event.cursor` as a deduplication key in your sink.
- Wrap downstream writes in a transaction with a unique constraint that you can swallow.
- For batch writes, prefer upsert (`action="upsert"` with `conflict_columns=...`) over plain insert.

See [Semantics — Duplicate Windows](docs/03-semantics.md#13-duplicate-and-reprocessing-windows) for the full guarantee model and the windows in which duplicates can be observed.

## Documentation

- Full docs: [yeongseon.github.io/azure-functions-db-python](https://yeongseon.github.io/azure-functions-db-python/)
- Examples: `examples/`
- [Architecture](docs/02-architecture.md)
- [Semantics](docs/03-semantics.md)
- [Python API Spec](docs/04-python-api-spec.md)
- [Adapter SDK](docs/05-adapter-sdk.md)

## Ecosystem

Part of the **Azure Functions Python DX Toolkit**:

| Package | Role |
|---------|------|
| [azure-functions-openapi-python](https://github.com/yeongseon/azure-functions-openapi-python) | OpenAPI spec generation and Swagger UI |
| [azure-functions-validation-python](https://github.com/yeongseon/azure-functions-validation-python) | Request/response validation and serialization |
| **azure-functions-db-python** | SQLAlchemy-powered DB integration helpers (poll-based pseudo trigger, input/output/client injection) |
| [azure-functions-langgraph-python](https://github.com/yeongseon/azure-functions-langgraph-python) | LangGraph deployment adapter for Azure Functions |
| [azure-functions-scaffold-python](https://github.com/yeongseon/azure-functions-scaffold-python) | Project scaffolding CLI |
| [azure-functions-logging-python](https://github.com/yeongseon/azure-functions-logging-python) | Structured logging and observability |
| [azure-functions-doctor-python](https://github.com/yeongseon/azure-functions-doctor-python) | Pre-deploy diagnostic CLI |
| [azure-functions-durable-graph-python](https://github.com/yeongseon/azure-functions-durable-graph-python) | Manifest-first graph runtime with Durable Functions *(experimental)* |
| [azure-functions-cookbook-python](https://github.com/yeongseon/azure-functions-cookbook-python) | Recipes and examples |

## Disclaimer

This project is an independent community project and is not affiliated with,
endorsed by, or maintained by Microsoft.

Azure and Azure Functions are trademarks of Microsoft Corporation.

## License

MIT
