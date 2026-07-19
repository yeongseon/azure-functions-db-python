# Beyond Azure SQL Bindings: SQLAlchemy-powered DB integrations for Azure Functions Python

> **Status: draft.** Positioning / blog draft tracked by issue #107. Lives under
> `drafts/` so it is **not** published to the docs site. Edit freely before
> posting to Medium / dev.to. Update the links marked _(verify)_ before publishing.

*A Python-first, SQLAlchemy-based approach to PostgreSQL, MySQL, SQLite, and
custom data sources in Azure Functions — without waiting for a native binding
extension.*

---

## The gap this fills

Microsoft already ships [official Azure SQL bindings](https://learn.microsoft.com/azure/azure-functions/functions-bindings-azure-sql)
for Azure Functions, including a [SQL trigger](https://learn.microsoft.com/azure/azure-functions/functions-bindings-azure-sql-trigger)
backed by SQL Change Tracking. If you are on **Azure SQL Database** or **SQL
Server** and those bindings cover your scenario, use them — they are
runtime-native, host-managed, and give you exactly-once semantics on managed
sinks.

But a lot of Python teams on Azure Functions are *not* only on Azure SQL. They
run PostgreSQL, MySQL, SQLite, Oracle, CockroachDB, DuckDB — or they need to
react to changes in a non-SQL source entirely. For them the official bindings
leave four gaps:

- **No generic SQLAlchemy trigger** — the official SQL bindings target Azure SQL
  / SQL Server only.
- **No unified multi-dialect binding layer** — every other database means
  hand-rolled integration code.
- **No SQLAlchemy-native reader/writer injection** for the v2 programming model.
- **No first-class polling primitive** with checkpoint, lease, batching, and
  at-least-once delivery on top of the timer trigger.

[`azure-functions-db`](https://pypi.org/project/azure-functions-db/) fills that
gap: SQLAlchemy-powered binding-style decorators plus a poll-based pseudo
trigger that works with **any database that ships a SQLAlchemy dialect**.

> For the full axis-by-axis breakdown (databases, trigger mechanism, scaling,
> delivery guarantee, checkpoint storage, local testing, SQLAlchemy/BYOD,
> production readiness), see the
> [comparison page](https://yeongseon.github.io/azure-functions-db-python/) _(verify link)_
> and [ADR-006 — Python Wrapper over Native Extension](https://github.com/yeongseon/azure-functions-db/blob/main/docs/27-ADR-006-no-native-extension.md).

## Not a native binding — and that's a deliberate choice

`azure-functions-db` does **not** register native Azure Functions bindings with
the host. The `@db.input` / `@db.output` / `@db.trigger` decorators are Python
function wrappers that resolve data, inject writers, or poll for changes around
your handler. That trade-off (no native binding metadata / scale-controller
integration, in exchange for any-dialect support and local-first testability) is
documented up front — see ADR-006. Being explicit about it is the point: you
know exactly what you are and aren't getting.

## A runnable example, end to end (PostgreSQL polling trigger)

The [`postgresql-poll-trigger`](https://github.com/yeongseon/azure-functions-db/tree/main/examples/postgresql-poll-trigger)
example is fully runnable with Docker Compose (PostgreSQL 16 + Azurite for the
checkpoint store). The core is a timer-driven trigger that polls an `orders`
table and projects each change into a `processed_orders` table:

```python
import azure.functions as func
from azure.storage.blob import ContainerClient
from azure_functions_db import (
    BlobCheckpointStore, DbBindings, DbOut, EngineProvider, RowChange, SqlAlchemySource,
)

app = func.FunctionApp()
db = DbBindings()
engine_provider = EngineProvider()

source = SqlAlchemySource(
    url="%SOURCE_DB_URL%",
    table="orders",
    schema="public",
    cursor_column="updated_at",
    pk_columns=["id"],
    engine_provider=engine_provider,
)

checkpoint_store = BlobCheckpointStore(
    container_client=ContainerClient.from_connection_string(
        conn_str="%AzureWebJobsStorage%", container_name="db-state",
    ),
    source_fingerprint=source.source_descriptor.fingerprint,
)

@app.function_name(name="orders_poll")
@app.schedule(schedule="0 */1 * * * *", arg_name="timer", use_monitor=True)
@db.trigger(arg_name="events", source=source, checkpoint_store=checkpoint_store)
@db.output("out", url="%DEST_DB_URL%", table="processed_orders",
           action="upsert", conflict_columns=["order_id", "source_cursor"],
           engine_provider=engine_provider)
def orders_poll(timer, events: list[RowChange], out: DbOut) -> None:
    out.set([
        {
            "order_id": e.pk["id"],
            "source_cursor": e.cursor[0],
            "customer_name": e.after["customer_name"],
            "amount": e.after["amount"],
            "status": e.after["status"],
        }
        for e in events if e.after is not None
    ])
```

One `docker compose up`, apply `schema.sql`, `func start`, insert a couple of
rows — and you watch changes flow into the projection table on the next tick.
No cloud resources required to see it work.

## At-least-once delivery and idempotent handlers

The polling trigger provides **at-least-once** delivery. Duplicates can occur
during process crashes, lease transitions, or checkpoint-commit failures, so
**handlers must be idempotent**. The example encodes the canonical pattern: the
destination table is keyed on the composite `(order_id, source_cursor)` —
i.e. `(event.pk, event.cursor)` — and written with `action="upsert"`. A replay
of the same `RowChange` collides on the exact same key with identical values, so
the second write is a byte-identical no-op.

That distinction matters. Keying on `order_id` alone gives you a *latest-state
projection* where an out-of-order replay of an older event could overwrite a
newer one. Keying on `(pk, cursor)` makes "redelivery = no-op" precisely true.
When your sink has no upsert (e.g. SQL Server), the
[`mssql-poll-trigger`](https://github.com/yeongseon/azure-functions-db/tree/main/examples/mssql-poll-trigger)
example shows the fallback: `inject_writer` + `insert`, swallowing the
primary-key violation on replay.

## Bring your own database (any SQLAlchemy URL)

The built-in extras (`postgres`, `mysql`, `mssql`) just bundle common drivers.
The bindings and `SqlAlchemySource` work with **any** SQLAlchemy dialect:

```python
from azure_functions_db import SqlAlchemySource

source = SqlAlchemySource(
    url="oracle+oracledb://user:pass@host:1521/mydb",
    table="orders",
    cursor_column="updated_at",
    pk_columns=["id"],
)
```

Install the driver, use the SQLAlchemy URL, pass `engine_kwargs` if the dialect
needs them — Oracle, CockroachDB, DuckDB, and friends all work. See the
[`byod_oracle`](https://github.com/yeongseon/azure-functions-db/tree/main/examples/byod_oracle)
example.

## When there's no SQLAlchemy dialect: the `SourceAdapter` extension point

If your source isn't a SQL database at all — MongoDB, Kafka, a REST API — you
implement the [`SourceAdapter`](https://github.com/yeongseon/azure-functions-db/blob/main/docs/05-adapter-sdk.md)
protocol and hand it to `db.trigger(source=...)`. The polling machinery
(checkpoint, lease, batching, idempotent delivery) is reused unchanged; you only
supply "how do I fetch changes since this cursor?". That keeps the same
at-least-once contract across wildly different sources.

## Where it fits

`azure-functions-db` is part of the **Azure Functions Python DX Toolkit** — a
family of packages bringing a FastAPI-like developer experience to Azure
Functions:

- [azure-functions-openapi](https://github.com/yeongseon/azure-functions-openapi-python) — OpenAPI + Swagger UI
- [azure-functions-validation](https://github.com/yeongseon/azure-functions-validation-python) — request/response validation
- **azure-functions-db** — SQLAlchemy DB integration (this post)
- [azure-functions-logging](https://github.com/yeongseon/azure-functions-logging-python) — structured logging
- [azure-functions-scaffold](https://github.com/yeongseon/azure-functions-scaffold-python) — project scaffolding CLI
- [azure-functions-doctor](https://github.com/yeongseon/azure-functions-doctor-python) — pre-deploy diagnostics

## Try it

```bash
pip install azure-functions-db[postgres]
```

- Package: <https://pypi.org/project/azure-functions-db/>
- Docs: <https://yeongseon.github.io/azure-functions-db-python/>
- Source & examples: <https://github.com/yeongseon/azure-functions-db>

*This is an independent community project and is not affiliated with, endorsed
by, or maintained by Microsoft. Azure and Azure Functions are trademarks of
Microsoft Corporation.*
