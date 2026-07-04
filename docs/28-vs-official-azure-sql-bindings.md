# `azure-functions-db` vs Official Azure SQL Bindings

`azure-functions-db-python` and Microsoft's [official Azure SQL bindings](https://learn.microsoft.com/azure/azure-functions/functions-bindings-azure-sql) solve overlapping problems in different ways. This page is a **side-by-side comparison** on every axis that matters when picking between them.

For the underlying design rationale of the Python wrapper model, see
[ADR-006 — Python Wrapper over Native Azure Functions Extension](27-ADR-006-no-native-extension.md).

## Quick pick

- **Azure SQL / SQL Server only, and you want the Functions host to manage everything?** → prefer the [official Azure SQL bindings](https://learn.microsoft.com/azure/azure-functions/functions-bindings-azure-sql).
- **PostgreSQL, MySQL, SQLite, or any other SQLAlchemy dialect?** → this package.
- **Non-SQL data source (Mongo, Kafka, HTTP API) that still fits a polling model?** → this package via [`SourceAdapter`](05-adapter-sdk.md).
- **Need runtime-native binding metadata visible to Portal and scale controllers?** → the official extension. This package deliberately does not register with the host.
- **Need `pip install` and a fully Python toolchain?** → this package.

## At a glance

| Axis                              | Official Azure SQL bindings                       | `azure-functions-db-python`                                    |
| --------------------------------- | ------------------------------------------------- | -------------------------------------------------------------- |
| Databases                         | Azure SQL Database, SQL Server                    | PostgreSQL, MySQL, SQL Server + any SQLAlchemy dialect (BYOD)  |
| Trigger mechanism                 | SQL Change Tracking (CT)                          | Cursor-column polling on the timer trigger                     |
| Trigger scaling                   | Functions extension scale controller              | Timer schedule × `PollTrigger` batch size                      |
| Delivery guarantee                | Exactly-once (per official docs, on managed sinks) | At-least-once — handlers must be idempotent                    |
| Checkpoint storage                | Leases table in the source DB (`az_func` schema)  | Azure Blob Storage via `BlobCheckpointStore`                   |
| Extension language                | C# / .NET (WebJobs SDK extension)                 | Pure Python decorators                                         |
| Distribution                      | Extension bundle / `func extensions install`      | `pip install azure-functions-db[<extra>]`                      |
| Native binding metadata           | Yes (registered with the Functions host)          | No — the host only sees the underlying timer trigger           |
| Portal binding visualization      | Yes                                               | No — only the timer trigger appears                            |
| Local testing without host        | Requires Functions host + `func start`            | Plain `pytest` — the wrapper is a Python function              |
| SQLAlchemy support                | Not applicable                                    | First-class — every read / write / poll goes through SQLAlchemy |
| Custom source support             | No                                                | Yes — implement `SourceAdapter` for non-SQL sources            |

The rest of this page expands each axis with the trade-offs and the concrete implications for Python teams on Azure Functions.

## 1. Supported databases

**Official Azure SQL bindings**

- Azure SQL Database and SQL Server (managed and self-hosted).
- The trigger specifically requires [SQL Change Tracking](https://learn.microsoft.com/sql/relational-databases/track-changes/enable-and-disable-change-tracking-sql-server) to be enabled on the target table.
- No PostgreSQL, MySQL, SQLite, or non-SQL sources.

**`azure-functions-db-python`**

- Built-in extras with tested drivers:
  - `azure-functions-db[postgres]` — `psycopg`
  - `azure-functions-db[mysql]` — `PyMySQL`
  - `azure-functions-db[mssql]` — `pyodbc`
- BYOD (bring-your-own-dialect) for anything else with a [SQLAlchemy dialect](https://docs.sqlalchemy.org/en/20/dialects/): Oracle, DuckDB, CockroachDB, ClickHouse, Snowflake, and so on. The pattern is: install the driver, use the SQLAlchemy URL, done.
- Non-SQL sources via [`SourceAdapter`](05-adapter-sdk.md) for triggers only (input / output bindings remain SQLAlchemy-based).

## 2. Trigger mechanism — SQL Change Tracking vs cursor-column polling

**Official Azure SQL bindings**

- Uses [SQL Change Tracking](https://learn.microsoft.com/sql/relational-databases/track-changes/about-change-tracking-sql-server) on the source table. The extension queries `CHANGETABLE(CHANGES ...)` to discover inserts, updates, and deletes since the last delivered change version.
- Change Tracking must be enabled on the database and on each triggered table (`ALTER DATABASE ... SET CHANGE_TRACKING = ON`).
- Deletes are visible.
- The extension owns the change version watermark; it lives in leases stored in the source database.

**`azure-functions-db-python`**

- Uses [**cursor-column polling**](24-polling-runtime-semantics.md): on each timer tick, `SqlAlchemySource` selects rows where the configured cursor column (e.g. `updated_at`, `id`, `version`) is greater than the last committed checkpoint, ordered by cursor + primary key, capped at the batch size.
- Requires you to model a **monotonically non-decreasing cursor column** on the source table. This is a schema constraint but works on every RDBMS.
- Deletes are **not** detected by cursor-column polling. If you need delete detection, use a soft-delete pattern (a `deleted_at` column) or a companion tombstone table that is itself cursor-column polled. Alternatively, implement a custom `SourceAdapter` that surfaces deletes from your DB's own change log (e.g. Postgres logical replication, MySQL binlog).
- Cross-dialect: the same trigger model works for PostgreSQL, MySQL, SQL Server, SQLite, Oracle, and any other SQLAlchemy dialect. The trade-off is that no dialect gets native change-log semantics — everyone gets cursor polling.

See [Semantics § 1.2 Source Preconditions](03-semantics.md#12-source-preconditions) for the exact contract the cursor column must satisfy.

## 3. Scaling — extension scale controller vs timer schedule

**Official Azure SQL bindings**

- Integrates with the Functions [scale controller](https://learn.microsoft.com/azure/azure-functions/event-driven-scaling) via the WebJobs SDK. When Change Tracking reports a growing backlog on the source table, the host can scale worker instances up.
- The scale decision is host-managed; the extension surfaces backlog metrics natively.

**`azure-functions-db-python`**

- No dedicated DB scale controller. `@db.trigger` runs on top of a real `@app.schedule` timer trigger, so worker scaling follows the **timer trigger's own scale-controller path**. On the Consumption / Elastic Premium plans, the Functions host still scales workers, just via the timer's dispatch rate rather than a bespoke DB-backlog probe.
- Throughput is bounded by **timer cadence × `PollTrigger` batch size**:
  - Example: `schedule="0 */1 * * * *"` (every minute) with `PollTrigger(batch_size=500)` = up to 500 rows per minute per instance.
  - Tighten cadence and raise batch size for higher throughput; loosen both for lower cost.
- If you need push-based sub-second scaling for Azure SQL specifically, prefer the official extension. Cross-dialect users generally accept the timer cadence because polling is the trade for the wider database reach.

See [ADR-006 § Functions scale-controller integration is not worth the extension cost for polling workloads](27-ADR-006-no-native-extension.md#functions-scale-controller-integration-is-not-worth-the-extension-cost-for-polling-workloads) for the full rationale.

## 4. Delivery guarantee

**Official Azure SQL bindings**

- Per the [official documentation](https://learn.microsoft.com/azure/azure-functions/functions-bindings-azure-sql-trigger?tabs=python-v2%2Cisolated-process%2Cnodejs-v4&pivots=programming-language-python), the SQL trigger provides **exactly-once** delivery **for each change on managed sinks**. Duplicates can still happen on the write side unless the sink itself is idempotent — the guarantee is about change-consumption, not end-to-end.
- Lease bookkeeping lives in `az_func.Leases_<table_id>` in the source database.

**`azure-functions-db-python`**

- **At-least-once** delivery by design ([ADR-004](19-ADR-004-at-least-once-default.md)). Duplicates can be observed during:
  - Process crashes between handler success and checkpoint commit.
  - Lease transitions when a partition moves between instances.
  - Checkpoint store commit failures.
- Handlers **must** be idempotent. The recommended patterns:
  - Use `event.pk` plus `event.cursor` as a natural deduplication key at the sink.
  - Prefer upsert (`@db.output(..., action="upsert", conflict_columns=[...])`) over plain insert.
  - Wrap downstream multi-statement writes in [`DbWriter.transaction()`](https://github.com/yeongseon/azure-functions-db-python#atomic-multi-statement-writes--dbwritertransaction) so a partial write is rolled back on crash and safely re-run.

See [Semantics § 1.3 Duplicate and Reprocessing Windows](03-semantics.md#13-duplicate-and-reprocessing-windows) for the formal duplicate-window model and [Polling Runtime & Failure Scenarios](24-polling-runtime-semantics.md) for the operational reference.

## 5. Checkpoint storage and lifecycle

**Official Azure SQL bindings**

- Lease and change-version bookkeeping live **in the source SQL database itself**, under the `az_func` schema (`az_func.Leases_<table_id>`).
- Lifecycle is managed by the extension: leases are created on first trigger run, updated per batch, and cleaned up when the trigger is removed.
- Requires write access to the source database from the Functions identity.

**`azure-functions-db-python`**

- Checkpoints live in **Azure Blob Storage** via [`BlobCheckpointStore`](https://github.com/yeongseon/azure-functions-db-python/blob/main/src/azure_functions_db/state/blob.py). One blob per source (identified by `source_descriptor.fingerprint`).
- No schema changes required in the source database. The source database is treated as read-only from the trigger's perspective (unless the handler explicitly writes back through `@db.output` or `DbWriter`).
- Lifecycle:
  - Blob is created on first successful tick, updated after each batch's handler-success + checkpoint-commit pair.
  - Fingerprint changes (e.g. cursor column or table changes) create a **new** blob — the old checkpoint is not migrated automatically, which is a safety feature: schema changes should be intentional.
  - Deletion of the blob resets the trigger to the beginning of the cursor sequence.
- See [Checkpoint & Lease Spec](06-checkpoint-lease-spec.md) for the full contract.

**Trade-off summary:**

| Aspect                                     | Official (SQL leases table) | This package (Blob store) |
| ------------------------------------------ | --------------------------- | ------------------------- |
| Source DB writes required                  | Yes                         | No                        |
| Extra Azure resource                       | No                          | Yes (storage account)     |
| Cross-region checkpoint replication        | Via SQL replication         | Via storage geo-replication |
| Visibility to DB admins                    | Yes — queryable in SQL      | No — opaque blob          |
| Portable across DB engines                 | SQL-only                    | Any SQLAlchemy dialect    |

## 6. Local testing experience

**Official Azure SQL bindings**

- Requires the Functions host: `func start` with the extension installed via `func extensions install` or via the extension bundle.
- Local SQL Server / Azure SQL Edge container is typically needed to exercise Change Tracking end-to-end.
- Debugging spans two runtimes: the .NET Functions host (which hosts the extension) and the Python worker (which runs the handler).

**`azure-functions-db-python`**

- The wrapper is a plain Python function. You can exercise `@db.input`, `@db.output`, and `@db.trigger` end-to-end with **`pytest` and an in-process SQLite database** — no Functions host required.
- The polling loop can be driven directly by calling `PollTrigger.tick()` in a test. See the existing `tests/test_trigger_runner.py` for the pattern.
- For end-to-end examples against real databases, see [`examples/postgresql-poll-trigger/`](https://github.com/yeongseon/azure-functions-db-python/tree/main/examples/postgresql-poll-trigger) which uses docker-compose for PostgreSQL + Azurite.
- Debugging is a single process — set a breakpoint in the handler and step through the whole call chain including the polling loop.

## 7. SQLAlchemy compatibility and BYOD source adapters

**Official Azure SQL bindings**

- Not applicable. The extension speaks TDS directly through its own driver stack (SqlClient); it does not use SQLAlchemy.

**`azure-functions-db-python`**

- Every read, write, and poll goes through **SQLAlchemy 2.0+** with a **single sync engine path** per dialect. See [`ADR-002`](17-ADR-002-sqlalchemy-centric-adapter.md).
- Any dialect that ships a SQLAlchemy driver works — the built-in extras just bundle common drivers for convenience. Concretely:
  - Install the driver: `pip install oracledb`
  - Use the SQLAlchemy URL: `url="oracle+oracledb://user:pass@host:1521/db"`
  - Pass dialect-specific engine options via `engine_kwargs=...` — everything the underlying dialect supports (pool sizing, timeouts, isolation, custom event listeners) flows through unchanged.
- **BYOD source adapters** for non-SQL sources: implement the [`SourceAdapter`](05-adapter-sdk.md) protocol and pass it to `@db.trigger(source=...)`. The trigger no longer needs SQL at all — this is how MongoDB, Kafka, or REST-API sources are supported. Input / output bindings remain SQLAlchemy-based.
- Async handlers are supported (see [Async handlers](https://github.com/yeongseon/azure-functions-db-python#async-handlers)), but the internal engine remains sync — blocking DB calls are offloaded via `asyncio.to_thread`. Fully native asyncio drivers (`asyncpg`, `aiomysql`) are not used internally; if you need them, drive them yourself outside the binding.

## 8. Production readiness considerations

**Official Azure SQL bindings**

- Mature, first-party Microsoft product with an official SLA path via Azure support.
- Ships in the [extension bundle](https://learn.microsoft.com/azure/azure-functions/functions-bindings-register#extension-bundles) — no manual extension install for Python v2 apps that use the bundle.
- Native scale-controller integration reduces manual capacity planning.
- Portal binding visualization gives Ops teams a familiar surface.

**`azure-functions-db-python`**

- Community project maintained by the DX Toolkit authors; no Microsoft SLA. If you need first-party support, prefer the official extension.
- Before going to production, work through the [Production Checklist — Polling Trigger](26-polling-production-checklist.md) which covers:
  - Cursor column choice, indexing, and timezone handling (`updated_at` naivety, monotonicity guarantees).
  - Lease tuning (lease TTL, renewal interval, partition sizing under `BlobCheckpointStore`).
  - Batch size vs timer cadence for target throughput and end-to-end latency.
  - Handler idempotency patterns (upsert-on-conflict, dedupe helpers, external idempotency keys).
  - Observability wiring — connect the `MetricsCollector` protocol to your metrics backend, wire structured logs to Application Insights via [azure-functions-logging](https://github.com/yeongseon/azure-functions-logging-python).
- Recommended pool settings for Azure Functions (`pool_pre_ping=True`, `pool_recycle`, per-plan `pool_size`) are documented in [EngineProvider & Pooling](25-engine-provider-pooling.md) with per-dialect snippets.
- Delivery is at-least-once ([ADR-004](19-ADR-004-at-least-once-default.md)) — production sinks **must** tolerate duplicates. This is the single largest operational difference from the official extension.
- No native binding metadata means Portal binding visualization and any tooling that scrapes binding metadata will not see `@db.trigger` — only the underlying timer trigger. Plan operational dashboards around timer metrics + your own trigger-level metrics via `MetricsCollector`.

## Cross-references

- [ADR-006 — Python Wrapper over Native Azure Functions Extension](27-ADR-006-no-native-extension.md) — long-form rationale for the packaging decision this comparison rests on.
- [ADR-001 — Pseudo Trigger over Native Trigger](16-ADR-001-pseudo-trigger-over-native.md) — the data-flow half of the decision.
- [ADR-002 — SQLAlchemy-centric Adapter](17-ADR-002-sqlalchemy-centric-adapter.md) — why every dialect goes through SQLAlchemy.
- [ADR-004 — At-least-once as Default Guarantee](19-ADR-004-at-least-once-default.md) — the delivery contract.
- [Polling Runtime & Failure Scenarios](24-polling-runtime-semantics.md) — operational reference for tick lifecycle, duplicate windows, and recovery.
- [Production Checklist — Polling Trigger](26-polling-production-checklist.md) — pre-deployment verification.
- Microsoft Learn — [Azure Functions SQL bindings overview](https://learn.microsoft.com/azure/azure-functions/functions-bindings-azure-sql)
- Microsoft Learn — [Azure Functions SQL trigger](https://learn.microsoft.com/azure/azure-functions/functions-bindings-azure-sql-trigger)
- Microsoft Learn — [SQL Change Tracking overview](https://learn.microsoft.com/sql/relational-databases/track-changes/about-change-tracking-sql-server)
