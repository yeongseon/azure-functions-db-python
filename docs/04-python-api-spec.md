# Python API Spec

## 1. Design Goals

- Should integrate naturally with Azure Functions Python v2.
- Users should focus on handlers rather than DB polling orchestration.
- Simple cases should be completable in under 10 lines.
- Advanced users should be able to exercise fine-grained control via the imperative API.

## 2. Surface API

### 2.1 Recommended Approach: Helper + Azure Schedule Decorator

```python
import azure.functions as func
from azure_functions_db import PollTrigger, SqlAlchemySource, BlobCheckpointStore

app = func.FunctionApp()

orders_trigger = PollTrigger(
    name="orders",
    source=SqlAlchemySource(
        url="%ORDERS_DB_URL%",
        table="orders",
        schema="public",
        cursor_column="updated_at",
        pk_columns=["id"],
    ),
    checkpoint_store=BlobCheckpointStore(
        connection="AzureWebJobsStorage",
        container="db-state",
    ),
    batch_size=100,
)

@app.function_name(name="orders_poll")
@app.schedule(schedule="0 */1 * * * *", arg_name="timer", use_monitor=True)
def orders_poll(timer: func.TimerRequest) -> None:
    orders_trigger.run(timer=timer, handler=handle_orders)

def handle_orders(events, context):
    for event in events:
        print(event.pk, event.after)
```

### 2.2 Decorator API (DbFunctionApp)

```python
import azure.functions as func
from azure.storage.blob import ContainerClient
from azure_functions_db import BlobCheckpointStore, DbFunctionApp, RowChange, SqlAlchemySource

app = func.FunctionApp()
db = DbFunctionApp()

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
@db.db_trigger(arg_name="events", source=source, checkpoint_store=checkpoint_store)
def orders_poll(timer: func.TimerRequest, events: list[RowChange]) -> None:
    for event in events:
        print(event.pk, event.after)
```

The `DbFunctionApp` class provides Azure Functions-style decorators (`db_trigger`, `db_input`, `db_output`). Internally, `db_trigger` wraps `PollTrigger` and manages `__signature__` to hide injected parameters from the Azure runtime. Decorator order contract: Azure decorators outermost, db decorators closest to the function.

## 3. Core Types

### 3.1 PollTrigger
```python
class PollTrigger:
    def __init__(
        self,
        *,
        name: str,
        source: SourceAdapter,
        checkpoint_store: StateStore,
        batch_size: int = 100,
        max_batches_per_tick: int = 1,
        lease_ttl_seconds: int = 120,
        retry_policy: RetryPolicy | None = None,
    ): ...

    def run(self, *, timer: object, handler: Callable[..., Any]) -> int: ...
```

### 3.2 SqlAlchemySource
```python
class SqlAlchemySource(SourceAdapter):
    def __init__(
        self,
        *,
        url: str,
        table: str | None = None,
        schema: str | None = None,
        query: str | None = None,
        cursor_column: str,
        pk_columns: list[str],
        where: str | None = None,
        parameters: dict[str, object] | None = None,
        strategy: str = "cursor",
        operation_mode: str = "upsert_only",
    ): ...
```

### 3.3 PollContext
```python
@dataclass
class PollContext:
    poller_name: str
    invocation_id: str
    batch_id: str
    lease_owner: str
    checkpoint_before: dict
    checkpoint_after_candidate: dict | None
    tick_started_at: datetime
    source_name: str
```

### 3.4 RowChange
```python
@dataclass
class RowChange:
    event_id: str
    op: str                       # insert | update | upsert | delete | unknown
    source: SourceDescriptor
    cursor: CursorValue
    pk: dict[str, object]
    before: dict[str, object] | None
    after: dict[str, object] | None
    metadata: dict[str, object]
```

## 4. Handler Rules

Supported signatures:

```python
def handler(events): ...
def handler(events, context): ...
```

> **Note**: Async handlers are not supported and will raise `TypeError`.
> Async support may be added in a future version.

Rules:
- `events` may be empty. Default behavior is **empty batch skip**.
- If the handler raises an exception, the batch is treated as failed.
- Handlers should be idempotent whenever possible.

## 5. Source Definition Modes

### 5.1 Table Mode
- table
- schema
- cursor_column
- pk_columns

### 5.2 Query Mode
- query
- cursor_column alias
- pk column alias
- parameterized query

### 5.3 Outbox Mode
- table=`outbox_events`
- payload column
- status column (optional)

## 6. Recommended Default Settings

- `batch_size=100`
- `max_batches_per_tick=1`
- `lease_ttl_seconds=120`
- `use_monitor=True`
- `schedule=0 */1 * * * *` (every 1 minute)

## 7. Error Classification

```python
class PollerError(Exception): ...
class LeaseAcquireError(PollerError): ...
class SourceConfigurationError(PollerError): ...
class FetchError(PollerError): ...
class HandlerError(PollerError): ...
class CommitError(PollerError): ...
class LostLeaseError(PollerError): ...
class SerializationError(PollerError): ...
```

## 8. Future API

- Outbox strategy support
- Backfill mode
- Service Bus / Event Hub relay integration
- Pydantic model mapping for `db_trigger` events
- Partitioned polling

## 9. API Stability Policy

### Experimental
- CDC strategy
- relay mode
- dynamic partitioning

### Beta
- Pydantic mapping

### Stable Target
- PollTrigger
- SqlAlchemySource
- BlobCheckpointStore
- RowChange
- PollContext
- DbFunctionApp (db_trigger, db_input, db_output)
- DbReader
- DbWriter

## 10. Binding API

### 10.1 DbReader (imperative input binding)

```python
from azure_functions_db import DbReader

reader = DbReader(url="%DB_URL%", table="users")
user = reader.get(pk={"id": user_id})  # single row
users = reader.query("SELECT * FROM users WHERE active = :active", params={"active": True})  # query
```

### 10.2 DbWriter (imperative output binding)

```python
from azure_functions_db import DbWriter

writer = DbWriter(url="%DB_URL%", table="processed_orders")
writer.insert(data={"id": 1, "status": "done"})
writer.upsert(data={"id": 1, "status": "done"}, conflict_columns=["id"])
writer.insert_many(rows=[...])
writer.upsert_many(rows=[...], conflict_columns=["id"])
```

### 10.3 Combined Trigger + Binding Example

Using the decorator API with `DbFunctionApp`:

```python
import azure.functions as func
from azure.storage.blob import ContainerClient

from azure_functions_db import (
    BlobCheckpointStore,
    DbFunctionApp,
    DbWriter,
    EngineProvider,
    RowChange,
    SqlAlchemySource,
)

app = func.FunctionApp()
db = DbFunctionApp()

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
        conn_str="%AzureWebJobsStorage%",
        container_name="db-state",
    ),
    source_fingerprint=source.source_descriptor.fingerprint,
)

@app.function_name(name="orders_poll")
@app.schedule(schedule="0 */1 * * * *", arg_name="timer", use_monitor=True)
@db.db_trigger(arg_name="events", source=source, checkpoint_store=checkpoint_store)
@db.db_output(
    arg_name="writer",
    url="%DEST_DB_URL%",
    table="processed_orders",
    engine_provider=engine_provider,
)
def orders_poll(timer: func.TimerRequest, events: list[RowChange], writer: DbWriter) -> None:
    for event in events:
        if event.after is not None:
            writer.upsert(
                data={
                    "order_id": event.pk["id"],
                    "customer": event.after["name"],
                    "processed_at": str(event.cursor),
                },
                conflict_columns=["order_id"],
            )
```

Using the imperative API directly:

```python
import azure.functions as func
from azure_functions_db import PollTrigger, SqlAlchemySource, BlobCheckpointStore, DbWriter

app = func.FunctionApp()

orders_trigger = PollTrigger(
    name="orders",
    source=SqlAlchemySource(
        url="%ORDERS_DB_URL%",
        table="orders",
        schema="public",
        cursor_column="updated_at",
        pk_columns=["id"],
    ),
    checkpoint_store=BlobCheckpointStore(
        connection="AzureWebJobsStorage",
        container="db-state",
    ),
)

@app.function_name(name="orders_sync")
@app.schedule(schedule="0 */1 * * * *", arg_name="timer", use_monitor=True)
def orders_sync(timer: func.TimerRequest) -> None:
    orders_trigger.run(timer=timer, handler=handle_orders)

def handle_orders(events, context):
    writer = DbWriter(url="%ANALYTICS_DB_URL%", table="processed_orders")
    try:
        rows = [
            {"id": event.pk["id"], "status": "processed"}
            for event in events
        ]
        if rows:
            writer.upsert_many(rows=rows, conflict_columns=["id"])
    finally:
        writer.close()
```

### 10.4 DbReader Types

```python
class DbReader:
    def __init__(self, *, url: str, table: str | None = None, schema: str | None = None, engine_provider: EngineProvider | None = None): ...
    def get(self, *, pk: dict[str, object]) -> dict[str, object] | None: ...
    def query(self, sql: str, *, params: dict[str, object] | None = None) -> list[dict[str, object]]: ...
    def close(self) -> None: ...
    def __enter__(self) -> DbReader: ...
    def __exit__(self, exc_type, exc_val, exc_tb) -> None: ...
```

### 10.5 DbWriter Types

```python
class DbWriter:
    def __init__(self, *, url: str, table: str, schema: str | None = None, engine_provider: EngineProvider | None = None): ...
    def insert(self, *, data: dict[str, object]) -> None: ...
    def insert_many(self, *, rows: list[dict[str, object]]) -> None: ...
    def upsert(self, *, data: dict[str, object], conflict_columns: list[str]) -> None: ...
    def upsert_many(self, *, rows: list[dict[str, object]], conflict_columns: list[str]) -> None: ...
    def update(self, *, data: dict[str, object], pk: dict[str, object]) -> None: ...
    def delete(self, *, pk: dict[str, object]) -> None: ...
    def close(self) -> None: ...
    def __enter__(self) -> DbWriter: ...
    def __exit__(self, exc_type, exc_val, exc_tb) -> None: ...
```

Supported upsert dialects: PostgreSQL, SQLite, MySQL. Other dialects
raise ``ConfigurationError`` on upsert calls.

Thread Safety: Instances are **not** safe to share across concurrent threads
or async invocations. Create a separate ``DbWriter`` per function invocation.

### 10.6 Binding Error Classes

```python
class DbError(Exception): ...
class ConfigurationError(DbError): ...
class DbConnectionError(DbError): ...
class QueryError(DbError): ...
class WriteError(DbError): ...
```

### 10.7 Decorator API for Bindings (DbFunctionApp)

The `DbFunctionApp` class provides `db_input` and `db_output` decorators that inject
`DbReader` / `DbWriter` instances per invocation with automatic lifecycle management.

#### db_input (inject DbReader)

```python
from azure_functions_db import DbFunctionApp, DbReader

db = DbFunctionApp()

@db.db_input("reader", url="%DB_URL%", table="users")
def load_user(reader: DbReader) -> dict[str, object] | None:
    return reader.get(pk={"id": 42})

@db.db_input("reader", url="%DB_URL%")
def list_active(reader: DbReader) -> list[dict[str, object]]:
    return reader.query("SELECT * FROM users WHERE active = :active", params={"active": True})
```

#### db_output (inject DbWriter)

```python
from azure_functions_db import DbFunctionApp, DbWriter

db = DbFunctionApp()

@db.db_output("writer", url="%DB_URL%", table="orders")
def write_order(writer: DbWriter) -> None:
    writer.insert(data={"id": 1, "status": "pending", "total": 99.99})
    writer.upsert(data={"id": 1, "status": "shipped"}, conflict_columns=["id"])
```

Both decorators support sync and async handlers.  The injected instance is created
fresh per invocation and closed automatically in a `finally` block.

## 11. Shared Core API

### 11.1 DbConfig

```python
@dataclass(frozen=True)
class DbConfig:
    url: str
    table: str | None = None
    schema: str | None = None
```

### 11.2 EngineProvider (shared engine/pool management, lazy singleton per config)

```python
class EngineProvider:
    @classmethod
    def get_or_create(cls, config: DbConfig): ...

    @classmethod
    def dispose(cls, config: DbConfig) -> None: ...
```
