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

### 2.2 Decorator Sugar

```python
import azure.functions as func
from azure_functions_db import BlobCheckpointStore, SqlAlchemySource, db

app = func.FunctionApp()

@app.function_name(name="orders_poll")
@app.schedule(schedule="0 */1 * * * *", arg_name="timer", use_monitor=True)
@db.poll(
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
def handle_orders(events, context):
    ...
```

In the actual implementation, the decorator creates a wrapper while keeping the wrapper signature simple to avoid conflicts with Azure Functions decorators.

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

- `db.outbox(...)`
- `db.backfill(...)`
- `db.relay(service_bus=...)`
- `db.model(OrderModel)`
- `db.partitioned(...)`

## 9. API Stability Policy

### Experimental
- CDC strategy
- relay mode
- dynamic partitioning

### Beta
- decorator sugar
- Pydantic mapping

### Stable Target
- PollTrigger
- SqlAlchemySource
- BlobCheckpointStore
- RowChange
- PollContext

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
    def __init__(self, *, url: str, table: str | None = None, schema: str | None = None): ...
    def get(self, *, pk: dict[str, object]) -> dict[str, object] | None: ...
    def query(self, sql: str, *, params: dict[str, object] | None = None) -> list[dict[str, object]]: ...
    def close(self) -> None: ...
```

### 10.5 DbWriter Types

```python
class DbWriter:
    def __init__(self, *, url: str, table: str, schema: str | None = None): ...
    def insert(self, *, data: dict[str, object]) -> None: ...
    def upsert(self, *, data: dict[str, object], conflict_columns: list[str]) -> None: ...
    def update(self, *, data: dict[str, object], pk: dict[str, object]) -> None: ...
    def delete(self, *, pk: dict[str, object]) -> None: ...
    def insert_many(self, *, rows: list[dict[str, object]]) -> None: ...
    def upsert_many(self, *, rows: list[dict[str, object]], conflict_columns: list[str]) -> None: ...
    def close(self) -> None: ...
```

### 10.6 Binding Error Classes

```python
class DbError(Exception): ...
class ConnectionError(DbError): ...
class QueryError(DbError): ...
class WriteError(DbError): ...
class NotFoundError(DbError): ...
```

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
