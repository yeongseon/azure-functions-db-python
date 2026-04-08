# Trigger (Change Detection)

The `trigger` decorator provides poll-based change detection for relational
databases. It detects new and changed rows by tracking a cursor column.

## How It Works

1. A timer trigger fires on a schedule (e.g. every minute).
2. The trigger polls the database for rows where the cursor column is newer than the last checkpoint.
3. Changed rows are delivered as `RowChange` events to your handler.
4. On success, the checkpoint advances. On failure, the same batch is redelivered.

!!! warning "Pseudo trigger"
    This is not a native database trigger. It requires a real Azure Functions
    trigger (typically a timer) to fire the polling cycle.

!!! note "At-least-once delivery"
    Duplicates may occur during crashes or lease transitions. Handlers must
    be idempotent.

## Basic Example

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
        print(f"Order {event.pk}: {event.op}")
```

## RowChange Fields

Each event contains:

| Field | Type | Description |
| --- | --- | --- |
| `event_id` | `str` | Unique event identifier. |
| `op` | `str` | Operation type (`"insert"` or `"update"`). |
| `pk` | `dict` | Primary key values. |
| `before` | `dict \| None` | Previous row state (if available). |
| `after` | `dict \| None` | Current row state. |
| `cursor` | `CursorValue` | Cursor column value for this row. |

## SqlAlchemySource Configuration

| Parameter | Type | Description |
| --- | --- | --- |
| `url` | `str` | Database connection URL. Supports `%ENV_VAR%` substitution. |
| `table` | `str` | Table to poll for changes. |
| `schema` | `str` | Database schema (e.g. `"public"`). |
| `cursor_column` | `str` | Column used for change tracking (must be monotonically increasing). |
| `pk_columns` | `list[str]` | Primary key column names. |
| `engine_provider` | `EngineProvider` | Optional shared engine provider. |

## BlobCheckpointStore Configuration

| Parameter | Type | Description |
| --- | --- | --- |
| `container_client` | `ContainerClient` | Azure Blob Storage container client. |
| `source_fingerprint` | `str` | Unique identifier for this polling source. |

## Important Notes

- The `cursor_column` (e.g. `updated_at`) must be reliably updated by your
  application on every insert and update.
- Hard deletes are not captured by cursor-based polling.
- An empty batch (no changes) is a normal success case.
- Use [Azurite](https://github.com/Azure/Azurite) for local development
  checkpoint storage.
