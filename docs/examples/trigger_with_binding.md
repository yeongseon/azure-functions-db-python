# Trigger + Output Binding

Combine the trigger and output binding to process database changes and write
results to another table. This pattern is common for ETL pipelines, event
processing, and data synchronization.

## Declarative Pattern (output + DbOut)

Use `@db.output()` with `DbOut.set()` for straightforward writes:

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

## Imperative Pattern (inject_writer)

Use `@db.inject_writer()` for complex write logic (transactions, conditional
updates, multiple operations):

```python
from azure_functions_db import DbBindings, DbWriter, RowChange

db = DbBindings()


@db.trigger(arg_name="events", source=source, checkpoint_store=checkpoint_store)
@db.inject_writer("writer", url="%DEST_DB_URL%", table="processed_orders",
                  engine_provider=engine_provider)
def orders_poll_imperative(events: list[RowChange], writer: DbWriter) -> None:
    for event in events:
        if event.after is not None:
            writer.upsert(
                data={
                    "order_id": event.pk["id"],
                    "customer_name": event.after["name"],
                    "amount": event.after["amount"],
                    "processed_at": str(event.cursor),
                },
                conflict_columns=["order_id"],
            )
```

!!! note "Choosing between output and inject_writer"
    Use `output` for simple, bulk writes. Use `inject_writer` when you need
    per-row logic, conditional writes, or multiple operations per event.

## Shared Connection Pooling

Both examples above use `EngineProvider` to share a SQLAlchemy engine between
the trigger source and the output destination. This avoids creating redundant
connection pools when both source and destination use the same database server.

```python
engine_provider = EngineProvider()

# Pass to both source and output/writer
source = SqlAlchemySource(..., engine_provider=engine_provider)

@db.output("out", ..., engine_provider=engine_provider)
# or
@db.inject_writer("writer", ..., engine_provider=engine_provider)
```

If source and destination are on different servers, you can omit `engine_provider`
or use separate instances.

## Mutual Exclusivity

`output` and `inject_writer` cannot be used on the same handler. They are
mutually exclusive decorators. Choose one pattern per function.
