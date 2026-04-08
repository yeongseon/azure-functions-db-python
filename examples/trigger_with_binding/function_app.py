"""Trigger + Binding integration example.

Demonstrates using DbFunctionApp decorators to detect DB changes and write
processed results to a destination table.

Shows two patterns:
    1. ``db_output`` — return-value auto-write (declarative)
    2. ``db_writer`` — client injection (imperative, for complex operations)

Requirements:
    pip install azure-functions-db[postgres]
    # or: pip install azure-functions-db[all]

Environment variables:
    SOURCE_DB_URL: Source database connection URL
    DEST_DB_URL: Destination database connection URL
    AzureWebJobsStorage: Azure Storage connection string (for checkpoint)
"""

from __future__ import annotations

import azure.functions as func
from azure.storage.blob import ContainerClient

from azure_functions_db import (
    BlobCheckpointStore,
    DbFunctionApp,
    DbWriter,
    EngineProvider,
    SqlAlchemySource,
)
from azure_functions_db.trigger.events import RowChange

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
    url="%DEST_DB_URL%",
    table="processed_orders",
    action="upsert",
    conflict_columns=["order_id"],
    engine_provider=engine_provider,
)
def orders_poll(timer: func.TimerRequest, events: list[RowChange]) -> list[dict[str, object]]:
    del timer
    return [
        {
            "order_id": event.pk["id"],
            "customer_name": event.after["name"],
            "amount": event.after["amount"],
            "processed_at": str(event.cursor),
        }
        for event in events
        if event.after is not None
    ]


@app.function_name(name="orders_poll_imperative")
@app.schedule(schedule="0 */5 * * * *", arg_name="timer", use_monitor=True)
@db.db_trigger(arg_name="events", source=source, checkpoint_store=checkpoint_store)
@db.db_writer(
    "writer",
    url="%DEST_DB_URL%",
    table="processed_orders",
    engine_provider=engine_provider,
)
def orders_poll_imperative(
    timer: func.TimerRequest, events: list[RowChange], writer: DbWriter
) -> None:
    del timer
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
