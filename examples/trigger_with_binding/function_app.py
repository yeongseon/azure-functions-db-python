"""Trigger + Binding integration example.

Demonstrates using PollTrigger to detect DB changes and DbWriter to write
processed results to a destination table.  Uses EngineProvider for shared
connection pooling across trigger and bindings.

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
    DbWriter,
    EngineProvider,
    PollTrigger,
    SqlAlchemySource,
)
from azure_functions_db.trigger.events import RowChange

app = func.FunctionApp()

engine_provider = EngineProvider()

source = SqlAlchemySource(
    url="%SOURCE_DB_URL%",
    table="orders",
    schema="public",
    cursor_column="updated_at",
    pk_columns=["id"],
    engine_provider=engine_provider,
)

orders_trigger = PollTrigger(
    name="orders",
    source=source,
    checkpoint_store=BlobCheckpointStore(
        container_client=ContainerClient.from_connection_string(
            conn_str="%AzureWebJobsStorage%",
            container_name="db-state",
        ),
        source_fingerprint=source.source_descriptor.fingerprint,
    ),
    batch_size=100,
    max_batches_per_tick=1,
)


def process_orders(events: list[RowChange]) -> None:
    with DbWriter(
        url="%DEST_DB_URL%",
        table="processed_orders",
        engine_provider=engine_provider,
    ) as writer:
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


@app.function_name(name="orders_poll")
@app.schedule(schedule="0 */1 * * * *", arg_name="timer", use_monitor=True)
def orders_poll(timer: func.TimerRequest) -> None:
    orders_trigger.run(timer=timer, handler=process_orders)
