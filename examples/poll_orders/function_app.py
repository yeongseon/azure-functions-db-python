"""Example: Poll orders table for changes using DbBindings decorators.

Prerequisites:
    pip install azure-functions-db[postgres]

Environment variables:
    ORDERS_DB_URL: PostgreSQL connection string
    AzureWebJobsStorage: Azure Storage connection string
"""

from __future__ import annotations

import logging

import azure.functions as func
from azure.storage.blob import ContainerClient

from azure_functions_db import BlobCheckpointStore, DbBindings, RowChange, SqlAlchemySource

app = func.FunctionApp()
db = DbBindings()
logger = logging.getLogger(__name__)

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
    del timer
    logger.info("Processed %d order events", len(events))
    for event in events:
        logger.info("Order %s: %s -> %s", event.pk, event.op, event.after)
