"""Example: Poll orders table for changes using PollTrigger (imperative API).

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

from azure_functions_db import BlobCheckpointStore, PollTrigger, RowChange, SqlAlchemySource

app = func.FunctionApp()
logger = logging.getLogger(__name__)

# ── Source: PostgreSQL orders table ──────────────────────────────────
source = SqlAlchemySource(
    url="%ORDERS_DB_URL%",
    table="orders",
    schema="public",
    cursor_column="updated_at",
    pk_columns=["id"],
)

# ── PollTrigger: config holder ──────────────────────────────────────
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
)


# ── Handler ─────────────────────────────────────────────────────────
def handle_orders(events: list[RowChange]) -> None:
    for event in events:
        logger.info("Order %s: %s -> %s", event.pk, event.op, event.after)


# ── Azure Function (timer trigger) ─────────────────────────────────
@app.function_name(name="orders_poll")
@app.schedule(schedule="0 */1 * * * *", arg_name="timer", use_monitor=True)
def orders_poll(timer: func.TimerRequest) -> None:
    count = orders_trigger.run(timer=timer, handler=handle_orders)
    logger.info("Processed %d order events", count)
