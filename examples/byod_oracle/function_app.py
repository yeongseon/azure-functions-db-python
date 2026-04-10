"""Example: Bring Your Own Database — Oracle DB with azure-functions-db.

Demonstrates that azure-functions-db works with any SQLAlchemy dialect,
not just the built-in extras (PostgreSQL, MySQL, SQL Server).

This example uses Oracle via the ``oracledb`` driver. The same pattern
applies to CockroachDB, DuckDB, or any other database that has a
SQLAlchemy dialect — just swap the driver and connection URL.

Prerequisites:
    pip install azure-functions-db oracledb

Environment variables:
    ORACLE_DB_URL: Oracle connection string
        e.g. oracle+oracledb://user:pass@host:1521/?service_name=XEPDB1
    AzureWebJobsStorage: Azure Storage connection string (for checkpoint)

Note:
    Oracle is used here as a representative BYOD example.
    The exact connection URL format varies by driver — check your
    driver's documentation for the correct syntax.
"""

from __future__ import annotations

import logging

import azure.functions as func
from azure.storage.blob import ContainerClient

from azure_functions_db import (
    BlobCheckpointStore,
    DbBindings,
    DbOut,
    RowChange,
    SqlAlchemySource,
)

app = func.FunctionApp()
db = DbBindings()
logger = logging.getLogger(__name__)

# --- Trigger: poll Oracle table for changes ---

source = SqlAlchemySource(
    url="%ORACLE_DB_URL%",
    table="orders",
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


@app.function_name(name="oracle_orders_poll")
@app.schedule(schedule="0 */1 * * * *", arg_name="timer", use_monitor=True)
@db.trigger(arg_name="events", source=source, checkpoint_store=checkpoint_store)
def oracle_orders_poll(timer: func.TimerRequest, events: list[RowChange]) -> None:
    del timer
    logger.info("Processed %d order events from Oracle", len(events))
    for event in events:
        logger.info("Order %s: %s -> %s", event.pk, event.op, event.after)


# --- Input binding: read from Oracle ---


@app.function_name(name="oracle_get_orders")
@app.route(route="orders", methods=["GET"])
@db.input(
    "orders",
    url="%ORACLE_DB_URL%",
    query="SELECT * FROM orders WHERE status = :status",
    params={"status": "pending"},
)
def oracle_get_orders(
    req: func.HttpRequest, orders: list[dict]
) -> func.HttpResponse:
    del req
    return func.HttpResponse(
        f"Found {len(orders)} pending orders",
        status_code=200,
    )


# --- Output binding: write to Oracle ---


@app.function_name(name="oracle_create_order")
@app.route(route="orders", methods=["POST"])
@db.output("out", url="%ORACLE_DB_URL%", table="orders")
def oracle_create_order(req: func.HttpRequest, out: DbOut) -> func.HttpResponse:
    body = req.get_json()
    out.set(body)
    return func.HttpResponse("Created", status_code=201)
