"""SQL Server polling-trigger example.

This Function App polls a SQL Server ``dbo.orders`` table every minute via
``@db.trigger`` and writes a strictly-idempotent event projection into
``dbo.processed_orders``.

Unlike the PostgreSQL / MySQL examples, **SQL Server has no supported upsert
path** in this package (upsert is available for PostgreSQL, SQLite, and MySQL
only). So instead of ``@db.output(action="upsert", ...)`` this example uses
``@db.inject_writer`` and performs a plain ``insert`` per event, swallowing the
primary-key violation that a *replay* of an already-processed event produces.
This is the documented "when upsert is not available" fallback — the composite
primary key ``(order_id, source_cursor)`` makes a redelivery collide on the
exact same row, so the swallowed ``IntegrityError`` is a true no-op.

For production AAD / managed-identity authentication (instead of the SQL
username/password used here for local dev), see the companion example
``examples/managed-identity-mssql/``.

Required environment variables (see local.settings.json.example):

- AzureWebJobsStorage  Connection string for the checkpoint blob container.
                       Defaults to Azurite (``UseDevelopmentStorage=true``).
- SOURCE_DB_URL        SQLAlchemy mssql+pyodbc URL for the source database.
- DEST_DB_URL          SQLAlchemy mssql+pyodbc URL for the destination. May be
                       the same database; the bindings share an
                       ``EngineProvider`` so the connection pool is reused.

Delivery is at-least-once; see docs/24-polling-runtime-semantics.md.
"""

from __future__ import annotations

from datetime import datetime, timezone
import os

import azure.functions as func
from azure.storage.blob import ContainerClient
from sqlalchemy.exc import IntegrityError

from azure_functions_db import (
    BlobCheckpointStore,
    DbBindings,
    DbWriter,
    EngineProvider,
    RowChange,
    SqlAlchemySource,
    WriteError,
)

app = func.FunctionApp()
db = DbBindings()

# Module-level EngineProvider so the source and the writer share a single
# SQLAlchemy engine (and connection pool) per worker process.
engine_provider = EngineProvider()

# ``updated_at`` is maintained by an AFTER INSERT, UPDATE trigger in schema.sql
# so it is refreshed on every mutation. The framework appends ``id`` as the
# tiebreaker via ``pk_columns`` to give a total ordering.
source = SqlAlchemySource(
    url="%SOURCE_DB_URL%",
    table="orders",
    schema="dbo",
    cursor_column="updated_at",
    pk_columns=["id"],
    engine_provider=engine_provider,
)

# ``%VAR%`` placeholder syntax is resolved by this package for its own ``url=``
# arguments; the Azure Storage SDK does not perform that substitution, so we
# read the connection string from the environment directly here.
checkpoint_store = BlobCheckpointStore(
    container_client=ContainerClient.from_connection_string(
        conn_str=os.environ["AzureWebJobsStorage"],
        container_name="db-state",
    ),
    source_fingerprint=source.source_descriptor.fingerprint,
)


@app.function_name(name="orders_poll")
@app.schedule(schedule="0 */1 * * * *", arg_name="timer", use_monitor=True)
@db.trigger(arg_name="events", source=source, checkpoint_store=checkpoint_store)
@db.inject_writer(
    "writer",
    url="%DEST_DB_URL%",
    table="processed_orders",
    schema="dbo",
    engine_provider=engine_provider,
)
def orders_poll(
    timer: func.TimerRequest,
    events: list[RowChange],
    writer: DbWriter,
) -> None:
    """Project ``orders`` row changes into ``processed_orders``.

    Each projection row is keyed by ``(order_id, source_cursor)`` —
    i.e. ``(event.pk["id"], event.cursor[0])``. A replay of the same
    ``RowChange`` re-inserts the same composite key, so the resulting
    primary-key ``IntegrityError`` is swallowed and the write is a no-op.
    Any other write failure is re-raised so the batch is retried.
    """
    del timer

    if not events:
        return

    # ``event.cursor`` is aligned with the source's ``(cursor_column, *pk)``
    # ordering — here ``(updated_at, id)``. Element 0 is the source-side change
    # timestamp we persist as ``source_cursor``; ``processed_at`` is our own
    # observation wall-clock time.
    processed_at = datetime.now(timezone.utc)

    for event in events:
        if event.after is None:
            continue
        row = {
            "order_id": event.pk["id"],
            "source_cursor": event.cursor[0],
            "customer_name": event.after["customer_name"],
            "amount": event.after["amount"],
            "status": event.after["status"],
            "processed_at": processed_at,
        }
        try:
            writer.insert(data=row)
        except WriteError as exc:
            # A duplicate (order_id, source_cursor) means this event was already
            # projected on an earlier delivery — an idempotent no-op. Re-raise
            # anything that is not a primary-key / unique violation.
            if isinstance(exc.__cause__, IntegrityError):
                continue
            raise
