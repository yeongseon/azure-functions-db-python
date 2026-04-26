"""PostgreSQL polling-trigger example.

This Function App polls a PostgreSQL ``orders`` table every minute via
``@db.trigger`` and writes an idempotent projection into ``processed_orders``
via ``@db.output``.

Required environment variables (see local.settings.json.example):

- AzureWebJobsStorage  Connection string for the checkpoint blob container.
                       Defaults to Azurite (``UseDevelopmentStorage=true``).
- SOURCE_DB_URL        SQLAlchemy URL for the source database (the table
                       the trigger polls).
- DEST_DB_URL          SQLAlchemy URL for the destination database. May be
                       the same database; the bindings share an
                       ``EngineProvider`` so the connection pool is reused.

Delivery is at-least-once. The handler is intentionally idempotent: it
upserts on ``order_id`` so a replay during a commit failure, lease
transition, or process crash produces an identical write.

See:
    docs/24-polling-runtime-semantics.md  for the operational reference.
    docs/25-engine-provider-pooling.md    for pool tuning.
"""

from __future__ import annotations

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

# Module-level EngineProvider so the source and the output binding share
# a single SQLAlchemy engine (and therefore a single connection pool) per
# worker process.  See docs/25-engine-provider-pooling.md.
engine_provider = EngineProvider()

# The source query is `SELECT ... FROM orders ORDER BY updated_at, id`
# filtered by the last committed (cursor, pk) checkpoint.  ``updated_at``
# is maintained by a BEFORE INSERT OR UPDATE trigger in schema.sql so it
# is monotonically non-decreasing on every mutation.
source = SqlAlchemySource(
    url="%SOURCE_DB_URL%",
    table="orders",
    schema="public",
    cursor_column="updated_at",
    pk_columns=["id"],
    engine_provider=engine_provider,
)

# Checkpoint and lease are stored as a single JSON blob in the ``db-state``
# container.  Azurite creates the container on first use; in production,
# pre-create it and grant the Function App identity Storage Blob Data
# Contributor scoped to that container only.
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
def orders_poll(
    timer: func.TimerRequest,
    events: list[RowChange],
    out: DbOut,
) -> None:
    """Project ``orders`` row changes into ``processed_orders``.

    The handler is idempotent: replays of the same ``RowChange`` collide
    on ``order_id`` and become no-op upserts.  Use ``event.pk`` and
    ``event.cursor`` together if you need a stronger dedup key for a
    non-upsert sink (see docs/24-polling-runtime-semantics.md §9).
    """
    del timer

    if not events:
        return

    out.set([
        {
            "order_id": event.pk["id"],
            "customer_name": event.after["customer_name"],
            "amount": event.after["amount"],
            "status": event.after["status"],
            "processed_at": str(event.cursor),
        }
        for event in events
        if event.after is not None
    ])
