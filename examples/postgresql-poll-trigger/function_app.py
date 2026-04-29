"""PostgreSQL polling-trigger example.

This Function App polls a PostgreSQL ``orders`` table every minute via
``@db.trigger`` and writes a strictly-idempotent event projection into
``processed_orders`` via ``@db.output``.

Required environment variables (see local.settings.json.example):

- AzureWebJobsStorage  Connection string for the checkpoint blob container.
                       Defaults to Azurite (``UseDevelopmentStorage=true``).
- SOURCE_DB_URL        SQLAlchemy URL for the source database (the table
                       the trigger polls).
- DEST_DB_URL          SQLAlchemy URL for the destination database. May be
                       the same database; the bindings share an
                       ``EngineProvider`` so the connection pool is reused.

Delivery is at-least-once. The handler is intentionally idempotent: it
upserts on the composite key ``(order_id, source_cursor)`` so a replay
during a commit failure, lease transition, or process crash collides on
the exact same key and becomes a no-op write of identical data.

Why a composite key? A single ``order_id`` PK would be a *latest-state
projection* — replays still land in the same row, but a delayed replay of
an older event could overwrite a newer projection. Keying on
``(event.pk, event.cursor)`` instead makes "redelivery = byte-identical
no-op" precisely true and is the canonical dedup pattern for this
package.

See:
    docs/24-polling-runtime-semantics.md  for the operational reference.
    docs/25-engine-provider-pooling.md    for pool tuning.
"""

from __future__ import annotations

import os

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
#
# NOTE: ``%VAR%`` placeholder syntax is an Azure Functions binding-layer
# feature that this package resolves for its own ``url=...`` arguments.
# The Azure Storage SDK does *not* perform that substitution, so we read
# the connection string from the process environment directly here.
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
@db.output(
    "out",
    url="%DEST_DB_URL%",
    table="processed_orders",
    action="upsert",
    conflict_columns=["order_id", "source_cursor"],
    engine_provider=engine_provider,
)
def orders_poll(
    timer: func.TimerRequest,
    events: list[RowChange],
    out: DbOut,
) -> None:
    """Project ``orders`` row changes into ``processed_orders``.

    Every projection row is keyed by ``(order_id, source_cursor)`` —
    i.e. ``(event.pk["id"], event.cursor)``. A replay of the same
    ``RowChange`` produces an upsert against the same composite key with
    identical column values, so the second write is a true no-op.

    For a *latest-state* projection (one row per ``order_id``, last
    write wins) you would instead key on ``order_id`` alone — but be
    aware that out-of-order replays could then overwrite a newer state
    with an older one. See docs/24-polling-runtime-semantics.md §9.
    """
    del timer

    if not events:
        return

    # `event.cursor` is a tuple aligned with the source's
    # `(cursor_column, *pk_columns)` ordering — here `(updated_at, id)`. The
    # first element is the source-side change timestamp (TIMESTAMPTZ); we
    # persist that as `source_cursor`. `processed_at` records when *we*
    # observed the event, so it is wall-clock `now()`, not the source cursor.
    from datetime import datetime, timezone

    processed_at = datetime.now(timezone.utc)

    out.set([
        {
            "order_id": event.pk["id"],
            "source_cursor": event.cursor[0],
            "customer_name": event.after["customer_name"],
            "amount": event.after["amount"],
            "status": event.after["status"],
            "processed_at": processed_at,
        }
        for event in events
        if event.after is not None
    ])
