from azure.storage.blob import ContainerClient

from azure_functions_db import BlobCheckpointStore, DbBindings, RowChange, SqlAlchemySource

db = DbBindings()

source = SqlAlchemySource(
    url="postgresql+psycopg://postgres:postgres@localhost:5432/orders",
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


@db.trigger(arg_name="events", source=source, checkpoint_store=checkpoint_store)
def handler(events: list[RowChange]) -> None:
    for event in events:
        print(event.event_id, event.pk, event.after)
