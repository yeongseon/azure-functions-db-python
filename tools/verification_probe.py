from __future__ import annotations

import json
import os
from pathlib import Path
import sys

from azure.storage.blob import BlobServiceClient
from sqlalchemy import create_engine, text


def main() -> int:
    backend, destination = sys.argv[1:]
    if backend == "azurite":
        with BlobServiceClient.from_connection_string(
            os.environ["TEST_AZURITE_CONN_STR"],
            connection_timeout=5,
            read_timeout=5,
        ) as client:
            client.get_service_properties()
        metadata = {
            "service_version": "Azurite (external endpoint)",
            "driver": "azure-storage-blob",
        }
    else:
        engine = create_engine(os.environ[f"TEST_{backend.upper()}_URL"])
        try:
            with engine.connect() as connection:
                connection.execute(text("SELECT 1"))
                metadata = {
                    "service_version": str(engine.dialect.server_version_info),
                    "driver": engine.dialect.driver,
                }
        finally:
            engine.dispose()
    Path(destination).write_text(json.dumps(metadata))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
