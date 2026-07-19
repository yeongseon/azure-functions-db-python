"""Example: Azure SQL / SQL Server with **Managed Identity** (AAD auth).

Demonstrates connecting ``azure-functions-db`` to Azure SQL Database or SQL
Server using an Azure Active Directory (Entra ID) access token instead of a
SQL username/password. The token is acquired with ``azure-identity`` and
injected into the ODBC connection via the driver-specific
``SQL_COPT_SS_ACCESS_TOKEN`` attribute.

How the token is wired in
    The binding decorators (``@db.input`` / ``@db.output`` / ``@db.inject_*``)
    accept an ``engine_provider``. We subclass :class:`EngineProvider` and
    inject a SQLAlchemy ``creator`` factory into the engine configuration. The
    ``creator`` is invoked every time the pool opens a **new** physical
    connection, so each new connection carries a freshly acquired token.

Why a ``creator`` (not a static token in the URL/connect_args)?
    An AAD access token is short-lived (typically ~1 hour). A token baked into a
    connection string would go stale while a long-running Functions worker keeps
    the pooled connection alive. With a ``creator`` plus ``pool_recycle``,
    connections — and their tokens — are refreshed well within the token
    lifetime. ``DefaultAzureCredential.get_token(...)`` caches internally and
    only hits the network when the cached token is near expiry, so calling it
    per new connection is cheap.

Prerequisites:
    - Python: ``pip install azure-functions-db[mssql] azure-identity``
    - OS driver: **Microsoft ODBC Driver 18 for SQL Server** on the host/image.
        - Linux:  https://learn.microsoft.com/sql/connect/odbc/linux-mac/installing-the-microsoft-odbc-driver-for-sql-server
        - Windows: install the "ODBC Driver 18 for SQL Server" MSI.
    - Azure: the Function App's system- or user-assigned managed identity must
      be a contained database user with the needed roles, e.g.

        CREATE USER [my-func-app] FROM EXTERNAL PROVIDER;
        ALTER ROLE db_datareader ADD MEMBER [my-func-app];
        ALTER ROLE db_datawriter ADD MEMBER [my-func-app];

      See https://learn.microsoft.com/azure/azure-sql/database/authentication-aad-configure
      for the full AAD / managed identity setup (admin assignment, contained
      users, and role grants).

Environment variables:
    MSSQL_SERVER:   e.g. ``my-server.database.windows.net``
    MSSQL_DATABASE: e.g. ``orders``
    AZURE_CLIENT_ID (optional): client id of a **user-assigned** managed
        identity. Omit to use the system-assigned identity / DefaultAzureCredential
        chain (also works locally via ``az login`` for development).

Reference:
    - pyodbc + AAD access token:
      https://learn.microsoft.com/sql/connect/odbc/using-azure-active-directory#authenticating-with-an-access-token
    - azure-identity overview:
      https://learn.microsoft.com/python/api/overview/azure/identity-readme
"""

from __future__ import annotations

import dataclasses
import os
import struct
from typing import Any

import azure.functions as func
from azure.identity import DefaultAzureCredential
import pyodbc
from sqlalchemy.engine import Engine

from azure_functions_db import DbBindings, DbOut, EngineProvider

# ODBC connection attribute id for a pre-acquired AAD access token, defined by
# the Microsoft ODBC Driver for SQL Server.
SQL_COPT_SS_ACCESS_TOKEN = 1256

# Token audience for Azure SQL Database / SQL Server.
_TOKEN_SCOPE = "https://database.windows.net/.default"

# One credential for the whole worker; it caches and refreshes tokens itself.
_credential = DefaultAzureCredential(
    managed_identity_client_id=os.environ.get("AZURE_CLIENT_ID"),
)


def _access_token_struct() -> bytes:
    """Acquire a fresh AAD token, packed in the ODBC-expected layout.

    The driver wants the UTF-16-LE encoded token prefixed with its 4-byte
    little-endian length.
    """
    token = _credential.get_token(_TOKEN_SCOPE).token
    token_bytes = token.encode("utf-16-le")
    return struct.pack(f"<I{len(token_bytes)}s", len(token_bytes), token_bytes)


def _pyodbc_connect() -> pyodbc.Connection:
    """SQLAlchemy ``creator``: one fresh, token-authenticated connection."""
    server = os.environ["MSSQL_SERVER"]
    database = os.environ["MSSQL_DATABASE"]
    conn_str = (
        "Driver={ODBC Driver 18 for SQL Server};"
        f"Server=tcp:{server},1433;"
        f"Database={database};"
        "Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;"
    )
    return pyodbc.connect(
        conn_str,
        attrs_before={SQL_COPT_SS_ACCESS_TOKEN: _access_token_struct()},
    )


class ManagedIdentityEngineProvider(EngineProvider):
    """EngineProvider that authenticates every new connection with an AAD token.

    Injects the ``creator`` factory (plus conservative pool settings so tokens
    are refreshed well before expiry) into the engine configuration built by the
    binding decorators.
    """

    def create_isolated_engine(self, config: Any) -> Engine:  # noqa: ANN401
        enriched = dataclasses.replace(
            config,
            engine_kwargs={
                **config.engine_kwargs,
                # Our token-authenticated connection factory.
                "creator": _pyodbc_connect,
                # Recycle connections after 30 min so a new token is acquired
                # well before the ~60 min token lifetime expires.
                "pool_recycle": 1800,
                # Transparently replace connections dropped by the server.
                "pool_pre_ping": True,
            },
        )
        return super().create_isolated_engine(enriched)


# A dialect-only URL: because a ``creator`` supplies the real connection,
# SQLAlchemy uses the URL solely to select the mssql+pyodbc dialect.
_MSSQL_URL = "mssql+pyodbc://"

app = func.FunctionApp()
db = DbBindings()
engine_provider = ManagedIdentityEngineProvider()


# --- Input binding: read from Azure SQL via managed identity ---


@app.function_name(name="mi_get_orders")
@app.route(route="orders", methods=["GET"])
@db.input(
    "orders",
    url=_MSSQL_URL,
    query="SELECT id, status FROM orders WHERE status = :status",
    params={"status": "pending"},
    engine_provider=engine_provider,
)
def mi_get_orders(req: func.HttpRequest, orders: list[dict]) -> func.HttpResponse:
    del req
    return func.HttpResponse(f"Found {len(orders)} pending orders", status_code=200)


# --- Output binding: write to Azure SQL via managed identity ---


@app.function_name(name="mi_create_order")
@app.route(route="orders", methods=["POST"])
@db.output(
    "out",
    url=_MSSQL_URL,
    table="orders",
    engine_provider=engine_provider,
)
def mi_create_order(req: func.HttpRequest, out: DbOut) -> func.HttpResponse:
    out.set(req.get_json())
    return func.HttpResponse("Created", status_code=201)
