# Managed Identity with Azure SQL / SQL Server

The repository ships a ready example,
[`examples/managed-identity-mssql/`](https://github.com/yeongseon/azure-functions-db-python/tree/main/examples/managed-identity-mssql),
that connects `@db.input` / `@db.output` to Azure SQL Database or SQL Server
with a Microsoft Entra ID access token instead of a SQL username and password.
This page explains how to read, configure and review it. It does not add a
second implementation, and it does not change any library authentication
default.

## What the example does

| Piece | Where | Purpose |
|---|---|---|
| `ManagedIdentityEngineProvider` | `function_app.py` | `EngineProvider` subclass that adds a SQLAlchemy `creator`, `pool_recycle=1800` and `pool_pre_ping=True` to the engine configuration. |
| `_pyodbc_connect()` | `function_app.py` | The `creator`: opens a new `pyodbc` connection and passes a fresh token through `attrs_before={SQL_COPT_SS_ACCESS_TOKEN: ...}`. |
| `_access_token_struct()` | `function_app.py` | Calls `DefaultAzureCredential.get_token(...)` and packs the token as a 4-byte little-endian length followed by the UTF-16-LE bytes, as the ODBC driver expects. |
| `_MSSQL_URL = "mssql+pyodbc://"` | `function_app.py` | A dialect-only URL: the `creator` supplies the real connection, so no host, user or password appears in a URL. |
| `mi_get_orders`, `mi_create_order` | `function_app.py` | `GET /orders` (input binding) and `POST /orders` (output binding). |

Because the `creator` runs for every **new** physical connection and
connections are recycled after 30 minutes, a long-lived worker does not keep
using an expired token.

## Prerequisites

- Python packages: `pip install azure-functions-db[mssql] azure-identity`
  (the example's `requirements.txt` lists them).
- OS driver: Microsoft ODBC Driver 18 for SQL Server on the host or image. The
  `pyodbc` wheel does not bundle it.
- Database permission for the **runtime identity** (the Function App's system-
  or user-assigned managed identity). Run this as a Microsoft Entra
  administrator of the database, replacing the placeholder name with your
  Function App's identity name:

    ```sql
    CREATE USER [<your-function-app-name>] FROM EXTERNAL PROVIDER;
    ALTER ROLE db_datareader ADD MEMBER [<your-function-app-name>];
    ALTER ROLE db_datawriter ADD MEMBER [<your-function-app-name>];
    ```

    Grant only what the handlers need: `db_datareader` for `GET /orders`,
    `db_datawriter` (INSERT on `orders`) for `POST /orders`. Full setup:
    [Configure Microsoft Entra authentication](https://learn.microsoft.com/azure/azure-sql/database/authentication-aad-configure).

## Configuration (no passwords)

Only host names and an optional client id are configured. Every value below is
a placeholder; nothing here is a secret and no SQL password is stored anywhere.

| Setting | Required | Placeholder value |
|---|---|---|
| `MSSQL_SERVER` | yes | `<your-server>.database.windows.net` |
| `MSSQL_DATABASE` | yes | `<your-database>` |
| `AZURE_CLIENT_ID` | no | client id of a **user-assigned** identity; omit for system-assigned |

Inspect the example without deploying anything:

```bash
cat examples/managed-identity-mssql/local.settings.sample.json
python -m pytest tests/test_managed_identity_mssql_example.py --no-cov -q
```

The sample settings file keeps `AzureWebJobsStorage` pointed at Azurite
(`UseDevelopmentStorage=true`). Copy it to `local.settings.json` and edit the
two placeholders only if you intend to run the app yourself.

## What a local check does and does not prove

`tests/test_managed_identity_mssql_example.py` is service-free: it replaces
`azure.identity` and `pyodbc` with in-process fakes and checks the token
packing, the `creator` arguments, the engine options and that the documented
settings and SQL grants match the example source. It needs no Azure
subscription, network or database.

Running the example locally after `az login` uses **your developer
credential** through `DefaultAzureCredential`. That shows the code path works,
but it does **not** certify Azure managed identity: the managed identity, its
database user and role grants, network access and ODBC driver in the deployed
Function App must be verified in Azure separately.

## See also

- [`examples/managed-identity-mssql/README.md`](https://github.com/yeongseon/azure-functions-db-python/blob/main/examples/managed-identity-mssql/README.md)
- [EngineProvider & Pooling](../25-engine-provider-pooling.md)
- [pyodbc with a Microsoft Entra access token](https://learn.microsoft.com/sql/connect/odbc/using-azure-active-directory#authenticating-with-an-access-token)
