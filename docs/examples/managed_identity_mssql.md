# Managed Identity for Azure SQL / SQL Server

The repository ships a runnable example, [`examples/managed-identity-mssql`](https://github.com/yeongseon/azure-functions-db-python/tree/main/examples/managed-identity-mssql),
that connects to Azure SQL Database or SQL Server with a Microsoft Entra ID access token instead of a
SQL username and password. This page explains how to read it, what it needs, and how to try it
without deploying anything. The code and its own README stay the source of truth; this page does not
add a second implementation.

## What the example shows

- A custom `EngineProvider` (`ManagedIdentityEngineProvider`) passed to `@db.input` and `@db.output`
  through `engine_provider=...`.
- A SQLAlchemy `creator` that opens a new `pyodbc` connection and attaches a fresh access token
  (`SQL_COPT_SS_ACCESS_TOKEN`) each time the pool needs a new physical connection.
- `pool_recycle=1800` and `pool_pre_ping=True`, so connections, and the tokens on them, are replaced
  well inside a token's lifetime of about one hour.
- Two HTTP functions: `GET /orders` reads pending orders (input binding) and `POST /orders` inserts
  an order from the request body (output binding).

See [EngineProvider & Pooling](../25-engine-provider-pooling.md) for how an engine provider is used.

## What it needs

| Need | Detail |
| --- | --- |
| Packages | `pip install azure-functions-db[mssql] azure-identity` |
| OS driver | Microsoft ODBC Driver 18 for SQL Server on the host or image. The `pyodbc` wheel does not bundle it. |
| Database user | The Function App's identity must be a contained user of the database, for example `CREATE USER [<function-app-name>] FROM EXTERNAL PROVIDER;`, with `db_datareader` for reading and `db_datawriter` for writing. |
| Settings | `MSSQL_SERVER` (for example `<server>.database.windows.net`) and `MSSQL_DATABASE`. Optionally `AZURE_CLIENT_ID` for a user-assigned identity; leave it out for a system-assigned one. |

`<function-app-name>`, `<server>` and every value in the example's `local.settings.sample.json` are
placeholders. Replace them with your own and keep real values out of version control.

The example grants only read and write on the tables it uses. Grant your identity no more than your
function needs.

## Inspect and configure it without deploying

1. Read `examples/managed-identity-mssql/function_app.py`. It is one file with the token code, the
   engine provider and the two functions.
2. Copy `local.settings.sample.json` to `local.settings.json` and set `MSSQL_SERVER` and
   `MSSQL_DATABASE`. The file holds no password and the example never asks for one.
3. To see the token code run against a database you can reach, sign in with `az login` and start the
   app with Azure Functions Core Tools. `DefaultAzureCredential` then uses your developer sign-in.

!!! warning
    A local run with `az login` uses your own identity and your own database permissions. It shows that
    the connection code works. It does not show that the Function App's managed identity is set up
    correctly in Azure; only a run with that identity can show that.

## Related

- [Client Injection](client_injection.md) for imperative reads and writes.
- [Security](../12-security.md) and [Deployment Guide](../13-deployment-guide.md).
- Microsoft: [Configure Microsoft Entra authentication for Azure SQL](https://learn.microsoft.com/azure/azure-sql/database/authentication-aad-configure)
  and [pyodbc with an access token](https://learn.microsoft.com/sql/connect/odbc/using-azure-active-directory#authenticating-with-an-access-token).
