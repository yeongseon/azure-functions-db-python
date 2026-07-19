# Managed Identity for Azure SQL / SQL Server

Connect `azure-functions-db` to **Azure SQL Database** or **SQL Server** using an
Azure Active Directory (Entra ID) **access token** — no SQL username/password.

The token is acquired with [`azure-identity`](https://learn.microsoft.com/python/api/overview/azure/identity-readme)
and injected into each ODBC connection via the driver's
`SQL_COPT_SS_ACCESS_TOKEN` attribute.

## How it works

`@db.input` / `@db.output` accept an `engine_provider`. This example subclasses
`EngineProvider` (`ManagedIdentityEngineProvider`) and injects a SQLAlchemy
[`creator`](https://docs.sqlalchemy.org/en/20/core/engines.html#custom-dbapi-args)
factory that opens a fresh, token-authenticated `pyodbc` connection each time the
pool needs a new physical connection.

### Token refresh

An AAD token lives ~1 hour. Rather than bake a token into a connection string
(which would go stale on a long-lived worker), the `creator` acquires a fresh
token per **new** connection, and `pool_recycle=1800` recycles connections every
30 minutes — comfortably inside the token lifetime.
`DefaultAzureCredential.get_token(...)` caches the token internally and only
performs a network round-trip when the cached token nears expiry, so calling it
per new connection is cheap.

## Prerequisites

### Python

```bash
pip install azure-functions-db[mssql] azure-identity
```

### OS driver — Microsoft ODBC Driver 18 for SQL Server

- **Linux:** <https://learn.microsoft.com/sql/connect/odbc/linux-mac/installing-the-microsoft-odbc-driver-for-sql-server>
- **Windows:** install the "ODBC Driver 18 for SQL Server" MSI.

The `pyodbc` wheel does **not** bundle the driver — it must be present on the
host / container image.

### Azure — managed identity as a database user

Grant the Function App's system- or user-assigned managed identity access as a
contained database user:

```sql
CREATE USER [my-func-app] FROM EXTERNAL PROVIDER;
ALTER ROLE db_datareader ADD MEMBER [my-func-app];
ALTER ROLE db_datawriter ADD MEMBER [my-func-app];
```

Full setup (AAD admin assignment, contained users, role grants):
<https://learn.microsoft.com/azure/azure-sql/database/authentication-aad-configure>

## Configuration

| Setting            | Required | Description                                                                 |
| ------------------ | -------- | --------------------------------------------------------------------------- |
| `MSSQL_SERVER`     | yes      | e.g. `my-server.database.windows.net`                                       |
| `MSSQL_DATABASE`   | yes      | e.g. `orders`                                                               |
| `AZURE_CLIENT_ID`  | no       | Client id of a **user-assigned** identity. Omit for system-assigned.        |

Locally, sign in with `az login` — `DefaultAzureCredential` picks up your
developer credentials, so the same code runs without a managed identity.

## Endpoints

| Method | Route     | Function          | Behavior                                  |
| ------ | --------- | ----------------- | ----------------------------------------- |
| `GET`  | `/orders` | `mi_get_orders`   | Reads pending orders (input binding).     |
| `POST` | `/orders` | `mi_create_order` | Inserts an order from the body (output).  |

## References

- pyodbc + AAD access token: <https://learn.microsoft.com/sql/connect/odbc/using-azure-active-directory#authenticating-with-an-access-token>
- `azure-identity`: <https://learn.microsoft.com/python/api/overview/azure/identity-readme>
