# Security

Security guidelines for using `azure-functions-db-python` in production.

## Principles

- Do not embed secrets in code.
- Use managed identity, environment variables, or a secret store.
- Minimize checkpoint storage permissions.
- Prefer read-only database accounts for polling.

## Connection Configuration

Use environment variable substitution for connection URLs:

```python
@db.input("user", url="%DB_URL%", table="users", pk={"id": 1})
```

The `%DB_URL%` placeholder is resolved from the environment at runtime.
Never hardcode connection strings in source code.

In production, consider:

- **Azure Managed Identity** for passwordless database access.
- **Azure Key Vault** references in Application Settings.
- Keeping development and production credentials strictly separate.

## Database Permissions

Apply the principle of least privilege for each use case:

### Polling (trigger)

- `SELECT` on the target table.
- Schema metadata read access (if required by the driver).

### Read binding (input / inject_reader)

- `SELECT` on the target table.

### Write binding (output / inject_writer)

- `INSERT`, `UPDATE`, `DELETE` as needed for the operation.
- For upsert, the account needs both `INSERT` and `UPDATE`.

### Checkpoint storage

- Read and write access on the checkpoint blob container.
- Quarantine container write access (if configured).

## Raw SQL

`DbReader.query()` and `scalar()` run the SQL string you give them. The library does not parse,
restrict or rewrite it (`one()` and `one_or_none()` pass their SQL through the same way), so a
statement that writes is sent to the database like any other: `DbReader` is **not** an
authorization boundary. It never commits (each call ends with a rollback), but do not rely on that
either: an engine configured with `isolation_level="AUTOCOMMIT"` commits every statement. Use
`DbWriter` to write, and a read-only database role to guarantee reads only.

### Bind values

Pass untrusted *values* as `:name` placeholders with `params`; never format them into the SQL.
Bound values reach the driver separately from the SQL text, so it treats them as data whatever
they contain:

```python
rows = reader.query("SELECT id FROM users WHERE name = :name", params={"name": name})
```

`query()` and `scalar()` emit a `UserWarning` when `params` is omitted. `params={}` is the
explicit form for trusted constant SQL and does not warn.

### Allow-list identifiers

Placeholders stand for values only, never for table or column names or SQL keywords:
`SELECT :col FROM users` returns the string you bound, not that column. If a name must vary, map
the untrusted input to an application-owned constant and reject anything unknown:

```python
ORDER_BY = {"name": "name", "newest": "created_at DESC"}
order_by = ORDER_BY[sort]  # KeyError for anything not listed: reject the request
rows = reader.query(f"SELECT id FROM users ORDER BY {order_by}", params={})
```

### Use a read-only role

Connect readers with a role that can only `SELECT` the tables they query (see
[Database Permissions](#database-permissions)). For example, on PostgreSQL:

```sql
CREATE ROLE reports_reader LOGIN PASSWORD '<from your secret store>';
GRANT SELECT ON users, orders TO reports_reader;
```

### Diagnostics

When a connection, engine creation or SQL execution fails, `query()` and `scalar()` raise
`QueryError` (`DbConnectionError` if the engine cannot be created) with a fixed message that
contains no connection URL, SQL, parameter values or driver message. The original exception is
chained as `__cause__`, and SQLAlchemy's message includes the SQL and the bound parameter values:
do not return tracebacks to callers or write `__cause__` to logs they can read. Leave `echo` off
(`DbConfig.echo` defaults to `False`); SQLAlchemy's echo output includes SQL and parameter values.

## Data Minimization

- Use the `query` parameter to project only the columns you need.
- Exclude sensitive columns (passwords, tokens, PII) from trigger payloads.
- The trigger delivers full row snapshots by default — restrict columns at the query level.

## Logging Safety

The following must never appear in log output:

- Full connection strings
- Passwords or tokens
- Sensitive row data (PII, financial data)
- Raw secrets

`azure-functions-db-python` uses structured logging. Review your handler code to
ensure sensitive data from query results is not logged inadvertently.

## Multi-Tenant Considerations

When handling multiple tenant databases:

- Use separate poller namespaces per tenant.
- Use separate checkpoint paths (blob prefixes or containers).
- Use separate database credentials per tenant.
- Never share checkpoint state across tenants.

## Supply Chain Security

- Pin dependencies in `requirements.txt` or a lock file.
- Run vulnerability scanning in CI (e.g. `pip-audit`, `safety`).
- Database drivers are installed as optional extras — only install what you need:
    - `azure-functions-db[postgres]`
    - `azure-functions-db[mysql]`
    - `azure-functions-db[mssql]`

## Reporting Security Issues

If you discover a security vulnerability:

- **Do not** open a public GitHub issue.
- Use [GitHub Security Advisories](https://github.com/yeongseon/azure-functions-db-python/security/advisories) to report privately.
- Or contact the maintainer directly.
