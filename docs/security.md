# Security

Security guidelines for using `azure-functions-db` in production.

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

`azure-functions-db` uses structured logging. Review your handler code to
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
- Use [GitHub Security Advisories](https://github.com/yeongseon/azure-functions-db/security/advisories) to report privately.
- Or contact the maintainer directly.
