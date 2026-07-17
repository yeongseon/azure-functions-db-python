# Security / Configuration

## 1. Principles

- Do not embed secrets in code.
- Use managed identity / environment variables / secret store where possible.
- Minimize checkpoint storage permissions.
- Prefer read-only DB accounts.

## 2. Connection Information

### Azure Functions Side
- `AzureWebJobsStorage`
- DB connection setting (e.g., `ORDERS_DB_URL`)

### Recommendations
- In production, consider using managed identity or secret references instead of plain passwords.
- Keep development/test and production values separate.

## 3. Least Privilege

### DB Account
For MVP polling, the following is typically sufficient:
- SELECT on the target table
- Metadata/schema read if required

### Storage Account
- checkpoint/lease blob read/write
- quarantine container write if required

## 4. Data Minimization

The payload delivered to the handler should support an option to project only the minimum required columns.
Recommend operational guidance to exclude sensitive columns from projection by default.

## 5. Protecting Sensitive Information in Logs

The following must not appear in logs:
- Full connection strings
- Passwords/tokens
- Full sensitive row payloads
- Raw secrets

## 6. Multi-Tenant Considerations

The initial version assumes app-level trust.
When handling multiple tenant DBs simultaneously:
- Separate poller namespaces
- Separate checkpoint paths
- Separate quarantine paths
- Separate per-tenant DB credentials

## 7. Supply Chain

- Dependency pinning
- Maintain lock file
- Vulnerability scan in CI
- Separate optional drivers as extras

Examples:
- `azure-functions-db[postgres]`
- `azure-functions-db[mysql]`
- `azure-functions-db[mssql]`
