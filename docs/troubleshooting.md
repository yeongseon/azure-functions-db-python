# Troubleshooting

Common issues and solutions for `azure-functions-db`.

## Installation Issues

### ImportError: No module named `azure_functions_db`

- Confirm installation ran in the correct virtual environment.
- Run `python -m pip install azure-functions-db[postgres]`.
- Verify with `python -c "import azure_functions_db"`.
- Check that the active environment matches the one your Function App uses.

### Driver not found (psycopg, pymysql, pyodbc)

- Confirm you installed the correct database extra:
    - PostgreSQL: `pip install azure-functions-db[postgres]`
    - MySQL: `pip install azure-functions-db[mysql]`
    - SQL Server: `pip install azure-functions-db[mssql]`
- Verify the driver is installed: `pip show psycopg` (or `pymysql`, `pyodbc`).
- SQL Server requires ODBC Driver 17+ installed at the OS level.

### SQLAlchemy version mismatch

- This package requires SQLAlchemy 2.0+.
- Check your version: `pip show sqlalchemy`.
- If an older version is installed transitively, pin `sqlalchemy>=2.0` in your requirements.

## Connection Issues

### Connection refused or timeout

- Verify the database URL format is correct for your driver:
    - PostgreSQL: `postgresql+psycopg://user:pass@host:5432/db`
    - MySQL: `mysql+pymysql://user:pass@host:3306/db`
    - SQL Server: `mssql+pyodbc://user:pass@host:1433/db?driver=ODBC+Driver+17+for+SQL+Server`
- Confirm the database server is running and accessible from your network.
- Check firewall rules — Azure Functions may need outbound access configured.

### Environment variable not resolved

- Connection URLs use `%VAR_NAME%` syntax for environment variable substitution.
- Confirm the variable is set: `echo $DB_URL`.
- For local development, set variables in `local.settings.json` under `Values`.
- Partial substitution is supported: `postgresql+psycopg://%DB_USER%:%DB_PASS%@%DB_HOST%/mydb`.

## Decorator Issues

### TypeError: unexpected keyword argument

- Check that decorator parameter names match the current API.
- Common renames: `db_trigger` is now `trigger`, `db_input` is now `input`, `db_output` is now `output`.
- The main class is `DbBindings` (not `DbFunctionApp`).

### Multiple decorators conflict

- `output` and `inject_writer` are mutually exclusive on the same handler.
- Multiple `output` decorators on the same handler are not allowed.
- `input` and `inject_reader` can coexist but typically serve different use cases.

### Decorator order matters

- Place database decorators (`@db.input(...)`, `@db.output(...)`) closest to the function definition.
- Azure Functions decorators (`@app.route(...)`, `@app.schedule(...)`) go above.

## Trigger Issues

### No events received

- Confirm the `cursor_column` (e.g. `updated_at`) is being updated on every row change by your application.
- Confirm the checkpoint store (Azure Blob Storage or Azurite) is accessible.
- Verify the timer trigger is firing: check Azure Functions logs for timer invocations.
- On first run with no checkpoint, all existing rows matching the query will be delivered.

### Duplicate events

- This is expected behavior with at-least-once delivery.
- Duplicates can occur during process crashes, lease transitions, or commit failures.
- Handlers must be idempotent — design operations to be safely re-applied.

### Hard deletes not detected

- Cursor-based polling only detects inserts and updates.
- Hard deletes (removing rows) are not captured.
- Consider using soft deletes (a `deleted_at` column) instead.

## Output Issues

### DbOut.set() not writing

- Confirm `.set()` is called before the handler returns.
- Confirm the table name and column names match your database schema.
- Check for exceptions in the Azure Functions log output.

### Wrong table or columns

- The `table` parameter must match the actual database table name.
- Column names in the dict passed to `.set()` must match table columns exactly.

## Azure Deployment Issues

### Works locally but fails in Azure

- Confirm `requirements.txt` includes the driver extra: `azure-functions-db[postgres]`.
- Confirm environment variables are set in Azure Portal > Function App > Configuration.
- Verify the deployed Python runtime version matches your local version.
- Rebuild deployment artifacts from a clean environment to avoid stale dependencies.

### Checkpoint storage not accessible

- Confirm `AzureWebJobsStorage` is configured in Application Settings.
- Confirm the blob container exists or the app has permission to create it.
- For local development, use Azurite: `AzureWebJobsStorage=UseDevelopmentStorage=true`.
