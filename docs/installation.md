# Installation

`azure-functions-db` targets the **Azure Functions Python v2 programming model**.

## Requirements

- Python 3.11+
- `azure-functions`
- SQLAlchemy 2.0+

> This package does not support the legacy `function.json`-based v1 programming model.

## Version Compatibility

| Component | Supported Range | Notes |
| --- | --- | --- |
| Python | 3.11+ | Project metadata declares `>=3.11,<3.15`. |
| SQLAlchemy | 2.0+ | Core and ORM. |
| `azure-functions` | Required | Use with Python v2 decorator-based `FunctionApp`. |

Database driver compatibility depends on the extra you install:

| Database | Extra | Driver |
| --- | --- | --- |
| PostgreSQL | `postgres` | [psycopg](https://www.psycopg.org/) |
| MySQL | `mysql` | [PyMySQL](https://pymysql.readthedocs.io/) |
| SQL Server | `mssql` | [pyodbc](https://github.com/mkleehammer/pyodbc) |

## From PyPI

Install with the driver extra for your database:

```bash
# Pick your database
pip install azure-functions-db[postgres]
pip install azure-functions-db[mysql]
pip install azure-functions-db[mssql]

# Multiple databases
pip install azure-functions-db[postgres,mysql]

# All drivers
pip install azure-functions-db[all]
```

Your Function App dependencies should include:

```text
azure-functions
azure-functions-db[postgres]
```

## Verify Installation

Run the following command after installation:

```bash
python -c "import azure_functions_db; print(azure_functions_db.__version__)"
```

Expected outcome:

- The command prints a version string such as `0.1.0`.
- No import errors are raised.

You can also verify package metadata:

```bash
pip show azure-functions-db
```

Check that your active environment is the same one used by your Function App.

## Local Development

```bash
git clone https://github.com/yeongseon/azure-functions-db.git
cd azure-functions-db
make install
```

All project maintenance commands should go through the Makefile.

## Upgrading

Upgrade to the latest published version:

```bash
pip install --upgrade azure-functions-db[postgres]
```

Recommended upgrade workflow:

1. Upgrade in a dedicated virtual environment.
2. Confirm compatible `azure-functions` and SQLAlchemy versions.
3. Run your local Azure Functions smoke tests.
4. Confirm decorator behavior and binding contracts match expectations.

For deterministic deployments, pin an explicit version in your dependency file.

## Troubleshooting

### ImportError: No module named `azure_functions_db`

- Confirm installation ran in the correct environment.
- Run `python -m pip install azure-functions-db[postgres]`.
- Verify with `python -c "import azure_functions_db"`.

### Driver not found (psycopg, pymysql, pyodbc)

- Confirm you installed the correct extra: `pip install azure-functions-db[postgres]`.
- Run `pip show psycopg` (or `pymysql`, `pyodbc`) to verify the driver is present.
- SQL Server requires ODBC Driver 17+ installed at the OS level.

### SQLAlchemy version mismatch

- This package requires SQLAlchemy 2.0+. Check with `pip show sqlalchemy`.
- If an older version is installed transitively, pin `sqlalchemy>=2.0` and reinstall.

### Runtime dependency drift in Azure

- Rebuild deployment artifacts from a clean environment.
- Confirm `requirements.txt` includes both `azure-functions` and `azure-functions-db[postgres]`.
- Verify the deployed Python runtime version is compatible with your lockfile.
