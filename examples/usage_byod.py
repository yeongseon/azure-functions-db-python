"""Minimal BYOD (Bring Your Own Database) example.

Shows the simplest possible usage of azure-functions-db with a
non-built-in database. This example uses Oracle, but the same pattern
works with any SQLAlchemy dialect:

    CockroachDB:  cockroachdb://user:pass@host:26257/db
    DuckDB:       duckdb:///path/to/local.db
    SQLite:       sqlite:///path/to/local.db
    Firebird:     firebird+fdb://user:pass@host/db

Prerequisites:
    pip install azure-functions-db <your-driver>
    # e.g. pip install azure-functions-db oracledb

Usage:
    ORACLE_DB_URL="oracle+oracledb://user:pass@host:1521/?service_name=XEPDB1"
    python examples/usage_byod.py
"""

from __future__ import annotations

import os

from azure_functions_db import DbBindings, DbReader

db = DbBindings()

# The only thing that changes for BYOD is the URL and the driver install.
# Everything else — decorators, DbReader, DbWriter, SqlAlchemySource — is identical.

url = os.environ.get("ORACLE_DB_URL")
if not url:
    raise SystemExit(
        "Set ORACLE_DB_URL environment variable, e.g.:\n"
        '  export ORACLE_DB_URL="oracle+oracledb://user:pass@host:1521/?service_name=XEPDB1"'
    )


@db.inject_reader("reader", url=url, table="orders")
def read_orders(reader: DbReader) -> None:
    # Single row lookup
    order = reader.get(pk={"id": 1})
    print("Single order:", order)

    # Query
    rows = reader.query(
        "SELECT * FROM orders WHERE status = :status",
        params={"status": "pending"},
    )
    print(f"Pending orders: {len(rows)}")
    for row in rows:
        print(f"  #{row['id']}: {row['status']}")


if __name__ == "__main__":
    read_orders()
