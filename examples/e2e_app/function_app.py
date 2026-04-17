"""E2E test function app for azure-functions-db-python.

Exposes minimal HTTP routes that exercise each decorator/binding mode
so the e2e test suite can verify wiring against a real Azure Functions host.

Required environment variable:
    E2E_DB_URL — SQLAlchemy connection URL for the test database.
"""

from __future__ import annotations

import json
import os

import azure.functions as func

from azure_functions_db import DbBindings, DbOut, DbReader, DbWriter

app = func.FunctionApp()
db = DbBindings()

DB_URL = os.environ.get("E2E_DB_URL", "sqlite:///e2e_test.db")


# ── Health ────────────────────────────────────────────────────────────────


@app.route(route="health", auth_level=func.AuthLevel.ANONYMOUS)
def health(req: func.HttpRequest) -> func.HttpResponse:
    """Liveness probe used by e2e warmup loop."""
    return func.HttpResponse(json.dumps({"status": "ok"}), mimetype="application/json")


# ── Setup (creates table for tests) ───────────────────────────────────────


@app.route(route="setup", auth_level=func.AuthLevel.ANONYMOUS, methods=["POST"])
def setup_table(req: func.HttpRequest) -> func.HttpResponse:
    """Create the e2e_items table if it doesn't exist."""
    from sqlalchemy import Column, Integer, MetaData, String, Table, create_engine

    engine = create_engine(DB_URL)
    metadata = MetaData()
    Table(
        "e2e_items",
        metadata,
        Column("id", Integer, primary_key=True),
        Column("name", String(100)),
        Column("status", String(50)),
    )
    metadata.create_all(engine)
    engine.dispose()
    return func.HttpResponse(json.dumps({"created": "e2e_items"}), mimetype="application/json")


# ── Output binding (insert) ──────────────────────────────────────────────


@app.route(route="items", auth_level=func.AuthLevel.ANONYMOUS, methods=["POST"])
@db.output("out", url=DB_URL, table="e2e_items")
def create_item(req: func.HttpRequest, out: DbOut) -> func.HttpResponse:
    """Insert a row via output binding."""
    data = req.get_json()
    out.set(data)
    return func.HttpResponse(json.dumps(data), status_code=201, mimetype="application/json")


# ── Input binding (by PK) ────────────────────────────────────────────────


@app.route(route="items/{item_id}", auth_level=func.AuthLevel.ANONYMOUS, methods=["GET"])
@db.input(
    "item",
    url=DB_URL,
    table="e2e_items",
    pk=lambda req: {"id": int(req.route_params["item_id"])},
)
def get_item(req: func.HttpRequest, item: dict | None) -> func.HttpResponse:
    """Fetch a single row by PK via input binding."""
    if item is None:
        return func.HttpResponse(
            json.dumps({"error": "not found"}), status_code=404, mimetype="application/json"
        )
    return func.HttpResponse(json.dumps(item), mimetype="application/json")


# ── Input binding (by query) ─────────────────────────────────────────────


@app.route(route="items", auth_level=func.AuthLevel.ANONYMOUS, methods=["GET"])
@db.input(
    "items",
    url=DB_URL,
    query="SELECT * FROM e2e_items",
)
def list_items(req: func.HttpRequest, items: list) -> func.HttpResponse:
    """Fetch all rows via input binding query mode."""
    return func.HttpResponse(json.dumps(items), mimetype="application/json")


# ── inject_reader ─────────────────────────────────────────────────────────


@app.route(route="reader/{item_id}", auth_level=func.AuthLevel.ANONYMOUS, methods=["GET"])
@db.inject_reader("reader", url=DB_URL, table="e2e_items")
def read_item(req: func.HttpRequest, reader: DbReader) -> func.HttpResponse:
    """Read a row using inject_reader."""
    item_id = int(req.route_params["item_id"])
    row = reader.get(pk={"id": item_id})
    if row is None:
        return func.HttpResponse(
            json.dumps({"error": "not found"}), status_code=404, mimetype="application/json"
        )
    return func.HttpResponse(json.dumps(row), mimetype="application/json")


# ── inject_writer ─────────────────────────────────────────────────────────


@app.route(route="writer", auth_level=func.AuthLevel.ANONYMOUS, methods=["POST"])
@db.inject_writer("writer", url=DB_URL, table="e2e_items")
def write_item(req: func.HttpRequest, writer: DbWriter) -> func.HttpResponse:
    """Insert a row using inject_writer."""
    data = req.get_json()
    writer.insert(data=data)
    return func.HttpResponse(json.dumps(data), status_code=201, mimetype="application/json")
