"""Offline demo: ``DbWriter.transaction()`` commit and rollback boundaries (SQLite only).

Run it from the repository root after ``pip install -e .``::

    python examples/transaction_rollback.py

A temporary SQLite file and synthetic rows are used; nothing connects to Azure or a
server. The recipe shows the boundary of the tested API on this backend only:

* writes inside ``with writer.transaction():`` commit together when the block exits
  normally;
* an exception raised by the handler body, or a ``WriteError`` from a later statement,
  rolls back every write made inside that block, so no partial write is committed;
* a write made *outside* the block (each ``DbWriter`` call is its own short transaction)
  stays committed even if the handler fails afterwards.

The transaction scope ends when the ``with`` block exits. It covers the SQL statements
of that one writer and nothing else: it does not include the trigger checkpoint, other
systems, or retries, so it does not make trigger delivery exactly-once (see
docs/24-polling-runtime-semantics.md).
"""

from __future__ import annotations

import contextlib
from pathlib import Path
import tempfile

from sqlalchemy import create_engine, text

from azure_functions_db import DbWriter, WriteError


def create_database(path: Path) -> str:
    """Create an empty ``orders`` table and return the SQLite URL."""
    url = f"sqlite:///{path}"
    engine = create_engine(url)
    try:
        with engine.begin() as conn:
            conn.execute(text("CREATE TABLE orders (id INTEGER PRIMARY KEY, status TEXT)"))
    finally:
        engine.dispose()
    return url


def order_ids(url: str) -> list[int]:
    engine = create_engine(url)
    try:
        with engine.connect() as conn:
            return [row[0] for row in conn.execute(text("SELECT id FROM orders ORDER BY id"))]
    finally:
        engine.dispose()


def commit_two_rows(url: str) -> None:
    """Both inserts are committed together when the block exits normally."""
    with DbWriter(url=url, table="orders") as writer, writer.transaction():
        writer.insert(data={"id": 1, "status": "new"})
        writer.insert(data={"id": 2, "status": "new"})


def handler_raises_after_first_insert(url: str) -> None:
    """A failing handler body: the already-executed insert is rolled back."""
    with DbWriter(url=url, table="orders") as writer, writer.transaction():
        writer.insert(data={"id": 10, "status": "new"})
        raise RuntimeError("handler failed after the first write")


def second_statement_violates_primary_key(url: str) -> WriteError:
    """A failing statement (duplicate primary key) rolls back the earlier insert too."""
    try:
        with DbWriter(url=url, table="orders") as writer, writer.transaction():
            writer.insert(data={"id": 20, "status": "new"})
            writer.insert(data={"id": 1, "status": "duplicate of a committed row"})
    except WriteError as exc:
        return exc
    raise RuntimeError("the duplicate key was expected to fail")


def write_outside_transaction_then_fail(url: str) -> None:
    """A write outside the block is its own transaction and survives a later failure."""
    with DbWriter(url=url, table="orders") as writer:
        writer.insert(data={"id": 30, "status": "committed on its own"})
        with writer.transaction():
            writer.insert(data={"id": 31, "status": "inside"})
            raise RuntimeError("handler failed inside the block")


def run_demo(directory: Path) -> dict[str, list[int]]:
    """Run every scenario and return the committed ids after each one."""
    url = create_database(directory / "orders.db")
    seen: dict[str, list[int]] = {}
    commit_two_rows(url)
    seen["after_commit"] = order_ids(url)
    with contextlib.suppress(RuntimeError):
        handler_raises_after_first_insert(url)
    seen["after_handler_failure"] = order_ids(url)
    second_statement_violates_primary_key(url)
    seen["after_write_error"] = order_ids(url)
    with contextlib.suppress(RuntimeError):
        write_outside_transaction_then_fail(url)
    seen["after_outside_write_then_failure"] = order_ids(url)
    return seen


def main() -> None:
    with tempfile.TemporaryDirectory() as directory:
        for step, ids in run_demo(Path(directory)).items():
            print(f"{step}: {ids}")


if __name__ == "__main__":
    main()
