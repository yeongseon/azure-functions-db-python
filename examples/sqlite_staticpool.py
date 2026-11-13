"""Offline demo: a shared in-memory SQLite database through ``StaticPool`` (no credentials).

Run it from the repository root after ``pip install -e .``::

    python examples/sqlite_staticpool.py

``StaticPool`` keeps exactly one DBAPI connection, so every checkout sees the same
in-memory SQLite database. That is useful for local demos and tests. It is **not** a
production pool: one connection serialises all work, it is not meant for concurrent
threads, and separate Azure Functions workers or scale-out instances never share it.
Use the pool settings in docs/25-engine-provider-pooling.md for real databases.

The demo builds the engine through ``EngineProvider``, runs two sequential queries on
it, and always disposes it in a ``finally`` block. Disposing drops the in-memory
database; the next engine for the same config starts empty.
"""

from __future__ import annotations

from sqlalchemy import text
from sqlalchemy.pool import StaticPool

from azure_functions_db import DbConfig, DbReader, DbWriter, EngineProvider

CONFIG = DbConfig(
    connection_url="sqlite://",
    connect_args={"check_same_thread": False},
    engine_kwargs={"poolclass": StaticPool},
)


def run_demo() -> dict[str, object]:
    """Create a table, write once, read twice, then dispose and show the fresh state."""
    provider = EngineProvider()
    seen: dict[str, object] = {}
    try:
        engine = provider.get_engine(CONFIG)
        seen["pool"] = type(engine.pool).__name__
        with engine.begin() as conn:
            conn.execute(text("CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT)"))

        with DbWriter(db_config=CONFIG, table="notes", engine_provider=provider) as writer:
            writer.insert(data={"id": 1, "body": "first"})

        with DbReader(db_config=CONFIG, engine_provider=provider) as reader:
            seen["first_query"] = reader.query("SELECT id, body FROM notes ORDER BY id", params={})
            with DbWriter(db_config=CONFIG, table="notes", engine_provider=provider) as writer:
                writer.insert(data={"id": 2, "body": "second"})
            seen["second_query"] = reader.scalar("SELECT COUNT(*) FROM notes", params={})
        seen["same_engine_after_close"] = provider.get_engine(CONFIG) is engine

        # Cleanup semantics: dispose_all() empties the cache and drops the in-memory
        # database, so the next engine for the same config is new and empty.
        provider.dispose_all()
        rebuilt = provider.get_engine(CONFIG)
        seen["rebuilt_is_new_engine"] = rebuilt is not engine
        with rebuilt.connect() as conn:
            tables = conn.execute(text("SELECT name FROM sqlite_master")).fetchall()
        seen["tables_after_dispose"] = [row[0] for row in tables]
    finally:
        provider.dispose_all()
    return seen


def main() -> None:
    for key, value in run_demo().items():
        print(f"{key}: {value}")


if __name__ == "__main__":
    main()
