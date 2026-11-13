"""Offline demo: reuse and separation of engines in one ``EngineProvider`` (SQLite only).

Run it from the repository root after ``pip install -e .``::

    python examples/engine_provider_reuse.py

Two temporary SQLite files stand in for two independent databases. One module-level
``EngineProvider`` (the pattern recommended for warm invocations) serves both:

* equal configurations share one engine, so a reader and a writer for database A use the
  same pool;
* a different URL, or the same URL with different ``engine_kwargs``, gets its own engine and
  never sees the other database's rows;
* ``DbReader`` / ``DbWriter`` created with the provider never dispose its engines when
  they close: the provider owns them, and ``dispose_all()`` is the cleanup.

Engines live inside one worker process only. Nothing here implies a connection pool is
shared across Azure Functions workers or scale-out instances.
"""

from __future__ import annotations

from pathlib import Path
import tempfile

from sqlalchemy import create_engine, text

from azure_functions_db import DbConfig, DbReader, DbWriter, EngineProvider


def create_database(path: Path, label: str) -> str:
    """Create a SQLite file with one ``items`` row holding *label*; return its URL."""
    url = f"sqlite:///{path}"
    engine = create_engine(url)
    try:
        with engine.begin() as conn:
            conn.execute(text("CREATE TABLE items (id INTEGER PRIMARY KEY, label TEXT)"))
            conn.execute(text("INSERT INTO items VALUES (1, :label)"), {"label": label})
    finally:
        engine.dispose()
    return url


def read_label(provider: EngineProvider, config: DbConfig) -> object:
    """One 'invocation': a short-lived reader on the shared provider."""
    with DbReader(db_config=config, table="items", engine_provider=provider) as reader:
        row = reader.get(pk={"id": 1})
    return None if row is None else row["label"]


def run_demo(directory: Path) -> dict[str, object]:
    url_a = create_database(directory / "a.db", "database-a")
    url_b = create_database(directory / "b.db", "database-b")
    config_a = DbConfig(connection_url=url_a)
    config_a_equal = DbConfig(connection_url=url_a)
    config_b = DbConfig(connection_url=url_b)
    config_a_pre_ping = DbConfig(connection_url=url_a, engine_kwargs={"pool_pre_ping": True})

    provider = EngineProvider()  # module-level in a real app, so it spans warm invocations
    seen: dict[str, object] = {}
    try:
        engine_a = provider.get_engine(config_a)
        seen["equal_config_shares_engine"] = provider.get_engine(config_a_equal) is engine_a
        seen["other_url_separate_engine"] = provider.get_engine(config_b) is not engine_a
        seen["other_engine_kwargs_separate_engine"] = (
            provider.get_engine(config_a_pre_ping) is not engine_a
        )

        seen["read_a"] = read_label(provider, config_a)
        seen["read_b"] = read_label(provider, config_b)

        with DbWriter(db_config=config_a, table="items", engine_provider=provider) as writer:
            writer.update(data={"label": "database-a-updated"}, pk={"id": 1})
        seen["read_a_after_update"] = read_label(provider, config_a)
        seen["read_b_after_update"] = read_label(provider, config_b)

        # Warm invocations: many short-lived readers, one engine, never disposed by close().
        for _ in range(3):
            read_label(provider, config_a)
        seen["engine_stable_across_invocations"] = provider.get_engine(config_a) is engine_a

        provider.dispose_all()
        seen["engine_rebuilt_after_dispose_all"] = provider.get_engine(config_a) is not engine_a
    finally:
        provider.dispose_all()
    return seen


def main() -> None:
    with tempfile.TemporaryDirectory() as directory:
        for key, value in run_demo(Path(directory)).items():
            print(f"{key}: {value}")


if __name__ == "__main__":
    main()
