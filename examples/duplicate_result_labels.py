"""Offline demo: a query whose result columns share a label (SQLite, no credentials).

Run it from the repository root after ``pip install -e .``::

    python examples/duplicate_result_labels.py

A join can return two columns with the same label. ``DbReader.scalar()`` picks the
first column by position, while ``query()``/``one()`` build one dict per row, and a dict
holds one value per key, so the later column replaces the earlier one. SQLAlchemy's own
``row._mapping`` behaves the same way as ``query()``.
"""

from __future__ import annotations

from pathlib import Path
import tempfile

from sqlalchemy import create_engine, text

from azure_functions_db import DbReader

DUPLICATE_LABEL_SQL = (
    "SELECT u.name AS name, a.name AS name, u.id AS id "
    "FROM users u JOIN accounts a ON a.user_id = u.id WHERE u.id = :uid"
)


def create_database(path: Path) -> str:
    """Create a throwaway SQLite file with synthetic rows and return its URL."""
    url = f"sqlite:///{path}"
    engine = create_engine(url)
    try:
        with engine.begin() as conn:
            conn.execute(text("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)"))
            conn.execute(text("CREATE TABLE accounts (user_id INTEGER, name TEXT)"))
            conn.execute(text("INSERT INTO users VALUES (1, 'user-name')"))
            conn.execute(text("INSERT INTO accounts VALUES (1, 'account-name')"))
    finally:
        engine.dispose()
    return url


def run(url: str) -> dict[str, object]:
    """Return what each public reader API yields for the duplicate-label query."""
    params: dict[str, object] = {"uid": 1}
    with DbReader(url=url) as reader:
        return {
            "scalar": reader.scalar(DUPLICATE_LABEL_SQL, params=params),
            "query": reader.query(DUPLICATE_LABEL_SQL, params=params),
            "one": reader.one(DUPLICATE_LABEL_SQL, params=params),
        }


def driver_mapping(url: str) -> dict[str, object]:
    """What SQLAlchemy's own ``row._mapping`` returns for the same statement."""
    engine = create_engine(url)
    try:
        with engine.connect() as conn:
            row = conn.execute(text(DUPLICATE_LABEL_SQL), {"uid": 1}).one()
            return {"keys": list(row._mapping.keys()), "mapping": dict(row._mapping)}
    finally:
        engine.dispose()


def main() -> None:
    with tempfile.TemporaryDirectory() as directory:
        url = create_database(Path(directory) / "demo.db")
        for name, value in run(url).items():
            print(f"{name:7}: {value}")
        print("driver :", driver_mapping(url))


if __name__ == "__main__":
    main()
