from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import pytest
from sqlalchemy.engine import Engine
from sqlalchemy.pool import NullPool

from azure_functions_db.adapter.sqlalchemy import SqlAlchemySource
from azure_functions_db.core.config import DbConfig
from azure_functions_db.core.engine import EngineProvider
from azure_functions_db.trigger.errors import SourceConfigurationError


@pytest.mark.parametrize("shared", [False, True])
def test_source_applies_config_when_engine_is_initialized(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, shared: bool
) -> None:
    # Given a resolved URL and non-default pool, driver and engine options.
    monkeypatch.setenv("SOURCE_DB_URL", f"sqlite:///{tmp_path / 'source.db'}")
    config = DbConfig(
        connection_url="%SOURCE_DB_URL%",
        pool_recycle=123,
        echo=True,
        connect_args={"timeout": 0.321},
        engine_kwargs={"poolclass": NullPool, "pool_pre_ping": True},
    )
    provider = EngineProvider() if shared else None
    source = SqlAlchemySource(
        db_config=config,
        query="SELECT 1 AS id",
        cursor_column="id",
        pk_columns=["id"],
        engine_provider=provider,
    )
    try:
        # When the source executes a real SQLite query.
        assert source.fetch(None, 10) == [{"id": 1}]

        # Then configuration and ownership match both construction paths.
        engine = source._engine
        assert engine is not None
        assert isinstance(engine.pool, NullPool)
        assert engine.pool._pre_ping is True
        assert engine.pool._recycle == 123
        assert engine.echo is True
        assert source._owns_engine is (not shared)
        with engine.connect() as connection:
            assert connection.exec_driver_sql("PRAGMA busy_timeout").scalar_one() == 321
        with patch.object(engine, "dispose", wraps=engine.dispose) as dispose:
            source.dispose()
            assert dispose.call_count == (0 if shared else 1)
        if provider is not None:
            assert provider.get_engine(source._db_config) is engine
    finally:
        source.dispose()
        if provider is not None:
            provider.dispose_all()


def test_source_disposes_private_engine_when_reflection_fails(tmp_path: Path) -> None:
    # Given a private engine configured for a missing table.
    source = SqlAlchemySource(
        db_config=DbConfig(
            connection_url=f"sqlite:///{tmp_path / 'missing.db'}",
            engine_kwargs={"poolclass": NullPool},
        ),
        table="missing",
        cursor_column="id",
        pk_columns=["id"],
    )
    # When reflection fails, then ownership is released and the pool is disposed.
    with patch.object(Engine, "dispose", autospec=True) as dispose:
        with pytest.raises(SourceConfigurationError):
            source.fetch(None, 10)
        dispose.assert_called_once()
    assert source._engine is None
    assert source._owns_engine is False
