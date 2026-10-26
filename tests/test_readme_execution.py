import os
from pathlib import Path
import re

from azure.storage.blob import ContainerClient
import pytest
from sqlalchemy import create_engine

from azure_functions_db import BlobCheckpointStore

ROOT = Path(__file__).resolve().parents[1]


def test_documented_dbconfig_import_executes() -> None:
    namespace: dict[str, object] = {}
    exec("from azure_functions_db import DbBindings, DbConfig, EngineProvider", namespace)
    assert namespace["DbConfig"] is not None


@pytest.mark.parametrize(
    "filename", ["README.md", "README.ko.md", "README.ja.md", "README.zh-CN.md"]
)
def test_readme_blob_blocks_build_with_environment(
    filename: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(
        "AzureWebJobsStorage", os.environ.get("TEST_AZURITE_CONN_STR", "UseDevelopmentStorage=true")
    )
    monkeypatch.setenv("ORDERS_DB_URL", "sqlite://")
    monkeypatch.setenv("SOURCE_DB_URL", "sqlite://")
    monkeypatch.setenv("DEST_DB_URL", "sqlite://")
    blocks = re.findall(r"```python\n(.*?)```", (ROOT / filename).read_text(), re.S)
    selected = [block for block in blocks if "ContainerClient.from_connection_string" in block]
    assert len(selected) == 2
    for block in selected:
        namespace: dict[str, object] = {}
        exec(compile(block, filename, "exec"), namespace)
        store = namespace["checkpoint_store"]
        assert isinstance(store, BlobCheckpointStore)
        if os.environ.get("TEST_AZURITE_CONN_STR"):
            store._container_client.create_container()
            try:
                lease = store.acquire_lease("snippet", 30)
                store.commit_checkpoint("snippet", {"cursor": [1, 1]}, lease)
                assert store.load_checkpoint("snippet") == {"cursor": [1, 1]}
                store.release_lease("snippet", lease)
            finally:
                store._container_client.delete_container()
                store._container_client.close()


def test_blob_sdk_does_not_expand_functions_placeholders() -> None:
    with pytest.raises(ValueError):
        ContainerClient.from_connection_string("%AzureWebJobsStorage%", "db-state")


def test_quick_start_executes_reads_and_writes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    url = f"sqlite:///{tmp_path / 'quickstart.db'}"
    monkeypatch.setenv("DB_URL", url)
    engine = create_engine(url)
    with engine.begin() as connection:
        connection.exec_driver_sql(
            "CREATE TABLE users (id INTEGER PRIMARY KEY, active BOOLEAN, email TEXT)"
        )
        connection.exec_driver_sql("INSERT INTO users VALUES (1, 1, 'example@example.invalid')")
        connection.exec_driver_sql(
            "CREATE TABLE orders (id INTEGER PRIMARY KEY, status TEXT, total FLOAT)"
        )
    text = (ROOT / "README.md").read_text().split("## Quick Start", 1)[1]
    snippet = re.findall(r"```python\n(.*?)```", text, re.S)[0]
    namespace: dict[str, object] = {}
    exec(compile(snippet, "README.md", "exec"), namespace)
    exec("list_active_users(); assert create_order() == 'Created'", namespace)
    assert "example@example.invalid" in capsys.readouterr().out
    with engine.connect() as connection:
        assert (
            connection.exec_driver_sql("SELECT status FROM orders WHERE id=1").scalar_one()
            == "pending"
        )
    engine.dispose()
