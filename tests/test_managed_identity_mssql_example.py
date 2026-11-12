"""Service-free checks for examples/managed-identity-mssql and its guide (#518).

``azure.identity`` and ``pyodbc`` are replaced with in-process fakes, so no Azure
subscription, network, ODBC driver or database is involved. These checks do NOT
certify Azure managed identity; they pin the example's wiring and its documentation.
"""

from __future__ import annotations

from collections.abc import Iterator
import importlib.util
from pathlib import Path
import re
import struct
import sys
from types import ModuleType, SimpleNamespace
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from azure_functions_db import EngineProvider
from azure_functions_db.core.config import DbConfig

ROOT = Path(__file__).resolve().parents[1]
EXAMPLE_DIR = ROOT / "examples" / "managed-identity-mssql"
GUIDE = ROOT / "docs" / "examples" / "managed_identity_mssql.md"
FAKE_TOKEN = "fake-token-value"


class FakeCredential:
    def __init__(self, **kwargs: object) -> None:
        self.kwargs = kwargs
        self.scopes: list[str] = []

    def get_token(self, scope: str) -> SimpleNamespace:
        self.scopes.append(scope)
        return SimpleNamespace(token=FAKE_TOKEN)


@pytest.fixture
def example(monkeypatch: pytest.MonkeyPatch) -> Iterator[ModuleType]:
    identity = ModuleType("azure.identity")
    identity.DefaultAzureCredential = FakeCredential  # type: ignore[attr-defined]
    pyodbc = ModuleType("pyodbc")
    pyodbc.Connection = object  # type: ignore[attr-defined]
    pyodbc.connect = MagicMock(name="connect")  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "azure.identity", identity)
    monkeypatch.setitem(sys.modules, "pyodbc", pyodbc)
    monkeypatch.setenv("MSSQL_SERVER", "placeholder.database.windows.net")
    monkeypatch.setenv("MSSQL_DATABASE", "placeholder-db")
    monkeypatch.setenv("AZURE_CLIENT_ID", "00000000-0000-0000-0000-000000000000")
    spec = importlib.util.spec_from_file_location("mi_example", EXAMPLE_DIR / "function_app.py")
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    yield module


def test_example_registers_both_documented_functions(example: ModuleType) -> None:
    names = {f.get_function_name() for f in example.app.get_functions()}
    assert names == {"mi_get_orders", "mi_create_order"}


def test_credential_uses_optional_client_id(example: ModuleType) -> None:
    assert example._credential.kwargs == {
        "managed_identity_client_id": "00000000-0000-0000-0000-000000000000"
    }


def test_token_is_packed_as_length_prefixed_utf16le(example: ModuleType) -> None:
    packed = example._access_token_struct()
    payload = FAKE_TOKEN.encode("utf-16-le")
    assert packed == struct.pack("<I", len(payload)) + payload
    assert example._credential.scopes == ["https://database.windows.net/.default"]


def test_creator_passes_fresh_token_and_no_sql_password(example: ModuleType) -> None:
    example._pyodbc_connect()
    connect: Any = sys.modules["pyodbc"].connect
    conn_str = connect.call_args.args[0]
    assert "Server=tcp:placeholder.database.windows.net,1433;" in conn_str
    assert "Database=placeholder-db;" in conn_str
    assert "Encrypt=yes;TrustServerCertificate=no;" in conn_str
    assert not re.search(r"\b(UID|PWD|Password)\s*=", conn_str, re.I)
    assert list(connect.call_args.kwargs["attrs_before"]) == [example.SQL_COPT_SS_ACCESS_TOKEN]
    # Every new connection asks for a token again (the credential caches internally).
    example._pyodbc_connect()
    assert len(example._credential.scopes) == 2


def test_provider_adds_creator_and_recycle_settings(example: ModuleType) -> None:
    provider = example.ManagedIdentityEngineProvider()
    assert isinstance(provider, EngineProvider)
    config = DbConfig(connection_url="mssql+pyodbc://", engine_kwargs={"pool_pre_ping": False})
    with patch("azure_functions_db.core.engine.create_engine") as create:
        provider.create_isolated_engine(config)
    assert create.call_args.args[0] == "mssql+pyodbc://"
    options = create.call_args.kwargs
    assert options["creator"] is example._pyodbc_connect
    assert options["pool_recycle"] == 1800
    assert options["pool_pre_ping"] is True


def test_documented_settings_match_example_source() -> None:
    source = (EXAMPLE_DIR / "function_app.py").read_text()
    guide = GUIDE.read_text()
    sample = (EXAMPLE_DIR / "local.settings.sample.json").read_text()
    for setting in ("MSSQL_SERVER", "MSSQL_DATABASE", "AZURE_CLIENT_ID"):
        assert setting in source
        assert f"`{setting}`" in guide
    for setting in ("MSSQL_SERVER", "MSSQL_DATABASE"):
        assert f'"{setting}"' in sample
    # No SQL secret is configured or documented for this example.
    assert not re.search(r"password|pwd|uid", sample, re.I)


def test_guide_grants_match_example_docstring() -> None:
    source = (EXAMPLE_DIR / "function_app.py").read_text()
    guide = GUIDE.read_text()
    for statement in ("FROM EXTERNAL PROVIDER", "db_datareader", "db_datawriter"):
        assert statement in source
        assert statement in guide
    assert "<your-function-app-name>" in guide


def test_guide_states_local_login_is_not_a_managed_identity_proof() -> None:
    guide = " ".join(GUIDE.read_text().split())
    assert "does **not** certify Azure managed identity" in guide


def test_guide_is_cross_linked_and_published() -> None:
    assert "examples/managed_identity_mssql.md" in (ROOT / "mkdocs.yml").read_text()
    assert "examples/managed_identity_mssql/" in (ROOT / "docs" / "llms.txt").read_text()
    assert "docs/examples/managed_identity_mssql.md" in (EXAMPLE_DIR / "README.md").read_text()
    assert "examples/managed-identity-mssql/" in GUIDE.read_text()
    for target in re.findall(r"\]\((\.\./[^)#]+\.md)\)", GUIDE.read_text()):
        assert (GUIDE.parent / target).is_file(), target
