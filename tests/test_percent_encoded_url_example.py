"""Offline regression for percent-encoded credentials in connection URLs (#519)."""

from pathlib import Path
import re
import subprocess  # nosec B404  # runs the repo's own example with a fixed argv
import sys
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy.engine import make_url

from azure_functions_db.core.config import DbConfig, resolve_db_config
from azure_functions_db.core.engine import EngineProvider
from examples import percent_encoded_url as demo

ROOT = Path(__file__).resolve().parents[1]


def test_encoded_credentials_survive_expansion_unchanged() -> None:
    effective = demo.expand()
    assert effective == demo.EXPECTED_EFFECTIVE_URL
    parsed = make_url(effective)
    assert parsed.username == demo.EXPECTED_USERNAME
    assert parsed.password == demo.EXPECTED_SECRET
    assert parsed.host == "db.example.invalid"


def test_env_value_that_looks_like_a_token_is_not_expanded_again() -> None:
    # The value of ENC_SECRET contains text shaped like another token and like
    # percent-escapes. Expansion is single-pass, so none of it is touched.
    template = "postgresql+psycopg://svc:%ENC_SECRET%@%DB_HOST%/orders"
    env = {"ENC_SECRET": "a%40b%DB_HOST%c", "DB_HOST": "db.example.invalid"}
    assert demo.expand(template, env) == (
        "postgresql+psycopg://svc:a%40b%DB_HOST%c@db.example.invalid/orders"
    )


def test_literal_percent_escape_still_works_beside_encoded_bytes() -> None:
    assert demo.expand("x%%y%40z", {}) == "x%y%40z"


def test_engine_receives_the_effective_url_exactly(monkeypatch: pytest.MonkeyPatch) -> None:
    # Given the binding-style config path (resolve_db_config) with the demo template.
    monkeypatch.setenv("DB_HOST", "db.example.invalid")
    config: DbConfig = resolve_db_config(url=demo.URL_TEMPLATE, db_config=None)
    # When the provider builds an engine (create_engine is mocked: no driver, no network).
    with patch("azure_functions_db.core.engine.create_engine", return_value=MagicMock()) as create:
        EngineProvider().create_isolated_engine(config)
    # Then the driver URL is expanded once and NOT decoded or re-substituted.
    assert create.call_args.args[0] == demo.EXPECTED_EFFECTIVE_URL


def test_masked_url_never_contains_secret() -> None:
    rendered = demo.masked(demo.expand())
    assert rendered == "postgresql+psycopg://app%40example:***@db.example.invalid:5432/orders"
    assert demo.EXPECTED_SECRET not in rendered
    assert "s3%2Fcr%40t%25" not in rendered


def test_example_script_runs_offline_and_does_not_print_secret() -> None:
    result = subprocess.run(  # nosec B603  # fixed argv, no shell
        [sys.executable, str(ROOT / "examples" / "percent_encoded_url.py")],
        capture_output=True,
        text=True,
        check=True,
        cwd=ROOT,
        env={"PYTHONPATH": f"{ROOT / 'src'}", "PATH": ""},
    )
    assert "app%40example:***@db.example.invalid" in result.stdout
    assert "s3/cr@t%" not in result.stdout
    assert "s3%2Fcr%40t%25" not in result.stdout


def test_docs_show_the_same_template_and_effective_url() -> None:
    text = (ROOT / "docs" / "25-engine-provider-pooling.md").read_text()
    assert demo.URL_TEMPLATE in text
    assert demo.EXPECTED_EFFECTIVE_URL in text
    assert re.search(r"examples/percent_encoded_url\.py", text)
