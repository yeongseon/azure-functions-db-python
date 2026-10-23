from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from tools import check_deployed_version, verification_host, verification_probe
from tools.verification_evidence import Summary, redact, save

ROOT = Path(__file__).resolve().parents[1]


@pytest.mark.parametrize("header", ["Authorization", "Proxy-Authorization"])
@pytest.mark.parametrize(
    "value", ['"Bearer CANARY token"', "'Basic CANARY=='", "SharedKey a:CANARY"]
)
def test_header_redacts_whole_credential(header: str, value: str) -> None:
    assert "CANARY" not in redact(f"{header}: {value}")


@pytest.mark.parametrize(
    "text",
    [
        '{"password": "alpha CANARY beta"}',
        '{"token": "with \\"escaped\\" CANARY"}',
        "SharedAccessKey=CANARY;name=ok",
        "InstrumentationKey=CANARY;",
        "x-functions-key=CANARY",
        "postgresql+psycopg://user:CANARY@localhost/db",
        '<failure message="Authorization: Bearer CANARY">x</failure>',
    ],
)
def test_redacts_quoted_connection_and_xml_credentials(text: str) -> None:
    assert "CANARY" not in redact(text)


def test_save_preserves_json_binary_and_symlink(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("MY_API_TOKEN", 'pre;fix&CANARY "value"')
    (tmp_path / "extra.json").write_text(json.dumps({"nested": ['pre;fix&CANARY "value"', 123]}))
    png = b"\x89PNG\xff\x00"
    (tmp_path / "chart.png").write_bytes(png)
    outside = tmp_path.parent / (tmp_path.name + "-outside.log")
    outside.write_text("password=CANARY")
    (tmp_path / "link.log").symlink_to(outside)
    save(tmp_path, Summary(1, "test", ('Authorization: "Bearer CANARY"',), 1, 1, 0, 0, "PASS", 0))
    assert "CANARY" not in (tmp_path / "summary.json").read_text()
    assert json.loads((tmp_path / "extra.json").read_text())["nested"][1] == 123
    assert "CANARY" not in (tmp_path / "extra.json").read_text()
    assert (tmp_path / "chart.png").read_bytes() == png
    assert outside.read_text() == "password=CANARY"


@pytest.mark.parametrize("backend", ["postgres", "mysql", "mssql"])
def test_probe_rejects_sqlite_url_for_other_backends(tmp_path: Path, backend: str) -> None:
    result = subprocess.run(
        [sys.executable, "-m", "tools.verification_probe", backend, str(tmp_path / "probe.json")],
        env=dict(os.environ, **{f"TEST_{backend.upper()}_URL": "sqlite://"}),
        cwd=ROOT,
        capture_output=True,
        text=True,
    )
    assert result.returncode != 0
    assert not (tmp_path / "probe.json").exists()


@pytest.mark.parametrize(
    ("backend", "dialect"),
    [("postgres", "postgresql"), ("mysql", "mysql"), ("mssql", "mssql"), ("sqlite", "sqlite")],
)
def test_probe_accepts_dialect_names(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, backend: str, dialect: str
) -> None:
    engine = MagicMock()
    engine.dialect.name = dialect
    engine.dialect.driver = "driver"
    engine.dialect.server_version_info = (1,)
    monkeypatch.setattr(verification_probe, "create_engine", lambda url: engine)
    monkeypatch.setenv(f"TEST_{backend.upper()}_URL", f"{dialect}+driver://")
    monkeypatch.setattr(sys, "argv", ["probe", backend, str(tmp_path / "probe.json")])
    assert verification_probe.main() == 0


def test_verify_clears_make_command_line_backend(tmp_path: Path) -> None:
    # Given real GNU make propagation, with all lanes replaced by observable local targets.
    script = tmp_path / "lane.py"
    script.write_text(
        "import os,sys\nfrom pathlib import Path\n"
        "from tools.verification_evidence import Summary,save\n"
        "lane=sys.argv[1]\n"
        "assert os.environ.get('DB_BACKEND') != 'sqlite'\n"
        "save(Path('test-results')/lane, Summary(1,lane,(),1,1,0,0,'PASS',0))\n"
    )
    (tmp_path / "Makefile").write_text(
        f"export PYTHONPATH := {ROOT}\nverify:\n\t{sys.executable} -m tools.verification verify\n"
        "check test integration host-smoke wheel-smoke build docs:\n"
        f"\t{sys.executable} {script} $@\n"
    )
    result = subprocess.run(
        ["make", "verify", "DB_BACKEND=sqlite"], cwd=tmp_path, capture_output=True, text=True
    )
    assert result.returncode == 0, result.stdout + result.stderr


@pytest.mark.parametrize("target", ["doctor", "report", "clean"])
def test_maintenance_does_not_bootstrap_hatch(tmp_path: Path, target: str) -> None:
    # Given a fresh Hatch data directory and a Python with no development modules.
    (tmp_path / "Makefile").write_text((ROOT / "Makefile").read_text())
    (tmp_path / "tools").symlink_to(ROOT / "tools", target_is_directory=True)
    environment = dict(os.environ, HATCH_DATA_DIR=str(tmp_path / "hatch-data"), VIRTUAL_ENV="")
    environment.pop("CI", None)
    result = subprocess.run(
        ["make", target], cwd=tmp_path, env=environment, capture_output=True, text=True
    )
    assert not (tmp_path / "hatch-data" / "env").exists(), result.stdout + result.stderr
    assert "Creating environment" not in result.stderr


def test_check_saves_failure_log_and_summary(tmp_path: Path) -> None:
    (tmp_path / "Makefile").write_text(
        (ROOT / "Makefile").read_text()
        + "\nstyle:\n\t@echo 'Authorization: Bearer CANARY'; exit 7\n"
    )
    environment = dict(
        os.environ, PYTHONPATH=str(ROOT), GITHUB_STEP_SUMMARY=str(tmp_path / "job-summary")
    )
    result = subprocess.run(
        ["make", "check", "CI=true", f"PYTHON={sys.executable}"],
        cwd=tmp_path,
        env=environment,
        capture_output=True,
        text=True,
    )
    assert result.returncode != 0
    summary = json.loads((tmp_path / "test-results/check/summary.json").read_text())
    assert summary["status"] == "FAIL"
    assert "CANARY" not in (tmp_path / "test-results/check/logs/runner.log").read_text()
    assert "check: FAIL" in (tmp_path / "job-summary").read_text()


def test_host_waits_for_success_after_checkpoint(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(ROOT)
    monkeypatch.setattr("tools.verification_host.shutil.which", lambda tool: tool)
    monkeypatch.setenv("TEST_AZURITE_CONN_STR", "test")
    from contextlib import nullcontext

    monkeypatch.setattr(verification_host, "azurite", lambda directory: nullcontext())
    polls: list[int] = []

    @contextmanager
    def fake_child(command: list[str], log: Path, app: Path) -> Iterator[SimpleNamespace]:
        log.parent.mkdir(parents=True, exist_ok=True)
        log.write_text("")
        with sqlite3.connect(app / "host-smoke.db") as connection:
            connection.execute("CREATE TABLE trigger_state (id INTEGER)")
            connection.execute("INSERT INTO trigger_state VALUES (1)")

        def poll() -> None:
            polls.append(1)
            if len(polls) >= 2:
                log.write_text("Executed 'Functions.db_trigger_host_smoke' (Succeeded")
            return None

        yield SimpleNamespace(poll=poll)

    monkeypatch.setattr(verification_host, "child", fake_child)
    monkeypatch.setattr("tools.verification_host.time.sleep", lambda duration: None)
    assert verification_host.main() == 0
    assert len(polls) >= 2


@pytest.mark.parametrize("failure", [TimeoutError(), ConnectionError(), OSError()])
def test_identity_recovers_after_network_readiness_error(
    monkeypatch: pytest.MonkeyPatch, failure: OSError
) -> None:
    monkeypatch.setenv("E2E_BASE_URL", "https://example.test")
    monkeypatch.setenv("EXPECTED_VERSION", "candidate")
    response = MagicMock()
    response.__enter__.return_value.status = 200
    response.__enter__.return_value.read.return_value = b'{"version":"candidate"}'
    opener = MagicMock(side_effect=[failure, response])
    monkeypatch.setattr("urllib.request.urlopen", opener)
    monkeypatch.setattr("tools.check_deployed_version.time.sleep", lambda duration: None)
    assert check_deployed_version.main() == 0
    assert opener.call_count == 2


def test_identity_retries_readiness_but_rejects_reachable_mismatch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("E2E_BASE_URL", "https://example.test")
    monkeypatch.setenv("EXPECTED_VERSION", "candidate")
    responses = iter([None, "candidate"])
    monkeypatch.setattr(
        check_deployed_version, "fetch_deployed_version", lambda *args, **kwargs: next(responses)
    )
    monkeypatch.setattr("time.sleep", lambda duration: None)
    assert check_deployed_version.main() == 0
    monkeypatch.setattr(
        check_deployed_version, "fetch_deployed_version", lambda *args, **kwargs: "wrong"
    )
    assert check_deployed_version.main() == 1
