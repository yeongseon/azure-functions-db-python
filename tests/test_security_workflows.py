from pathlib import Path
import re
import tomllib

ROOT = Path(__file__).resolve().parents[1]


def test_release_security_gate_is_required_and_not_advisory() -> None:
    # Given the publish workflow; When its audit job is inspected; Then no bypass is permitted.
    workflow = (ROOT / ".github/workflows/publish-pypi.yml").read_text()
    gate = workflow.split("\n  runtime-dependency-audit:\n", 1)[1].split("\n  publish:\n", 1)[0]
    publish = workflow.split("\n  publish:\n", 1)[1].split("\n  notify-cookbook:\n", 1)[0]
    needs = re.search(r"needs: \[(.*?)\]", publish)
    assert needs is not None
    assert "runtime-dependency-audit" in needs[1]
    assert "continue-on-error" not in gate
    assert "|| true" not in gate
    assert "contents: read" in gate
    assert "id-token: write" not in gate
    assert "needs: build" in gate
    assert "name: dist" in gate
    assert "candidate-dist/*.whl" in gate
    assert 'test "${#wheels[@]}" -eq 1' in gate
    assert "venv --without-pip" in gate
    assert "pip-audit==2.10.1" in gate
    assert "python -m tools.dependency_audit_policy" in gate
    assert "if: always()" in gate
    assert "if-no-files-found: error" in gate


def test_audit_matrix_covers_supported_python_and_all_runtime_extras() -> None:
    # Given declared runtime extras; When compared to the matrix; Then each closure is audited.
    config = tomllib.loads((ROOT / "pyproject.toml").read_text())
    extras = set(config["project"]["optional-dependencies"]) - {"dev", "docs"}
    workflow = (ROOT / ".github/workflows/publish-pypi.yml").read_text()
    matrix = workflow.split("\n  runtime-dependency-audit:\n", 1)[1].split("    steps:\n", 1)[0]
    closures = re.search(r"closure: \[(.*?)\]", matrix)
    assert closures is not None
    assert set(re.findall(r'"([^"]+)"', closures[1])) == {"base", *extras}
    interpreters = re.search(r"interpreter: \[(.*?)\]", matrix)
    assert interpreters is not None
    assert set(re.findall(r'"([^"]+)"', interpreters[1])) == {"3.11", "3.12", "3.13", "3.14"}


def test_static_scanners_keep_blocking_execution_and_failure_evidence() -> None:
    # Given static security jobs; When checked; Then failures cannot be swallowed.
    workflow = (ROOT / ".github/workflows/security.yml").read_text()
    assert "continue-on-error" not in workflow
    assert "|| true" not in workflow
    assert "make security" in workflow
    assert "hatch run security-evidence" in workflow
    assert "hatch run security-typecheck" in workflow
    assert "semgrep/semgrep-action@713efdd345f3035192eaa63f56867b88e63e4e5d" in workflow
    assert 'semgrep --outcome "$SCAN_OUTCOME"' in workflow
    assert workflow.count("if-no-files-found: error") == 2
    assert workflow.count("if: always()") >= 3


def test_scheduled_development_audit_remains_reporting_only_and_redacted() -> None:
    # Given scheduled reporting; When inspected; Then it reuses policy without raw tool output.
    workflow = (ROOT / ".github/workflows/dependency-audit.yml").read_text()
    assert workflow.count("continue-on-error: true") == 2
    assert "--development" in workflow
    assert "tools.dependency_audit_policy" in workflow
    assert "tee" not in workflow
    assert "if: always()" in workflow
