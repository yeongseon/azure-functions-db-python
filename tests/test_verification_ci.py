from pathlib import Path
import re

ROOT = Path(__file__).resolve().parents[1]


def required_commands(text: str) -> list[str]:
    return re.findall(
        r"(?ms)^      - name: Run (?:integration|Azurite) tests\n(.*?)(?=^      - |\Z)", text
    )


def test_required_database_steps_never_mask_failures() -> None:
    # Given the actual required backend and Azurite steps.
    text = (ROOT / ".github/workflows/db-integration.yml").read_text()
    blocks = required_commands(text)
    # When examining their exit-code handling, then no failure can be overwritten.
    assert len(blocks) == 2
    for block in blocks:
        assert "|| true" not in block
        assert "|| pytest" not in block
        assert "continue-on-error" not in block
        assert "if: always()" not in block
        assert "tools.verification backend" in block


def test_database_matrix_and_always_uploaded_evidence_remain() -> None:
    # Given the required database workflow, when examining its retained contract.
    text = (ROOT / ".github/workflows/db-integration.yml").read_text()
    # Then all four backends and failure artifacts remain required.
    assert "db: [sqlite, postgres, mysql, mssql]" in text
    assert "backend azurite --owned" in text
    upload = text.split("      - name: Upload integration test report", 1)[1]
    assert "if: always()" in upload
    assert "path: test-results/**" in upload
