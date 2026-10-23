from pathlib import Path

import pytest


@pytest.fixture(autouse=True)
def isolate_verification_job_summary(
    request: pytest.FixtureRequest, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Keep synthetic verification outcomes out of the enclosing CI job summary."""
    if request.node.path.name.startswith("test_verification"):
        monkeypatch.setenv("GITHUB_STEP_SUMMARY", str(tmp_path / "job-summary"))
