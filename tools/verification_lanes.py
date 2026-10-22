from __future__ import annotations

from contextlib import nullcontext
from dataclasses import replace
import json
import os
from pathlib import Path
import shutil
import sys
import tempfile
import xml.etree.ElementTree as ET

from tools.verification_evidence import Summary, collect, publish, redact, save
from tools.verification_process import run
from tools.verification_services import (
    MissingPrerequisite,
    ServiceFailure,
    azurite,
    configured,
    database,
    preflight,
)


def fresh(directory: Path) -> None:
    if directory.exists():
        shutil.rmtree(directory)
    (directory / "logs").mkdir(parents=True)


def backend_lane(backend: str, owned: bool = False, extra: tuple[str, ...] = ()) -> Summary:
    directory = Path("test-results/integration") / backend
    fresh(directory)
    target = (
        "tests/integration/test_checkpoint_store_live.py"
        if backend == "azurite"
        else "tests/integration/db"
    )
    command = [
        sys.executable,
        "-m",
        "pytest",
        target,
        "-o",
        "addopts=",
        "--no-cov",
        "-m",
        "integration",
        "-p",
        "tools.verification_selection",
        "--tb=short",
        *extra,
        "--junitxml=" + str(directory / "junit.xml"),
    ]
    service = (
        (azurite(directory) if backend == "azurite" else database(backend, directory))
        if owned
        else nullcontext()
    )
    code, reason, blocked = 0, "", False
    cleanup, readiness = "not owned", "not checked"
    try:
        with service, configured("VERIFICATION_DB_BACKEND", backend):
            preflight(backend, directory, wait=owned)
            readiness = "ready"
            with tempfile.TemporaryDirectory(prefix="db-junit-") as temporary:
                raw = Path(temporary) / "junit.xml"
                actual = [*command[:-1], "--junitxml=" + str(raw)]
                code = run(actual, directory / "logs/runner.log")
                if raw.exists():
                    (directory / "junit.xml").write_text(redact(raw.read_text()))
        cleanup = "complete" if owned else "external resources untouched"
    except MissingPrerequisite as error:
        blocked, code, reason = True, 2, str(error)
    except (ServiceFailure, OSError) as error:
        code, reason = 1, str(error)
    except KeyboardInterrupt:
        code, reason = 130, "Interrupted; owned resources cleaned"
    if owned and cleanup == "not owned":
        cleanup = "cleanup attempted; see logs"
    try:
        summary = collect(directory, command, code)
    except (ET.ParseError, ValueError) as error:
        summary = Summary(
            1,
            backend,
            tuple(command),
            0,
            0,
            0,
            0,
            "INCOMPLETE",
            2,
            reason=f"Invalid JUnit: {error}",
        )
    metadata = (
        json.loads((directory / "probe.json").read_text())
        if (directory / "probe.json").exists()
        else {}
    )
    if backend == "azurite" and (directory / "logs/version.log").exists():
        metadata["service_version"] = (directory / "logs/version.log").read_text().strip()
    summary = replace(
        summary,
        backend=backend,
        readiness=readiness,
        cleanup=cleanup,
        service_version=metadata.get("service_version", "unknown"),
        driver=metadata.get("driver", "unavailable"),
        status="INCOMPLETE" if blocked else summary.status,
        reason=reason or summary.reason,
    )
    summary = save(directory, summary)
    publish(summary)
    return summary


def execute(lane: str, extra: list[str]) -> Summary:
    directory = Path("test-results") / lane
    fresh(directory)
    command = [sys.executable, "-m", "pytest"]
    if lane == "test":
        command += [
            "tests",
            "-m",
            "not integration and not azurite and not host_e2e and not azure_e2e",
            "--cov-report=xml:" + str(directory / "coverage.xml"),
        ]
    else:
        command += ["tests/e2e/azure", "-m", "azure_e2e", "--no-cov"]
    command += [*extra, "--junitxml=" + str(directory / "junit.xml")]
    code, reason, blocked = 0, "", False
    try:
        if lane == "azure-e2e":
            missing = [
                key for key in ("E2E_BASE_URL", "EXPECTED_VERSION") if not os.environ.get(key)
            ]
            if missing:
                blocked, code, reason = True, 2, "Missing prerequisites: " + ", ".join(missing)
            else:
                code = run(
                    [sys.executable, "tools/check_deployed_version.py"],
                    directory / "logs/identity.log",
                )
        if not code:
            code = run(command, directory / "logs/runner.log")
    except KeyboardInterrupt:
        code, reason = 130, "Interrupted; owned processes terminated"
    try:
        summary = collect(directory, command, code)
    except (ET.ParseError, ValueError) as error:
        summary = Summary(
            1,
            lane,
            tuple(command),
            0,
            0,
            0,
            0,
            "INCOMPLETE",
            2,
            reason=f"Invalid evidence: {error}",
        )
    summary = save(
        directory,
        replace(
            summary,
            reason=reason or summary.reason,
            status="INCOMPLETE" if blocked else summary.status,
        ),
    )
    publish(summary)
    return summary


def focused(lane: str, command: list[str]) -> Summary:
    directory = Path("test-results") / lane
    fresh(directory)
    missing = (
        [tool for tool in ("func", "azurite") if shutil.which(tool) is None]
        if lane == "host-smoke"
        else []
    )
    code = 2 if missing else run(command, directory / "logs/runner.log", 1200)
    summary = Summary(
        1,
        lane,
        tuple(command),
        1,
        int(code == 0),
        int(code != 0),
        0,
        "INCOMPLETE" if missing else ("FAIL" if code else "PASS"),
        code,
        reason="Missing prerequisites: " + ", ".join(missing) if missing else "",
    )
    if lane == "host-smoke" and not missing:
        summary = collect(directory, command, code)
    summary = save(directory, summary)
    publish(summary)
    return summary
