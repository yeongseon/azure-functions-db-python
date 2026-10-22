from __future__ import annotations

from dataclasses import asdict, dataclass, replace
from importlib.metadata import PackageNotFoundError, version
import json
import os
from pathlib import Path
import platform
import re
import subprocess
from typing import Literal
import xml.etree.ElementTree as ET

Status = Literal["PASS", "FAIL", "INCOMPLETE", "NOT_APPLICABLE"]


def read_xml_root(path: Path) -> ET.Element:
    """Parse a locally generated report, refusing entity declarations."""
    data = path.read_bytes()
    if b"<!ENTITY" in data.upper():
        raise ValueError("XML reports must not declare entities")
    return ET.fromstring(data)


@dataclass(frozen=True, slots=True)
class Summary:
    schema_version: int
    lane: str
    command: tuple[str, ...]
    selected: int
    passed: int
    failed: int
    skipped: int
    status: Status
    exit_code: int
    required: bool = True
    scope: str = "PARTIAL"
    supporting_files: tuple[str, ...] = ()
    reason: str = ""
    backend: str = ""
    service_version: str = ""
    driver: str = ""
    readiness: str = "not checked"
    cleanup: str = "not owned"


def redact(text: str) -> str:
    text = re.sub(r"([\w+.-]+://)[^/\s<>\"']*@", r"\1[REDACTED]@", text)
    text = re.sub(r"(?i)(DefaultEndpointsProtocol=)[^\s<\"']+", r"\1[REDACTED]", text)
    text = re.sub(
        r"(?i)(?<![\w-])((?:[\w-]*(?:password|secret|token)|AccountKey|SharedAccessSignature|"
        r"AzureWebJobsStorage|Authorization|sig|sas|code|api[_-]?key)"
        r"[\"']?\s*[:=]\s*[\"']?)[^\s;&<\"']+",
        r"\1[REDACTED]",
        text,
    )
    for key, value in os.environ.items():
        if len(value) >= 6 and re.search(r"(?i)secret|token|password|conn|_URL$|accountkey", key):
            text = text.replace(value, "[REDACTED]")
    return text


def environment() -> dict[str, str]:
    metadata = {"python": platform.python_version(), "platform": platform.platform()}
    for package in (
        "azure-functions-db",
        "azure-functions",
        "sqlalchemy",
        "azure-storage-blob",
        "psycopg",
        "pymysql",
        "pyodbc",
    ):
        try:
            metadata[package] = version(package)
        except PackageNotFoundError:
            metadata[package] = "not installed"
    result = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        capture_output=True,
        text=True,
        timeout=10,
        check=False,
    )
    metadata["commit"] = result.stdout.strip() if result.returncode == 0 else "unknown"
    for key in ("GITHUB_RUN_ID", "GITHUB_SHA", "EXPECTED_VERSION"):
        if key in os.environ:
            metadata[key] = redact(os.environ[key])
    return metadata


def collect(directory: Path, command: list[str], exit_code: int) -> Summary:
    cases: list[ET.Element] = []
    for path in directory.rglob("*junit.xml"):
        cases.extend(read_xml_root(path).iter("testcase"))
    skipped = sum(case.find("skipped") is not None for case in cases)
    failed = sum(
        case.find("failure") is not None or case.find("error") is not None for case in cases
    )
    passed = len(cases) - skipped - failed
    status: Status = "PASS"
    reason = ""
    if failed or exit_code not in (0, 5):
        status, reason = "FAIL", "Test/process failure, collection error, interruption or timeout"
    elif not cases or not passed or skipped:
        status, reason = "INCOMPLETE", "Required selection is empty, all skipped or contains skips"
    elif exit_code:
        status, reason = "FAIL", "Nonzero pytest exit"
    return Summary(
        1,
        directory.name,
        tuple(command),
        len(cases),
        passed,
        failed,
        skipped,
        status,
        exit_code or int(status != "PASS"),
        reason=reason,
    )


def publish(summary: Summary) -> None:
    text = redact(
        f"{summary.lane}: {summary.status} | scope={summary.scope} selected={summary.selected} "
        f"passed={summary.passed} failed={summary.failed} skipped={summary.skipped} "
        f"exit={summary.exit_code} | {summary.reason}"
    )
    print(text)
    if destination := os.environ.get("GITHUB_STEP_SUMMARY"):
        with Path(destination).open("a", encoding="utf-8") as stream:
            stream.write(f"\n- {text}\n")


def save(directory: Path, summary: Summary) -> Summary:
    directory.mkdir(parents=True, exist_ok=True)
    for path in directory.rglob("*"):
        if path.is_file():
            path.write_text(redact(path.read_text(encoding="utf-8")), encoding="utf-8")
    (directory / "environment.json").write_text(json.dumps(environment(), indent=2) + "\n")
    summary = replace(
        summary,
        supporting_files=tuple(
            str(path)
            for path in sorted(directory.rglob("*"))
            if path.is_file() and path.name != "summary.json"
        ),
    )
    serialized = redact(json.dumps(asdict(summary), indent=2))
    (directory / "summary.json").write_text(serialized + "\n", encoding="utf-8")
    return Summary(**json.loads(serialized))


def report(root: Path) -> int:
    paths = sorted(root.rglob("summary.json"))
    for path in paths:
        publish(Summary(**json.loads(path.read_text())))
    if not paths:
        print("No saved verification evidence")
    return 0 if paths else 1
