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

from tools.verification_encoding import redact_encoded
from tools.verification_html import static_report

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


_REDACTED = "[REDACTED]"
_QUOTED = r"\"(?:[^\"\\]|\\.)*\"|'(?:[^'\\]|\\.)*'"
_AUTHORIZATION = re.compile(
    r"(?im)(?P<key>(?:proxy-)?authorization[\"']?\s*[:=]\s*)(?P<value>" + _QUOTED + r"|[^\r\n\"<]+)"
)
_URL_CREDENTIALS = re.compile(r"([\w+.-]+://)(?P<value>[^/\s<>\"']*)@")
_SECRET_FIELD = re.compile(
    r"(?i)(?<![\w-])(?P<key>(?:[\w.-]*(?:token|password|passwd|secret|api[_-]?key|"
    r"connection[_-]?string)|AccountKey|SharedAccessKey|SharedAccessSignature|"
    r"AzureWebJobsStorage|InstrumentationKey|x-functions-key|sas)"
    r"[\"']?\s*[:=]\s*)(?P<value>" + _QUOTED + r"|[^\s;&<>\"',]+)"
)
_QUERY_CODE = re.compile(r"(?i)(?P<key>[?&]code=)(?P<value>" + _QUOTED + r"|[^\s;&<>\"'\]\)]+)")
_SIGNATURE = re.compile(
    r"(?i)(?<![\w-])(?P<key>sig[\"']?\s*[:=]\s*)(?P<value>"
    + _QUOTED
    + r"|(?=[A-Za-z0-9_+/=%.-]{8,}(?:[^\w+/=%.-]|$))"
    r"(?=[A-Za-z0-9_+/=%.-]*[A-Za-z])[A-Za-z0-9_+/=%.-]+)"
)
_FUNCTION_KEY = re.compile(
    r"(?i)(?<![\w-])(?:code|key|x-functions-key|_?master|default|function|host|secret|token)"
    r"[\"']?\s*[:=]\s*[\"']?(?P<value>[A-Za-z0-9_-]{40,64}={0,2})(?![A-Za-z0-9_-])"
)
_SECRET_ENV_NAME = re.compile(
    r"(?i)secret|token|password|conn|_URL$|string|accountkey|functions.key|api.?key|sas"
)
_SENSITIVE_NAME = re.compile(
    r"(?i)^(?:(?:proxy-)?authorization|[\w.-]*(?:password|passwd|secret|token|api[_-]?key|"
    r"connection[_-]?string)|accountkey|sharedaccesskey|sharedaccesssignature|"
    r"azurewebjobsstorage|instrumentationkey|sig|sas|code|x-functions-key|(?:set-)?cookie)$"
)
_TEXT_SUFFIXES = frozenset({".log", ".txt", ".xml", ".html", ".json", ".md", ".out"})


def _mask(match: re.Match[str]) -> str:
    value = match.group("value")
    quote = value[0] if value[:1] in {'"', "'"} else ""
    return f"{match.group('key')}{quote}{_REDACTED}{quote}"


def _known_secrets() -> list[str]:
    values = {
        value
        for key, value in os.environ.items()
        if len(value) >= 6 and _SECRET_ENV_NAME.search(key)
    }
    escaped = {json.dumps(value)[1:-1] for value in values}
    return sorted(values | escaped, key=len, reverse=True)


def redact(text: str) -> str:
    """Mask complete known secrets before consuming quoted credential values."""
    for value in _known_secrets():
        text = text.replace(value, _REDACTED)
    text = redact_encoded(
        text,
        (_AUTHORIZATION, _URL_CREDENTIALS, _SECRET_FIELD, _QUERY_CODE, _SIGNATURE, _FUNCTION_KEY),
        _known_secrets(),
    )
    text = _AUTHORIZATION.sub(_mask, text)
    text = _URL_CREDENTIALS.sub(r"\1" + _REDACTED + "@", text)
    text = _QUERY_CODE.sub(_mask, text)
    text = _SIGNATURE.sub(_mask, text)
    return _SECRET_FIELD.sub(_mask, text)


def redact_json(value: object) -> object:
    """Redact string leaves before serialization to preserve JSON syntax."""
    if isinstance(value, str):
        return redact(value)
    if isinstance(value, (list, tuple)):
        return [redact_json(item) for item in value]
    if isinstance(value, dict):
        return {
            redact(str(key)): _REDACTED
            if _SENSITIVE_NAME.fullmatch(str(key))
            else redact_json(item)
            for key, item in value.items()
        }
    return value


def dump_json(value: object) -> str:
    return json.dumps(redact_json(value), indent=2) + "\n"


def _redact_file_text(suffix: str, text: str) -> str:
    if suffix == ".xml":
        if "<!ENTITY" in text.upper():
            return "<redacted-report>Entity declarations refused</redacted-report>"
        try:
            root = ET.fromstring(text)
        except ET.ParseError:
            return "<redacted-report>Malformed XML discarded</redacted-report>"
        for element in root.iter():
            element.text = redact(element.text) if element.text else element.text
            element.tail = redact(element.tail) if element.tail else element.tail
            sensitive_property = _SENSITIVE_NAME.fullmatch(element.get("name", ""))
            for key, value in element.attrib.items():
                element.set(
                    key,
                    _REDACTED
                    if _SENSITIVE_NAME.fullmatch(key) or (key == "value" and sensitive_property)
                    else redact(value),
                )
        return ET.tostring(root, encoding="unicode")
    if suffix == ".html":
        # A static escaped view retains the report without executing embedded scripts or
        # reintroducing credentials through HTML attributes or pytest-html's JSON payload.
        return static_report(text, lambda value: _redact_file_text(".json", value))
    if suffix == ".json":
        try:
            return dump_json(json.loads(text))
        except ValueError:
            return redact(text)
    return redact(text)


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
        if path.is_symlink() or not path.is_file() or path.suffix not in _TEXT_SUFFIXES:
            continue
        try:
            original = path.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            continue
        path.write_text(_redact_file_text(path.suffix, original), encoding="utf-8")
    (directory / "environment.json").write_text(dump_json(environment()))
    summary = replace(
        summary,
        supporting_files=tuple(
            str(path)
            for path in sorted(directory.rglob("*"))
            if not path.is_symlink() and path.is_file() and path.name != "summary.json"
        ),
    )
    serialized = dump_json(asdict(summary))
    (directory / "summary.json").write_text(serialized + "\n", encoding="utf-8")
    return Summary(**json.loads(serialized))


def report(root: Path) -> int:
    paths = sorted(root.rglob("summary.json"))
    for path in paths:
        publish(Summary(**json.loads(path.read_text())))
    if not paths:
        print("No saved verification evidence")
    return 0 if paths else 1
