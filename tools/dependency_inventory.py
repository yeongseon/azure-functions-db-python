"""Verify candidate identity and freeze only its installed third-party closure."""

from email.parser import Parser
from importlib.metadata import distributions
from pathlib import Path
import re
from zipfile import ZipFile

from tools.security_report import EvidenceError


def canonical(name: str) -> str:
    return re.sub(r"[-_.]+", "-", name.lower())


def wheel_identity(wheel: Path) -> tuple[str, str]:
    with ZipFile(wheel) as archive:
        entries = [name for name in archive.namelist() if name.endswith(".dist-info/METADATA")]
        if len(entries) != 1:
            raise EvidenceError("candidate metadata missing or ambiguous")
        metadata = Parser().parsestr(archive.read(entries[0]).decode("utf-8"))
    name, version = metadata.get("Name"), metadata.get("Version")
    if not name or not version:
        raise EvidenceError("candidate identity missing")
    return canonical(name), version


def freeze_inventory(site: Path, candidate: tuple[str, str]) -> dict[str, str]:
    installed: dict[str, str] = {}
    for distribution in distributions(path=[str(site)]):
        name = canonical(distribution.metadata["Name"])
        version = distribution.version
        if not re.fullmatch(r"[a-z0-9][a-z0-9-]*", name) or not re.fullmatch(
            r"[A-Za-z0-9.+!-]+", version
        ):
            raise EvidenceError("invalid installed identity")
        if name in installed:
            raise EvidenceError("duplicate installed identity")
        installed[name] = version
    name, version = candidate
    name = canonical(name)
    if installed.get(name) != version:
        raise EvidenceError("candidate identity mismatch or absence")
    del installed[name]
    if not installed:
        raise EvidenceError("empty third-party inventory")
    return installed
