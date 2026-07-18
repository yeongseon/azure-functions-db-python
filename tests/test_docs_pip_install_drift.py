"""Guards against drift between the documented ``pip install`` token and the
canonical distribution name declared in ``pyproject.toml``.

The package was renamed once already (dropping a ``-python`` suffix), and the
install command is duplicated across the English README, three i18n READMEs,
and the ``docs/`` tree. A stale token there sends users to a non-existent or
wrong distribution. This test fails CI whenever any ``pip install`` reference to
*this* package (any ``azure[-_]functions[-_]db*`` spelling) does not resolve to
the canonical ``project.name``.

Third-party ``pip install`` lines (pytest, build, ...) are intentionally
ignored — only self-references are asserted.
"""

from __future__ import annotations

from pathlib import Path
import re

_NAME_RE = re.compile(r'^name\s*=\s*["\'](?P<name>[^"\']+)["\']', re.MULTILINE)

_REPO_ROOT = Path(__file__).resolve().parent.parent
_PYPROJECT = _REPO_ROOT / "pyproject.toml"

# Any spelling that refers to THIS package, e.g.
#   azure-functions-db, azure_functions_db, azure-functions-db-python
# optionally followed by an extras group like [postgres] or [postgres,mysql].
_SELF_TOKEN_RE = re.compile(
    r"pip install\s+"
    r"(?P<name>azure[-_]functions[-_]db[-_a-z]*)"
    r"(?P<extras>\[[a-z0-9,\-_ ]*\])?",
    re.IGNORECASE,
)


def _canonical_name() -> str:
    text = _PYPROJECT.read_text(encoding="utf-8")
    match = _NAME_RE.search(text)
    assert match is not None, "could not find project name in pyproject.toml"
    return match.group("name")


def _doc_files() -> list[Path]:
    files = sorted(_REPO_ROOT.glob("README*.md"))
    files += sorted((_REPO_ROOT / "docs").rglob("*.md"))
    return files


def test_pip_install_tokens_match_canonical_name() -> None:
    canonical = _canonical_name()
    offenders: list[str] = []

    for path in _doc_files():
        text = path.read_text(encoding="utf-8")
        for match in _SELF_TOKEN_RE.finditer(text):
            name = match.group("name")
            if name != canonical:
                rel = path.relative_to(_REPO_ROOT)
                offenders.append(f"{rel}: '{name}' (expected '{canonical}')")

    assert not offenders, (
        "Documented `pip install` token(s) drifted from the canonical "
        f"pyproject project.name '{canonical}':\n  " + "\n  ".join(offenders)
    )


def test_guard_sees_at_least_one_self_reference() -> None:
    # Sanity check: if the docs stop mentioning the install command entirely,
    # the drift guard above would pass vacuously — catch that here.
    canonical = _canonical_name()
    found = any(
        match.group("name") == canonical
        for path in _doc_files()
        for match in _SELF_TOKEN_RE.finditer(path.read_text(encoding="utf-8"))
    )
    assert found, "No `pip install azure-functions-db` reference found in docs/READMEs."
