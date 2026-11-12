"""Service-free checks for the managed-identity example guide.

The guide documents ``examples/managed-identity-mssql``. These checks read files only: no Azure
resource, credential or database is needed, and they say nothing about whether a real managed
identity works.
"""

from __future__ import annotations

from pathlib import Path
import re

_REPO_ROOT = Path(__file__).resolve().parent.parent
_GUIDE = _REPO_ROOT / "docs" / "examples" / "managed_identity_mssql.md"
_EXAMPLE = _REPO_ROOT / "examples" / "managed-identity-mssql"


def _guide_text() -> str:
    return _GUIDE.read_text(encoding="utf-8")


def test_guide_is_in_the_site_navigation() -> None:
    mkdocs = (_REPO_ROOT / "mkdocs.yml").read_text(encoding="utf-8")

    assert "examples/managed_identity_mssql.md" in mkdocs


def test_example_files_named_in_the_guide_exist() -> None:
    named = set(re.findall(r"`(examples/managed-identity-mssql/[\w./-]+)`", _guide_text()))

    assert named, "the guide names no file of the example"
    missing = sorted(path for path in named if not (_REPO_ROOT / path).exists())
    assert missing == [], f"the guide names files that do not exist: {missing}"


def test_example_directory_link_points_at_the_example() -> None:
    assert _EXAMPLE.is_dir()
    assert "/tree/main/examples/managed-identity-mssql)" in _guide_text()


def test_relative_links_resolve() -> None:
    links = re.findall(r"\]\((?!https?://|#)([^)#]+)", _guide_text())

    broken = sorted(link for link in links if not (_GUIDE.parent / link).resolve().exists())
    assert broken == [], f"relative links that do not resolve: {broken}"


def test_guide_does_not_ask_for_a_sql_password() -> None:
    text = _guide_text().lower()

    assert "password=" not in text
    assert "pwd=" not in text
