"""Keep the troubleshooting guide's normalizer migration entry executable."""

from pathlib import Path
import re

import pytest

from azure_functions_db import ConfigurationError
from azure_functions_db.core.types import SourceDescriptor
from azure_functions_db.trigger import default_normalizer

ROOT = Path(__file__).resolve().parents[1]
DOC = ROOT / "docs" / "troubleshooting.md"
HEADING = "### ConfigurationError: `default_normalizer requires source cursor metadata`"


def _section() -> str:
    text = DOC.read_text()
    start = text.index(HEADING)
    end = text.index("\n### ", start + len(HEADING))
    return text[start:end]


def test_documented_failure_matches_runtime_error() -> None:
    descriptor = SourceDescriptor(name="orders", kind="sqlalchemy", fingerprint="demo")
    with pytest.raises(ConfigurationError, match="default_normalizer requires source cursor"):
        default_normalizer({"id": 1}, descriptor)


def test_documented_make_normalizer_fix_executes() -> None:
    blocks = re.findall(r"```python\n(.*?)```", _section(), re.S)
    assert len(blocks) == 1
    namespace: dict[str, object] = {}
    exec(compile(blocks[0], str(DOC), "exec"), namespace)
    event = namespace["event"]
    assert getattr(event, "cursor") == (100, 1, 7)
    assert getattr(event, "pk") == {"tenant_id": 1, "id": 7}


def test_entry_links_migration_guide_and_states_guarantee() -> None:
    section = _section()
    assert "(33-default-normalizer-migration.md)" in section
    assert (ROOT / "docs" / "33-default-normalizer-migration.md").is_file()
    assert "at-least-once" in section
    assert "not exactly-once" in section
