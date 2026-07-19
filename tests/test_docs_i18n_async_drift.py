"""Guards against drift between the English README's async-handler rejection
wording and its translated counterparts in the i18n READMEs.

The English README documents an important behavioral contract: ``@db.trigger``
rejects async handlers at decoration time by raising ``ConfigurationError``, and
``PollTrigger.run()`` additionally raises ``TypeError`` as a defensive runtime
guard. That contract is asserted in code by
``tests/test_decorator.py::test_trigger_async_handler_rejected`` (the
``ConfigurationError`` path) and
``tests/test_poll_trigger.py::test_run_rejects_async_handler`` /
``test_async_handler_callable_object`` (the ``TypeError`` path).

The translated READMEs must not silently omit this exception note. This test
fails CI whenever an i18n README stops mentioning both documented exception
types near an async-handler discussion.
"""

from __future__ import annotations

from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent

# Translated READMEs that must carry the async-rejection wording.
_I18N_READMES = ("README.ko.md", "README.ja.md", "README.zh-CN.md")

# The documented exception types are code identifiers, so they appear verbatim
# in every language variant.
_REQUIRED_TOKENS = ("ConfigurationError", "TypeError", "@db.trigger")


def test_i18n_readmes_document_async_rejection() -> None:
    offenders: list[str] = []

    for name in _I18N_READMES:
        path = _REPO_ROOT / name
        assert path.exists(), f"missing i18n README: {name}"
        text = path.read_text(encoding="utf-8")
        missing = [token for token in _REQUIRED_TOKENS if token not in text]
        if missing:
            offenders.append(f"{name}: missing {missing}")

    assert not offenders, (
        "i18n README(s) dropped the async-handler rejection wording "
        "(ConfigurationError at decoration, TypeError from PollTrigger.run):\n  "
        + "\n  ".join(offenders)
    )


def test_english_readme_documents_async_rejection() -> None:
    # Sanity check: the guard above is only meaningful if the English source of
    # truth still carries the wording it expects translations to mirror.
    text = (_REPO_ROOT / "README.md").read_text(encoding="utf-8")
    for token in _REQUIRED_TOKENS:
        assert token in text, f"English README missing async-rejection token: {token}"
