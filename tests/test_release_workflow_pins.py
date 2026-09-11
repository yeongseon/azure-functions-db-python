"""Regression guard for the release-gate drift-lint.

Exercises tools/lint_release_workflows.py against this repo (must be clean) and
against synthetic drift (must be caught). Keeps the vendored lint honest.

Family-agnostic: the runtime-tier assertions are derived from the lint's own
``REQUIRED_RUNTIME_TIERS`` config, so this same test file is valid across the
cookbook / runtime-gate / minimal repo families.
"""

from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parent.parent
_LINT_PATH = _REPO_ROOT / "tools" / "lint_release_workflows.py"

_spec = importlib.util.spec_from_file_location("lint_release_workflows", _LINT_PATH)
assert _spec and _spec.loader
lint_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(lint_mod)


def _needs(*extra: str) -> str:
    """Build a ``publish`` job stub with the universal needs plus ``extra``."""
    items = list(lint_mod.UNIVERSAL_REQUIRED_NEEDS) + list(extra)
    return f"  publish:\n    needs: [{', '.join(items)}]\n"


def test_repo_release_workflows_are_clean() -> None:
    """The committed gate workflows must satisfy the canonical pins + gate."""
    assert lint_mod.lint() == []


def test_detects_non_canonical_pin() -> None:
    text = "      - uses: actions/checkout@" + "0" * 40 + " # v7.0.1\n"
    errors = lint_mod.check_pins(text, "fake.yml")
    assert errors and "expected canonical" in errors[0]


def test_detects_annotation_drift() -> None:
    sha, _ = lint_mod.CANONICAL_ACTIONS["actions/checkout"]
    text = f"      - uses: actions/checkout@{sha} # v6\n"
    errors = lint_mod.check_pins(text, "fake.yml")
    assert errors and "annotation" in errors[0]


def test_detects_unpinned_tag() -> None:
    text = "      - uses: actions/checkout@v7.0.1\n"
    errors = lint_mod.check_pins(text, "fake.yml")
    assert errors and "not pinned to a 40-hex SHA" in errors[0]


def test_detects_missing_verify_azure_certification() -> None:
    # Drop verify-azure-certification while keeping build + lib-tests + tiers.
    tiers = ", ".join(lint_mod.REQUIRED_RUNTIME_TIERS)
    joined = "build, lib-tests" + (f", {tiers}" if tiers else "")
    text = f"  publish:\n    needs: [{joined}]\n"
    errors = lint_mod.check_publish_needs(text, "publish-pypi.yml")
    assert any("verify-azure-certification" in e for e in errors)


def test_detects_regressed_needs() -> None:
    text = "  publish:\n    needs: [build, lib-tests]\n"
    errors = lint_mod.check_publish_needs(text, "publish-pypi.yml")
    assert any("regressed to build+lib-tests" in e for e in errors)


@pytest.mark.skipif(
    not lint_mod.REQUIRED_RUNTIME_TIERS,
    reason="minimal family has no required runtime tier",
)
def test_detects_missing_runtime_tier() -> None:
    # Universal needs present but the family-required runtime tier missing.
    text = _needs()
    errors = lint_mod.check_publish_needs(text, "publish-pypi.yml")
    assert any("runtime tier" in e for e in errors)


def test_parses_block_style_needs() -> None:
    all_needs = list(lint_mod.UNIVERSAL_REQUIRED_NEEDS) + list(lint_mod.REQUIRED_RUNTIME_TIERS)
    body = "  publish:\n    needs:\n"
    for item in all_needs:
        body += f"      - {item}\n"
    body += "    runs-on: ubuntu-latest\n"
    assert lint_mod.check_publish_needs(body, "publish-pypi.yml") == []


# --- e2e-azure certification-artifact guards -------------------------------
#
# publish-pypi.yml's verify-azure-certification gate downloads the `azure-cert`
# artifact and asserts cert/certification.json matches the release commit +
# version. Nothing else guards that e2e-azure.yml actually PRODUCES that
# artifact, so a well-meaning edit could silently break every release. These
# text-based checks (dependency-free, mirroring the lint's style) keep the
# producer and consumer in lockstep.

_E2E_AZURE = _REPO_ROOT / ".github" / "workflows" / "e2e-azure.yml"


def _e2e_azure_text() -> str:
    return _E2E_AZURE.read_text(encoding="utf-8")


def test_e2e_azure_uploads_azure_cert_artifact() -> None:
    """The cert consumer (publish gate) needs e2e-azure to upload `azure-cert`."""
    text = _e2e_azure_text()
    assert "name: azure-cert" in text, (
        "e2e-azure.yml must upload an `azure-cert` artifact; "
        "publish-pypi.yml's verify-azure-certification gate depends on it."
    )
    assert "cert/certification.json" in text, (
        "e2e-azure.yml must write cert/certification.json (the record the "
        "publish gate parses)."
    )


def test_e2e_azure_exposes_ref_and_version_inputs() -> None:
    """AGENTS.md dispatches `-f ref=... -f version=...`; the inputs must exist."""
    text = _e2e_azure_text()
    for token in ("workflow_dispatch:", "      ref:", "      version:"):
        assert token in text, f"e2e-azure.yml is missing dispatch input marker: {token!r}"


def test_e2e_azure_certification_records_required_fields() -> None:
    """The record must carry the fields the publish gate asserts on."""
    text = _e2e_azure_text()
    for field in ('"commit":', '"version":', '"result":'):
        assert field in text, (
            f"cert/certification.json is missing required field {field!r} "
            "asserted by publish-pypi.yml verify-azure-certification."
        )


def test_e2e_azure_bicep_template_exists() -> None:
    """The deploy step runs `az deployment group create --template-file
    infra/main.bicep`; that file must exist and be referenced, or the real-Azure
    deploy (and thus certification) fails before it can produce `azure-cert`."""
    text = _e2e_azure_text()
    assert "--template-file infra/main.bicep" in text, (
        "e2e-azure.yml must deploy infra via `--template-file infra/main.bicep`."
    )
    bicep = _REPO_ROOT / "infra" / "main.bicep"
    assert bicep.is_file(), (
        "infra/main.bicep is referenced by e2e-azure.yml's Deploy infra step "
        "but is missing; the real-Azure deploy cannot run without it."
    )
