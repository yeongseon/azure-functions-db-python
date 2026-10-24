"""The broad PostgreSQL ``0.0.0.0`` firewall rule must stay an explicit, test-only opt-in."""

from pathlib import Path
import re
import shlex

import yaml

_ROOT = Path(__file__).resolve().parents[1]
_BICEP = _ROOT / "infra" / "main.bicep"
_WORKFLOW = _ROOT / ".github" / "workflows" / "e2e-azure.yml"


def test_broad_firewall_rule_is_off_by_default_and_needs_both_flags() -> None:
    # Given the deployment template; When its firewall policy is read; Then deny is the default.
    bicep = _BICEP.read_text(encoding="utf-8")
    assert re.search(r"^param allowAllAzureServicesToPostgres bool = false$", bicep, re.MULTILINE)
    assert re.search(
        r"^resource postgresAllowAzure \S+ = "
        r"if \(enablePostgres && allowAllAzureServicesToPostgres\) \{$",
        bicep,
        re.MULTILINE,
    )


def test_e2e_workflow_opts_in_explicitly_for_its_ephemeral_resource_group() -> None:
    # Given the e2e deployment step; When its az command is parsed; Then the opt-in is an argument.
    workflow = yaml.safe_load(_WORKFLOW.read_text(encoding="utf-8"))
    steps = workflow["jobs"]["deploy_and_test"]["steps"]
    (deploy,) = [s for s in steps if "--template-file infra/main.bicep" in s.get("run", "")]
    words = shlex.split(deploy["run"].replace("\\\n", " "), comments=True)
    assert "allowAllAzureServicesToPostgres=true" in words
