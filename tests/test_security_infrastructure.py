from pathlib import Path
import re

ROOT = Path(__file__).resolve().parents[1]


def test_postgres_broad_firewall_requires_explicit_opt_in() -> None:
    # Given the deployment template; When its firewall policy is inspected; Then deny is default.
    template = (ROOT / "infra/main.bicep").read_text()
    assert re.search(r"param allowAllAzureServicesToPostgres bool = false\b", template)
    assert re.search(
        r"resource postgresAllowAzure .* = if "
        r"\(enablePostgres && allowAllAzureServicesToPostgres\)",
        template,
    )


def test_e2e_explicitly_opts_in_and_masks_generated_credentials() -> None:
    # Given ephemeral CI deployment; When shell commands are inspected; Then opt-in is explicit.
    workflow = (ROOT / ".github/workflows/e2e-azure.yml").read_text()
    assert "allowAllAzureServicesToPostgres=true" in workflow
    assert 'PW="Pg$(openssl rand -hex 16)Az9"' in workflow
    assert workflow.index('echo "::add-mask::$PW"') < workflow.index('echo "password=$PW"')
    assert workflow.index('echo "::add-mask::$URL"') < workflow.index(
        '--settings E2E_DB_URL="$URL"'
    )
    assert "GITHUB_RUN_ID}${GITHUB_RUN_ATTEMPT}" in workflow
    assert "set -x" not in workflow
    for line in workflow.splitlines():
        if "echo " in line and any(token in line for token in ("$PW", "$URL")):
            assert "::add-mask::" in line or '>> "$GITHUB_OUTPUT"' in line
    assert "AccountKey=" not in workflow


def test_cleanup_runs_after_failure_without_swallowing_delete_errors() -> None:
    # Given a separate cleanup job; When deployment fails; Then deletion is still requested.
    workflow = (ROOT / ".github/workflows/e2e-azure.yml").read_text()
    cleanup = workflow.split("\n  cleanup:\n", 1)[1]
    assert "if: always() && needs.check_secrets.outputs.has_azure == 'true'" in cleanup
    assert "needs: [check_secrets, deploy_and_test]" in cleanup
    assert 'az group delete --name "$RG" --yes --no-wait' in cleanup
    assert "|| true" not in cleanup
    assert "continue-on-error:" not in cleanup


def test_e2e_artifacts_are_limited_to_reports_and_certification() -> None:
    # Given diagnostic uploads; When their paths are inspected; Then no settings dump is uploaded.
    workflow = (ROOT / ".github/workflows/e2e-azure.yml").read_text()
    paths = re.findall(r"^          path: (.+)$", workflow, re.MULTILINE)
    assert set(paths) == {"candidate-dist", "cert/", "e2e-report/"}
    assert "appsettings list" not in workflow
    assert "listKeys" not in workflow
    assert not re.search(r"cat .*?(?:\.env|appsettings|deployment\.json)", workflow)
