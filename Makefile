VENV_DIR := .venv
PYTHON ?= python
HATCH ?= hatch
ifeq ($(CI),)
RUN := $(HATCH) run python
else
RUN := $(PYTHON)
endif

.PHONY: bootstrap ensure-hatch install install-ci
bootstrap ensure-hatch:
	@command -v $(HATCH) >/dev/null || { echo "Install Hatch in an isolated tool environment first"; exit 1; }
install: ensure-hatch
	@$(HATCH) env create
	@if [ -z "$$CI" ]; then $(MAKE) precommit-install; fi
install-ci:
	@$(PYTHON) -c 'import sys; assert (3, 11) <= sys.version_info[:2] < (3, 15), sys.version'
	@$(PYTHON) -m pip install -e ".[dev,docs,all]"

.PHONY: shell reset hatch-clean precommit precommit-install
shell: ensure-hatch
	@$(HATCH) shell
reset: clean-all install
hatch-clean: ensure-hatch
	@$(HATCH) env remove
precommit: ensure-hatch
	@$(HATCH) run precommit
precommit-install: ensure-hatch
	@$(HATCH) run precommit-install

.PHONY: format format-check format-check-changed style typecheck lint security lint-workflows check check-all
format:
	@$(RUN) -m ruff format src tests tools examples benchmarks
format-check:
	@$(RUN) -m ruff format --check src tests tools examples benchmarks
format-check-changed:
	@$(RUN) tools/check_pr_format.py "$(BASE)" "$(HEAD)"
style: format-check
	@$(RUN) -m ruff check src tests tools examples benchmarks
typecheck:
	@$(RUN) -m mypy src tests tools/verification*.py
lint: style typecheck
security:
	@$(RUN) -m bandit -r src
lint-workflows:
	@$(RUN) tools/lint_release_workflows.py
	@$(RUN) tools/lint_workflow_pins.py
	@$(RUN) tools/lint_hatch_matrix.py
check: style typecheck security lint-workflows
check-all: verify

.PHONY: test integration integration-local verify report doctor clean
test integration integration-local verify report doctor clean:
	@$(RUN) -m tools.verification $@

.PHONY: cov e2e-azure host-smoke wheel-smoke build docs docs-serve
cov:
	@$(RUN) -m tools.verification test --cov-report=html
e2e-azure:
	@$(RUN) -m tools.verification azure-e2e
host-smoke:
	@$(RUN) -m tools.verification focused host-smoke -m tools.verification_host
wheel-smoke:
	@$(RUN) -m tools.verification focused wheel-smoke -m tools.verification_wheel
build:
	@$(RUN) -m tools.verification focused build -m build --no-isolation
docs:
	@$(RUN) -m tools.verification focused docs -m mkdocs build --strict
docs-serve:
	@$(RUN) -m mkdocs serve

.PHONY: publish-test version clean-all help
publish-test: ensure-hatch
	@$(HATCH) publish --repo test
version: ensure-hatch
	@$(HATCH) version
clean-all: clean
	@find . -type d -name "__pycache__" -not -path "./.git/*" -exec rm -rf {} +
	@find . -type f \( -name "*.pyc" -o -name "*.pyo" \) -not -path "./.git/*" -delete
	@rm -rf .DS_Store $(VENV_DIR)
help:
	@echo "Canonical: install install-ci doctor format format-check check test integration integration-local verify e2e-azure report clean"
	@echo "check-all: exact alias of verify; DB_BACKEND selects PARTIAL integration only"
	@echo "Focused: lint style typecheck security lint-workflows cov host-smoke wheel-smoke build docs docs-serve"
	@echo "Maintenance: bootstrap ensure-hatch shell reset hatch-clean clean-all precommit precommit-install publish-test version"
