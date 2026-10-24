VENV_DIR := .venv
PYTHON := $(VENV_DIR)/bin/python
PIP := $(VENV_DIR)/bin/pip
HATCH := $(VENV_DIR)/bin/hatch

.PHONY: bootstrap
bootstrap:
	@if [ ! -d "$(VENV_DIR)" ]; then \
		echo "Creating virtual environment..."; \
		python3 -m venv $(VENV_DIR); \
	fi
	@echo "Ensuring Hatch is installed in virtual environment..."
	@$(PIP) install --upgrade pip > /dev/null
	@$(PIP) install hatch > /dev/null
	@echo "Hatch installed at $(HATCH)"

.PHONY: ensure-hatch
ensure-hatch: bootstrap

.PHONY: install
install: ensure-hatch
	@$(HATCH) env create
	@if [ -n "$$CI" ]; then \
		echo "CI detected: skipping pre-commit hook installation"; \
	else \
		$(MAKE) precommit-install; \
	fi

.PHONY: shell
shell: ensure-hatch
	@$(HATCH) shell

.PHONY: reset
reset: clean-all install
	@echo "Project reset complete."

.PHONY: hatch-clean
hatch-clean: ensure-hatch
	@$(HATCH) env remove || echo "No hatch environment to remove"

.PHONY: format
format: ensure-hatch
	@$(HATCH) run format

.PHONY: format-check
format-check: ensure-hatch
	@$(HATCH) run format-check

.PHONY: format-check-changed
format-check-changed: ensure-hatch
	@$(HATCH) run python tools/check_pr_format.py "$(BASE)" "$(HEAD)"

.PHONY: style
style: ensure-hatch
	@$(HATCH) run style

.PHONY: typecheck
typecheck: ensure-hatch
	@$(HATCH) run typecheck

.PHONY: lint
lint: ensure-hatch
	@$(HATCH) run lint

.PHONY: security
security: ensure-hatch
	@$(HATCH) run security

# Release gate: audit the third-party runtime closure of the exact candidate wheel.
# Independent of Hatch on purpose: each CI matrix cell audits with its own interpreter.
# Usage: make audit-wheel WHEEL=dist/<wheel>.whl [EXTRA=all] [REPORT=report.json]
AUDIT_VENV ?= .audit-venv
EXTRA ?= all
PIP_AUDIT_SPEC ?= pip-audit==2.10.1
IGNORE_FILE ?= security/pip-audit-ignore.txt
.PHONY: audit-wheel
audit-wheel:
	@test -f "$(WHEEL)" || { echo "WHEEL=<path to candidate wheel> is required" >&2; exit 2; }
	@rm -rf $(AUDIT_VENV) && mkdir -p $(AUDIT_VENV) && python -m venv --without-pip $(AUDIT_VENV)/target && python -m venv $(AUDIT_VENV)/tool
	@python -m pip --python $(AUDIT_VENV)/target/bin/python install -q "$(WHEEL)[$(EXTRA)]"
	@python -m pip --python $(AUDIT_VENV)/target/bin/python freeze --exclude azure-functions-db > $(AUDIT_VENV)/frozen.txt
	@test -s $(AUDIT_VENV)/frozen.txt || { echo "empty closure: nothing was audited" >&2; exit 3; }
	@$(AUDIT_VENV)/tool/bin/python -m pip install -q "$(PIP_AUDIT_SPEC)"
	@awk -v today="$$(date +%F)" '/^[[:space:]]*(#|$$)/ {next} \
		NF < 4 || $$3 !~ /^[0-9]{4}-[0-9]{2}-[0-9]{2}$$/ || $$3 <= today { print "invalid or expired exception: " $$0 > "/dev/stderr"; bad=1; next } \
		{ print "--ignore-vuln " $$1 } END { exit bad }' $(IGNORE_FILE) > $(AUDIT_VENV)/ignores.txt
	@$(AUDIT_VENV)/tool/bin/pip-audit -r $(AUDIT_VENV)/frozen.txt --no-deps --disable-pip --strict --progress-spinner off \
		$$(cat $(AUDIT_VENV)/ignores.txt) $(if $(REPORT),-f json -o $(REPORT)) \
		|| { rc=$$?; test -z "$(REPORT)" || cat "$(REPORT)"; exit $$rc; }

.PHONY: check
check: ensure-hatch
	@$(MAKE) lint
	@$(MAKE) typecheck
	@echo "Lint and type check passed."

.PHONY: lint-workflows
lint-workflows: ensure-hatch
	@$(HATCH) run python tools/lint_release_workflows.py
	@$(HATCH) run python tools/lint_workflow_pins.py
	@$(HATCH) run python tools/lint_hatch_matrix.py

.PHONY: check-all
check-all: ensure-hatch
	@$(MAKE) lint-workflows
	@$(MAKE) check
	@$(MAKE) test
	@$(MAKE) security
	@echo "All checks passed including tests and security scan."

.PHONY: precommit
precommit: ensure-hatch
	@$(HATCH) run precommit

.PHONY: precommit-install
precommit-install: ensure-hatch
	@$(HATCH) run precommit-install

.PHONY: test
test: ensure-hatch
	@echo "Running tests..."
	@$(HATCH) run test

.PHONY: cov
cov: ensure-hatch
	@$(HATCH) run cov
	@echo "Open htmlcov/index.html in your browser to view the coverage report."
	@echo "coverage.xml generated for Codecov upload."

.PHONY: build
build: ensure-hatch
	@$(HATCH) build

.PHONY: publish-test
publish-test: ensure-hatch
	@$(HATCH) publish --repo test

.PHONY: version
version: ensure-hatch
	@echo "Current version:"
	@$(HATCH) version

.PHONY: docs
docs:
	@if [ -n "$$CI" ]; then \
		echo "CI detected: running MkDocs directly"; \
		python -m pip install --upgrade pip > /dev/null 2>&1 || true; \
		pip install "mkdocs<2.0" "mkdocs-material<10.0" "mkdocstrings[python]<2.0" > /dev/null 2>&1; \
		mkdocs build; \
	else \
		$(MAKE) ensure-hatch > /dev/null; \
		$(HATCH) run mkdocs build; \
	fi

.PHONY: docs-serve
docs-serve: ensure-hatch
	@$(HATCH) run docs

.PHONY: doctor
doctor:
	@echo "Python version:" && $(PYTHON) --version
	@echo "Installed packages:" && $(HATCH) env run pip list || echo "No hatch environment found"
	@echo "Pre-commit hook installed:"
	@if [ -f .git/hooks/pre-commit ]; then echo yes; else echo no; fi

.PHONY: clean
clean:
	@rm -rf *.egg-info dist build __pycache__ .pytest_cache

.PHONY: clean-all
clean-all: clean
	@find . -type d -name "__pycache__" -exec rm -rf {} +
	@find . -type f \( -name "*.pyc" -o -name "*.pyo" \) -delete
	@rm -rf .mypy_cache .ruff_cache .pytest_cache .coverage coverage.xml htmlcov .DS_Store $(VENV_DIR) site

.PHONY: help
help:
	@echo "Available commands:" && \
	grep -E '^\.PHONY: ' Makefile | cut -d ':' -f2 | xargs -n1 echo "  - make"
