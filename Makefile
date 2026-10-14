# ApexBet — developer & operator Makefile
#
# Everything runs locally with NO AWS account, no paid service and no external
# AI API key. All data is synthetic.
#
# Typical first run:
#   make setup          # create .venv + install deps
#   make demo           # full end-to-end demo (offline, memory transport)
#
# With Docker + a real broker:
#   make up             # start the 'demo' profile (Redpanda + API + consumer)
#   make run            # generate -> process -> gold -> quality -> reconcile
#   make up-full        # heavier 'full' profile (LocalStack, Airflow, ...)
#
# `make help` lists every target.

SHELL := /bin/bash
.DEFAULT_GOAL := help

# --- interpreter: prefer the uv-managed venv, fall back to system python ---
PY  := $(shell [ -x .venv/bin/python ] && echo .venv/bin/python || echo python3)
BIN := $(shell [ -x .venv/bin/apexbet ] && echo .venv/bin/apexbet || echo ".venv/bin/apexbet")
export APEXBET_LOG_JSON ?= false

# Offline-safe transport for local targets that do not need a broker.
MEMORY_ENV := APEXBET_TRANSPORT=memory

.DEFAULT_SHELL := $(SHELL)

##@ Setup

.PHONY: setup
setup: ## Create the virtualenv and install the project with dev extras
	@command -v uv >/dev/null 2>&1 && uv venv --python 3.11 .venv || python3 -m venv .venv
	@.venv/bin/python -m pip install --upgrade pip
	.venv/bin/python -m pip install -e ".[dev,infra]"
	@echo "✔ setup complete — run 'make demo'"

.PHONY: setup-spark
setup-spark: ## Install optional Spark/Iceberg extras
	.venv/bin/python -m pip install -e ".[spark]"

.PHONY: setup-infra
setup-infra: ## Install the optional HCL2 parser for offline Terraform validation
	.venv/bin/python -m pip install -e ".[infra]"

##@ Run (Docker)

.PHONY: up
up: ## Start the 'demo' profile (Redpanda + API + consumer)
	docker compose --profile demo up -d --build
	@echo "waiting for services..."
	@docker compose --profile demo ps

.PHONY: up-full
up-full: ## Start the 'full' profile (adds LocalStack, Airflow, Prometheus, Grafana, Spark, Ollama)
	docker compose --profile full up -d --build
	@docker compose --profile full ps

.PHONY: down
down: ## Stop all services (keeps volumes)
	docker compose --profile demo --profile full down

.PHONY: down-clean
down-clean: ## Stop all services and DELETE volumes + local lake state
	docker compose --profile demo --profile full down -v
	rm -rf lake
	@echo "✔ volumes and ./lake removed"

.PHONY: logs
logs: ## Tail logs from the running profile
	docker compose logs -f --tail=100

.PHONY: ps
ps: ## Show running services
	docker compose ps

##@ Run (local, no Docker)

.PHONY: demo
demo: ## End-to-end acceptance demo (offline; memory transport)
	$(MEMORY_ENV) $(BIN) demo --count 8000

.PHONY: run
run: ## Full pipeline once: generate -> stream -> Bronze -> Silver -> Gold -> DQ -> reconcile
	$(BIN) run --count 5000

.PHONY: generate
generate: ## Generate synthetic events to ./lake/raw_events.jsonl
	$(BIN) generate --count 10000 --dirty --out ./lake/raw_events.jsonl

.PHONY: generate-clean
generate-clean: ## Generate a clean (no anomalies) event file
	$(BIN) generate --count 10000 --out ./lake/raw_events_clean.jsonl

.PHONY: ingest
ingest: ## Publish generated events into the configured transport
	$(BIN) ingest --count 5000 --dirty

.PHONY: process
process: ## Run the streaming consumer pipeline (Bronze + Silver)
	$(BIN) process --count 5000

.PHONY: analytics
analytics: ## Build Gold marts (DuckDB) and print key results
	$(BIN) analytics

.PHONY: quality
quality: ## Run data-quality checks over Silver and store results
	$(BIN) quality

.PHONY: reconcile
reconcile: ## Run financial reconciliation and store the report
	$(BIN) reconcile

.PHONY: anomaly
anomaly: ## Inject a controlled anomaly and detect it
	$(BIN) anomaly --inject --count 3000

.PHONY: anomaly-eval
anomaly-eval: ## Run the reproducible anomaly-detection evaluation
	$(BIN) anomaly-eval

.PHONY: enrich
enrich: ## Run enrichment over Silver (rules mode by default)
	$(BIN) enrich --mode rules

.PHONY: serve
serve: ## Run the FastAPI ingestion + read API (uvicorn)
	$(BIN) serve --host 0.0.0.0 --port 8000

.PHONY: status
status: ## Print transport/storage health and the latest run report
	$(BIN) status

.PHONY: schemas
schemas: ## Export JSON Schemas for all event contracts
	$(BIN) schemas --out docs/data-engineering/event-schemas.json

##@ Tests & quality

.PHONY: test
test: ## Run the full test suite
	$(PY) -m pytest

.PHONY: test-unit
test-unit: ## Run unit tests only (fast, no infra)
	$(PY) -m pytest tests/unit

.PHONY: test-contract
test-contract: ## Run transport + event contract tests (skips missing infra)
	$(PY) -m pytest tests/contract

.PHONY: test-reliability
test-reliability: ## Run reliability tests (restart/replay/DLQ/retry)
	$(PY) -m pytest tests/reliability

.PHONY: test-integration
test-integration: ## Run integration tests (needs Docker services; skips if absent)
	$(PY) -m pytest tests/integration -m integration

.PHONY: coverage
coverage: ## Run tests with coverage and print a report
	$(PY) -m pytest --cov=apexbet --cov-report=term-missing --cov-report=xml

.PHONY: lint
lint: ## Ruff lint + format check (matches CI; no writes)
	.venv/bin/ruff check src tests dags
	.venv/bin/ruff format --check src tests dags

.PHONY: format
format: ## Ruff formatter + import sort (writes changes)
	.venv/bin/ruff format src tests dags
	.venv/bin/ruff check --fix src tests dags

.PHONY: typecheck
typecheck: ## mypy static type-check
	.venv/bin/mypy src/apexbet

.PHONY: validate-dbt
validate-dbt: ## Compile + run every dbt model against local DuckDB
	$(BIN) validate-dbt

.PHONY: check
check: lint typecheck test ## Lint + typecheck + tests (CI gate locally)

.PHONY: check-all
check-all: check dashboard-check ## Backend + dashboard gates together

##@ Infrastructure

.PHONY: terraform-validate
terraform-validate: ## Validate Terraform (fmt+validate if binary present, else HCL2)
	$(BIN) terraform-validate --root infra/terraform

.PHONY: terraform-fmt
terraform-fmt: ## Format Terraform files (requires the terraform binary)
	@command -v terraform >/dev/null 2>&1 && terraform fmt -recursive infra/terraform \
	  || echo "terraform binary not installed — skipping"

.PHONY: docker-build
docker-build: ## Build the platform image
	docker build -t apexbet:local .

.PHONY: compose-config
compose-config: ## Validate the docker-compose file for both profiles
	docker compose --profile demo config >/dev/null && echo "demo profile: OK"
	docker compose --profile full config >/dev/null && echo "full profile: OK"

##@ Dashboard (React frontend)

FRONTEND_DIR := frontend

.PHONY: frontend-install
frontend-install: ## Install dashboard dependencies (npm)
	cd $(FRONTEND_DIR) && npm install

.PHONY: frontend
frontend: ## Run the dashboard dev server (expects `make serve` in another terminal)
	cd $(FRONTEND_DIR) && npm run dev

.PHONY: frontend-lint
frontend-lint: ## Lint the dashboard (eslint)
	cd $(FRONTEND_DIR) && npm run lint

.PHONY: frontend-typecheck
frontend-typecheck: ## Type-check the dashboard (tsc)
	cd $(FRONTEND_DIR) && npm run typecheck

.PHONY: frontend-test
frontend-test: ## Run dashboard unit/component tests (vitest)
	cd $(FRONTEND_DIR) && npm run test

.PHONY: frontend-e2e
frontend-e2e: ## Run dashboard end-to-end browser tests (needs API + dev server running)
	cd $(FRONTEND_DIR) && npm run test:e2e

.PHONY: frontend-build
frontend-build: ## Build the dashboard for production (vite build)
	cd $(FRONTEND_DIR) && npm run build

.PHONY: dashboard-check
dashboard-check: frontend-lint frontend-typecheck frontend-test ## Frontend gate: lint + typecheck + tests

.PHONY: screenshots
screenshots: ## Capture dashboard screenshots into docs/assets (needs API + dev server running)
	cd $(FRONTEND_DIR) && npm run screenshots

##@ Benchmarks

.PHONY: bench
bench: ## Quick benchmark (100k events, memory transport)
	$(MEMORY_ENV) $(PY) -m apexbet.benchmark --events 100000 --transport memory \
	  --out benchmark_results.json

.PHONY: bench-1m
bench-1m: ## Full benchmark (1M events); slow — writes benchmark_results_1m.json
	$(MEMORY_ENV) $(PY) -m apexbet.benchmark --events 1000000 --transport memory \
	  --out benchmark_results_1m.json

.PHONY: bench-broker
bench-broker: ## Benchmark over the real broker (requires `make up`)
	$(PY) -m apexbet.benchmark --events 100000 --transport redpanda \
	  --out benchmark_results_redpanda.json

##@ Housekeeping

.PHONY: clean
clean: ## Remove local caches and generated artifacts (keeps ./lake)
	rm -rf .pytest_cache .mypy_cache .ruff_cache .coverage coverage.xml htmlcov
	find . -type d -name __pycache__ -prune -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name '*.pyc' -delete 2>/dev/null || true

.PHONY: clean-lake
clean-lake: ## Delete local lake state (bronze/silver/gold/reports)
	rm -rf lake
	@echo "✔ ./lake removed"

.PHONY: help
help: ## Show this help
	@awk 'BEGIN {FS = ":.*##"; printf "\nApexBet make targets\n\n"} \
	  /^[a-zA-Z0-9_.-]+:.*?##/ { printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2 } \
	  /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) } ' $(MAKEFILE_LIST)
