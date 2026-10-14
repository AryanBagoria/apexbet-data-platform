# Changelog

All notable changes to ApexBet are documented here.
The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and
this project adheres to [Semantic Versioning](https://semver.org/).

## [Unreleased]

### Added

- **Interactive web dashboard** (`frontend/`, React + TypeScript + Vite): eight
  pages (Overview, Event Stream, Pipeline Lineage, Data Pipeline, Data Quality,
  Enrichment & Anomalies, Analytics, Pipeline Controls) wired to real backend
  data, with no mock fixtures.
- **Dashboard serving layer** (`src/apexbet/serving/`): typed, paginated,
  bounded read endpoints under `/api/v1/dashboard/*` plus an honest
  "unavailable" contract when artefacts are missing.
- **Safe demo controls** (`src/apexbet/demo_control.py`): bounded, validated,
  background-thread demo jobs (max 50,000 events) exposed via the API — no shell
  or arbitrary code execution.
- **Interactive pipeline lineage** graph (React Flow) whose node statuses derive
  from real state, with a click-through details panel.
- **Frontend tests**: 18 Vitest unit/component tests and 9 Playwright e2e tests.
- **CI**: new `frontend` (lint/types/unit/build) and `e2e-dashboard` (Playwright
  against a live API + dev server) jobs.
- **Screenshots** captured from the running app in `docs/assets/`
  (`make screenshots`).
- **Docs**: `docs/dashboard/README.md`.

## [0.1.0] — 2026-10-14

First complete, locally runnable release of the ApexBet data platform. Everything
runs offline with no AWS account, no paid service, and no external AI API key,
using synthetic data only.

### Added

- **Synthetic generator** covering 15 sportsbook/casino event types with a fixed
  seed, configurable scale, and controlled anomaly injection (`clean` / `demo`).
- **FastAPI ingestion service** with bearer-token auth, protected path prefixes,
  request-body limits, per-second rate limiting, and Prometheus metrics.
- **Three transports behind one interface**: `memory` (offline/tests), `kafka`
  (Redpanda), and `kinesis` (LocalStack). Transport is selected explicitly and an
  unknown value fails fast — there is no silent fallback.
- **Streaming consumer pipeline** with a dedup window, retry-with-backoff+jitter,
  dead-letter queue, event-time watermark/late handling, and checkpointing.
- **Medallion lake**: Bronze (partitioned Parquet + quarantine + failure
  fallback), Silver (typed, deduplicated, late-aware), Gold.
- **Gold analytics** with 9 DuckDB marts: betting activity, bet outcomes, event
  volume by minute, latency distribution, financial ledger, responsible gambling,
  casino activity, data completeness, and event counts.
- **Data quality**: 11 rules over Silver events with stored results.
- **Reconciliation**: 9 source-to-target financial reconciliation rules with a
  stored report; monetary totals reconcile exactly on matched dimensions.
- **Anomaly detection**: deterministic statistical rules (traffic, rejection,
  latency, dedup, DQ, reconciliation, suspicious-pattern, enrichment, lag) plus
  an optional scikit-learn Isolation Forest baseline.
- **Reproducible anomaly evaluation** (`apexbet anomaly-eval`) over four seeded,
  labelled scenarios with measured precision/recall.
- **Enrichment** in three modes — `rules` (always available), `local_ml`
  (scikit-learn), and `llm` (optional local Ollama) — schema-validated and
  non-blocking; failures do not stop the pipeline.
- **Spark job** and **4 Airflow DAGs** (batch transformations, enrichment,
  freshness & quality, backfill).
- **dbt project** (`sql/dbt/`): 3 staging, 2 intermediate, 5 marts models, plus
  macros and singular tests, validated offline via `apexbet validate-dbt` (no dbt
  runtime required).
- **Observability**: Prometheus scrape config + alert rules, Grafana provisioning
  and dashboard.
- **Docker**: `Dockerfile` and `docker-compose.yml` with `demo` and `full`
  profiles.
- **Terraform**: `infra/terraform/` with 7 modules / 24 resources for an
  *optional* AWS deployment — **validate-only**, never applied.
- **CI/CD**: GitHub Actions pipeline (quality, test, e2e demo, docker, terraform,
  security).
- **Benchmark harness** (`src/apexbet/benchmark.py`) with full-context JSON
  reports; see [docs/testing/benchmarks.md](docs/testing/benchmarks.md).
- **Tests**: `unit`, `contract`, `reliability`, and `integration` layers;
  infrastructure-dependent tests self-skip when Docker is unavailable.
- **Documentation**: architecture, data-engineering, enrichment, infrastructure,
  operations, and testing docs with Mermaid diagrams, plus `Makefile`,
  `.env.example`, `CONTRIBUTING.md`, and `SECURITY.md`.

### Design decisions

See [docs/DECISIONS.md](docs/DECISIONS.md) for the full log (D-001 …). Notable:

- Money is stored as **integer minor units**; `Decimal` only at boundaries,
  `ROUND_HALF_UP`.
- One `ObjectStore` interface with `local` and `s3` backends.
- Enrichment is **optional**; rules always available.
- Terraform is **validate-only** in this repo; no cloud resources are touched.

### Known limitations

- Integration tests require Docker and self-skip otherwise.
- Benchmarks measure a single local process against a local broker — not a
  production capacity claim.
- Some LocalStack/AWS features are paid-plan only and are documented, not depended
  on.
- dbt is validated without the dbt runtime (no `dbt-duckdb` adapter installed);
  the SQL is exercised through the in-repo validator.
