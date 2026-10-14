# Project Status

Summary of what is implemented and the results measured on the current checkout.

## Implemented

| # | Component | Notes |
|---|-----------|-------|
| 1 | Synthetic event generator | 15 event types, reproducible seed, controllable anomaly injection |
| 2 | FastAPI ingestion service | auth, rate limiting, body limits, Prometheus metrics |
| 3 | Transport adapters | in-memory, Redpanda (Kafka API), Kinesis via LocalStack |
| 4 | Consumer pipeline | dedup window, DLQ, retries with backoff, watermark, checkpointing |
| 5 | Bronze storage | partitioned Parquet + quarantine partition |
| 6 | Silver transformations | stable schema, dedup, late-event classification |
| 7 | Enrichment | rules (default), optional scikit-learn model, optional local LLM |
| 8 | Gold datasets | 9 analytical marts via DuckDB |
| 9 | Data quality & reconciliation | 11 DQ rules + 9 reconciliation rules |
| 10 | Spark processing | `src/apexbet/spark/jobs.py` |
| 11 | Airflow orchestration | 4 DAGs in `dags/` |
| 12 | Anomaly detection | statistical rules + optional Isolation Forest + `anomaly-eval` harness |
| 13 | Observability | Prometheus config + alerts, Grafana provisioning + dashboard |
| 14 | LocalStack integration | Kinesis/S3 emulation; integration tests self-skip without Docker |
| 15 | Terraform | 7 modules / 24 resources; validate-only |
| 16 | CI/CD & security | `.github/workflows/ci.yml` + secret scan |
| 17 | Dashboard | React + TypeScript UI with 8 pages over the real API |
| 18 | Benchmarks | `src/apexbet/benchmark.py`; see `docs/testing/benchmarks.md` |

## Measured results

End-to-end run (`apexbet run --count 6000`, memory transport):

- consumed 6000, bronze_written 6000, silver_written 5932.
- quarantined 28, duplicates 40, late 124, dlq 0.
- Gold: 9 marts built.
- Data quality: DQ007 caught 50 missing-reference events; DQ009 caught a
  settlement discrepancy; REC004/REC005 monetary totals match exactly (diff 0).

Acceptance demo (`apexbet demo --count 6000`): `DEMO PASSED` — all events
consumed, bronze == source, 9 Gold marts built, 1000 enrichments succeeded,
anomaly injection detected (4 alerts).

Quality gates on this checkout:

- `ruff check src tests dags` → All checks passed.
- `ruff format --check src tests dags` → 82 files already formatted.
- `mypy src/apexbet` → no issues (51 source files).
- `pytest` → 214 tests, 0 failures, 22 skipped (skips need Docker services).
- Coverage → 82%.
- Frontend: `eslint` clean, `tsc --noEmit` clean, `vitest` 18 passed,
  production build succeeds, Playwright e2e 9 passed.
- `apexbet validate-dbt` → all 10 dbt models compile + run.
- `apexbet terraform-validate` → HCL parses; 12 files, 7 modules, 24 resources,
  26 variables (validated via the HCL2 parser; the `terraform` binary is not
  installed — install the `infra` extra for the fallback, or the real binary).
- Benchmarks (100k, memory transport): generation ≈38k eps, publish ≈28–36k eps,
  pipeline ≈900–2,000 eps single process. The 1M run is pending. See
  `docs/testing/benchmarks.md`.

## Environment

- macOS, arm64 (Apple Silicon), 10 CPUs.
- Python 3.11 via a `uv`-managed `.venv`.
- Node.js 18+ for the dashboard.
- Docker optional (Redpanda/Kinesis/Spark/Airflow run in the `full` profile).
