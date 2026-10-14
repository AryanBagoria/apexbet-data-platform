# Decisions (ADR log)

Concise architecture decision records. Newest additions at the bottom.

## D-001 — Python 3.11 via `uv` virtualenv
System Python is 3.9.6 (too old for modern Pydantic v2 features / typing).
Use Homebrew Python 3.11.15 with a committed, reproducible `uv`-managed
`.venv` and a `pyproject.toml` dependency set. `uv` was already installed and
is fast/deterministic.

## D-002 — Two transports, one contract, no silent fallback
Redpanda (Kafka API) is the dependable local streaming backend. Kinesis runs
against LocalStack. Both implement the same `Transport` interface and share
identical event contracts and transformation logic, but their delivery,
ordering, checkpointing and retention semantics differ and are documented
explicitly. Transport is chosen by `APEXBET_TRANSPORT`; an unknown value fails
fast rather than falling back.

## D-003 — Money as integer minor units
All monetary values are `int` minor units (cents). Decimal is used only at
boundaries for rounding (ROUND_HALF_UP). This makes reconciliation exact and
reproducible and avoids float drift in aggregations.

## D-004 — Single storage interface with local and S3 backends
`StorageBackend` abstracts object I/O. `local` writes under `./lake` for
zero-dependency demos; `s3` targets LocalStack S3 (or MinIO) via boto3 with an
endpoint URL. The medallion layout is identical across backends.

## D-005 — Config via pydantic-settings
One `Settings` object, environment-driven, with offline-safe defaults. No
credentials required for any core path. Real AWS is opt-in via
`APEXBET_USE_REAL_AWS=1`.

## D-006 — Enrichment is optional
Enrichment has three modes: `rules` (always available), `local_ml` (sklearn),
`llm` (optional Ollama). Financial settlement, account restrictions and other
consequential decisions use deterministic rules only. Enrichment output is
schema-validated, idempotent, and non-blocking with respect to ingestion.

## D-007 — Anomaly detection starts deterministic
Rules-based statistical thresholds first (traffic spikes, rejection-rate,
latency, lag, dedup spikes, DQ degradation, reconciliation gaps), then an
Isolation Forest baseline. Scores are operational signals, never proof of
fraud.

## D-008 — LocalStack edition
Target LocalStack community (free) for S3, Kinesis, SQS, SNS, IAM/STS, and
CloudWatch basics, all pinned. Services that require a paid plan are documented
as such and never become hard dependencies of the minimum demo.

## D-009 — Iceberg is optional
Apache Iceberg is attempted where the local Spark + warehouse setup is stable;
otherwise the Parquet pipeline remains complete and Iceberg is documented as an
optional extension. No untested Iceberg claims.

## D-010 — dbt is a validated SQL-first path, not a runtime dependency
The dbt project under `sql/dbt/` defines staging → intermediate → marts models,
but the installed environment has only the Snowflake adapter (no `dbt-duckdb`).
Rather than force an adapter install, `src/apexbet/dbt_validate.py` compiles and
runs every model against local DuckDB with lightweight Jinja/ref resolution.
This keeps the SQL honest (it actually executes and returns rows) without adding
a fragile runtime dependency. The CI job reuses the same validator.

## D-011 — Pinned web-stack versions for test compatibility
FastAPI 0.115.14 / Starlette 0.41.3 / httpx 0.27.2 are pinned because newer
Starlette + httpx combinations broke `fastapi.testclient.TestClient`. Pinning
keeps the ingestion test suite deterministic offline.

## D-012 — Four-tier test layout
Tests are split into `unit` (pure logic), `contract` (shared behaviour every
transport/event-envelope must honour), `reliability` (dedup, retry, DLQ,
watermark), and `integration` (real Docker services). Integration and
infra-dependent contract tests **self-skip** when the infrastructure is absent,
so a green offline run never masquerades as infra coverage.

## D-013 — Terraform is validate-only in this repository
The Terraform under `infra/terraform/` (7 modules / 24 resources) is designed for
an *optional* AWS deployment but is **never applied** by this repo. Where the
`terraform` binary is absent, `src/apexbet/infra_validate.py` validates the HCL
with an HCL2 parser; CI runs the real `terraform fmt -check` + `validate`.

