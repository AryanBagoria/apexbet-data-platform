# Documentation

Index of the documentation set. Every diagram is Mermaid and every number is
measured, not invented.

## Start here

| Doc | Read it for |
|-----|-------------|
| [../README.md](../README.md) | What the project is, quickstart, repository layout. |
| [architecture/README.md](architecture/README.md) | System design, layer boundaries, end-to-end diagrams. |
| [IMPLEMENTATION_PLAN.md](IMPLEMENTATION_PLAN.md) | Goal, constraints, milestone list. |
| [PROGRESS.md](PROGRESS.md) | Implemented components and measured results. |
| [DECISIONS.md](DECISIONS.md) | Decision log with rationale. |

## By topic

| Area | Docs |
|------|------|
| **Dashboard** | [dashboard/README.md](dashboard/README.md) — the React/TypeScript UI, its pages, and API endpoints |
| **Data engineering** | [data-engineering/README.md](data-engineering/README.md) — medallion model, event schemas, DuckDB marts · [data-engineering/dbt.md](data-engineering/dbt.md) — dbt project & offline validation · [data-engineering/event-schemas.json](data-engineering/event-schemas.json) — generated JSON Schemas |
| **Enrichment & anomalies** | [enrichment/README.md](enrichment/README.md) — enrichment modes, anomaly detection, evaluation harness |
| **Infrastructure** | [infrastructure/README.md](infrastructure/README.md) — Docker profiles, LocalStack, Terraform (validate-only) |
| **Operations** | [operations/README.md](operations/README.md) — runbooks, monitoring, failure modes |
| **Testing** | [testing/README.md](testing/README.md) — strategy & gates · [testing/benchmarks.md](testing/benchmarks.md) — measured throughput |

## Conventions

- **Real vs emulated** is always called out explicitly.
- **Money** is described in integer minor units; `Decimal` only at boundaries.
- **Diagrams** are Mermaid, so they render on GitHub and stay in sync with the
  code.
