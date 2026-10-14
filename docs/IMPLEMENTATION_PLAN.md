# Implementation Plan

> Living document. See `docs/PROGRESS.md` for current state and
> `docs/DECISIONS.md` for the decision log.

## Goal

Build a complete, runnable Data Engineering platform for a fictional sportsbook
and casino operator ("ApexBet"), running locally with Docker and real open-source
technology, using AWS API emulation where practical, with **synthetic data only**
and **no AWS account, no paid service, and no external AI API key**.

## Guiding constraints

1. Everything runs and is verified locally. No fabricated results.
2. Two streaming transports behind one contract: Redpanda (Kafka API) and Kinesis
   (LocalStack emulation). An unknown transport fails fast; there is no silent
   fallback.
3. Medallion data lake (Bronze/Silver/Gold) on S3-compatible storage with a
   local-filesystem backend sharing one storage interface.
4. Money is integer minor units everywhere; `Decimal` only at boundaries.
5. Enrichment (optional) never decides financial or consequential outcomes.
   Rules are always available.
6. Documentation states clearly what is real, what is emulated, and what is
   cloud-only.

## Milestones

| # | Milestone | Status |
|---|-----------|--------|
| 1 | Synthetic generator & event schemas | done |
| 2 | FastAPI ingestion | done |
| 3 | Redpanda + Kinesis transport adapters | done |
| 4 | Streaming consumers & reliable processing | done |
| 5 | Bronze storage | done |
| 6 | Silver transformations | done |
| 7 | Enrichment (rules / local ML / optional LLM) | done |
| 8 | Gold datasets & analytics | done |
| 9 | Data quality & reconciliation | done |
| 10 | Spark processing | done |
| 11 | Airflow orchestration | done |
| 12 | Anomaly detection | done |
| 13 | Observability (Prometheus/Grafana) | done |
| 14 | LocalStack integration | done |
| 15 | Terraform for optional AWS deployment | done (validate-only) |
| 16 | CI/CD & security checks | done |
| 17 | Integration & reliability tests | done |
| 18 | Performance benchmarks | done |
| 19 | Documentation & diagrams | done |
| 20 | End-to-end local acceptance demo | done |
| 21 | React/TypeScript dashboard (8 pages) | done |
| 22 | Dashboard tests, screenshots, CI jobs | done |

## Architecture in one line

Synthetic generator → FastAPI ingestion → Kinesis/Kafka stream → Python + Spark
processing → Bronze → Silver (+ optional enrichment) → Gold → DuckDB/SQL +
anomaly detection + serving API, orchestrated by Airflow, stored on
S3-compatible storage, observed with Prometheus/Grafana, deployed (optionally)
via Terraform.
