# Security Policy

## Scope

ApexBet is a **local-first demonstration platform**. It processes **synthetic
data only** and is designed to run on a developer's machine with no AWS account,
no paid service, and no external AI API key. It does not operate a public
service, does not store real personal data, and must never be pointed at real
betting, payment, or customer systems.

Because of this, the realistic security concerns are:

- accidental credential/secret leakage into the repository,
- an unsafe default that could enable real cloud usage,
- dependency vulnerabilities,
- injection-style bugs in SQL/parquet handling.

## Supported versions

| Version | Supported |
|---------|-----------|
| `main` (0.1.x) | ✅ |

## Reporting a vulnerability

Please **do not** open a public issue for a security concern. Instead, report it
privately via GitHub's "Report a vulnerability" (Security Advisories) on the
repository, or email the maintainer listed in the repository profile. Include:
reproduction steps, affected file/command, impact, and any suggested fix. We aim
to acknowledge within a few days.

## Security posture by design

| Control | Implementation |
|---------|----------------|
| **No real cloud by default** | `APEXBET_USE_REAL_AWS=0`; LocalStack endpoints are explicit. Nothing here creates or destroys real AWS resources. |
| **Secrets never required** | LocalStack uses dummy `test/test` creds. No real credentials are read or needed. |
| **Secrets never committed** | `.gitignore` blocks `.env*` (except `.env.example`), `*.pem`, `*.key`, `secrets/`, `terraform.tfstate*`, `*.tfvars`. CI runs a secret scan. |
| **CI secret scanning** | The `security` CI job scans for credential-like patterns. |
| **Ingestion auth** | `APEXBET_API_TOKENS` + protected path prefixes; empty tokens = local-only convenience, documented as such. |
| **Input limits** | Request body caps (`APEXBET_API_MAX_BODY_BYTES`) and rate limiting (`APEXBET_API_RATE_LIMIT_PER_SEC`). |
| **Least-privilege Terraform** | IAM policies are scoped to the resources the platform actually needs; Terraform is **validate-only** here. |
| **No arbitrary code execution from data** | Event payloads are schema-validated (Pydantic, `extra="forbid"`); unparseable records are quarantined/DLQ'd, not executed. |
| **SQL safety** | DuckDB access goes through controlled helpers and parameterised/structured writes; no untrusted string interpolation into SQL. |

## Running safely

- Keep `APEXBET_USE_REAL_AWS=0`.
- Never place real credentials in `.env`.
- Do not deploy the Terraform modules without a deliberate, separate review —
  this repository is intended to be validated, not applied.
- The optional LLM mode talks to a **local** Ollama endpoint only; do not point
  it at an external paid API on behalf of this project.

## Out of scope

- Real customer-facing hardening (this is not a public service).
- The security of third-party Docker images (Redpanda, LocalStack, Airflow,
  Grafana, Ollama, etc.) beyond pinning their versions.
- LocalStack paid-plan features.

## Disclosure

We'll credit reporters who wish to be acknowledged once a fix is available.
