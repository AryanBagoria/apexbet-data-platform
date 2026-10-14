# Testing — ApexBet

> Every number in this documentation set is **measured**, never invented. If a
> test needs Docker or LocalStack and those are absent, it **self-skips** rather
> than silently passing.

## Test layout

```
tests/
├── unit/          pure logic, no external services      (always run)
├── contract/      cross-implementation interface checks  (always run)
├── reliability/   retries, dedup, DLQ, idempotency       (always run)
├── integration/   real Docker services (Redpanda, LocalStack)  (self-skip)
└── conftest.py    shared fixtures (stores, transports, generators)
```

| Layer | Path | Runs without Docker? | What it proves |
|-------|------|----------------------|----------------|
| Unit | `tests/unit/` | ✅ | Money math, schemas, generator, Silver rules, DQ, reconciliation, anomaly, enrichment, analytics, CLI helpers. |
| Contract | `tests/contract/` | ✅ | Both transports and the event envelope honour the same interface — so swapping Redpanda↔Kinesis changes nothing by contract. |
| Reliability | `tests/reliability/` | ✅ | Exactly-once-ish behaviour: dedup window, retries with backoff, DLQ routing, watermark/late handling. |
| Integration | `tests/integration/` | ❌ self-skips | The real broker (Redpanda) and LocalStack services respond as expected. |

### Why contract + reliability are separate from unit

The platform's central promise is that **two transports are interchangeable and
testing one is testing both by contract**. A contract test asserts the shared
*behaviour* (publish/subscribe/commit semantics, envelope shape) against every
adapter. Reliability tests assert the failure-mode *guarantees* (a replayed
event is deduplicated, an unparseable record lands in the DLQ). Keeping these
distinct makes regressions obvious in CI.

## Running the suite

```bash
make test          # full suite (Docker-dependent tests self-skips)
make test-unit     # fast: unit + contract + reliability only
make coverage      # full suite with an HTML+terminal coverage report
```

Dashboard checks (from `frontend/`, or via the `make frontend-*` targets):

```bash
npm run lint        # eslint
npm run typecheck   # tsc --noEmit
npm run test        # vitest unit/component tests
npm run build       # production build
npm run test:e2e    # Playwright (needs the API + dev server running)
```

Direct pytest equivalent:

```bash
APEXBET_TRANSPORT=memory .venv/bin/pytest -q
```

Integration tests activate automatically once infra is up:

```bash
make up            # Redpanda + LocalStack (demo profile)
.venv/bin/pytest -q -m integration
```

### Markers

Defined in `pyproject.toml` (`--strict-markers` is on, so unknown markers fail):

| Marker | Meaning |
|--------|---------|
| `integration` | Requires running local infrastructure (Docker services). |
| `aws` | Requires LocalStack or real AWS. |
| `slow` | Long-running; can be deselected for a quick loop. |

```bash
.venv/bin/pytest -q -m "not slow"
```

## CI

`.github/workflows/ci.yml` runs eight jobs:

```mermaid
flowchart LR
    Q[quality<br/>ruff + mypy] --> T[test<br/>pytest + dbt-validate]
    T --> E[e2e<br/>demo | grep DEMO PASSED]
    E --> F[frontend<br/>lint + types + unit + build]
    F --> FE[e2e-dashboard<br/>Playwright vs live API]
    FE --> D[docker<br/>compose config + build]
    D --> TF[terraform<br/>fmt + validate]
    TF --> S[security<br/>audit + secret scan]
```

The `e2e` job runs the offline end-to-end demo and **grep-asserts** the
`DEMO PASSED` banner — a green build without that string is a build failure, not
a silent pass. The `e2e-dashboard` job starts the API and the Vite dev server in
the same step as the Playwright run, because background processes do not survive
between GitHub Actions steps.

## Quality gates

| Gate | Command | Expectation |
|------|---------|-------------|
| Lint | `make lint` | `ruff check` → "All checks passed!" |
| Types | `make typecheck` | `mypy src/apexbet` → "Success" |
| Tests | `make test` | 0 failures (infra tests self-skip) |
| dbt | `apexbet validate-dbt` | every model compiles + runs |
| TF | `apexbet terraform-validate` | HCL parses (or real `terraform validate` in CI) |

## Benchmarks

Measured throughput numbers live in [`benchmarks.md`](./benchmarks.md), produced
by the harness in `src/apexbet/benchmark.py`. Re-run them any time:

```bash
make bench        # 100k events, memory transport
make bench-1m     # 1M events
make bench-broker # 100k over the real Redpanda broker (needs `make up`)
```

Each run writes a JSON report with full context (hardware, Docker allocation,
concurrency, event size, config, duration) so a number is never bare.

## Honest limitations

- Integration/contract-with-infra tests **skip** when Docker is unavailable;
  they are not counted as passes in that state.
- The benchmark is a single local process against a local broker — **not** a
  production capacity claim.
- Coverage excludes `cli/` (thin argument-parsing wrappers around tested code).
- No test asserts cloud-only behaviour (e.g. real Kinesis, IAM) because the
  project intentionally never touches a real AWS account.
