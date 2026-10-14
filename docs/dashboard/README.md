# Dashboard

> The React + TypeScript dashboard: a live window onto the pipeline. It reads only
> from the backend API; there are no frontend fixtures or hardcoded counters.

## What it is

A local, dark-mode operations console with eight pages. Every value comes from a
typed FastAPI endpoint that aggregates real pipeline artefacts (Bronze/Silver/
Gold on the object store, the local DuckDB warehouse, run reports, DQ,
reconciliation and anomaly results). When an artefact is missing, the UI shows an
empty or "Unavailable" state with an explanation rather than a placeholder number.

> The **APEXBET — SYNTHETIC DATA DEMONSTRATION** banner is always visible. The app
> never implies it processes real customers, accounts or money.

## Pages

| Page | Route | What it shows |
|------|-------|---------------|
| Overview | `/` | KPI cards (totals, rates, latency), throughput and latency charts, pipeline health |
| Event Stream | `/streaming` | Events per minute, recent-activity table, a UI-only pause control |
| Pipeline Lineage | `/lineage` | Graph of the pipeline stages; click a node for details |
| Data Pipeline | `/layers` | Bronze/Silver/Gold statistics + a paginated dataset browser |
| Data Quality | `/quality` | 11 DQ rules + 9 reconciliation findings, searchable/filterable |
| Enrichment & Anomalies | `/ai` | Enrichment records + anomaly alerts; modes shown separately |
| Analytics | `/analytics` | Gold-mart charts and tables (labelled synthetic) |
| Pipeline Controls | `/demo` | Bounded, validated buttons that run real backend jobs |

## Design principles

1. **No fabricated data.** Unavailable metrics render as `Unavailable` with a
   reason. Charts plot only what the API returns.
2. **Totals vs. rates.** Each KPI is tagged `total`, `rate`, `gauge`, or
   `latency` so cumulative counts are never confused with per-second rates.
   "Processing latency" is the gap between an event's ingestion timestamp and
   Silver processing; with the synthetic generator that includes simulated event
   age, so it is not a real-time latency measurement.
3. **Accurate animation.** Lineage edges animate only while the backend reports
   recent data; a stopped pipeline is not animated.
4. **No false control.** Pausing the event feed pauses the UI only, and the page
   says so. Jobs that exceed the resolver's backoff are reported as failed.
5. **Bounded everywhere.** Tables are paginated (≤ 200 rows/page); inputs are
   clamped; jobs are capped at 50,000 events.

## Structure

```
frontend/
├── src/components/   # charts (Recharts) + UI primitives
├── src/pages/        # one file per page
├── src/hooks/        # TanStack Query hooks, one per endpoint
├── src/services/     # typed API client + formatting helpers
├── src/types/        # typed mirror of the backend contracts
├── src/layouts/      # sidebar + topbar shell
├── src/styles/       # design tokens + primitives
├── src/test/         # Vitest unit/component tests
└── e2e/              # Playwright browser smoke tests
```

Keep business logic in the backend: the frontend never re-implements a pipeline
transformation, it only formats and displays.

## Running it

```bash
make frontend-install    # once
make serve               # terminal 1 — API on :8000
make frontend            # terminal 2 — Vite on :5173
```

The dev server proxies `/api` and `/health` to the backend, so the browser sees a
single origin. CORS is also configured server-side (`APEXBET_CORS_ORIGINS`).

## Gates

```bash
make frontend-lint       # eslint
make frontend-typecheck  # tsc --noEmit
make frontend-test       # vitest
make frontend-e2e        # Playwright (needs serve + frontend running)
make screenshots         # regenerate docs/assets/*.png
make check-all           # backend + dashboard gates together
```

## Backend endpoints consumed

| Endpoint | Purpose |
|----------|---------|
| `GET /api/v1/dashboard/health` | pipeline status, transport, storage, layer stats |
| `GET /api/v1/dashboard/overview` | KPI snapshot |
| `GET /api/v1/dashboard/throughput` | events/minute by type |
| `GET /api/v1/dashboard/latency` | p50/p95/p99 by event type |
| `GET /api/v1/dashboard/layers` | Bronze/Silver/Gold object stats |
| `GET /api/v1/dashboard/datasets` · `/preview` | browse real stored data |
| `GET /api/v1/dashboard/quality` · `/reconciliation` | DQ + reconciliation results |
| `GET /api/v1/dashboard/enrichment` · `/enrichment/summary` | enrichment records + totals |
| `GET /api/v1/dashboard/anomalies` | anomaly alerts |
| `GET /api/v1/dashboard/lineage` · `/nodes/{id}` | lineage graph + node detail |
| `POST /api/v1/dashboard/demo/{kind}` · `GET /demo/job/{id}` | bounded demo jobs |
