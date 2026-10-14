# Data engineering

This document describes the data model, the medallion layers, monetary handling,
transformation logic, data quality, reconciliation and analytics. Every claim maps
to code in `src/apexbet`.

## 1. Event model

Every event is one JSON object with a **common envelope** and a **typed payload**.

```mermaid
erDiagram
    ENVELOPE ||--|| PAYLOAD : contains
    ENVELOPE {
      string event_id PK
      string event_type
      string schema_version
      string event_timestamp
      string ingestion_timestamp
      string producer
      string account_id FK
      string session_id
      string correlation_id
      string partition_key
      string trace_id
      json payload
    }
```

* `event_id` is the dedup key (`evt_<uuid4>`).
* `event_type` is one of **15** registered types.
* `schema_version` is policed: `1.0.0 … 1.2.0` accepted; older/newer rejected.
* Timestamps are UTC ISO-8601. Four notions of time are kept distinct:
  event-time, ingestion-time, processing-time, and (derived) processing latency.
* Monetary fields are **integer minor units** (see §4).

### The 15 event types

```mermaid
mindmap
  root((Event types))
    Betting
      BetPlaced
      BetAccepted
      BetRejected
      BetSettled
    Market data
      OddsUpdated
      MarketOpened
      MarketSuspended
    Casino
      GameSessionStarted
      GameSessionEnded
    Payments
      DepositRecorded
      WithdrawalRecorded
    Account & risk
      AccountStatusChanged
      ResponsibleGamblingLimitChanged
      RiskSignalGenerated
    Platform
      PipelineHealthEvent
```

The canonical JSON Schemas are generated from the Pydantic models and written to
[`event-schemas.json`](./event-schemas.json) via `apexbet schemas`.

## 2. Medallion layers

```mermaid
flowchart LR
  subgraph Bronze["Bronze — raw, immutable, replayable"]
    B1["original_event_json (verbatim)"]
    B2["original_payload"]
    B3["routing + processing metadata"]
    B4["partitioned by event_type / event_date"]
  end
  subgraph Silver["Silver — clean, conformed, deduped"]
    S1["fixed, stable schema"]
    S2["normalized ids + UTC timestamps"]
    S3["reference enrichment (sport/league)"]
    S4["late-event classification"]
  end
  subgraph Gold["Gold — analytics-ready"]
    G1["9 DuckDB marts"]
    G2["DQ results"]
    G3["Reconciliation findings"]
    G4["Anomaly alerts"]
  end
  Bronze --> Silver --> Gold
```

### Storage layout (object keys)

```
bronze/event_type=<snake_case>/event_date=YYYY-MM-DD/part-<uuid>.parquet
bronze/quarantine/event_type=<...>/event_date=<...>/part-<uuid>.parquet
bronze/_parquet_failed/...            # JSONL fallback if parquet encoding fails
silver/events/event_date=YYYY-MM-DD/part-<uuid>.parquet
gold/ai_enrichment/enrich_date=YYYY-MM-DD/part-<uuid>.parquet
checkpoints/<group_id>/<topic>.json
reports/run_<run_id>.json
reports/latest.json
```

The same key layout is used by **both** storage backends (`local` filesystem and
`s3` via boto3 against LocalStack/MinIO).

## 3. Silver transformation

```mermaid
flowchart TB
  A["raw event"] --> B{"validate_event()"}
  B -->|invalid| Q["quarantine"]
  B -->|valid| C{"money valid?"}
  C -->|no| Q
  C -->|yes| D["standardize ids"]
  D --> E["normalize timestamps (UTC)"]
  E --> F["late-event classification vs watermark"]
  F --> G["reference enrichment"]
  G --> H["add derived category + flags"]
  H --> I["SilverRecord (fixed schema)"]
  I --> J{"seen before?"}
  J -->|yes| K["drop (dedup)"]
  J -->|no| L["write parquet"]
```

The transformation is a **pure function** (`silver.transform.transform_event`) so
the streaming Python processor and the Spark batch job share one implementation and
can never diverge. The Silver schema is **fixed**: optional enrichment fields are
always present (as `null` when not derivable) so every parquet file unifies cleanly.

## 4. Money handling

```mermaid
flowchart LR
  M["Major units (e.g. 12.34)"] -->|"to_minor() ROUND_HALF_UP"| N["Integer minor units (1234)"]
  N -->|"exact integer arithmetic"| AGG["Aggregations · reconciliation"]
  N -->|"to_major() at the boundary"| OUT["Display / API output"]
```

* All monetary values are stored and aggregated as **integer minor units**.
* `Decimal` is used only at the boundary, with `ROUND_HALF_UP`, for deterministic
  conversion. There is no floating-point drift in any total.
* Validation (`silver.transform._validate_money`) rejects non-integers, negatives
  and out-of-range magnitudes, quarantining the offending event.

## 5. Data quality (11 rules)

```mermaid
flowchart TB
  DQ["DataQualityEngine.run_all()"] --> R1["DQ001 required fields"]
  DQ --> R2["DQ002 schema compatibility"]
  DQ --> R3["DQ003 valid timestamps"]
  DQ --> R4["DQ004 valid monetary values"]
  DQ --> R5["DQ005 unique event ids"]
  DQ --> R6["DQ006 supported event type"]
  DQ --> R7["DQ007 referential integrity"]
  DQ --> R8["DQ008 lifecycle consistency"]
  DQ --> R9["DQ009 settlement discrepancy"]
  DQ --> R10["DQ010 record count match"]
  DQ --> R11["DQ011 freshness"]
```

Each rule returns a structured `QualityResult` (rule id, dataset, severity, record
count, failure count, diagnostics, sample). Failing records are collected into a
merged quarantine list (one entry per source event, with the union of offending
rules).

## 6. Reconciliation (9 rules)

```mermaid
flowchart LR
  B["Bronze (source)"] --> R{"reconcile()"}
  S["Silver (target)"] --> R
  R --> C1["REC001 event counts"]
  R --> C2["REC002 accepted bets"]
  R --> C3["REC003 settled bets"]
  R --> C4["REC004 settled stake total"]
  R --> C5["REC005 payout total"]
  R --> C6["REC006 missing settlements"]
  R --> C7["REC007 duplicate settlements"]
  R --> C8["REC008 ledger entries"]
  R --> C9["REC009 invalid transitions"]
```

Severity is **quarantine-aware**: a difference that is fully explained by records
the pipeline deliberately quarantined or deduplicated is downgraded to `info` with
`resolution_status=EXPLAINED_BY_…`, while an unexplained difference stays
`critical`. Monetary totals (REC004/REC005) are compared **like-for-like** over the
distinct bet ids present on both sides, so quarantined or deduplicated events never
masquerade as monetary discrepancies.

## 7. Gold marts

```mermaid
flowchart TB
  SIL["silver_events"] --> G1["gold_betting_activity"]
  SIL --> G2["gold_bet_outcomes"]
  SIL --> G3["gold_event_volume_by_minute"]
  SIL --> G4["gold_latency_distribution"]
  SIL --> G5["gold_financial_ledger"]
  SIL --> G6["gold_responsible_gambling"]
  SIL --> G7["gold_casino_activity"]
  SIL --> G8["gold_data_completeness"]
  SIL --> G9["gold_event_counts"]
```

Built by `apexbet.analytics.duckdb_engine.AnalyticsEngine.build_gold()` using DuckDB
SQL directly over the Silver parquet. An equivalent, SQL-first **dbt** project lives
under `sql/dbt` (staging → intermediate → marts); see
[`docs/data-engineering/dbt.md`](./dbt.md).

## 8. Reproducibility & lineage

```mermaid
flowchart LR
  SEED["seed + config"] --> GEN["deterministic generator"]
  GEN --> EV["events"]
  EV --> COR["correlation_id"]
  COR --> BRZ["Bronze"]
  BRZ --> SIL["Silver"]
  SIL --> GLD["Gold"]
  SIL --> ENR["enrichment (event_id lineage)"]
```

* The generator is seeded (`common.rng.rng_for`) — the same seed + config yield the
  same stream, so runs and benchmarks are reproducible.
* `correlation_id` threads through related events (e.g. a bet's placed → accepted →
  settled chain).
* Enrichment records carry `event_id` and `source_dataset` for lineage back to
  Silver.
