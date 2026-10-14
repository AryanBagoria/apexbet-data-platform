# Architecture

> ApexBet is a **local, AWS-simulated, real-time gambling data platform** built on
> real open-source technology, using **synthetic data only**. It is designed to be
> run end-to-end on a single laptop with **no AWS account, no paid service, and no
> external AI API key**. Every diagram below reflects the code in `src/apexbet`.

## 1. System context

```mermaid
C4Context
    title ApexBet — system context (local, synthetic)
    Person(engineer, "Data engineer", "Runs the platform, inspects data & dashboards")
    Person(analyst, "Analyst", "Queries Gold marts with SQL / the read API")
    System_Boundary(edge, "Edge / ingestion") {
      System(gen, "Synthetic generator", "15 event types, reproducible, anomaly injection")
      System(api, "Ingestion API", "FastAPI: validate + publish only")
    }
    System_Boundary(stream, "Streaming (choose one)") {
      System(redpanda, "Redpanda", "Kafka-compatible broker")
      System(kinesis, "Kinesis", "LocalStack emulation")
    }
    System_Boundary(core, "Processing & storage") {
      System(pipeline, "Stream consumer", "Bronze + Silver, dedup, DLQ, checkpoint")
      System(lake, "Lake", "Object store: local FS or S3/MinIO")
      System(gold, "Gold / DuckDB", "9 SQL marts + DQ + reconciliation + anomalies")
    }
    System_Boundary(obs, "Observability") {
      System(prom, "Prometheus")
      System(graf, "Grafana")
    }
    Rel(engineer, gen, "Runs")
    Rel(gen, api, "HTTP (or direct publish)")
    Rel(api, redpanda, "publish")
    Rel(api, kinesis, "PutRecords")
    Rel(redpanda, pipeline, "consume")
    Rel(kinesis, pipeline, "GetRecords")
    Rel(pipeline, lake, "write Bronze/Silver parquet")
    Rel(lake, gold, "read Silver")
    Rel(analyst, gold, "SQL / API")
    Rel(api, prom, "metrics")
    Rel(pipeline, prom, "metrics")
    Rel(prom, graf, "scrape")
```

## 2. Container / component view

```mermaid
flowchart TB
  subgraph Clients
    GEN["Synthetic generator<br/>(src/apexbet/generator)"]
    CLI["apexbet CLI<br/>(src/apexbet/cli)"]
  end

  subgraph Ingestion
    API["FastAPI app<br/>validate → publish<br/>(src/apexbet/ingestion)"]
  end

  subgraph Transports["Transport abstraction (no silent fallback)"]
    MEM["memory<br/>(tests/demo)"]
    RPA["redpanda<br/>(confluent-kafka)"]
    KIN["kinesis<br/>(boto3 / LocalStack)"]
  end

  subgraph Processing
    PIPE["Pipeline<br/>dedup · watermark · DLQ · checkpoint<br/>(src/apexbet/consumers)"]
    BRONZE["BronzeWriter<br/>(src/apexbet/bronze)"]
    SILVER["Silver transform<br/>(src/apexbet/silver)"]
    ENR["Enrichment<br/>rules / local_ml / llm<br/>(src/apexbet/enrichment)"]
  end

  subgraph Analytics
    DUCK["DuckDB engine<br/>9 Gold marts<br/>(src/apexbet/analytics)"]
    DQ["Data quality<br/>11 rules"]
    REC["Reconciliation<br/>9 rules"]
    ANOM["Anomaly detection<br/>rules + Isolation Forest"]
  end

  STORE[("Object store<br/>local FS or S3/MinIO")]

  GEN --> API
  CLI --> GEN
  API --> Transports
  Transports --> PIPE
  PIPE --> BRONZE --> STORE
  PIPE --> SILVER --> STORE
  STORE --> DUCK
  STORE --> DQ
  STORE --> REC
  DUCK --> ANOM
  STORE --> ENR --> STORE
```

## 3. Medallion data flow

```mermaid
flowchart LR
  RAW["Raw event JSON<br/>envelope + typed payload"] -->|"validate (Pydantic)"| BRZ
  BRZ["Bronze<br/>verbatim + routing metadata<br/>partitioned parquet"] -->|"transform"| SIL
  SIL["Silver<br/>normalized · deduped<br/>reference-enriched · late-classified"] -->|"aggregate"| GLD
  GLD["Gold<br/>9 DuckDB marts<br/>betting · finance · casino · DQ"]
  BRZ -.->|"quarantine"| QUAR["Quarantine<br/>bronze/quarantine/*"]
  SIL -.->|"rejects"| QUAR
  BRZ -->|"replay source"| SIL
```

Every event is **always** written to Bronze verbatim (audit + replay), even if it
is later quarantined. Silver is the single canonical clean dataset; Gold is derived
and rebuildable from Silver at any time.

## 4. Processing sequence (one event)

```mermaid
sequenceDiagram
    autonumber
    participant G as Generator
    participant A as Ingestion API
    participant T as Transport
    participant P as Pipeline
    participant B as Bronze
    participant S as Silver
    participant D as DuckDB/Gold

    G->>A: POST /api/v1/events (envelope + payload)
    A->>A: validate_events() (schema + version + payload)
    A->>T: publish_many(topic, batch)
    P->>T: consume(topic, group)
    T-->>P: StreamMessage[]
    P->>B: add(raw) + flush (always, verbatim)
    P->>S: transform_event(raw, watermark)
    alt valid + not duplicate
        S-->>P: SilverRecord
        P->>S: buffer + flush parquet
    else invalid
        P->>B: write_quarantine(...)
    else duplicate
        P->>P: drop (dedup window)
    end
    P->>T: commit(messages) + checkpoint()
    D->>S: load_silver()
    D->>D: build_gold() (9 marts)
```

## 5. Reliability model

```mermaid
stateDiagram-v2
    [*] --> Consume
    Consume --> Process: messages
    Process --> BronzeWrite
    BronzeWrite --> Transform
    Transform --> Dedup
    Dedup --> SilverWrite: new event
    Dedup --> Drop: duplicate
    Transform --> Quarantine: invalid
    Consume --> DLQ: decode error
    SilverWrite --> Checkpoint
    BronzeWrite --> Retry: transient error
    Retry --> BronzeWrite: backoff
    Retry --> Quarantine: retries exhausted
    Checkpoint --> Consume
    Consume --> Idle
    Idle --> Flush: idle timeout
    Flush --> [*]
```

* **At-least-once** delivery from both brokers; **exactly-once business effect**
  is achieved downstream by `event_id` dedup + idempotent writes.
* **Bounded retries** with full-jitter exponential backoff (`common.retry`).
* **Dead-letter** routing for structurally broken records (bad JSON).
* **Quarantine** (a lake prefix) for records that fail validation but are
  well-formed JSON — kept for inspection and replay.
* **Checkpointing** persists the committed cursor to the lake after each flush, so
  a restart resumes without reprocessing.

## 6. Two transports, one contract

```mermaid
flowchart TB
  BUILDER["build_transport(name)"]
  BUILDER -->|"name == 'memory'"| M["MemoryTransport"]
  BUILDER -->|"name == 'redpanda'"| R["KafkaTransport"]
  BUILDER -->|"name == 'kinesis'"| K["KinesisTransport"]
  BUILDER -->|"unknown"| E["raise TransportError<br/>(no silent fallback)"]
  M --> P["Transport Protocol<br/>publish · consume · commit · to_dlq · health"]
  R --> P
  K --> P
```

| Aspect | Redpanda (Kafka) | Kinesis (LocalStack / AWS) |
|---|---|---|
| Delivery | at-least-once | at-least-once |
| Ordering | per partition (key = `account_id`) | per shard (PartitionKey) |
| Commit | explicit offset commit | **no** broker-side commit; sequence no. per shard |
| Replay | seek/consumer-group reset | `AT_TIMESTAMP` / `TRIM_HORIZON` within retention |
| Retention | topic config | 24h default (configurable) |
| DLQ | sibling topic | sibling stream (no native DLQ) |

The transport is chosen by `APEXBET_TRANSPORT`. An unknown value **fails fast**.

## 7. Deployment topologies

```mermaid
flowchart LR
  subgraph Laptop["Laptop (this repo)"]
    L1["demo profile:<br/>Redpanda + API + consumer"]
    L2["full profile:<br/>+ LocalStack, Airflow,<br/>Prometheus, Grafana, Spark, Ollama"]
  end
  subgraph Optional["AWS (Terraform — validate only, never applied here)"]
    A1["S3 lake"]
    A2["Kinesis stream"]
    A3["KMS + IAM + Secrets"]
    A4["CloudWatch alarms + SNS"]
    A5["Glue catalog + Athena"]
    A6["ECR"]
  end
  L2 -.->|"same code, real endpoints<br/>(opt-in, not run by default)"| Optional
```

The Terraform under `infra/terraform` describes an *optional* AWS deployment. It is
**validated only** (`terraform fmt/validate` locally, or the HCL2 fallback when the
binary is unavailable) and is **never applied automatically**.

## 8. Trust boundaries & non-goals

```mermaid
flowchart TB
  subgraph Deterministic["Deterministic rules"]
    RULES["Rules: validation, money math,<br/>settlement checks, DQ, reconciliation"]
  end
  subgraph Optional["Optional, advisory"]
    ML["Local ML enrichment"]
    LLM["Local LLM enrichment"]
    IF["Isolation Forest anomaly scores"]
  end
  RULES -->|"decides"| DEC["Settlement · limits · quarantine · pass/fail"]
  ML -.->|"informs only"| HUMAN["Human review / dashboards"]
  LLM -.->|"informs only"| HUMAN
  IF -.->|"informs only"| HUMAN
```

* Settlement and account restrictions are decided by rules only.
* This is **synthetic data only**; there is no connection to any real betting,
  payment or customer system, and no real money.
* LocalStack community edition is targeted; paid-plan-only features are documented,
  not silently depended upon.

## 9. Serving and dashboard

The dashboard is a read-only client. It never writes pipeline state; the only
write path from the browser is the bounded Pipeline Controls API, which runs
predefined jobs server-side.

```mermaid
flowchart LR
  STORE[("Object store<br/>Bronze/Silver/Gold parquet")]
  DUCK[("DuckDB<br/>warehouse + 9 marts")]
  DS["DashboardService<br/>(src/apexbet/serving/dashboard.py)"]
  ROUTER["Dashboard API<br/>/api/v1/dashboard/*<br/>(src/apexbet/serving/router.py)"]
  DEMO["DemoControl<br/>bounded jobs<br/>(src/apexbet/demo_control.py)"]
  PROXY["Vite dev proxy<br/>127.0.0.1:5173 → 127.0.0.1:8000"]
  UI["React dashboard<br/>8 pages"]

  STORE --> DS
  DUCK --> DS
  DS --> ROUTER
  ROUTER --> PROXY
  DEMO --> ROUTER
  PROXY --> UI
  UI -->|"POST /demo/{kind}"| PROXY
```

Every read endpoint returns `available: false` with a `detail` message when an
artefact is missing, so the UI shows an empty state rather than a fabricated
number. Demo-control jobs are clamped to `MAX_EVENTS` (50,000) and validated
server-side; there is no shell or arbitrary-code path.

## 10. CI and testing architecture

```mermaid
flowchart TB
  subgraph Static["Static checks"]
    RUFF["ruff check + format --check"]
    MYPY["mypy src/apexbet"]
    ESL["eslint + tsc"]
  end
  subgraph Tests["Test tiers (pytest)"]
    UNIT["unit"]
    CONTRACT["contract"]
    REL["reliability"]
    INT["integration<br/>(self-skips without Docker)"]
  end
  subgraph Front["Dashboard tests"]
    VIT["vitest unit/component"]
    PW["Playwright e2e<br/>(API + dev server in one step)"]
  end
  subgraph Build["Build & infra validation"]
    DEMO["apexbet demo → grep DEMO PASSED"]
    DBT["apexbet validate-dbt"]
    DOCK["docker compose config + build"]
    TF["terraform fmt + validate"]
    SEC["pip-audit + gitleaks"]
  end
  Static --> Tests --> Front --> Build
```

All jobs run in `.github/workflows/ci.yml`. Docker-dependent tests self-skip
rather than pass silently when services are absent.
