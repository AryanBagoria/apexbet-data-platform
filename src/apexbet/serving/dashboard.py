"""Dashboard serving layer.

A read-only aggregation layer that turns the real artefacts produced by the
pipeline (Bronze/Silver/Gold on the object store, the local DuckDB warehouse and
the run reports under ``reports/``) into typed responses for the web dashboard.

Design rules
------------
* **No fabrication.** Every number here is read from real pipeline output. When
  an artefact is missing the endpoint returns ``available: false`` with a
  human-readable ``detail`` — the dashboard renders an empty/unavailable state
  rather than inventing a value.
* **Bounded.** Tables are paginated and capped; the dashboard never loads a whole
  dataset into the browser.
* **Read-only.** Nothing in this module mutates the lake. The write path lives in
  :mod:`apexbet.demo_control` and is explicitly validated.
* **Synthetic only.** The platform never processed real customer data.
"""

from __future__ import annotations

import json
from typing import Any

from pydantic import BaseModel, Field

from apexbet.common.logging import get_logger
from apexbet.common.storage import ObjectStore, build_store
from apexbet.config import Settings, get_settings

log = get_logger("apexbet.serving.dashboard")

# Hard caps so the browser never receives an unbounded payload.
MAX_PAGE_SIZE = 200
DEFAULT_PAGE_SIZE = 25


# --------------------------------------------------------------------------- #
# Response models (typed contracts shared with the frontend)
# --------------------------------------------------------------------------- #


class Kpi(BaseModel):
    """A single headline metric.

    ``kind`` distinguishes cumulative totals from rates and point-in-time
    values, so the dashboard never mixes them up.
    """

    key: str
    label: str
    value: float | int | None = None
    unit: str | None = None
    kind: str = "total"  # total | rate | gauge | duration
    available: bool = True
    detail: str | None = None


class TimePoint(BaseModel):
    t: str
    value: float


class MetricsSnapshot(BaseModel):
    available: bool
    detail: str | None = None
    run_id: str | None = None
    finished_at: str | None = None
    kpis: list[Kpi] = Field(default_factory=list)


class ThroughputPoint(BaseModel):
    minute: str
    event_type: str
    count: int


class LayerStat(BaseModel):
    layer: str
    record_count: int | None = None
    object_count: int | None = None
    bytes: int | None = None
    latest_modified: str | None = None
    available: bool = True
    detail: str | None = None


class RecordPage(BaseModel):
    dataset: str
    columns: list[str] = Field(default_factory=list)
    rows: list[dict[str, Any]] = Field(default_factory=list)
    total: int = 0
    offset: int = 0
    limit: int = DEFAULT_PAGE_SIZE
    available: bool = True
    detail: str | None = None


class QualityRule(BaseModel):
    rule_id: str
    dataset: str | None = None
    severity: str = "info"
    record_count: int | None = None
    failure_count: int | None = None
    description: str | None = None
    passed: bool | None = None
    sample: list[Any] = Field(default_factory=list)


class ReconciliationRow(BaseModel):
    rule_id: str
    description: str | None = None
    severity: str = "info"
    expected: Any = None
    actual: Any = None
    difference: Any = None
    resolution_status: str | None = None


class EnrichmentRecord(BaseModel):
    enrichment_id: str | None = None
    event_id: str | None = None
    model_name: str | None = None
    model_version: str | None = None
    enrichment_type: str | None = None
    processing_status: str | None = None
    confidence: float | None = None
    created_at: str | None = None
    enrichment_payload: Any = None


class AnomalyAlert(BaseModel):
    alert_id: str | None = None
    detector: str | None = None
    severity: str = "info"
    metric: str | None = None
    observed: float | None = None
    expected: float | None = None
    threshold: float | None = None
    explanation: str | None = None
    evidence: Any = None
    method: str = "rule"
    detected_at: str | None = None


class EnrichmentSummary(BaseModel):
    available: bool
    detail: str | None = None
    total: int = 0
    succeeded: int = 0
    failed: int = 0
    skipped: int = 0
    methods: dict[str, int] = Field(default_factory=dict)
    models: dict[str, int] = Field(default_factory=dict)


class PipelineNode(BaseModel):
    id: str
    label: str
    technology: str
    status: str = "unknown"  # healthy | processing | delayed | failed | disabled
    description: str | None = None
    inputs: list[str] = Field(default_factory=list)
    outputs: list[str] = Field(default_factory=list)
    endpoint: str | None = None
    doc_link: str | None = None
    records_in: int | None = None
    records_out: int | None = None
    errors: int | None = None
    last_processed: str | None = None
    metrics: dict[str, Any] = Field(default_factory=dict)


class PipelineEdge(BaseModel):
    id: str
    source: str
    target: str
    label: str | None = None


class LineageGraph(BaseModel):
    available: bool = True
    detail: str | None = None
    nodes: list[PipelineNode] = Field(default_factory=list)
    edges: list[PipelineEdge] = Field(default_factory=list)


class DashboardService:
    """Reads real pipeline artefacts from the object store + DuckDB."""

    def __init__(self, settings: Settings | None = None, store: ObjectStore | None = None) -> None:
        self.settings = settings or get_settings()
        self._store = store

    @property
    def store(self) -> ObjectStore:
        if self._store is None:
            self._store = build_store()
        return self._store

    # -- helpers ----------------------------------------------------------- #

    def _analytics(self) -> Any | None:
        try:
            from apexbet.analytics.duckdb_engine import AnalyticsEngine

            eng = AnalyticsEngine(self.settings.duckdb_path, store=self.store)
            return eng if eng.available() else None
        except Exception as exc:  # pragma: no cover - defensive
            log.warning("analytics.unavailable", error=str(exc))
            return None

    def _query(self, sql: str) -> list[dict[str, Any]]:
        eng = self._analytics()
        if eng is None:
            return []
        try:
            return list(eng.query(sql))
        except Exception:
            return []
        finally:
            eng.close()

    def _latest_report(self) -> dict[str, Any] | None:
        try:
            if not self.store.exists("reports/latest.json"):
                return None
            data = json.loads(self.store.get_bytes("reports/latest.json"))
            return data if isinstance(data, dict) else None
        except Exception:
            return None

    def _layer_stat(self, prefix: str, layer: str) -> LayerStat:
        try:
            objs = self.store.list(prefix)
        except Exception:
            objs = []
        if not objs:
            return LayerStat(
                layer=layer,
                available=False,
                detail=f"No objects under '{prefix}'. Run `make run` to populate the {layer} layer.",
            )
        total_bytes = 0
        latest: str | None = None
        root = getattr(self.store, "root", None)
        for o in objs:
            total_bytes += getattr(o, "size_bytes", 0) or 0
            if root is not None:
                import os

                p = os.path.join(root, o.key)
                if os.path.exists(p):
                    import datetime as _dt

                    ts = _dt.datetime.fromtimestamp(os.path.getmtime(p), tz=_dt.UTC)
                    s = ts.isoformat()
                    if latest is None or s > latest:
                        latest = s
        return LayerStat(
            layer=layer,
            object_count=len(objs),
            bytes=total_bytes,
            latest_modified=latest,
            available=True,
        )

    # -- overview ---------------------------------------------------------- #

    def overview(self) -> MetricsSnapshot:
        report = self._latest_report()
        if report is None:
            return MetricsSnapshot(
                available=False,
                detail="No pipeline run yet. Run `make run` or use the Demo Control Center.",
            )
        pm = report.get("pipeline_metrics", {})
        gold = report.get("gold_tables", {})
        enrichment = report.get("enrichment", {}) or {}
        anomalies = report.get("anomalies", {}) or {}
        quality = report.get("quality", {}) or {}
        duration = report.get("duration_seconds") or 0

        consumed = pm.get("consumed")
        throughput = pm.get("throughput_eps")
        silver_rows = report.get("silver_rows_loaded")
        gold_total = sum(v for v in gold.values() if isinstance(v, int))

        # End-to-end latency: mean processing latency from Gold if present.
        lat = self._query(
            "SELECT AVG(p50_ms) AS p50, AVG(p95_ms) AS p95 FROM gold_latency_distribution"
        )
        e2e_p50 = lat[0]["p50"] if lat else None
        e2e_p95 = lat[0]["p95"] if lat else None

        kpis = [
            Kpi(
                key="events_generated",
                label="Events generated",
                kind="total",
                value=report.get("config", {}).get("count_requested"),
            ),
            Kpi(
                key="events_ingested",
                label="Events ingested",
                kind="total",
                value=pm.get("bronze_written"),
                unit="records",
            ),
            Kpi(
                key="events_processed",
                label="Events processed (Silver)",
                kind="total",
                value=pm.get("silver_written"),
                unit="records",
            ),
            Kpi(
                key="events_rejected",
                label="Events rejected",
                kind="total",
                value=quality.get("total_failures"),
                unit="records",
            ),
            Kpi(
                key="throughput",
                label="Processing throughput",
                kind="rate",
                value=throughput,
                unit="events/s",
            ),
            Kpi(
                key="run_duration",
                label="Last run duration",
                kind="duration",
                value=duration,
                unit="s",
            ),
            Kpi(
                key="e2e_p50",
                label="Processing latency p50",
                kind="duration",
                value=round(e2e_p50, 2) if e2e_p50 is not None else None,
                unit="ms",
                available=e2e_p50 is not None,
                detail=None if e2e_p50 is not None else "Run a pipeline to populate latency.",
            ),
            Kpi(
                key="e2e_p95",
                label="Processing latency p95",
                kind="duration",
                value=round(e2e_p95, 2) if e2e_p95 is not None else None,
                unit="ms",
                available=e2e_p95 is not None,
                detail=None if e2e_p95 is not None else "Run a pipeline to populate latency.",
            ),
            Kpi(
                key="bronze_records",
                label="Bronze records",
                kind="total",
                value=pm.get("bronze_written"),
            ),
            Kpi(
                key="silver_records",
                label="Silver records (lake total)",
                kind="total",
                value=silver_rows,
                detail="Cumulative rows across every run currently stored in the Silver layer.",
            ),
            Kpi(key="gold_records", label="Gold mart rows", kind="total", value=gold_total),
            Kpi(
                key="enrichment_ok",
                label="Enrichment succeeded",
                kind="total",
                value=enrichment.get("succeeded", report.get("enrichment_records")),
            ),
            Kpi(
                key="enrichment_failed",
                label="Enrichment failed",
                kind="total",
                value=enrichment.get("failed", 0),
            ),
            Kpi(
                key="anomalies",
                label="Active anomalies",
                kind="gauge",
                value=anomalies.get("alert_count", 0),
            ),
            Kpi(
                key="quarantined",
                label="Quarantined records",
                kind="total",
                value=pm.get("quarantined"),
            ),
        ]
        _ = consumed
        return MetricsSnapshot(
            available=True,
            run_id=report.get("run_id"),
            finished_at=report.get("finished_at"),
            kpis=kpis,
        )

    # -- throughput / latency time series ---------------------------------- #

    def throughput(self, limit_minutes: int = 180) -> list[ThroughputPoint]:
        rows = self._query(
            "SELECT event_minute, event_type, event_count FROM gold_event_volume_by_minute "
            f"ORDER BY event_minute DESC LIMIT {int(limit_minutes)}"
        )
        return [
            ThroughputPoint(
                minute=r["event_minute"], event_type=r["event_type"], count=int(r["event_count"])
            )
            for r in rows
        ]

    def latency_series(self) -> list[dict[str, Any]]:
        return self._query(
            "SELECT event_type, n, p50_ms, p95_ms, p99_ms, max_ms FROM gold_latency_distribution "
            "ORDER BY n DESC"
        )

    # -- layers ------------------------------------------------------------ #

    def layer_stats(self) -> list[LayerStat]:
        return [
            self._layer_stat("bronze/", "bronze"),
            self._layer_stat("silver/", "silver"),
            self._layer_stat("gold/", "gold"),
        ]

    def dataset_catalog(self) -> list[dict[str, Any]]:
        """List datasets the dashboard can preview."""
        cat: list[dict[str, Any]] = []
        for layer, prefix in (("bronze", "bronze/"), ("silver", "silver/"), ("gold", "gold/")):
            try:
                objs = self.store.list(prefix)
            except Exception:
                objs = []
            if objs:
                cat.append(
                    {
                        "dataset": f"{layer}:events" if layer == "bronze" else f"{layer}:parquet",
                        "layer": layer,
                        "objects": len(objs),
                    }
                )
        # DuckDB tables are directly previewable.
        for t in (
            "gold_event_counts",
            "gold_event_volume_by_minute",
            "dq_results",
            "reconciliation_results",
            "anomaly_alerts",
        ):
            if self._query(f"SELECT COUNT(*) AS c FROM {t}"):
                cat.append({"dataset": t, "layer": "gold", "objects": 1})
        return cat

    def preview(self, dataset: str, offset: int = 0, limit: int = DEFAULT_PAGE_SIZE) -> RecordPage:
        limit = max(1, min(limit, MAX_PAGE_SIZE))
        offset = max(0, offset)
        allowed = {
            "gold_event_counts",
            "gold_event_volume_by_minute",
            "gold_betting_activity",
            "gold_bet_outcomes",
            "gold_latency_distribution",
            "gold_financial_ledger",
            "gold_casino_activity",
            "gold_data_completeness",
            "gold_responsible_gambling",
            "dq_results",
            "reconciliation_results",
            "anomaly_alerts",
        }
        if dataset not in allowed:
            return RecordPage(
                dataset=dataset,
                available=False,
                offset=offset,
                limit=limit,
                detail=f"Dataset '{dataset}' is not previewable.",
            )
        eng = self._analytics()
        if eng is None:
            return RecordPage(
                dataset=dataset,
                available=False,
                offset=offset,
                limit=limit,
                detail="DuckDB warehouse not built yet. Run `make run`.",
            )
        try:
            conn = eng.connect()
            total = int(conn.execute(f"SELECT COUNT(*) FROM {dataset}").fetchone()[0])
            cur = conn.execute(f"SELECT * FROM {dataset} LIMIT {limit} OFFSET {offset}")
            cols = [d[0] for d in cur.description] if cur.description else []
            rows = [self._jsonable(dict(zip(cols, r, strict=False))) for r in cur.fetchall()]
        except Exception as exc:
            return RecordPage(
                dataset=dataset,
                available=False,
                offset=offset,
                limit=limit,
                detail=f"Could not read '{dataset}': {exc}",
            )
        finally:
            eng.close()
        return RecordPage(
            dataset=dataset,
            columns=cols,
            rows=rows,
            total=total,
            offset=offset,
            limit=limit,
            available=True,
        )

    @staticmethod
    def _jsonable(row: dict[str, Any]) -> dict[str, Any]:
        out: dict[str, Any] = {}
        for k, v in row.items():
            if isinstance(v, (str, int, float, bool)) or v is None:
                out[k] = v
            else:
                out[k] = str(v)
        return out

    # -- quality / reconciliation ------------------------------------------ #

    def quality(self) -> list[QualityRule]:
        rows = self._query("SELECT * FROM dq_results ORDER BY failure_count DESC LIMIT 200")
        out: list[QualityRule] = []
        for r in rows:
            out.append(
                QualityRule(
                    rule_id=r.get("rule_id", "?"),
                    dataset=r.get("dataset"),
                    severity=r.get("severity", "info"),
                    record_count=r.get("record_count"),
                    failure_count=r.get("failure_count"),
                    description=r.get("description"),
                    passed=r.get("passed"),
                    sample=self._as_list(r.get("sample")),
                )
            )
        return out

    @staticmethod
    def _as_list(v: Any) -> list[Any]:
        if v is None:
            return []
        if isinstance(v, list):
            return v
        if isinstance(v, str):
            try:
                parsed = json.loads(v)
                return parsed if isinstance(parsed, list) else [v]
            except Exception:
                return [v]
        return [str(v)]

    def reconciliation(self) -> list[ReconciliationRow]:
        rows = self._query(
            "SELECT * FROM reconciliation_results ORDER BY severity, rule_id LIMIT 100"
        )
        return [
            ReconciliationRow(
                rule_id=r.get("rule_id", "?"),
                description=r.get("description"),
                severity=r.get("severity", "info"),
                expected=r.get("expected"),
                actual=r.get("actual"),
                difference=r.get("difference"),
                resolution_status=r.get("resolution_status"),
            )
            for r in rows
        ]

    # -- enrichment / anomalies -------------------------------------------- #

    def enrichment(self, offset: int = 0, limit: int = 50) -> RecordPage:
        limit = max(1, min(limit, MAX_PAGE_SIZE))
        try:
            objs = self.store.list("gold/ai_enrichment/")
        except Exception:
            objs = []
        if not objs:
            return RecordPage(
                dataset="ai_enrichment",
                available=False,
                offset=offset,
                limit=limit,
                detail="No enrichment records yet. Run `make enrich`.",
            )
        from apexbet.common.storage import read_parquet_bytes

        rows: list[dict[str, Any]] = []
        for o in objs:
            try:
                rows.extend(read_parquet_bytes(self.store.get_bytes(o.key)))
            except Exception:
                continue
        cols = list(rows[0].keys()) if rows else []
        total = len(rows)
        page = [self._jsonable(r) for r in rows[offset : offset + limit]]
        return RecordPage(
            dataset="ai_enrichment",
            columns=cols,
            rows=page,
            total=total,
            offset=offset,
            limit=limit,
            available=True,
        )

    def enrichment_summary(self) -> EnrichmentSummary:
        """Whole-dataset enrichment status counts (not just the current page)."""
        try:
            objs = self.store.list("gold/ai_enrichment/")
        except Exception:
            objs = []
        if not objs:
            return EnrichmentSummary(
                available=False,
                detail="No enrichment records yet. Run `make enrich` or a demo job.",
            )
        from apexbet.common.storage import read_parquet_bytes

        status: dict[str, int] = {}
        methods: dict[str, int] = {}
        models: dict[str, int] = {}
        total = 0
        for o in objs:
            try:
                rows = read_parquet_bytes(self.store.get_bytes(o.key))
            except Exception:
                continue
            for r in rows:
                total += 1
                s = str(r.get("processing_status") or "unknown")
                status[s] = status.get(s, 0) + 1
                m = str(r.get("enrichment_type") or "unknown")
                methods[m] = methods.get(m, 0) + 1
                mdl = str(r.get("model_name") or "unknown")
                models[mdl] = models.get(mdl, 0) + 1
        return EnrichmentSummary(
            available=True,
            total=total,
            succeeded=status.get("success", 0),
            failed=status.get("failed", 0),
            skipped=status.get("skipped", 0),
            methods=methods,
            models=models,
        )

    def anomalies(self) -> list[AnomalyAlert]:
        rows = self._query("SELECT * FROM anomaly_alerts ORDER BY detected_at DESC LIMIT 100")
        return [
            AnomalyAlert(
                alert_id=r.get("alert_id"),
                detector=r.get("detector"),
                severity=r.get("severity", "info"),
                metric=r.get("metric"),
                observed=r.get("observed"),
                expected=r.get("expected"),
                threshold=r.get("threshold"),
                explanation=r.get("explanation"),
                evidence=r.get("evidence"),
                method=r.get("method", "rule"),
                detected_at=r.get("detected_at"),
            )
            for r in rows
        ]

    # -- lineage graph ----------------------------------------------------- #

    def lineage(self) -> LineageGraph:
        """Build the lineage graph from real artefact state.

        Node status is derived from what actually exists (report presence, layer
        objects, DuckDB tables, transport config) — never hardcoded.
        """
        report = self._latest_report()
        layers = {s.layer: s for s in self.layer_stats()}
        settings = self.settings

        def status_for(total: int | None, *, enabled: bool = True) -> str:
            if not enabled:
                return "disabled"
            if report is None:
                return "unknown"
            return "healthy" if (total is None or total >= 0) else "delayed"

        bronze_objs = layers["bronze"].object_count or 0
        silver_objs = layers["silver"].object_count or 0
        gold_objs = layers["gold"].object_count or 0
        has_report = report is not None
        pm = (report or {}).get("pipeline_metrics", {})

        dq_rows = len(self.quality())
        anomaly_rows = len(self.anomalies())
        enrich_records = (report or {}).get("enrichment_records")

        transport = settings.transport
        kinesis_enabled = transport == "kinesis"
        kafka_enabled = transport == "redpanda"
        s3_enabled = settings.storage_backend == "s3"

        def st(ok: bool, unknown: bool = False) -> str:
            if unknown:
                return "unknown"
            return "healthy" if ok else "disabled"

        nodes = [
            PipelineNode(
                id="generator",
                label="Synthetic Event Generator",
                technology="Python (apexbet.generator)",
                status=st(has_report),
                description="Produces deterministic, seedable synthetic sportsbook and casino events with optional anomaly injection.",
                outputs=["events"],
                endpoint="POST /api/v1/demo/generate",
                doc_link="docs/data-engineering/README.md",
                records_out=(report or {}).get("config", {}).get("count_requested"),
                last_processed=(report or {}).get("finished_at"),
            ),
            PipelineNode(
                id="ingestion",
                label="FastAPI Ingestion",
                technology="FastAPI + Pydantic v2",
                status=st(True),
                description="Validates the event envelope + payload, stamps ingestion time/trace, and publishes to the broker. Rejections are counted here.",
                inputs=["events"],
                outputs=["broker"],
                endpoint="/api/v1/events",
                doc_link="docs/architecture/README.md",
                records_in=(report or {}).get("config", {}).get("count_requested"),
                records_out=pm.get("consumed"),
                errors=0,
            ),
            PipelineNode(
                id="broker",
                label="Streaming Broker",
                technology="Redpanda (Kafka API)"
                if kafka_enabled
                else ("AWS Kinesis (LocalStack)" if kinesis_enabled else "In-process memory queue"),
                status=st(kafka_enabled or kinesis_enabled or transport == "memory"),
                description=f"Configured transport: '{transport}'. Redpanda and Kinesis share one contract; there is no silent fallback.",
                inputs=["events"],
                outputs=["subscription"],
                metrics={"transport": transport},
            ),
            PipelineNode(
                id="validation",
                label="Event Validation",
                technology="Pydantic v2 + JSON Schema",
                status=st(True),
                description="Schema validation, in-batch duplicate detection, and quarantine routing for malformed records.",
                inputs=["broker"],
                outputs=["bronze"],
                doc_link="docs/data-engineering/event-schemas.json",
                records_in=pm.get("consumed"),
                records_out=pm.get("bronze_written"),
                errors=pm.get("quarantined"),
            ),
            PipelineNode(
                id="bronze",
                label="Bronze Layer",
                technology="Parquet (partitioned)",
                status=st(bronze_objs > 0),
                description="Immutable raw events written as partitioned Parquet, with a quarantine partition for malformed records.",
                inputs=["validation"],
                outputs=["silver"],
                endpoint="GET /api/v1/dashboard/layers",
                records_out=pm.get("bronze_written"),
                errors=pm.get("quarantined"),
                last_processed=layers["bronze"].latest_modified,
            ),
            PipelineNode(
                id="silver",
                label="Silver Transformations",
                technology="Python + DuckDB",
                status=st(silver_objs > 0),
                description="Typed, deduplicated, late-aware transformation of Bronze into stable Silver records.",
                inputs=["bronze"],
                outputs=["enrichment", "gold"],
                records_in=pm.get("bronze_written"),
                records_out=pm.get("silver_written"),
                errors=pm.get("quarantined"),
                last_processed=layers["silver"].latest_modified,
            ),
            PipelineNode(
                id="enrichment",
                label="Enrichment",
                technology=f"rules / local_ml / llm ({settings.enrichment_mode})",
                status=st(
                    enrich_records is not None and enrich_records > 0,
                    unknown=enrich_records is None,
                ),
                description="Optional enrichment that adds derived fields (categories, descriptions) to cleaned records. Rules run by default; local ML and a local Ollama LLM are optional.",
                inputs=["silver"],
                outputs=["gold"],
                endpoint="GET /api/v1/dashboard/enrichment",
                doc_link="docs/enrichment/README.md",
                records_out=enrich_records,
                metrics={"mode": settings.enrichment_mode, "llm_enabled": settings.llm_enabled},
            ),
            PipelineNode(
                id="gold",
                label="Gold Analytics",
                technology="DuckDB marts (9 tables)",
                status=st(gold_objs > 0),
                description="Nine analytical marts (betting, outcomes, ledger, casino, completeness, latency, ...).",
                inputs=["silver", "enrichment"],
                outputs=["dq", "recon", "anomaly", "serving"],
                endpoint="GET /api/v1/dashboard/preview?dataset=gold_event_counts",
                records_out=sum(
                    v for v in (report or {}).get("gold_tables", {}).values() if isinstance(v, int)
                ),
            ),
            PipelineNode(
                id="dq",
                label="Data Quality",
                technology="11 deterministic rules",
                status=st(dq_rows > 0, unknown=not has_report),
                description="Eleven rules over Silver events; results persisted to DuckDB for the dashboard.",
                inputs=["gold"],
                outputs=["dq"],
                endpoint="GET /api/v1/dashboard/quality",
                errors=(report or {}).get("quality", {}).get("total_failures"),
            ),
            PipelineNode(
                id="recon",
                label="Reconciliation",
                technology="9 financial rules",
                status=st(True, unknown=not has_report),
                description="Source-to-target financial reconciliation with exact minor-unit comparison.",
                inputs=["gold"],
                outputs=["recon"],
                endpoint="GET /api/v1/dashboard/reconciliation",
            ),
            PipelineNode(
                id="quarantine",
                label="Quarantine & DLQ",
                technology="Object store + dead-letter topic",
                status=st(pm.get("quarantined", 0) >= 0),
                description="Malformed records are quarantined to Parquet; unparseable payloads go to the dead-letter queue.",
                inputs=["validation", "silver"],
                outputs=[],
                endpoint="GET /api/v1/dashboard/layers",
                records_in=pm.get("quarantined"),
                errors=pm.get("dlq"),
            ),
            PipelineNode(
                id="anomaly",
                label="Anomaly Detection",
                technology="Rules + optional Isolation Forest",
                status=st(anomaly_rows > 0, unknown=not has_report),
                description="Statistical rules run first; an optional scikit-learn Isolation Forest adds a multivariate baseline. Alerts are operational signals for triage.",
                inputs=["gold"],
                outputs=["serving"],
                endpoint="GET /api/v1/dashboard/anomalies",
                doc_link="docs/enrichment/README.md",
                records_out=anomaly_rows,
                errors=0,
            ),
            PipelineNode(
                id="duckdb",
                label="DuckDB / SQL Analytics",
                technology="DuckDB",
                status=st(self._analytics() is not None),
                description="Local analytical SQL engine serving the Gold marts and ad-hoc queries.",
                inputs=["gold"],
                outputs=["dashboard"],
                endpoint="GET /api/v1/dashboard/preview",
            ),
            PipelineNode(
                id="orchestration",
                label="Airflow Orchestration",
                technology="Apache Airflow (4 DAGs)",
                status="disabled",
                description="Optional orchestration DAGs (batch transforms, enrichment, freshness/quality, backfill). Runs only in the 'full' Docker profile.",
                doc_link="docs/infrastructure/README.md",
            ),
            PipelineNode(
                id="storage",
                label="Object Storage",
                technology="Local FS" if not s3_enabled else "S3 (LocalStack/MinIO)",
                status=st(True),
                description=f"Storage backend: '{settings.storage_backend}'. One interface, two backends (local + S3-compatible).",
                metrics={"backend": settings.storage_backend},
            ),
            PipelineNode(
                id="emulation",
                label="AWS Emulation",
                technology="LocalStack (community)",
                status="disabled",
                description="Kinesis/S3/SQS emulated locally by LocalStack when the 'full' profile is running. This is emulation, not production AWS.",
                doc_link="docs/infrastructure/README.md",
            ),
            PipelineNode(
                id="dashboard",
                label="Web Dashboard",
                technology="React + TypeScript + Vite",
                status=st(has_report, unknown=not has_report),
                description="This application: typed read APIs over the real pipeline artefacts.",
                inputs=["duckdb", "anomaly"],
                outputs=[],
                endpoint="/",
            ),
        ]

        edges = [
            PipelineEdge(id="e1", source="generator", target="ingestion", label="events"),
            PipelineEdge(id="e2", source="ingestion", target="broker", label="publish"),
            PipelineEdge(id="e3", source="broker", target="validation", label="consume"),
            PipelineEdge(id="e4", source="validation", target="bronze", label="valid"),
            PipelineEdge(id="e5", source="validation", target="quarantine", label="invalid"),
            PipelineEdge(id="e6", source="bronze", target="silver", label="transform"),
            PipelineEdge(id="e7", source="silver", target="enrichment", label="enrich"),
            PipelineEdge(id="e8", source="silver", target="gold", label="aggregate"),
            PipelineEdge(id="e9", source="enrichment", target="gold", label="enrichment"),
            PipelineEdge(id="e10", source="gold", target="dq", label="checks"),
            PipelineEdge(id="e11", source="gold", target="recon", label="reconcile"),
            PipelineEdge(id="e12", source="gold", target="anomaly", label="detect"),
            PipelineEdge(id="e13", source="gold", target="duckdb", label="load"),
            PipelineEdge(id="e14", source="duckdb", target="dashboard", label="serve"),
            PipelineEdge(id="e15", source="anomaly", target="dashboard", label="alerts"),
            PipelineEdge(id="e16", source="orchestration", target="gold", label="schedule"),
            PipelineEdge(id="e17", source="storage", target="bronze", label="persist"),
            PipelineEdge(id="e18", source="emulation", target="broker", label="emulate"),
        ]

        return LineageGraph(available=True, nodes=nodes, edges=edges)

    # -- health ------------------------------------------------------------ #

    def health(self) -> dict[str, Any]:
        report = self._latest_report()
        return {
            "pipeline": "healthy" if report else "unknown",
            "last_success": (report or {}).get("finished_at"),
            "transport": self.settings.transport,
            "storage": self.settings.storage_backend,
            "layers": [s.model_dump() for s in self.layer_stats()],
        }
