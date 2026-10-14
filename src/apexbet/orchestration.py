"""End-to-end orchestration.

`run_full_pipeline` runs: generate -> publish -> consume(Bronze+Silver) ->
quality -> Gold -> reconciliation -> anomaly -> enrichment -> persist results.
It is the backbone used by both the CLI and the demo, and by the Airflow DAGs
(which call the same functions so logic never diverges).
"""

from __future__ import annotations

import json
import os
import uuid
from typing import Any

from apexbet.common.ids import new_correlation_id
from apexbet.common.logging import get_logger
from apexbet.common.storage import ObjectStore, build_store, read_parquet_bytes
from apexbet.common.timeutil import to_iso, utcnow
from apexbet.config import Settings, get_settings
from apexbet.consumers import Pipeline
from apexbet.generator import AnomalyInjection, GeneratorConfig, SyntheticEventGenerator
from apexbet.transports import build_transport

log = get_logger("apexbet.orchestration")


def _gen_events(
    count: int,
    seed: int,
    anomalies: AnomalyInjection,
    num_accounts: int = 2000,
    num_matches: int = 120,
) -> list[dict[str, Any]]:
    gen = SyntheticEventGenerator(
        GeneratorConfig(
            seed=seed, num_accounts=num_accounts, num_matches=num_matches, anomalies=anomalies
        )
    )
    return list(gen.generate(count))


def read_all(store: ObjectStore, prefix: str) -> list[dict[str, Any]]:
    """Read every parquet object under a prefix into a list of row dicts."""
    rows: list[dict[str, Any]] = []
    for info in store.list(prefix):
        if info.key.endswith(".parquet"):
            try:
                rows.extend(read_parquet_bytes(store.get_bytes(info.key)))
            except Exception as exc:  # pragma: no cover
                log.error("read.parquet_failed", key=info.key, error=str(exc))
    return rows


def read_bronze_events(store: ObjectStore) -> list[dict[str, Any]]:
    """Read Bronze and reconstruct the original event envelopes.

    Bronze stores the original event JSON verbatim in ``original_event_json``;
    reconstruction lets reconciliation compare source (Bronze) against target
    (Silver) using the same event shape.
    """
    out: list[dict[str, Any]] = []
    for row in read_all(store, "bronze/event_type="):
        raw = row.get("original_event_json")
        if raw:
            try:
                out.append(json.loads(raw))
                continue
            except Exception:
                pass
        # fall back to the flattened columns
        out.append(
            {
                "event_id": row.get("event_id"),
                "event_type": row.get("event_type"),
                "schema_version": row.get("schema_version"),
                "event_timestamp": row.get("event_timestamp"),
                "partition_key": row.get("partition_key"),
                "payload": json.loads(row.get("original_payload") or "{}"),
            }
        )
    return out


def _write_json(store: ObjectStore, key: str, obj: Any) -> None:
    store.put_bytes(
        key, json.dumps(obj, default=str, indent=2).encode("utf-8"), content_type="application/json"
    )


def _store_table(store: ObjectStore, engine: Any, table: str, rows: list[dict[str, Any]]) -> None:
    """Persist a list of row dicts as a DuckDB table via a temp parquet file.

    Uses parquet (not JSON string literals) so DuckDB gets a real typed relation
    and column names are preserved exactly.
    """
    import tempfile

    from apexbet.common.storage import parquet_bytes

    conn = engine.connect()
    tmpdir = tempfile.mkdtemp(prefix="apexbet-table-")
    try:
        path = os.path.join(tmpdir, f"{table}.parquet")
        with open(path, "wb") as fh:
            fh.write(parquet_bytes(rows))
        conn.execute(f"CREATE OR REPLACE TABLE {table} AS SELECT * FROM read_parquet('{path}')")
    finally:
        import shutil

        shutil.rmtree(tmpdir, ignore_errors=True)


def run_pipeline(
    *,
    count: int = 5000,
    seed: int = 1337,
    anomalies: AnomalyInjection | None = None,
    settings: Settings | None = None,
    transport: Any | None = None,
    store: ObjectStore | None = None,
    num_accounts: int = 2000,
    num_matches: int = 120,
    enrich: bool = True,
) -> dict[str, Any]:
    """Run generate -> ingest -> process. Returns a run report."""
    settings = settings or get_settings()
    store = store or build_store()
    transport = transport or build_transport(settings.transport, settings=settings)
    anomalies = anomalies or AnomalyInjection()

    run_id = f"run_{uuid.uuid4().hex[:10]}"
    correlation_id = new_correlation_id()
    started = utcnow()

    events = _gen_events(count, seed, anomalies, num_accounts, num_matches)

    # Publish (ingest). Invalid events still go into the stream; the pipeline
    # quarantines them, mirroring real validation-at-the-edge behaviour.
    items = [(e["partition_key"], json.dumps(e).encode("utf-8")) for e in events]
    transport.publish_many(settings.kafka_topic, items)

    pipeline = Pipeline(
        transport,
        store,
        source_topic=settings.kafka_topic,
        dlq_topic=settings.kafka_dlq_topic,
        group_id=settings.kafka_group_id,
        dedup_window_seconds=settings.dedup_window_seconds,
        allowed_lateness_seconds=settings.allowed_lateness_seconds,
        bronze_flush_records=settings.bronze_flush_records,
        bronze_flush_seconds=settings.bronze_flush_seconds,
        max_retries=settings.max_retries,
        retry_base_seconds=settings.retry_base_seconds,
    )
    # Drain until the stream is empty.
    pipeline.run(idle_stop_seconds=1.0)
    metrics = pipeline.metrics.to_dict()

    # Scope all downstream analytics to the events published by THIS run so
    # reconciliation and quality compare like-for-like even when the lake
    # already holds data from earlier runs.
    run_event_ids = {e["event_id"] for e in events}

    # Silver -> DuckDB -> Gold
    from apexbet.analytics.duckdb_engine import AnalyticsEngine

    engine = AnalyticsEngine(settings.duckdb_path, store=store)
    silver_rows = engine.load_silver()
    gold_counts = engine.build_gold()

    # Data quality on this run's silver
    silver_events = [
        r for r in read_all(store, "silver/events") if r.get("event_id") in run_event_ids
    ]
    from apexbet.quality.engine import DataQualityEngine, quality_summary

    dqe = DataQualityEngine()
    q_results, q_quarantine = dqe.run_all(
        [
            {
                "event_id": r.get("event_id"),
                "event_type": r.get("event_type"),
                "schema_version": r.get("schema_version"),
                "event_timestamp": r.get("event_timestamp"),
                "partition_key": r.get("partition_key"),
                "payload": r.get("payload"),
            }
            for r in silver_events
        ],
        expected_count=None,
    )
    q_sum = quality_summary(q_results)

    # Persist quality results into DuckDB so the API can serve them.
    if q_results:
        _store_table(store, engine, "dq_results", [r.to_dict() for r in q_results])

    # Reconciliation (bronze vs silver) scoped to this run.
    bronze_events = [e for e in read_bronze_events(store) if e.get("event_id") in run_event_ids]
    from apexbet.reconciliation.engine import reconcile

    report = reconcile(
        bronze_events,
        silver_events,
        processed_at=to_iso(utcnow()),
        known_quarantined=metrics.get("quarantined", 0),
        known_duplicates=metrics.get("duplicates", 0),
        quarantined_event_ids=pipeline.quarantined_event_ids,
    )
    if report.findings:
        _store_table(
            store, engine, "reconciliation_results", [f.to_dict() for f in report.findings]
        )

    # Anomaly detection
    from apexbet.anomaly.detector import AnomalyDetector

    detector = AnomalyDetector()
    per_min = engine.query(
        "SELECT event_minute, SUM(event_count) AS c FROM gold_event_volume_by_minute "
        "GROUP BY 1 ORDER BY 1"
    )
    series = [(r["event_minute"], float(r["c"])) for r in per_min]
    detector.detect_volume(series)
    detections = engine.query(
        "SELECT substr(event_timestamp,1,16) AS m, "
        "SUM(CASE WHEN event_type='BetRejected' THEN 1 ELSE 0 END) AS rej, COUNT(*) AS n "
        "FROM silver_events GROUP BY 1 ORDER BY 1"
    )
    detector.detect_ratio_spike(
        [(r["m"], float(r["rej"]), int(r["n"])) for r in detections], metric="rejection_rate"
    )
    detector.detect_duplicate_spike(
        metrics.get("duplicates", 0) / max(1, metrics.get("consumed", 1))
    )

    if detector.alerts:
        _store_table(store, engine, "anomaly_alerts", [a.to_dict() for a in detector.alerts])

    # Enrichment (rules by default; optional ML/LLM)
    enrichment_stats: dict[str, Any] = {}
    enrich_count = 0
    if enrich:
        from apexbet.enrichment.enricher import EnrichmentRunner, build_enricher

        enricher = build_enricher(
            settings.enrichment_mode,
            llm_base_url=settings.llm_base_url,
            llm_model=settings.llm_model,
            llm_timeout=settings.llm_timeout_seconds,
        )
        runner = EnrichmentRunner(enricher)
        sample = silver_events[: min(1000, len(silver_events))]
        records = runner.run(sample)
        if records:
            key = (
                f"gold/ai_enrichment/enrich_date={to_iso(utcnow())[:10]}/"
                f"part-{uuid.uuid4().hex[:12]}.parquet"
            )
            from apexbet.common.storage import parquet_bytes

            # Flatten payload for parquet friendliness.
            flat = []
            for r in records:
                r2 = dict(r)
                r2["enrichment_payload"] = json.dumps(r2.get("enrichment_payload", {}))
                flat.append(r2)
            store.put_bytes(key, parquet_bytes(flat), content_type="application/vnd.apache.parquet")
            enrich_count = len(records)
        enrichment_stats = runner.stats.to_dict()

    finished = utcnow()
    report_obj = {
        "run_id": run_id,
        "correlation_id": correlation_id,
        "started_at": to_iso(started),
        "finished_at": to_iso(finished),
        "duration_seconds": round((finished - started).total_seconds(), 3),
        "config": {
            "count_requested": count,
            "seed": seed,
            "anomalies": anomalies.to_dict(),
            "transport": settings.transport,
        },
        "pipeline_metrics": metrics,
        "silver_rows_loaded": silver_rows,
        "gold_tables": gold_counts,
        "quality": q_sum,
        "quality_results": [r.to_dict() for r in q_results],
        "quarantine_records": len(q_quarantine),
        "reconciliation": report.summary(),
        "anomalies": detector.summary(),
        "enrichment": enrichment_stats,
        "enrichment_records": enrich_count,
        "bronze_objects": len(store.list("bronze")),
        "silver_objects": len(store.list("silver")),
    }
    _write_json(store, f"reports/run_{run_id}.json", report_obj)
    _write_json(store, "reports/latest.json", report_obj)
    engine.close()
    return report_obj


def inject_anomaly(
    *, count: int = 3000, seed: int = 999, settings: Settings | None = None
) -> dict[str, Any]:
    """Inject a controlled anomaly burst (traffic spike + rejection spike +
    suspicious transactions) and process it, so the detector has something real
    to find. Returns the run report."""
    anomalies = AnomalyInjection(
        traffic_spike=True,
        spike_multiplier=10.0,
        rejection_spike=True,
        suspicious_transaction_pattern=True,
        duplicate_rate=0.05,
    )
    return run_pipeline(count=count, seed=seed, anomalies=anomalies, settings=settings)
