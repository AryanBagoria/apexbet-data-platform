"""Shared helpers for Airflow DAGs.

Airflow is part of the *extended* ("full") profile. DAGs call the same
library functions used by the CLI so orchestration never duplicates business
logic. Tasks are idempotent: re-running a task overwrites the same partitions /
recomputes the same deterministic outputs.

This module imports Airflow lazily so the rest of the repo (and unit tests) do
not require Airflow to be installed.
"""

from __future__ import annotations

import json
import os
from datetime import timedelta


def default_args(owner: str = "apexbet") -> dict:
    return {
        "owner": owner,
        "depends_on_past": False,
        "retries": 2,
        "retry_delay": timedelta(minutes=1),
        "execution_timeout": timedelta(minutes=30),
        "email_on_failure": False,
    }


def lake_path(*parts: str) -> str:
    root = os.environ.get("APEXBET_LAKE_ROOT", "./lake")
    return os.path.join(root, *parts)


def report_path(name: str) -> str:
    return lake_path("reports", f"airflow_{name}.json")


def write_report(name: str, payload: dict) -> str:
    path = report_path(name)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(payload, fh, indent=2, default=str)
    return path


# Each task function below is intentionally thin and calls the platform library.
def task_build_silver() -> dict:
    """Rebuild Silver from Bronze (batch)."""
    from apexbet.common.storage import build_store
    from apexbet.orchestration import read_all, read_bronze_events
    from apexbet.silver.transform import dedupe_events, transform_event

    store = build_store()
    bronze = read_bronze_events(store)
    records = []
    quarantined = 0
    for raw in bronze:
        res = transform_event(raw, allowed_lateness_seconds=600)
        if res.ok and res.record:
            records.append(res.record)
        else:
            quarantined += 1
    unique, dropped = dedupe_events(records)
    summary = {
        "bronze": len(bronze),
        "silver": len(unique),
        "deduped": dropped,
        "quarantined": quarantined,
        "existing_silver": len(read_all(store, "silver/events")),
    }
    return write_report("build_silver", summary) and summary


def task_build_gold() -> dict:
    from apexbet.analytics.duckdb_engine import AnalyticsEngine
    from apexbet.common.storage import build_store
    from apexbet.config import get_settings

    store = build_store()
    engine = AnalyticsEngine(get_settings().duckdb_path, store=store)
    engine.load_silver()
    counts = engine.build_gold()
    return write_report("build_gold", counts) and counts


def task_data_quality() -> dict:
    from apexbet.common.storage import build_store
    from apexbet.orchestration import read_all
    from apexbet.quality.engine import DataQualityEngine, quality_summary

    store = build_store()
    rows = read_all(store, "silver/events")
    events = [
        {
            "event_id": r.get("event_id"),
            "event_type": r.get("event_type"),
            "schema_version": r.get("schema_version"),
            "event_timestamp": r.get("event_timestamp"),
            "partition_key": r.get("partition_key"),
            "payload": r.get("payload"),
        }
        for r in rows
    ]
    results, quarantine = DataQualityEngine().run_all(events)
    summary = quality_summary(results)
    summary["quarantine_records"] = len(quarantine)
    return write_report("data_quality", summary) and summary


def task_reconcile() -> dict:
    from apexbet.common.storage import build_store
    from apexbet.common.timeutil import to_iso, utcnow
    from apexbet.orchestration import read_all, read_bronze_events
    from apexbet.reconciliation.engine import reconcile

    store = build_store()
    bronze = read_bronze_events(store)
    silver = read_all(store, "silver/events")
    report = reconcile(bronze, silver, processed_at=to_iso(utcnow()))
    return write_report("reconcile", report.summary()) and report.summary()


def task_enrichment_retry() -> dict:
    from apexbet.common.storage import build_store
    from apexbet.config import get_settings
    from apexbet.enrichment.enricher import EnrichmentRunner, build_enricher
    from apexbet.orchestration import read_all

    settings = get_settings()
    store = build_store()
    rows = read_all(store, "silver/events")[:1000]
    enricher = build_enricher(
        settings.enrichment_mode,
        llm_base_url=settings.llm_base_url,
        llm_model=settings.llm_model,
        llm_timeout=settings.llm_timeout_seconds,
    )
    runner = EnrichmentRunner(enricher)
    runner.run(rows)
    stats = runner.stats.to_dict()
    return write_report("enrichment_retry", stats) and stats


def task_anomaly_eval() -> dict:
    from apexbet.anomaly.evaluation import evaluate_all

    result = evaluate_all()
    return write_report("anomaly_eval", result) and result


def task_freshness() -> dict:
    from apexbet.common.storage import build_store
    from apexbet.common.timeutil import parse_iso, utcnow
    from apexbet.orchestration import read_all

    store = build_store()
    rows = read_all(store, "silver/events")
    if not rows:
        return {"freshness_ok": False, "detail": "no silver events"}
    newest = max(parse_iso(r["event_timestamp"]) for r in rows if r.get("event_timestamp"))
    age = (utcnow() - newest).total_seconds()
    ok = age < 3600
    result = {"freshness_ok": ok, "age_seconds": round(age, 1)}
    return write_report("freshness", result) and result
