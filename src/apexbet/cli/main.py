"""ApexBet command-line interface."""

from __future__ import annotations

import json
import sys

import typer
from rich.console import Console
from rich.table import Table

from apexbet.common.logging import configure_logging
from apexbet.config import get_settings

app = typer.Typer(add_completion=False, help="ApexBet data platform CLI (synthetic data only).")
console = Console()


def _setup() -> None:
    configure_logging()


@app.command()
def version() -> None:
    """Print the platform version."""
    from apexbet import __version__

    console.print(f"ApexBet {__version__}")


@app.command()
def schemas(out: str = typer.Option("docs/data-engineering/event-schemas.json")) -> None:
    """Export JSON Schemas for all event contracts."""
    import os

    from apexbet.schemas import export_json_schemas

    os.makedirs(os.path.dirname(out), exist_ok=True)
    export_json_schemas(out)
    console.print(f"[green]wrote[/green] {out}")


@app.command()
def generate(
    count: int = typer.Option(1000, help="Number of events to generate."),
    out: str = typer.Option("./lake/raw_events.jsonl", help="Output JSONL path (- to stdout)."),
    seed: int = typer.Option(1337),
    dirty: bool = typer.Option(False, help="Inject anomalies (dupes/late/invalid)."),
    accounts: int = typer.Option(2000),
    matches: int = typer.Option(120),
    report: str | None = typer.Option(None, help="Write a generation report JSON here."),
) -> None:
    """Generate synthetic events to a JSONL file (streamed, low memory)."""
    import time

    from apexbet.generator import AnomalyInjection, GeneratorConfig, SyntheticEventGenerator

    _setup()
    anomalies = AnomalyInjection.demo() if dirty else AnomalyInjection.clean()
    gen = SyntheticEventGenerator(
        GeneratorConfig(seed=seed, num_accounts=accounts, num_matches=matches, anomalies=anomalies)
    )
    start = time.time()
    n = 0
    from collections import Counter
    from contextlib import nullcontext

    by_type: Counter[str] = Counter()
    # stdout for "-"; otherwise open the file and close it deterministically.
    if out == "-":
        with nullcontext(sys.stdout) as sink:
            for env in gen.generate(count):
                sink.write(json.dumps(env, separators=(",", ":")) + "\n")
                by_type[env["event_type"]] += 1
                n += 1
    else:
        with open(out, "w", encoding="utf-8") as sink:
            for env in gen.generate(count):
                sink.write(json.dumps(env, separators=(",", ":")) + "\n")
                by_type[env["event_type"]] += 1
                n += 1
    elapsed = time.time() - start
    summary = {
        "generated": n,
        "seed": seed,
        "dirty": dirty,
        "elapsed_seconds": round(elapsed, 3),
        "throughput_eps": round(n / max(1e-9, elapsed), 1),
        "output": out,
        "by_type": dict(by_type),
        "config": gen.config.to_dict(),
    }
    if report:
        with open(report, "w", encoding="utf-8") as fh:
            json.dump(summary, fh, indent=2)
    console.print(json.dumps({k: v for k, v in summary.items() if k != "by_type"}, indent=2))


@app.command()
def ingest(
    count: int = typer.Option(2000),
    seed: int = typer.Option(1337),
    dirty: bool = typer.Option(True),
) -> None:
    """Publish generated events into the configured streaming backend."""
    from apexbet.generator import AnomalyInjection, GeneratorConfig, SyntheticEventGenerator
    from apexbet.transports import build_transport

    _setup()
    settings = get_settings()
    transport = build_transport(settings.transport, settings=settings)
    anomalies = AnomalyInjection.demo() if dirty else AnomalyInjection.clean()
    gen = SyntheticEventGenerator(GeneratorConfig(seed=seed, anomalies=anomalies))
    batch: list[tuple[str | None, bytes]] = []
    published = 0
    for env in gen.generate(count):
        batch.append((env["partition_key"], json.dumps(env).encode("utf-8")))
        if len(batch) >= 500:
            published += transport.publish_many(settings.kafka_topic, batch)
            batch = []
    if batch:
        published += transport.publish_many(settings.kafka_topic, batch)
    console.print(
        f"[green]published[/green] {published} events to {settings.kafka_topic} "
        f"({settings.transport})"
    )


@app.command()
def process(
    count: int = typer.Option(5000),
    seed: int = typer.Option(1337),
    dirty: bool = typer.Option(True),
    max_batches: int | None = typer.Option(None),
) -> None:
    """Run the streaming consumer pipeline (Bronze + Silver)."""
    from apexbet.common.storage import build_store
    from apexbet.consumers import Pipeline
    from apexbet.transports import build_transport

    _setup()
    settings = get_settings()
    transport = build_transport(settings.transport, settings=settings)
    store = build_store()
    pipeline = Pipeline(
        transport,
        store,
        source_topic=settings.kafka_topic,
        dlq_topic=settings.kafka_dlq_topic,
        group_id=settings.kafka_group_id,
        dedup_window_seconds=settings.dedup_window_seconds,
        allowed_lateness_seconds=settings.allowed_lateness_seconds,
    )
    result = pipeline.run(max_batches=max_batches, idle_stop_seconds=2.0)
    console.print(json.dumps(result, indent=2))


@app.command()
def run(
    count: int = typer.Option(5000),
    seed: int = typer.Option(1337),
    dirty: bool = typer.Option(True),
) -> None:
    """Run the full pipeline: generate -> process -> Gold -> quality -> reconcile -> enrich."""
    from apexbet.generator import AnomalyInjection
    from apexbet.orchestration import run_pipeline

    _setup()
    anomalies = AnomalyInjection.demo() if dirty else AnomalyInjection.clean()
    report = run_pipeline(count=count, seed=seed, anomalies=anomalies)
    _print_run_report(report)


@app.command()
def analytics() -> None:
    """Build Gold marts from Silver parquet and print key results."""
    from apexbet.analytics.duckdb_engine import AnalyticsEngine
    from apexbet.common.storage import build_store

    _setup()
    settings = get_settings()
    store = build_store()
    engine = AnalyticsEngine(settings.duckdb_path, store=store)
    n = engine.load_silver()
    counts = engine.build_gold()
    console.print(f"loaded {n} silver rows")
    table = Table(title="Gold marts")
    table.add_column("table")
    table.add_column("rows", justify="right")
    for k, v in counts.items():
        table.add_row(k, str(v))
    console.print(table)
    console.print("\n[bold]Bet outcomes[/bold]")
    for r in engine.query("SELECT * FROM gold_bet_outcomes"):
        console.print(r)


@app.command()
def quality(
    limit: int = typer.Option(100, help="Show first N silver events in the DQ run."),
) -> None:
    """Run data-quality checks over Silver events and store results."""
    from apexbet.common.storage import build_store
    from apexbet.orchestration import read_all
    from apexbet.quality.engine import DataQualityEngine, quality_summary

    _setup()
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
    engine = DataQualityEngine()
    results, quarantine = engine.run_all(events)
    summary = quality_summary(results)
    table = Table(title="Data quality")
    for c in ("rule", "severity", "records", "failures", "passed"):
        table.add_column(c)
    for r in results:
        table.add_row(
            r.rule_id,
            r.severity,
            str(r.record_count),
            str(r.failure_count),
            "yes" if r.passed else "[red]NO[/red]",
        )
    console.print(table)
    console.print(json.dumps(summary, indent=2))
    console.print(f"quarantined records: {len(quarantine)}")


@app.command()
def reconcile() -> None:
    """Run source-to-target financial reconciliation and store the report."""
    from apexbet.common.storage import build_store
    from apexbet.common.timeutil import to_iso, utcnow
    from apexbet.orchestration import read_all, read_bronze_events
    from apexbet.reconciliation.engine import reconcile as do_reconcile

    _setup()
    store = build_store()
    bronze = read_bronze_events(store)
    silver = read_all(store, "silver/events")
    report = do_reconcile(bronze, silver, processed_at=to_iso(utcnow()))
    table = Table(title="Reconciliation")
    for c in ("rule", "severity", "expected", "actual", "difference"):
        table.add_column(c)
    for f in report.findings:
        table.add_row(f.rule_id, f.severity, str(f.expected), str(f.actual), str(f.difference))
    console.print(table)


@app.command()
def anomaly(
    inject: bool = typer.Option(False, help="Inject a controlled anomaly burst first."),
    count: int = typer.Option(3000),
) -> None:
    """Run anomaly detection (optionally injecting a controlled anomaly)."""
    from apexbet.orchestration import inject_anomaly, run_pipeline

    _setup()
    if inject:
        report = inject_anomaly(count=count)
    else:
        settings = get_settings()
        report = run_pipeline(count=count, seed=settings.seed)
    _print_anomalies(report)


@app.command()
def enrich(
    limit: int = typer.Option(500),
    mode: str = typer.Option("rules", help="rules | local_ml | llm"),
) -> None:
    """Run enrichment over Silver events in the chosen mode."""
    from apexbet.common.storage import build_store, parquet_bytes
    from apexbet.enrichment.enricher import EnrichmentRunner, build_enricher
    from apexbet.orchestration import read_all

    _setup()
    settings = get_settings()
    store = build_store()
    rows = read_all(store, "silver/events")[:limit]
    enricher = build_enricher(
        mode,
        llm_base_url=settings.llm_base_url,
        llm_model=settings.llm_model,
        llm_timeout=settings.llm_timeout_seconds,
    )
    runner = EnrichmentRunner(enricher)
    records = runner.run(rows)
    flat = []
    for r in records:
        r2 = dict(r)
        r2["enrichment_payload"] = json.dumps(r2.get("enrichment_payload", {}))
        flat.append(r2)
    if flat:
        store.put_bytes(
            "gold/ai_enrichment/latest_enrichment.parquet",
            parquet_bytes(flat),
            content_type="application/vnd.apache.parquet",
        )
    console.print(json.dumps(runner.stats.to_dict(), indent=2))
    if records:
        console.print("[bold]sample enrichment[/bold]")
        console.print(json.dumps(records[0], indent=2))


@app.command()
def serve(
    host: str = typer.Option("0.0.0.0"),
    port: int = typer.Option(8000),
) -> None:
    """Run the FastAPI ingestion service (uvicorn)."""
    import uvicorn

    _setup()
    uvicorn.run("apexbet.ingestion.app:app", host=host, port=port, log_level="info")


@app.command()
def status() -> None:
    """Print current transport/storage health and latest run report."""
    from apexbet.common.storage import build_store
    from apexbet.transports import build_transport

    _setup()
    settings = get_settings()
    transport = build_transport(settings.transport, settings=settings)
    store = build_store()
    console.print("transport:", json.dumps(transport.health(), indent=2))
    console.print("storage:", json.dumps(store.health(), indent=2))
    if store.exists("reports/latest.json"):
        latest = json.loads(store.get_bytes("reports/latest.json"))
        console.print("latest run:", latest.get("run_id"), latest.get("finished_at"))
        console.print("pipeline:", json.dumps(latest.get("pipeline_metrics", {}), indent=2))


@app.command("anomaly-eval")
def anomaly_eval() -> None:
    """Run the reproducible anomaly-detection evaluation and print metrics."""
    from apexbet.anomaly.evaluation import evaluate_all

    _setup()
    results = evaluate_all()
    table = Table(title="Anomaly detection evaluation")
    table.add_column("scenario")
    table.add_column("TP", justify="right")
    table.add_column("FP", justify="right")
    table.add_column("FN", justify="right")
    table.add_column("precision", justify="right")
    table.add_column("recall", justify="right")
    for name, r in results.items():
        if name == "overall":
            continue
        e = r["evaluation"]
        table.add_row(
            name,
            str(e["true_positives"]),
            str(e["false_positives"]),
            str(e["false_negatives"]),
            str(e["precision"]),
            str(e["recall"]),
        )
    console.print(table)
    console.print("overall:", json.dumps(results["overall"]))


@app.command("terraform-validate")
def terraform_validate(root: str = typer.Option("infra/terraform")) -> None:
    """Validate Terraform (fmt+validate if binary present, else HCL2 parse)."""
    from apexbet.infra_validate import validate_all

    _setup()
    report = validate_all(root)
    console.print(json.dumps(report, indent=2))


@app.command("validate-dbt")
def validate_dbt(root: str = typer.Option("sql/dbt/models")) -> None:
    """Compile + run every dbt model against the local DuckDB (no dbt runtime)."""
    from apexbet.dbt_validate import validate_models

    _setup()
    settings = get_settings()
    report = validate_models(settings.duckdb_path, root=root)
    table = Table(title="dbt models")
    table.add_column("model")
    table.add_column("ok")
    table.add_column("rows / error")
    for name, r in report["models"].items():
        table.add_row(
            name, "yes" if r["ok"] else "[red]NO[/red]", str(r.get("rows", r.get("error", "")))
        )
    console.print(table)
    if not report["ok"]:
        raise typer.Exit(code=1)


@app.command()
def demo(
    count: int = typer.Option(8000),
    seed: int = typer.Option(1337),
) -> None:
    """Run the full end-to-end acceptance demonstration (synthetic only)."""
    from apexbet.demo import run_demo

    _setup()
    run_demo(count=count, seed=seed, console=console)


def _print_run_report(report: dict) -> None:
    pm = report["pipeline_metrics"]
    console.print(
        f"[bold green]run {report['run_id']}[/bold green] ({report['duration_seconds']}s)"
    )
    table = Table(title="Pipeline")
    table.add_column("metric")
    table.add_column("value", justify="right")
    for k in (
        "consumed",
        "bronze_written",
        "silver_written",
        "quarantined",
        "duplicates",
        "late_events",
        "dlq",
    ):
        table.add_row(k, str(pm.get(k)))
    table.add_row("throughput_eps", str(pm.get("throughput_eps")))
    console.print(table)
    q = report["quality"]
    console.print(f"quality: {q['failed_rules']} failed rules, {q['total_failures']} failures")
    rec = report["reconciliation"]
    console.print(
        f"reconciliation: {rec['total_findings']} findings, severities={rec['by_severity']}"
    )
    _print_anomalies(report)


def _print_anomalies(report: dict) -> None:
    an = report.get("anomalies", {})
    console.print(f"anomalies: {an.get('total_alerts', 0)} alerts {an.get('by_severity', {})}")
    for a in an.get("alerts", [])[:8]:
        console.print(f"  [{a['severity']}] {a['detector']}: {a['explanation']}")


if __name__ == "__main__":
    app()
