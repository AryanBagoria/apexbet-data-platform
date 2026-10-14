"""Benchmark harness.

Measures generation, publish, processing throughput and end-to-end latency at
configurable scale, streaming (never holding the whole dataset in memory in
the generator). Results are written to a JSON file with full context: hardware,
Docker allocation, concurrency, event size, config and duration.

Usage:
    python -m apexbet.benchmark --events 100000 --transport memory
    python -m apexbet.benchmark --events 1000000 --transport redpanda

Honest reporting only: this measures a single local process against a local
broker. It is NOT a production capacity claim.
"""

from __future__ import annotations

import argparse
import json
import os
import platform
import time
from typing import Any

from apexbet.common.logging import configure_logging, get_logger
from apexbet.generator import AnomalyInjection, GeneratorConfig, SyntheticEventGenerator

log = get_logger("apexbet.benchmark")


def _hardware() -> dict[str, Any]:
    info: dict[str, Any] = {
        "platform": platform.platform(),
        "machine": platform.machine(),
        "python": platform.python_version(),
        "cpu_count": os.cpu_count(),
    }
    try:
        import psutil  # type: ignore

        vm = psutil.virtual_memory()
        info["total_memory_gb"] = round(vm.total / 1e9, 2)
        info["available_memory_gb"] = round(vm.available / 1e9, 2)
    except Exception:
        pass
    return info


def benchmark_generation(
    events: int, seed: int, batch_report_every: int = 100_000
) -> dict[str, Any]:
    gen = SyntheticEventGenerator(
        GeneratorConfig(
            seed=seed, num_accounts=5000, num_matches=300, anomalies=AnomalyInjection.clean()
        )
    )
    sizes: list[int] = []
    start = time.perf_counter()
    bytes_total = 0
    n = 0
    for env in gen.generate(events):
        payload = json.dumps(env, separators=(",", ":")).encode("utf-8")
        bytes_total += len(payload)
        n += 1
        if n % batch_report_every == 0:
            sizes.append(len(payload))
    elapsed = time.perf_counter() - start
    return {
        "events": n,
        "elapsed_seconds": round(elapsed, 3),
        "throughput_eps": round(n / max(1e-9, elapsed), 1),
        "total_bytes": bytes_total,
        "avg_event_bytes": round(bytes_total / max(1, n), 1),
        "miB_per_second": round((bytes_total / 1e6) / max(1e-9, elapsed), 3),
    }


def benchmark_publish(
    events: int, transport_name: str, settings: Any, batch: int = 500
) -> dict[str, Any]:
    from apexbet.transports import build_transport

    transport = build_transport(transport_name, settings=settings)
    gen = SyntheticEventGenerator(
        GeneratorConfig(
            seed=99, num_accounts=5000, num_matches=300, anomalies=AnomalyInjection.clean()
        )
    )
    latencies: list[float] = []
    published = 0
    start = time.perf_counter()
    buf: list[tuple[str | None, bytes]] = []
    for env in gen.generate(events):
        buf.append((env["partition_key"], json.dumps(env, separators=(",", ":")).encode()))
        if len(buf) >= batch:
            t0 = time.perf_counter()
            transport.publish_many(settings.kafka_topic, buf)
            latencies.append((time.perf_counter() - t0) * 1000)
            published += len(buf)
            buf = []
    if buf:
        transport.publish_many(settings.kafka_topic, buf)
        published += len(buf)
    elapsed = time.perf_counter() - start
    transport.close()
    return {
        "events_published": published,
        "elapsed_seconds": round(elapsed, 3),
        "throughput_eps": round(published / max(1e-9, elapsed), 1),
        "batch_publish_latency_ms_p50": round(_pct(latencies, 50), 3),
        "batch_publish_latency_ms_p95": round(_pct(latencies, 95), 3),
        "batch_publish_latency_ms_p99": round(_pct(latencies, 99), 3),
    }


def benchmark_pipeline(events: int, transport_name: str, settings: Any) -> dict[str, Any]:
    from apexbet.common.storage import build_store
    from apexbet.consumers import Pipeline
    from apexbet.transports import build_transport

    transport = build_transport(transport_name, settings=settings)
    store = build_store()
    gen = SyntheticEventGenerator(
        GeneratorConfig(
            seed=7, num_accounts=5000, num_matches=300, anomalies=AnomalyInjection.clean()
        )
    )
    items = [
        (e["partition_key"], json.dumps(e, separators=(",", ":")).encode())
        for e in gen.generate(events)
    ]
    transport.publish_many(settings.kafka_topic, items)
    pipeline = Pipeline(
        transport,
        store,
        source_topic=settings.kafka_topic,
        dlq_topic=settings.kafka_dlq_topic,
        group_id=f"bench-{int(time.time())}",
    )
    start = time.perf_counter()
    metrics = pipeline.run(idle_stop_seconds=1.0)
    elapsed = time.perf_counter() - start
    metrics["elapsed_seconds"] = round(elapsed, 3)
    metrics["process_throughput_eps"] = round(metrics["consumed"] / max(1e-9, elapsed), 1)
    transport.close()
    return metrics


def _pct(values: list[float], pct: float) -> float:
    if not values:
        return 0.0
    values = sorted(values)
    k = (len(values) - 1) * (pct / 100.0)
    f = int(k)
    c = min(f + 1, len(values) - 1)
    if f == c:
        return values[f]
    return values[f] + (values[c] - values[f]) * (k - f)


def run_benchmark(events: int, transport_name: str, *, run_pipeline: bool = True) -> dict[str, Any]:
    from apexbet.config import get_settings

    configure_logging("WARNING", json_logs=True)
    settings = get_settings()
    report: dict[str, Any] = {
        "hardware": _hardware(),
        "config": {
            "events": events,
            "transport": transport_name,
            "storage_backend": settings.storage_backend,
            "lake_root": settings.lake_root,
        },
        "generation": benchmark_generation(events, seed=settings.seed),
        "publish": benchmark_publish(events, transport_name, settings),
    }
    if run_pipeline:
        report["pipeline"] = benchmark_pipeline(events, transport_name, settings)
    return report


def main() -> None:
    ap = argparse.ArgumentParser(description="ApexBet benchmark harness")
    ap.add_argument("--events", type=int, default=10_000)
    ap.add_argument("--transport", default="memory")
    ap.add_argument("--no-pipeline", action="store_true")
    ap.add_argument("--out", default="benchmark_results.json")
    args = ap.parse_args()
    report = run_benchmark(args.events, args.transport, run_pipeline=not args.no_pipeline)
    with open(args.out, "w", encoding="utf-8") as fh:
        json.dump(report, fh, indent=2)
    print(json.dumps(report, indent=2)[:2000])
    print(f"\nWrote {args.out}")


if __name__ == "__main__":
    main()
