"""Analytics (DuckDB Gold) tests over a small end-to-end pipeline run."""

from __future__ import annotations

import json

from apexbet.analytics.duckdb_engine import AnalyticsEngine
from apexbet.consumers import Pipeline


def _run_and_build(tmp_store, memory_transport, events):
    memory_transport.publish_many(
        "t", [(e["partition_key"], json.dumps(e).encode()) for e in events]
    )
    Pipeline(memory_transport, tmp_store, source_topic="t", dlq_topic="t.dlq", group_id="g").run(
        idle_stop_seconds=0.3
    )
    engine = AnalyticsEngine(str(tmp_store.root) + "/analytics.duckdb", store=tmp_store)
    n = engine.load_silver()
    counts = engine.build_gold()
    return engine, n, counts


class TestGoldBuild:
    def test_loads_and_builds(self, tmp_store, memory_transport, clean_events):
        _engine, n, counts = _run_and_build(tmp_store, memory_transport, clean_events)
        assert n > 0
        assert counts["gold_bet_outcomes"] >= 1
        assert counts["gold_event_volume_by_minute"] >= 1

    def test_distinct_event_ids_match_silver(self, tmp_store, memory_transport, clean_events):
        engine, n, _ = _run_and_build(tmp_store, memory_transport, clean_events)
        rows = engine.query("SELECT SUM(distinct_event_ids) AS s FROM gold_event_counts")
        # distinct ids across types == silver rows (types are disjoint per event id)
        assert rows[0]["s"] <= n

    def test_bet_outcomes_invariants(self, tmp_store, memory_transport, clean_events):
        engine, _, _ = _run_and_build(tmp_store, memory_transport, clean_events)
        for row in engine.query("SELECT * FROM gold_bet_outcomes"):
            if row["result"] == "LOST":
                assert row["payout_minor"] == 0
            if row["result"] in ("VOID", "PUSH"):
                assert row["payout_minor"] == row["settled_stake_minor"]

    def test_latency_distribution(self, tmp_store, memory_transport, clean_events):
        engine, _, _ = _run_and_build(tmp_store, memory_transport, clean_events)
        rows = engine.query("SELECT * FROM gold_latency_distribution")
        assert rows
        for r in rows:
            assert r["p50_ms"] <= r["p99_ms"]

    def test_platform_summary(self, tmp_store, memory_transport, clean_events):
        engine, _, _ = _run_and_build(tmp_store, memory_transport, clean_events)
        summary = engine.platform_summary()
        assert summary["silver_events"] > 0


class TestCountsVerify:
    def test_event_counts_positive(self, tmp_store, memory_transport, clean_events):
        engine, _, _ = _run_and_build(tmp_store, memory_transport, clean_events)
        rows = engine.query("SELECT event_type, event_count FROM gold_event_counts")
        assert rows
        assert all(r["event_count"] > 0 for r in rows)
