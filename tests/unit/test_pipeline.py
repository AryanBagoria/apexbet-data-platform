"""Bronze writer + pipeline processing tests."""

from __future__ import annotations

import json

from apexbet.bronze.writer import BronzeWriter, snake
from apexbet.common.storage import read_parquet_bytes
from apexbet.consumers import DedupWindow, Pipeline


class TestSnake:
    def test_snake(self):
        assert snake("BetPlaced") == "bet_placed"
        assert snake("OddsUpdated") == "odds_updated"
        assert snake("PipelineHealthEvent") == "pipeline_health_event"


class TestBronzeWriter:
    def test_flush_writes_parquet(self, tmp_store):
        w = BronzeWriter(
            store=tmp_store, flush_records=2, flush_seconds=999, event_date="2026-01-01"
        )
        w.add(
            {
                "event_id": "1",
                "event_type": "BetPlaced",
                "producer": "x",
                "event_timestamp": "2026-01-01T00:00:00Z",
                "payload": {"a": 1},
            }
        )
        assert w.buffered == 1
        w.add(
            {
                "event_id": "2",
                "event_type": "BetPlaced",
                "producer": "x",
                "event_timestamp": "2026-01-01T00:00:00Z",
                "payload": {"a": 2},
            }
        )
        assert w.should_flush()
        written = w.flush()
        assert written == 2
        objs = tmp_store.list("bronze/event_type=bet_placed")
        assert len(objs) == 1
        rows = read_parquet_bytes(tmp_store.get_bytes(objs[0].key))
        assert len(rows) == 2
        assert "original_payload" in rows[0]
        assert json.loads(rows[0]["original_event_json"])["event_id"] == "1"

    def test_quarantine_write(self, tmp_store):
        w = BronzeWriter(store=tmp_store, event_date="2026-01-01")
        w.write_quarantine("BetPlaced", [{"event_id": "x", "_reason": "bad"}])
        assert len(tmp_store.list("bronze/quarantine")) == 1

    def test_summary(self, tmp_store):
        w = BronzeWriter(store=tmp_store)
        w.add({"event_id": "1", "event_type": "BetPlaced", "payload": {}})
        assert w.summary()["total"] == 1


class TestDedupWindow:
    def test_within_window(self):
        d = DedupWindow(3600)
        d.add("a")
        assert d.seen_before("a")
        assert not d.seen_before("b")

    def test_expiry(self):
        d = DedupWindow(0)  # immediate expiry
        d.add("a", ts=0)
        assert not d.seen_before("a")


class TestPipeline:
    def test_processes_clean_events(self, published_pipeline):
        pipeline, _transport, _store, events = published_pipeline
        pipeline.run(idle_stop_seconds=0.2)
        assert pipeline.metrics.consumed == len(events)
        assert pipeline.metrics.bronze_written == len(events)
        assert pipeline.metrics.silver_written <= len(events)
        assert pipeline.metrics.quarantined == 0  # clean events

    def test_bronze_records_match_source(self, published_pipeline):
        pipeline, _transport, store, events = published_pipeline
        pipeline.run(idle_stop_seconds=0.2)
        from apexbet.orchestration import read_bronze_events

        bronze = read_bronze_events(store)
        assert len(bronze) == len(events)

    def test_dedup_collapses_duplicates(self, memory_transport, tmp_store):
        ev = {
            "event_id": "dup_1",
            "event_type": "OddsUpdated",
            "schema_version": "1.2.0",
            "event_timestamp": "2026-01-01T00:00:00Z",
            "partition_key": "m",
            "account_id": None,
            "payload": {
                "market_id": "mkt_1",
                "outcome_id": "HOME",
                "previous_odds": 2.0,
                "new_odds": 2.1,
            },
        }
        memory_transport.publish_many("t", [("m", json.dumps(ev).encode())] * 3)
        p = Pipeline(memory_transport, tmp_store, source_topic="t", dlq_topic="t.dlq", group_id="g")
        p.run(idle_stop_seconds=0.2)
        assert p.metrics.consumed == 3
        assert p.metrics.duplicates == 2
        assert p.metrics.silver_written == 1

    def test_dlq_on_bad_json(self, memory_transport, tmp_store):
        memory_transport.publish("t", "k", b"not json{{{")
        p = Pipeline(memory_transport, tmp_store, source_topic="t", dlq_topic="t.dlq", group_id="g")
        p.run(idle_stop_seconds=0.2)
        assert p.metrics.dlq == 1
        assert memory_transport.dlq_count() == 1

    def test_checkpoint_written(self, published_pipeline):
        pipeline, _transport, _store, events = published_pipeline
        pipeline.run(idle_stop_seconds=0.2)
        cp = pipeline.read_checkpoint()
        assert cp is not None
        assert cp["consumed"] == len(events)

    def test_invalid_events_quarantined(self, memory_transport, tmp_store):
        bad = {
            "event_id": "e1",
            "event_type": "BetPlaced",
            "schema_version": "1.2.0",
            "event_timestamp": "2026-01-01T00:00:00Z",
            "partition_key": "a",
            "payload": {
                "bet_id": "b",
                "market_id": "m",
                "outcome_id": "o",
                "stake_minor": -5,
                "currency": "USD",
                "decimal_odds": 2.0,
                "potential_return_minor": 0,
            },
        }
        memory_transport.publish("t", "a", json.dumps(bad).encode())
        p = Pipeline(memory_transport, tmp_store, source_topic="t", dlq_topic="t.dlq", group_id="g")
        p.run(idle_stop_seconds=0.2)
        assert p.metrics.quarantined == 1
        assert len(tmp_store.list("bronze/quarantine")) == 1


class TestRecovery:
    def test_restart_does_not_reprocess(self, memory_transport, tmp_store, clean_events):
        memory_transport.publish_many(
            "t", [(e["partition_key"], json.dumps(e).encode()) for e in clean_events[:100]]
        )
        p1 = Pipeline(
            memory_transport, tmp_store, source_topic="t", dlq_topic="t.dlq", group_id="grp"
        )
        p1.run(idle_stop_seconds=0.2)
        n1 = p1.metrics.consumed
        # A second pipeline with the same group in the same broker sees nothing new.
        p2 = Pipeline(
            memory_transport, tmp_store, source_topic="t", dlq_topic="t.dlq", group_id="grp"
        )
        p2.run(idle_stop_seconds=0.2)
        assert n1 == 100
        assert p2.metrics.consumed == 0
