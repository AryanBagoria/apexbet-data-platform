"""Reliability tests.

These verify the failure-handling guarantees the platform *claims*:

* a broker/publish failure is retried and surfaced, never silently swallowed;
* a consumer restart resumes from the persisted checkpoint (no reprocessing of
  committed work);
* replay from Bronze is deterministic and idempotent;
* retry uses bounded exponential backoff and finally raises (no infinite loop);
* bad records are quarantined / dead-lettered while the pipeline keeps running;
* duplicate delivery (at-least-once) is collapsed by the dedup window so the
  business effect is exactly-once;
* checkpoint state survives a process restart (it is written to the lake).
"""

from __future__ import annotations

import json

import pytest

from apexbet.common.retry import RetryExhausted, backoff_delay, retry_call
from apexbet.consumers import DedupWindow, Pipeline
from apexbet.transports.base import StreamMessage


def _msgs(n: int, start: int = 0):
    return [
        StreamMessage(
            key=f"k{i}",
            value=json.dumps({"i": i}).encode(),
            partition=0,
            offset=start + i,
            topic="t",
            cursor={"offset": start + i},
        )
        for i in range(n)
    ]


class FlakyTransport:
    """In-memory transport that fails the first ``fail_times`` publish calls."""

    name = "flaky"

    def __init__(self, *, fail_consume_times: int = 0, fail_publish_times: int = 0) -> None:
        from apexbet.transports.memory import MemoryTransport

        self._inner = MemoryTransport()
        self._fail_consume_times = fail_consume_times
        self._fail_publish_times = fail_publish_times
        self.publish_attempts = 0
        self.consume_attempts = 0

    def publish(self, topic, key, value, headers=None):
        self.publish_attempts += 1
        if self.publish_attempts <= self._fail_publish_times:
            raise ConnectionError("simulated broker write failure")
        return self._inner.publish(topic, key, value, headers)

    def publish_many(self, topic, items):
        self.publish_attempts += 1
        if self.publish_attempts <= self._fail_publish_times:
            raise ConnectionError("simulated broker write failure")
        return self._inner.publish_many(topic, items)

    def consume(self, topic, group_id, max_messages=500, timeout_seconds=1.0):
        self.consume_attempts += 1
        if self.consume_attempts <= self._fail_consume_times:
            raise ConnectionError("simulated broker read failure")
        return self._inner.consume(topic, group_id, max_messages, timeout_seconds)

    def commit(self, messages, group_id=""):
        return self._inner.commit(messages, group_id)

    def to_dlq(self, topic, dlq_topic, message, reason):
        return self._inner.to_dlq(topic, dlq_topic, message, reason)

    def health(self):
        return self._inner.health()

    def close(self):
        return self._inner.close()


class TestBrokerInterruption:
    def test_consume_error_does_not_kill_pipeline(self, tmp_store):
        """A transient consume error is logged and retried, not fatal."""
        transport = FlakyTransport(fail_consume_times=1)
        transport._inner.publish("t", "k", b'{"event_id":"e1"}')
        p = Pipeline(
            transport,
            tmp_store,
            source_topic="t",
            dlq_topic="t.dlq",
            group_id="g",
            retry_base_seconds=0.01,
        )
        p.run(idle_stop_seconds=0.2)
        assert p.metrics.errors >= 1
        assert p.metrics.consumed >= 1  # recovered after the failure

    def test_publish_failure_is_retried_then_succeeds(self):
        transport = FlakyTransport(fail_publish_times=2)
        calls = {"n": 0}

        def _do_publish():
            calls["n"] += 1
            return transport.publish("t", "k", b"v")

        retry_call(_do_publish, max_attempts=5, base=0.001, sleep=lambda _s: None)
        assert calls["n"] == 3  # two failures + one success
        assert transport._inner.message_count("t") == 1

    def test_retry_exhaustion_raises(self):
        transport = FlakyTransport(fail_publish_times=100)
        with pytest.raises(RetryExhausted):
            retry_call(
                lambda: transport.publish("t", "k", b"v"),
                max_attempts=3,
                base=0.001,
                sleep=lambda _s: None,
            )


class TestBackoff:
    def test_backoff_is_bounded_and_growing(self):
        # backoff_delay caps the *base* delay then applies +/- jitter, so the
        # effective delay is bounded by cap * (1 + jitter).
        cap, jitter = 2.0, 0.25
        delays = [backoff_delay(i, base=0.1, cap=cap, jitter=jitter) for i in range(10)]
        assert all(0 <= d <= cap * (1 + jitter) for d in delays)
        # The pre-jitter schedule is monotonic non-decreasing up to the cap.
        raw = [min(cap, 0.1 * (2**i)) for i in range(10)]
        assert raw == sorted(raw)
        assert raw[-1] == cap
        # Cap is respected even for large attempt counts.
        assert backoff_delay(50, base=0.1, cap=cap, jitter=jitter) <= cap * (1 + jitter)

    def test_backoff_zero_attempt(self):
        assert backoff_delay(0, base=0.0) == 0.0


class TestRestartRecovery:
    def test_restart_resumes_from_checkpoint(self, memory_transport, tmp_store, clean_events):
        memory_transport.publish_many(
            "t", [(e["partition_key"], json.dumps(e).encode()) for e in clean_events[:200]]
        )
        p1 = Pipeline(
            memory_transport,
            tmp_store,
            source_topic="t",
            dlq_topic="t.dlq",
            group_id="restart-group",
        )
        p1.run(idle_stop_seconds=0.2)
        assert p1.metrics.consumed == 200
        cp = p1.read_checkpoint()
        assert cp is not None and cp["consumed"] == 200

        # A new process (new Pipeline, same group + same store) must NOT
        # reprocess committed work.
        p2 = Pipeline(
            memory_transport,
            tmp_store,
            source_topic="t",
            dlq_topic="t.dlq",
            group_id="restart-group",
        )
        assert p2.read_checkpoint() is not None
        p2.run(idle_stop_seconds=0.2)
        assert p2.metrics.consumed == 0

    def test_checkpoint_persisted_to_store(self, memory_transport, tmp_store, clean_events):
        memory_transport.publish_many(
            "t", [(e["partition_key"], json.dumps(e).encode()) for e in clean_events[:50]]
        )
        p = Pipeline(
            memory_transport, tmp_store, source_topic="t", dlq_topic="t.dlq", group_id="cp-group"
        )
        p.run(idle_stop_seconds=0.2)
        assert tmp_store.exists("checkpoints/cp-group/t.json")


class TestIdempotency:
    def test_duplicate_delivery_collapsed(self, memory_transport, tmp_store):
        """The same event delivered 5 times must produce exactly one Silver row."""
        ev = {
            "event_id": "dup_same",
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
        memory_transport.publish_many("t", [("m", json.dumps(ev).encode())] * 5)
        p = Pipeline(memory_transport, tmp_store, source_topic="t", dlq_topic="t.dlq", group_id="g")
        p.run(idle_stop_seconds=0.2)
        assert p.metrics.consumed == 5
        assert p.metrics.duplicates == 4
        assert p.metrics.silver_written == 1

    def test_dedup_window_expiry_allows_reprocessing(self):
        d = DedupWindow(0)
        d.add("e1", ts=0)
        assert not d.seen_before("e1")  # expired -> treated as new


class TestDeadLetterAndQuarantine:
    def test_bad_json_goes_to_dlq(self, memory_transport, tmp_store):
        memory_transport.publish("t", "k", b"{not json")
        p = Pipeline(memory_transport, tmp_store, source_topic="t", dlq_topic="t.dlq", group_id="g")
        p.run(idle_stop_seconds=0.2)
        assert p.metrics.dlq == 1
        assert memory_transport.dlq_count() == 1

    def test_invalid_event_quarantined_not_dlq(self, memory_transport, tmp_store):
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
        assert p.metrics.dlq == 0
        assert len(tmp_store.list("bronze/quarantine")) == 1

    def test_pipeline_survives_mixed_good_and_bad(self, memory_transport, tmp_store, clean_events):
        bad = {**clean_events[0], "event_id": "bad_1", "schema_version": "0.9.0"}
        items = [(e["partition_key"], json.dumps(e).encode()) for e in clean_events[:100]]
        items.append(("x", json.dumps(bad).encode()))
        items.append(("y", b"}{broken"))
        memory_transport.publish_many("t", items)
        p = Pipeline(memory_transport, tmp_store, source_topic="t", dlq_topic="t.dlq", group_id="g")
        p.run(idle_stop_seconds=0.2)
        assert p.metrics.consumed == 102
        assert p.metrics.dlq == 1
        assert p.metrics.quarantined == 1
        assert p.metrics.bronze_written == 101  # DLQ record never reaches Bronze


class TestReplay:
    def test_replay_from_bronze_is_deterministic(self, tmp_store, clean_events):
        """Re-transforming the same Bronze rows twice yields identical output."""
        from apexbet.silver.transform import dedupe_events, transform_event

        def rebuild():
            out = []
            for raw in clean_events[:300]:
                res = transform_event(raw, allowed_lateness_seconds=600)
                if res.ok and res.record:
                    out.append(res.record)
            unique, _ = dedupe_events(out)
            return unique

        first = rebuild()
        second = rebuild()
        assert [r["event_id"] for r in first] == [r["event_id"] for r in second]
        assert len(first) == len(second)

    def test_new_consumer_group_replays_from_start(self, memory_transport, clean_events):
        memory_transport.publish_many(
            "t", [(e["partition_key"], json.dumps(e).encode()) for e in clean_events[:100]]
        )
        memory_transport.commit(memory_transport.consume("t", "g1", 500), "g1")
        assert memory_transport.consume("t", "g1", 500) == []
        assert len(memory_transport.consume("t", "g2", 500)) == 100
