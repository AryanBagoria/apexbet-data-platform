"""Transport adapter contract tests (memory + interface conformance)."""

from __future__ import annotations

import pytest

from apexbet.transports import Transport, TransportError, build_transport


class TestMemoryTransport:
    def test_is_transport(self):
        assert isinstance(build_transport("memory"), Transport)

    def test_publish_consume(self, memory_transport):
        memory_transport.publish("t", "k", b"v1")
        memory_transport.publish_many("t", [("k2", b"v2"), ("k3", b"v3")])
        msgs = memory_transport.consume("t", "g", 10)
        assert [m.value for m in msgs] == [b"v1", b"v2", b"v3"]

    def test_commit_advances_offset(self, memory_transport):
        memory_transport.publish_many("t", [("k", b"a"), ("k", b"b")])
        msgs = memory_transport.consume("t", "g", 10)
        memory_transport.commit(msgs, "g")
        assert memory_transport.consume("t", "g", 10) == []

    def test_consumer_groups_independent(self, memory_transport):
        memory_transport.publish("t", "k", b"a")
        memory_transport.commit(memory_transport.consume("t", "g1", 10), "g1")
        assert len(memory_transport.consume("t", "g2", 10)) == 1

    def test_dlq(self, memory_transport):
        memory_transport.publish("t", "k", b"bad")
        msg = memory_transport.consume("t", "g", 10)[0]
        memory_transport.to_dlq("t", "t.dlq", msg, "bad_payload")
        assert memory_transport.dlq_count() == 1
        dlq = memory_transport.consume("t.dlq", "g", 10)
        assert dlq[0].headers["dlq.reason"] == "bad_payload"

    def test_health(self, memory_transport):
        assert memory_transport.health()["status"] == "OK"

    def test_replay_from_offset_zero(self, memory_transport):
        memory_transport.publish_many("t", [("k", b"a"), ("k", b"b")])
        first = memory_transport.consume("t", "g", 10)
        memory_transport.commit(first, "g")
        # A brand-new group replays from the start.
        assert len(memory_transport.consume("t", "fresh", 10)) == 2


class TestFactory:
    def test_unknown_transport_raises(self):
        with pytest.raises(TransportError):
            build_transport("nonsense")

    def test_kinesis_requires_connection(self):
        # Kinesis against a dead endpoint should fail rather than silently fall back.
        import botocore.exceptions

        from apexbet.transports.base import TransportError
        from apexbet.transports.kinesis import KinesisTransport

        with pytest.raises((TransportError, botocore.exceptions.BotoCoreError)):
            KinesisTransport(stream="x", endpoint_url="http://127.0.0.1:1", auto_create=True)
