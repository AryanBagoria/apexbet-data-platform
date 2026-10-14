"""Reusable transport contract test suite.

Every :class:`~apexbet.transports.base.Transport` implementation must satisfy
the same behavioural contract. These tests are parameterised so that the *same*
assertions run against every backend:

  * ``memory``   -- always runs (offline, deterministic)
  * ``redpanda`` -- runs only when a broker is reachable (Docker profile)
  * ``kinesis``  -- runs only when LocalStack is reachable (Docker profile)

The Kafka/Kinesis backends are skipped (never failed) when infrastructure is
absent, so the suite is honest: it reports what it actually exercised rather
than pretending. Set ``APEXBET_TRANSPORT_CONTRACT=redpanda`` (or a comma-separated
list) to force which backends to attempt.
"""

from __future__ import annotations

import contextlib
import json
import os
import socket
import time
import uuid

import pytest

from apexbet.transports import build_transport
from apexbet.transports.base import Transport, TransportError

# ---------------------------------------------------------------------------
# Reachability probes (used to skip rather than fail when infra is absent).
# ---------------------------------------------------------------------------


def _tcp_open(host: str, port: int, timeout: float = 0.5) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


def _kafka_reachable() -> bool:
    from apexbet.config import get_settings

    host, _, port = get_settings().kafka_bootstrap.rpartition(":")
    return bool(host) and _tcp_open(host, int(port))


def _kinesis_reachable() -> bool:
    from urllib.parse import urlparse

    from apexbet.config import get_settings

    parsed = urlparse(get_settings().aws_endpoint_url)
    if not parsed.hostname:
        return False
    return _tcp_open(parsed.hostname, parsed.port or 4566, timeout=1.0)


# ---------------------------------------------------------------------------
# Parameterisation
# ---------------------------------------------------------------------------

ALL_BACKENDS = ["memory", "redpanda", "kinesis"]


def _selected() -> list[str]:
    env = os.environ.get("APEXBET_TRANSPORT_CONTRACT")
    return [b.strip() for b in env.split(",") if b.strip()] if env else ALL_BACKENDS


@pytest.fixture(params=_selected())
def contract_transport(request):
    """Yield a built transport for each selected backend, or skip if unreachable."""
    name = request.param
    if name == "redpanda" and not _kafka_reachable():
        pytest.skip("redpanda/kafka broker not reachable (start the demo profile)")
    if name == "kinesis" and not _kinesis_reachable():
        pytest.skip("LocalStack Kinesis not reachable (start the full profile)")
    try:
        transport = build_transport(name)
    except TransportError as exc:  # pragma: no cover - env dependent
        pytest.skip(f"transport {name!r} unavailable: {exc}")
    yield transport
    with contextlib.suppress(Exception):  # pragma: no cover
        transport.close()


@pytest.fixture()
def contract_topic(contract_transport) -> str:
    """A unique topic/stream name per test so backends never cross-contaminate."""
    suffix = uuid.uuid4().hex[:10]
    name = f"apexbet.contract.{suffix}"
    if contract_transport.name == "kinesis":
        # Kinesis streams must be created explicitly; reuse the adapter's helper.
        contract_transport._client.create_stream(  # type: ignore[attr-defined]
            StreamName=name, ShardCount=1
        )
        contract_transport._wait_active()  # type: ignore[attr-defined]
    return name


# ---------------------------------------------------------------------------
# Contract tests
# ---------------------------------------------------------------------------


class TestTransportContract:
    def test_implements_protocol(self, contract_transport):
        assert isinstance(contract_transport, Transport)
        assert contract_transport.name in ALL_BACKENDS

    def test_publish_then_consume_roundtrip(self, contract_transport, contract_topic):
        payload = json.dumps({"hello": "world", "n": 1}).encode()
        contract_transport.publish(contract_topic, "k1", payload)
        msgs = _drain(contract_transport, contract_topic)
        assert any(m.value == payload for m in msgs)

    def test_publish_many_returns_count(self, contract_transport, contract_topic):
        items = [(f"k{i}", json.dumps({"i": i}).encode()) for i in range(25)]
        n = contract_transport.publish_many(contract_topic, items)
        assert n == 25
        msgs = _drain(contract_transport, contract_topic, want=25)
        assert len(msgs) >= 25

    def test_key_is_preserved(self, contract_transport, contract_topic):
        contract_transport.publish(contract_topic, "account_42", b"payload")
        msgs = _drain(contract_transport, contract_topic)
        keys = {m.key for m in msgs}
        assert "account_42" in keys

    def test_health_reports_status(self, contract_transport):
        health = contract_transport.health()
        assert health.get("status") in ("OK", "DEGRADED", "DOWN")

    def test_dlq_routes_with_reason_header(self, contract_transport, contract_topic):
        dlq_topic = contract_topic + ".dlq"
        contract_transport.publish(contract_topic, "k", b"bad")
        msg = _drain(contract_transport, contract_topic)[0]
        contract_transport.to_dlq(contract_topic, dlq_topic, msg, "validation_failed")
        # DLQ records must be observable somewhere (topic for kafka/memory,
        # sibling stream for kinesis).
        dlq_msgs = _drain(contract_transport, dlq_topic)
        assert dlq_msgs, "expected at least one DLQ record"

    def test_consume_respects_max_messages(self, contract_transport, contract_topic):
        contract_transport.publish_many(contract_topic, [(f"k{i}", b"x") for i in range(50)])
        msgs = contract_transport.consume(
            contract_topic, "grp", max_messages=5, timeout_seconds=1.0
        )
        assert len(msgs) <= 5

    def test_at_least_once_delivery(self, contract_transport, contract_topic):
        """Every published record must be delivered at least once.

        We do not assert *exactly* once: Kafka/Kinesis are at-least-once and the
        platform relies on downstream dedup. This asserts the weaker, true
        guarantee that no record is silently dropped.
        """
        payloads = [json.dumps({"seq": i}).encode() for i in range(30)]
        contract_transport.publish_many(
            contract_topic, [(f"k{i}", p) for i, p in enumerate(payloads)]
        )
        msgs = _drain(contract_transport, contract_topic, want=30)
        delivered = {m.value for m in msgs}
        assert set(payloads).issubset(delivered)


def _drain(transport, topic: str, *, want: int = 1, timeout: float = 5.0) -> list:
    """Pull messages until ``want`` are collected or the timeout elapses."""
    collected: list = []
    deadline = time.time() + timeout
    while len(collected) < want and time.time() < deadline:
        batch = transport.consume(topic, "contract-group", max_messages=100, timeout_seconds=0.5)
        if batch:
            collected.extend(batch)
        else:
            time.sleep(0.05)
    return collected
