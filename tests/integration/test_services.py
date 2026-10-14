"""Integration tests against real local infrastructure.

These exercise the *two independent transports* end-to-end through the actual
pipeline (publish -> consume -> Bronze -> Silver) and the S3-compatible storage
backend. They are opt-in: each test is skipped unless the backing service is
reachable, so ``pytest`` stays green on a laptop with no Docker running.

Enable locally with::

    docker compose --profile demo up -d redpanda
    docker compose --profile full up -d localstack
    pytest -m integration

Markers: ``integration`` (needs Docker services) and ``aws`` (needs LocalStack).
"""

from __future__ import annotations

import json
import socket
import uuid

import pytest

from apexbet.common.storage import S3ObjectStore
from apexbet.config import get_settings
from apexbet.consumers import Pipeline
from apexbet.generator import AnomalyInjection, GeneratorConfig, SyntheticEventGenerator
from apexbet.transports import build_transport

pytestmark = pytest.mark.integration


def _tcp_open(host: str, port: int, timeout: float = 0.6) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


def _kafka_up() -> bool:
    host, _, port = get_settings().kafka_bootstrap.rpartition(":")
    return bool(host) and _tcp_open(host, int(port))


def _localstack_up() -> bool:
    from urllib.parse import urlparse

    p = urlparse(get_settings().aws_endpoint_url)
    return bool(p.hostname) and _tcp_open(p.hostname, p.port or 4566, timeout=1.0)


requires_kafka = pytest.mark.skipif(not _kafka_up(), reason="redpanda broker not reachable")
requires_localstack = pytest.mark.skipif(not _localstack_up(), reason="LocalStack not reachable")


@pytest.fixture()
def fresh_topic() -> str:
    return f"apexbet.it.{uuid.uuid4().hex[:10]}"


def _events(n: int = 1500):
    gen = SyntheticEventGenerator(
        GeneratorConfig(seed=5, num_accounts=200, num_matches=20, anomalies=AnomalyInjection.demo())
    )
    return list(gen.generate(n))


# ---------------------------------------------------------------------------
# Kafka / Redpanda
# ---------------------------------------------------------------------------


@requires_kafka
class TestRedpandaIntegration:
    def test_publish_consume_through_broker(self, fresh_topic):
        transport = build_transport("redpanda")
        transport._client_ensure_topic(fresh_topic) if hasattr(
            transport, "_client_ensure_topic"
        ) else None
        items = [(f"k{i}", json.dumps({"i": i}).encode()) for i in range(200)]
        n = transport.publish_many(fresh_topic, items)
        assert n == 200
        got = []
        import time

        deadline = time.time() + 15
        # retry-consume until we see all records or time out
        while len(got) < 200 and time.time() < deadline:
            got.extend(transport.consume(fresh_topic, "it-group", 500, 1.0))
        transport.close()
        assert {m.value for m in got} >= {v for _, v in items}

    def test_full_pipeline_over_redpanda(self, fresh_topic, tmp_store):
        transport = build_transport("redpanda")
        events = _events(1200)
        transport.publish_many(
            fresh_topic, [(e["partition_key"], json.dumps(e).encode()) for e in events]
        )
        pipeline = Pipeline(
            transport,
            tmp_store,
            source_topic=fresh_topic,
            dlq_topic=fresh_topic + ".dlq",
            group_id=f"it-{uuid.uuid4().hex[:8]}",
        )
        pipeline.run(idle_stop_seconds=2.0)
        transport.close()
        assert pipeline.metrics.consumed == len(events)
        assert pipeline.metrics.bronze_written == len(events)
        assert len(tmp_store.list("bronze/event_type=")) >= 1


# ---------------------------------------------------------------------------
# Kinesis / LocalStack
# ---------------------------------------------------------------------------


@requires_localstack
@pytest.mark.aws
class TestKinesisIntegration:
    @pytest.fixture()
    def kinesis_stream(self):
        transport = build_transport("kinesis")
        name = f"apexbet-it-{uuid.uuid4().hex[:10]}"
        transport._client.create_stream(StreamName=name, ShardCount=1)
        transport._wait_active()
        yield transport, name
        transport.close()

    def test_put_get_records(self, kinesis_stream):
        transport, name = kinesis_stream
        items = [(f"k{i}", json.dumps({"i": i}).encode()) for i in range(50)]
        n = transport.publish_many(name, items)
        assert n == 50
        import time

        got = []
        deadline = time.time() + 15
        while len(got) < 50 and time.time() < deadline:
            got.extend(transport.consume(name, "g", 500, 1.0))
        assert {m.value for m in got} >= {v for _, v in items}

    def test_shard_metadata(self, kinesis_stream):
        transport, _name = kinesis_stream
        assert len(transport.shard_ids()) >= 1
        health = transport.health()
        assert health["status"] == "OK"


# ---------------------------------------------------------------------------
# S3-compatible storage (LocalStack or MinIO)
# ---------------------------------------------------------------------------


@requires_localstack
@pytest.mark.aws
class TestS3StorageIntegration:
    def test_put_get_list_delete(self):
        store = S3ObjectStore(bucket="apexbet-it", prefix="it", auto_create=True)
        key = f"check/{uuid.uuid4().hex}.json"
        store.put_bytes(key, b'{"a":1}', content_type="application/json")
        assert store.get_bytes(key) == b'{"a":1}'
        assert store.exists(key)
        assert any(o.key.endswith(key) for o in store.list("check/"))
        store.delete(key)
        assert not store.exists(key)

    def test_health_probe(self):
        store = S3ObjectStore(bucket="apexbet-it", prefix="it", auto_create=True)
        assert store.health()["status"] == "OK"
