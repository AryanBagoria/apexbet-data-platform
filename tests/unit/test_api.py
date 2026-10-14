"""FastAPI ingestion tests using the in-process TestClient (no network)."""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from apexbet.common.storage import LocalObjectStore
from apexbet.config import Settings
from apexbet.ingestion.app import make_app
from apexbet.transports import build_transport


@pytest.fixture()
def client(tmp_path):
    transport = build_transport("memory")
    store = LocalObjectStore(str(tmp_path / "lake"))
    settings = Settings(kafka_topic="test.events", kinesis_stream="x")
    app = make_app(settings=settings, transport=transport, store=store)
    with TestClient(app) as c:
        c.apex_transport = transport  # type: ignore[attr-defined]
        yield c


def _valid_event(i=0):
    return {
        "event_id": f"evt_{i}",
        "event_type": "BetPlaced",
        "schema_version": "1.2.0",
        "event_timestamp": "2026-01-01T00:00:00Z",
        "partition_key": f"acct_{i}",
        "account_id": f"acct_{i}",
        "payload": {
            "bet_id": f"bet_{i}",
            "market_id": "mkt_1",
            "outcome_id": "HOME",
            "stake_minor": 1000,
            "currency": "USD",
            "decimal_odds": 2.5,
            "potential_return_minor": 2500,
        },
    }


class TestHealth:
    def test_health(self, client):
        r = client.get("/health")
        assert r.status_code == 200
        assert r.json()["status"] == "ok"

    def test_ready(self, client):
        r = client.get("/ready")
        assert r.status_code == 200
        assert r.json()["ready"] is True

    def test_metrics(self, client):
        client.get("/health")
        r = client.get("/metrics")
        assert r.status_code == 200
        assert "apexbet_api_requests_total" in r.text


class TestIngestion:
    def test_single_event(self, client):
        r = client.post("/api/v1/events", json=_valid_event())
        assert r.status_code == 200
        body = r.json()
        assert body["accepted"] == 1
        assert body["rejected"] == 0
        assert body["request_id"]

    def test_batch_events(self, client):
        r = client.post("/api/v1/events", json={"events": [_valid_event(i) for i in range(5)]})
        assert r.status_code == 200
        assert r.json()["accepted"] == 5

    def test_array_body(self, client):
        r = client.post("/api/v1/events", json=[_valid_event(1), _valid_event(2)])
        assert r.status_code == 200
        assert r.json()["accepted"] == 2

    def test_invalid_event_rejected(self, client):
        bad = _valid_event()
        bad["payload"] = {"bet_id": "x"}  # missing fields
        r = client.post("/api/v1/events", json=bad)
        assert r.status_code == 200
        body = r.json()
        assert body["rejected"] == 1
        assert len(body["errors"]) == 1

    def test_duplicate_in_batch_detected(self, client):
        ev = _valid_event()
        r = client.post("/api/v1/events", json=[ev, ev])
        body = r.json()
        assert body["accepted"] == 2
        assert body["duplicates_in_batch"] == 1

    def test_request_id_header_echoed(self, client):
        r = client.post(
            "/api/v1/events", json=_valid_event(), headers={"x-request-id": "my-req-123"}
        )
        assert r.headers["x-request-id"] == "my-req-123"

    def test_publishes_to_transport(self, client):
        client.post("/api/v1/events", json={"events": [_valid_event(i) for i in range(3)]})
        assert client.apex_transport.message_count("test.events") == 3  # type: ignore[attr-defined]


class TestBodyLimit:
    def test_oversized_body_rejected(self, tmp_path):
        transport = build_transport("memory")
        store = LocalObjectStore(str(tmp_path / "lake"))
        settings = Settings(api_max_body_bytes=100, kafka_topic="t")
        app = make_app(settings=settings, transport=transport, store=store)
        with TestClient(app) as c:
            big = {"events": [_valid_event(i) for i in range(50)]}
            r = c.post("/api/v1/events", json=big)
            assert r.status_code == 413


class TestAuth:
    def test_protected_requires_token(self, tmp_path):
        transport = build_transport("memory")
        store = LocalObjectStore(str(tmp_path / "lake"))
        settings = Settings(api_tokens="secret-token")
        app = make_app(settings=settings, transport=transport, store=store)
        with TestClient(app) as c:
            r = c.get("/api/v1/platform/summary")
            assert r.status_code == 401
            r2 = c.get("/api/v1/platform/summary", headers={"Authorization": "Bearer secret-token"})
            assert r2.status_code == 200

    def test_health_not_protected(self, tmp_path):
        settings = Settings(api_tokens="secret")
        app = make_app(
            settings=settings,
            transport=build_transport("memory"),
            store=LocalObjectStore(str(tmp_path / "lake")),
        )
        with TestClient(app) as c:
            assert c.get("/health").status_code == 200
