"""Tests for the dashboard serving layer + safe demo controls."""

from __future__ import annotations

import json

import pytest
from fastapi.testclient import TestClient

from apexbet.common.storage import LocalObjectStore
from apexbet.config import reset_settings_cache
from apexbet.demo_control import MAX_EVENTS, DemoController
from apexbet.ingestion.app import make_app
from apexbet.orchestration import run_pipeline
from apexbet.serving.dashboard import DashboardService


@pytest.fixture()
def lake(tmp_path, monkeypatch):
    """A real (small) pipeline run into a temp lake, with settings pointed at it."""
    root = str(tmp_path / "lake")
    monkeypatch.setenv("APEXBET_LAKE_ROOT", root)
    monkeypatch.setenv("APEXBET_DUCKDB_PATH", f"{root}/analytics.duckdb")
    monkeypatch.setenv("APEXBET_TRANSPORT", "memory")
    reset_settings_cache()
    store = LocalObjectStore(root)
    run_pipeline(count=1200, seed=7, store=store, enrich=True)
    yield root, store
    reset_settings_cache()


@pytest.fixture()
def client(lake):
    app = make_app()
    return TestClient(app)


def test_dashboard_health_and_layers(client):
    r = client.get("/api/v1/dashboard/health")
    assert r.status_code == 200
    body = r.json()
    assert body["pipeline"] in {"healthy", "unknown"}
    assert len(body["layers"]) == 3
    layers = {s["layer"]: s for s in body["layers"]}
    assert layers["bronze"]["available"] is True
    assert layers["bronze"]["object_count"] > 0


def test_overview_uses_real_counts(client):
    r = client.get("/api/v1/dashboard/overview")
    assert r.status_code == 200
    body = r.json()
    assert body["available"] is True
    kpis = {k["key"]: k for k in body["kpis"]}
    # Real report values: 1200 generated / ingested.
    assert kpis["events_generated"]["value"] == 1200
    assert kpis["events_ingested"]["value"] == 1200
    # Latest-run Silver <= latest-run Bronze (some are quarantined/deduped).
    assert kpis["events_processed"]["value"] <= kpis["bronze_records"]["value"]
    # "Silver records" is a cumulative lake total and is labelled as such so it
    # is never confused with the latest-run Bronze count.
    assert "lake total" in kpis["silver_records"]["label"].lower()
    assert kpis["silver_records"]["detail"]
    # kinds are labelled so totals are never mixed with rates.
    assert kpis["throughput"]["kind"] == "rate"


def test_lineage_graph_reflects_real_state(client):
    r = client.get("/api/v1/dashboard/lineage")
    assert r.status_code == 200
    g = r.json()
    ids = {n["id"] for n in g["nodes"]}
    # The real implemented stages must be present.
    for expected in (
        "generator",
        "ingestion",
        "broker",
        "validation",
        "bronze",
        "silver",
        "enrichment",
        "gold",
        "dq",
        "recon",
        "anomaly",
        "quarantine",
        "duckdb",
        "dashboard",
    ):
        assert expected in ids, expected
    # Edges must reference real nodes only.
    for e in g["edges"]:
        assert e["source"] in ids and e["target"] in ids
    # Statuses come from real state, not hardcoded strings.
    statuses = {n["status"] for n in g["nodes"]}
    assert statuses <= {"healthy", "processing", "delayed", "failed", "disabled", "unknown"}
    assert "healthy" in statuses


def test_node_details_endpoint(client):
    r = client.get("/api/v1/dashboard/nodes/bronze")
    assert r.status_code == 200
    assert r.json()["id"] == "bronze"
    assert client.get("/api/v1/dashboard/nodes/nope").status_code == 404


def test_quality_and_reconciliation(client):
    q = client.get("/api/v1/dashboard/quality").json()
    assert isinstance(q, list) and q
    assert {"rule_id", "severity", "failure_count"} <= set(q[0])
    rec = client.get("/api/v1/dashboard/reconciliation").json()
    assert isinstance(rec, list) and rec
    assert "rule_id" in rec[0]


def test_anomalies_and_enrichment(client):
    an = client.get("/api/v1/dashboard/anomalies").json()
    assert isinstance(an, list)
    en = client.get("/api/v1/dashboard/enrichment?limit=5").json()
    assert en["available"] is True
    assert en["total"] >= 1
    assert len(en["rows"]) <= 5
    summary = client.get("/api/v1/dashboard/enrichment/summary").json()
    assert summary["available"] is True
    # Status counts cover the whole dataset, not just a page.
    assert summary["total"] == en["total"]
    assert summary["succeeded"] + summary["failed"] + summary["skipped"] <= summary["total"]
    assert isinstance(summary["models"], dict)


def test_preview_pagination_and_guard(client):
    r = client.get("/api/v1/dashboard/preview?dataset=gold_event_counts&limit=5")
    assert r.status_code == 200
    body = r.json()
    assert body["available"] is True
    assert body["limit"] == 5
    assert len(body["rows"]) <= 5
    # Unknown datasets are refused, not silently allowed.
    bad = client.get("/api/v1/dashboard/preview?dataset=secrets").json()
    assert bad["available"] is False


def test_unavailable_states_are_honest(tmp_path, monkeypatch):
    """With no lake at all, endpoints must say unavailable, not invent values."""
    monkeypatch.setenv("APEXBET_LAKE_ROOT", str(tmp_path / "empty"))
    monkeypatch.setenv("APEXBET_DUCKDB_PATH", str(tmp_path / "empty" / "a.duckdb"))
    monkeypatch.setenv("APEXBET_TRANSPORT", "memory")
    reset_settings_cache()
    svc = DashboardService()
    ov = svc.overview()
    assert ov.available is False
    assert ov.detail
    graph = svc.lineage()
    # No data-driven node may claim "healthy" when there is no run at all;
    # only the explicitly "disabled" (optional) nodes and "unknown" are valid.
    data_nodes = {
        "generator",
        "bronze",
        "silver",
        "enrichment",
        "gold",
        "dq",
        "recon",
        "anomaly",
        "dashboard",
    }
    for n in graph.nodes:
        if n.id in data_nodes:
            assert n.status in {"unknown", "disabled"}, (n.id, n.status)
    reset_settings_cache()


def test_demo_endpoints_validate(client):
    ok = client.post("/api/v1/dashboard/demo/traffic_spike", json={"count": 500})
    assert ok.status_code == 200
    body = ok.json()
    assert body["status"] in {"queued", "running", "succeeded"}
    assert body["params"]["count"] == 500
    bad = client.post("/api/v1/dashboard/demo/evil", json={"count": 100})
    assert bad.status_code == 400
    # Out-of-range counts are rejected at the API boundary.
    over = client.post("/api/v1/dashboard/demo/traffic_spike", json={"count": 10**9})
    assert over.status_code == 422
    under = client.post("/api/v1/dashboard/demo/traffic_spike", json={"count": 1})
    assert under.status_code == 422


def test_demo_controller_rejects_unknown_and_clamps():
    c = DemoController()
    with pytest.raises(ValueError):
        c.submit("rm -rf /", {})
    # param cleaning clamps count to the hard maximum
    cleaned = c._clean_params({"count": 10**9})
    assert cleaned["count"] == MAX_EVENTS
    cleaned = c._clean_params({"count": -5})
    assert cleaned["count"] >= 100


def test_demo_controller_serialises():
    c = DemoController()
    c._current = "job_x"  # simulate a running job
    with pytest.raises(RuntimeError):
        c.submit("traffic_spike", {"count": 100})


def test_cors_headers_present(client):
    r = client.get("/api/v1/dashboard/health", headers={"Origin": "http://localhost:5173"})
    assert r.status_code == 200
    assert r.headers.get("access-control-allow-origin") == "http://localhost:5173"


def test_latest_report_shape(client):
    r = client.get("/api/v1/dashboard/overview").json()
    assert r["run_id"]
    assert r["finished_at"]
    json.dumps(r)  # serialisable
