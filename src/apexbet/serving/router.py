"""Dashboard API router.

Mounts the read + demo-control endpoints the web dashboard consumes. All routes
are typed (Pydantic response models) and read from real pipeline artefacts.
"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

from apexbet.demo_control import MAX_EVENTS, DemoController
from apexbet.serving.dashboard import (
    AnomalyAlert,
    DashboardService,
    EnrichmentSummary,
    LayerStat,
    MetricsSnapshot,
    PipelineNode,
    QualityRule,
    ReconciliationRow,
    RecordPage,
)

# A single process-wide controller; jobs are serialised internally.
demo_controller = DemoController()


class DemoRequest(BaseModel):
    count: int = Field(
        default=3000,
        ge=100,
        le=MAX_EVENTS,
        description="Number of synthetic events to generate (bounded).",
    )
    seed: int = Field(default=1337, ge=0)


class DemoJobResponse(BaseModel):
    job_id: str
    kind: str
    status: str
    started_at: str | None = None
    finished_at: str | None = None
    params: dict[str, Any] = Field(default_factory=dict)
    result: dict[str, Any] | None = None
    error: str | None = None
    log: list[str] = Field(default_factory=list)


class HealthResponse(BaseModel):
    pipeline: str
    last_success: str | None = None
    transport: str
    storage: str
    layers: list[LayerStat] = Field(default_factory=list)


class ThroughputResponse(BaseModel):
    available: bool
    detail: str | None = None
    points: list[dict[str, Any]] = Field(default_factory=list)


def build_router(service: DashboardService | None = None) -> APIRouter:
    svc = service or DashboardService()
    router = APIRouter(prefix="/api/v1/dashboard", tags=["dashboard"])

    @router.get("/health", response_model=HealthResponse)
    def health() -> HealthResponse:
        return HealthResponse(**svc.health())

    @router.get("/overview", response_model=MetricsSnapshot)
    def overview() -> MetricsSnapshot:
        return svc.overview()

    @router.get("/throughput", response_model=ThroughputResponse)
    def throughput(minutes: int = Query(default=180, ge=1, le=1440)) -> ThroughputResponse:
        pts = svc.throughput(limit_minutes=minutes)
        if not pts:
            return ThroughputResponse(
                available=False, detail="No event-volume data yet. Run a pipeline."
            )
        return ThroughputResponse(available=True, points=[p.model_dump() for p in pts])

    @router.get("/latency")
    def latency() -> dict[str, Any]:
        rows = svc.latency_series()
        if not rows:
            return {
                "available": False,
                "detail": "No latency data yet. Run a pipeline.",
                "latency": [],
            }
        return {"available": True, "latency": rows}

    @router.get("/layers", response_model=list[LayerStat])
    def layers() -> list[LayerStat]:
        return svc.layer_stats()

    @router.get("/datasets")
    def datasets() -> dict[str, Any]:
        return {"datasets": svc.dataset_catalog()}

    @router.get("/preview", response_model=RecordPage)
    def preview(
        dataset: str = Query(...),
        offset: int = Query(0, ge=0),
        limit: int = Query(25, ge=1, le=200),
    ) -> RecordPage:
        return svc.preview(dataset, offset=offset, limit=limit)

    @router.get("/quality", response_model=list[QualityRule])
    def quality() -> list[QualityRule]:
        return svc.quality()

    @router.get("/reconciliation", response_model=list[ReconciliationRow])
    def reconciliation() -> list[ReconciliationRow]:
        return svc.reconciliation()

    @router.get("/enrichment", response_model=RecordPage)
    def enrichment(
        offset: int = Query(0, ge=0), limit: int = Query(25, ge=1, le=200)
    ) -> RecordPage:
        return svc.enrichment(offset=offset, limit=limit)

    @router.get("/enrichment/summary", response_model=EnrichmentSummary)
    def enrichment_summary() -> EnrichmentSummary:
        return svc.enrichment_summary()

    @router.get("/anomalies", response_model=list[AnomalyAlert])
    def anomalies() -> list[AnomalyAlert]:
        return svc.anomalies()

    @router.get("/lineage")
    def lineage() -> dict[str, Any]:
        return svc.lineage().model_dump()

    @router.get("/nodes/{node_id}", response_model=PipelineNode)
    def node(node_id: str) -> PipelineNode:
        graph = svc.lineage()
        for n in graph.nodes:
            if n.id == node_id:
                return n
        raise HTTPException(status_code=404, detail=f"unknown node '{node_id}'")

    # -- demo controls (bounded, safe) ------------------------------------ #

    @router.post("/demo/{kind}", response_model=DemoJobResponse)
    def start_demo(kind: str, body: DemoRequest | None = None) -> DemoJobResponse:
        params = body.model_dump() if body else {}
        try:
            job = demo_controller.submit(kind, params)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except RuntimeError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        return DemoJobResponse(**job.to_dict())

    @router.get("/demo/job/{job_id}", response_model=DemoJobResponse)
    def demo_job(job_id: str) -> DemoJobResponse:
        job = demo_controller.get(job_id)
        if job is None:
            raise HTTPException(status_code=404, detail=f"unknown job '{job_id}'")
        return DemoJobResponse(**job.to_dict())

    @router.get("/demo/latest", response_model=DemoJobResponse | None)
    def demo_latest() -> DemoJobResponse | None:
        job = demo_controller.latest()
        return DemoJobResponse(**job.to_dict()) if job else None

    return router
