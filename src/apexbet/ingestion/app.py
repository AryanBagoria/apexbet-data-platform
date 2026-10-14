"""FastAPI ingestion service.

Endpoints (v1):
  * POST /api/v1/events           -- ingest one or a batch of events
  * GET  /health                  -- liveness (always cheap)
  * GET  /ready                   -- readiness (checks transport + storage)
  * GET  /metrics                 -- Prometheus exposition
  * GET  /api/v1/platform/summary -- platform summary (queries Gold/DuckDB)
  * GET  /api/v1/platform/data-quality
  * GET  /api/v1/platform/latency
  * GET  /api/v1/platform/anomalies
  * GET  /api/v1/platform/reconciliation

Design notes
------------
* Ingestion performs *validation + publish only*. No heavy transformation runs
  synchronously. Latency here is measured separately from end-to-end latency.
* Body size, rate and auth are configurable; protected routes require a bearer
  token when ``APEXBET_API_TOKENS`` is set.
* Structured request logging with a per-request request_id + correlation_id.
"""

from __future__ import annotations

import contextlib
import json
import time
from contextlib import asynccontextmanager
from typing import Any

from fastapi import Depends, FastAPI, Header, HTTPException, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, PlainTextResponse
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Gauge, Histogram, generate_latest
from pydantic import BaseModel, Field

from apexbet import __version__
from apexbet.common.ids import new_correlation_id, new_trace_id
from apexbet.common.logging import configure_logging, get_logger
from apexbet.common.timeutil import to_iso, utcnow
from apexbet.config import Settings, get_settings
from apexbet.schemas.events import validate_event

log = get_logger("apexbet.ingestion")

# -- Prometheus metrics (label cardinality controlled: no ids as labels) -----
API_REQUESTS = Counter(
    "apexbet_api_requests_total", "Total ingestion API requests", ["endpoint", "status"]
)
API_LATENCY = Histogram(
    "apexbet_api_request_latency_seconds",
    "Ingestion API request latency",
    ["endpoint"],
    buckets=(0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5),
)
EVENTS_PUBLISHED = Counter(
    "apexbet_events_published_total", "Events accepted and published", ["event_type"]
)
EVENTS_REJECTED = Counter(
    "apexbet_events_rejected_total", "Events rejected at validation", ["reason"]
)
PUBLISH_ERRORS = Counter("apexbet_publish_errors_total", "Transport publish errors")
INGEST_LAG = Histogram(
    "apexbet_ingest_lag_seconds",
    "event_timestamp -> ingestion latency",
    buckets=(0.01, 0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10, 30, 60),
)
UP = Gauge("apexbet_up", "1 when the ingestion service is up")


class EventIn(BaseModel):
    """A single event body. Kept permissive at the wire layer; the envelope +
    payload validators decide correctness."""

    model_config = {"extra": "allow"}


class IngestResponse(BaseModel):
    accepted: int
    rejected: int
    duplicates_in_batch: int
    request_id: str
    correlation_id: str
    errors: list[dict[str, Any]] = Field(default_factory=list)


def _extract_bearer(authorization: str | None) -> str | None:
    if not authorization:
        return None
    parts = authorization.split()
    if len(parts) == 2 and parts[0].lower() == "bearer":
        return parts[1]
    return None


def make_app(
    settings: Settings | None = None, transport: Any | None = None, store: Any | None = None
) -> FastAPI:
    """Application factory. Transport/store are injected for testability."""
    settings = settings or get_settings()
    configure_logging(settings.log_level, settings.log_json)

    state: dict[str, Any] = {"transport": transport, "store": store}

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        if state["transport"] is None:
            from apexbet.transports import build_transport

            state["transport"] = build_transport(settings.transport, settings=settings)
        if state["store"] is None:
            from apexbet.common.storage import build_store

            state["store"] = build_store()
        UP.set(1)
        log.info(
            "ingestion.startup",
            transport=settings.transport,
            storage=settings.storage_backend,
            version=__version__,
        )
        try:
            yield
        finally:
            UP.set(0)
            with contextlib.suppress(Exception):  # pragma: no cover
                state["transport"].close()
            log.info("ingestion.shutdown")

    app = FastAPI(
        title="ApexBet Ingestion API",
        version=__version__,
        description=(
            "Ingestion and read API for the ApexBet synthetic gambling data platform. "
            "Synthetic data only."
        ),
        lifespan=lifespan,
    )
    app.state.settings = settings
    app.state.data = state

    # -- CORS for the local dashboard (dev only; no credentials) ------------
    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.cors_origins_list,
        allow_credentials=False,
        allow_methods=["GET", "POST", "OPTIONS"],
        allow_headers=["*"],
    )

    # -- auth dependency ----------------------------------------------------
    def require_auth(request: Request, authorization: str | None = Header(default=None)) -> None:
        tokens = settings.token_set
        if not tokens:
            return  # auth disabled locally
        path = request.url.path
        if not any(path.startswith(p) for p in settings.protected_prefixes):
            return
        provided = _extract_bearer(authorization)
        if provided is None or provided not in tokens:
            raise HTTPException(status_code=401, detail="missing or invalid bearer token")

    # -- middleware: request id + latency + body limit ----------------------
    @app.middleware("http")
    async def _middleware(request: Request, call_next):  # type: ignore[no-untyped-def]
        rid = request.headers.get("x-request-id") or new_trace_id()
        cid = request.headers.get("x-correlation-id") or new_correlation_id()
        request.state.request_id = rid
        request.state.correlation_id = cid

        cl = request.headers.get("content-length")
        if cl is not None and cl.isdigit() and int(cl) > settings.api_max_body_bytes:
            API_REQUESTS.labels(endpoint=request.url.path, status="413").inc()
            return JSONResponse(status_code=413, content={"detail": "request body too large"})

        endpoint = request.url.path
        start = time.perf_counter()
        try:
            response = await call_next(request)
        except Exception as exc:  # pragma: no cover - safety net
            API_REQUESTS.labels(endpoint=endpoint, status="500").inc()
            log.exception("request.error", request_id=rid, error=str(exc))
            return JSONResponse(status_code=500, content={"detail": "internal error"})
        elapsed = time.perf_counter() - start
        API_LATENCY.labels(endpoint=endpoint).observe(elapsed)
        API_REQUESTS.labels(endpoint=endpoint, status=str(response.status_code)).inc()
        response.headers["x-request-id"] = rid
        response.headers["x-correlation-id"] = cid
        return response

    # -- endpoints ----------------------------------------------------------

    @app.post("/api/v1/events", response_model=IngestResponse)
    async def ingest_events(request: Request) -> IngestResponse:
        rid = request.state.request_id
        cid = request.state.correlation_id

        try:
            body = await request.json()
        except Exception:
            raise HTTPException(status_code=422, detail="body must be valid JSON") from None

        if isinstance(body, dict) and "events" in body and isinstance(body["events"], list):
            raw_events: list[dict[str, Any]] = body["events"]
        elif isinstance(body, list):
            raw_events = body
        elif isinstance(body, dict):
            raw_events = [body]
        else:
            raise HTTPException(status_code=422, detail="expected object or array of events")

        accepted = 0
        rejected = 0
        dupes = 0
        errors: list[dict[str, Any]] = []
        seen_ids: set[str] = set()
        transport = state["transport"]

        batch: list[tuple[str | None, bytes]] = []
        for raw in raw_events:
            result = validate_event(raw)
            if not result.ok or result.envelope is None:
                rejected += 1
                for issue in result.issues:
                    EVENTS_REJECTED.labels(reason=issue.code).inc()
                errors.append(
                    {
                        "event_id": raw.get("event_id"),
                        "issues": [i.model_dump() for i in result.issues],
                    }
                )
                continue
            env = result.envelope

            # in-batch duplicate detection (cheap; cross-batch dedup is downstream)
            if env.event_id in seen_ids:
                dupes += 1
            seen_ids.add(env.event_id)

            # stamp ingestion timestamp + trace, then publish raw bytes
            env.ingestion_timestamp = env.ingestion_timestamp or to_iso(utcnow())
            env.trace_id = env.trace_id or rid
            doc = env.model_dump(mode="json")

            try:
                from apexbet.common.timeutil import ms_between, parse_iso

                lag = ms_between(parse_iso(env.event_timestamp), utcnow()) / 1000.0
                if lag >= 0:
                    INGEST_LAG.observe(min(lag, 3600))
            except Exception:  # pragma: no cover
                pass

            batch.append((env.partition_key, json.dumps(doc).encode("utf-8")))
            EVENTS_PUBLISHED.labels(event_type=str(env.event_type)).inc()
            accepted += 1

        if batch:
            try:
                transport.publish_many(settings.kafka_topic, batch)
            except Exception as exc:
                PUBLISH_ERRORS.inc()
                log.error("publish.failed", error=str(exc), request_id=rid, count=len(batch))
                raise HTTPException(
                    status_code=503, detail="streaming backend unavailable"
                ) from exc

        log.info(
            "events.ingested",
            accepted=accepted,
            rejected=rejected,
            duplicates=dupes,
            request_id=rid,
            correlation_id=cid,
        )
        return IngestResponse(
            accepted=accepted,
            rejected=rejected,
            duplicates_in_batch=dupes,
            request_id=rid,
            correlation_id=cid,
            errors=errors[:20],
        )

    @app.get("/health")
    async def health() -> dict[str, Any]:
        return {"status": "ok", "version": __version__, "service": "ingestion"}

    @app.get("/ready")
    async def ready() -> JSONResponse:
        transport = state["transport"]
        store = state["store"]
        checks = {
            "transport": transport.health() if transport else {"status": "DOWN"},
            "storage": store.health() if store else {"status": "DOWN"},
        }
        ok = all(c.get("status") == "OK" for c in checks.values())
        return JSONResponse(status_code=200 if ok else 503, content={"ready": ok, "checks": checks})

    @app.get("/metrics")
    async def metrics() -> Response:
        return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)

    # -- read APIs (query local Gold/DuckDB if present) ---------------------
    def _analytics() -> Any | None:
        try:
            from apexbet.analytics.duckdb_engine import AnalyticsEngine

            eng = AnalyticsEngine(settings.duckdb_path, store=state["store"])
            if not eng.available():
                return None
            return eng
        except Exception:  # pragma: no cover
            return None

    @app.get("/api/v1/platform/summary", dependencies=[Depends(require_auth)])
    async def summary() -> dict[str, Any]:
        eng = _analytics()
        if eng is None:
            return {"available": False, "detail": "Gold layer not built yet. Run `make process`."}
        return {"available": True, "summary": eng.platform_summary()}

    @app.get("/api/v1/platform/data-quality", dependencies=[Depends(require_auth)])
    async def data_quality() -> dict[str, Any]:
        eng = _analytics()
        if eng is None:
            return {
                "available": False,
                "detail": "quality results not built yet. Run `make quality`.",
            }
        return {"available": True, "quality": eng.data_quality_summary()}

    @app.get("/api/v1/platform/latency", dependencies=[Depends(require_auth)])
    async def latency() -> dict[str, Any]:
        eng = _analytics()
        if eng is None:
            return {
                "available": False,
                "detail": "latency datasets not built yet. Run `make process`.",
            }
        return {"available": True, "latency": eng.latency_summary()}

    @app.get("/api/v1/platform/anomalies", dependencies=[Depends(require_auth)])
    async def anomalies() -> dict[str, Any]:
        eng = _analytics()
        if eng is None:
            return {"available": False, "detail": "no anomaly output yet. Run `make process`."}
        return {"available": True, "anomalies": eng.anomaly_summary()}

    @app.get("/api/v1/platform/reconciliation", dependencies=[Depends(require_auth)])
    async def reconciliation() -> dict[str, Any]:
        eng = _analytics()
        if eng is None:
            return {
                "available": False,
                "detail": "no reconciliation output yet. Run `make reconcile`.",
            }
        return {"available": True, "reconciliation": eng.reconciliation_summary()}

    # -- dashboard API (read + safe demo controls) --------------------------
    from apexbet.serving.router import build_router

    app.include_router(build_router())

    @app.get("/", response_class=PlainTextResponse)
    async def root() -> str:
        return f"ApexBet ingestion API v{__version__} — see /docs"

    return app


app = make_app()
