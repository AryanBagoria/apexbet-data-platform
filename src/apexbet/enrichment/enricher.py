"""Record enrichment.

Enrichment is *decoupled* from ingestion: it processes already-persisted Silver
events and writes structured enrichment records to a separate dataset. Raw
input and enrichment output are kept separate; lineage is preserved via
``event_id`` and the source Silver object key.

Three modes:
  1. ``rules``     -- deterministic, always available (default).
  2. ``local_ml``  -- an installed scikit-learn model (e.g. a TF-IDF + NB
                      classifier for event categorisation), optional.
  3. ``llm``       -- optional local Ollama runtime; the platform stays fully
                      functional when unavailable.

All model output is validated against the enrichment schema. Outputs are NOT
authoritative for financial settlement or account restrictions.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass
from typing import Any, Literal, Protocol

from pydantic import BaseModel, ConfigDict, Field, ValidationError

from apexbet.common.logging import get_logger
from apexbet.common.timeutil import to_iso, utcnow

log = get_logger("apexbet.enrichment")

PROMPT_VERSION = "v1"

EnrichmentType = Literal[
    "normalized_description",
    "category_explanation",
    "quality_issue_description",
    "anomaly_explanation",
    "incident_summary",
    "remediation_suggestion",
]


class EnrichmentRecord(BaseModel):
    """The enrichment schema. Validated before persistence."""

    model_config = ConfigDict(extra="forbid")

    enrichment_id: str
    event_id: str
    model_name: str
    model_version: str
    enrichment_type: EnrichmentType
    enrichment_payload: dict[str, Any]
    confidence: float | None = Field(default=None, ge=0.0, le=1.0)
    created_at: str = Field(default_factory=lambda: to_iso(utcnow()))
    processing_status: Literal["success", "failed", "skipped"] = "success"
    error_reason: str | None = None
    prompt_version: str | None = None
    # lineage
    source_dataset: str = "silver/events"
    source_event_type: str | None = None


@dataclass
class EnrichmentStats:
    attempted: int = 0
    succeeded: int = 0
    failed: int = 0
    skipped: int = 0
    total_latency_ms: float = 0.0

    def record(self, ok: bool, latency_ms: float, skipped: bool = False) -> None:
        self.attempted += 1
        self.total_latency_ms += latency_ms
        if skipped:
            self.skipped += 1
        elif ok:
            self.succeeded += 1
        else:
            self.failed += 1

    def to_dict(self) -> dict[str, Any]:
        n = max(1, self.attempted)
        return {
            **asdict(self),
            "success_rate": round(self.succeeded / n, 4),
            "failure_rate": round(self.failed / n, 4),
            "avg_latency_ms": round(self.total_latency_ms / n, 3),
        }

    @property
    def success_rate(self) -> float:
        return round(self.succeeded / max(1, self.attempted), 4)

    @property
    def failure_rate(self) -> float:
        return round(self.failed / max(1, self.attempted), 4)

    @property
    def avg_latency_ms(self) -> float:
        return round(self.total_latency_ms / max(1, self.attempted), 3)


class Enricher(Protocol):
    name: str
    version: str

    def enrich(self, event: dict[str, Any]) -> EnrichmentRecord: ...


def _enrichment_id(event_id: str, enricher: str, version: str) -> str:
    h = hashlib.sha256(f"{event_id}|{enricher}|{version}".encode()).hexdigest()[:24]
    return f"enr_{h}"


# ---------------------------------------------------------------------------
# 1) Rules-based enricher (always available)
# ---------------------------------------------------------------------------


class RulesEnricher:
    name = "rules-enricher"
    version = "1.0.0"

    _CATEGORY_EXPLAIN = {
        "BETTING": "A bet lifecycle event (placed/accepted/rejected) affecting wagering exposure.",
        "SETTLEMENT": "A bet settlement event that moves money between stake and payout.",
        "MARKET_DATA": "An odds/market event from the pricing feed; no direct customer funds.",
        "CASINO": "A casino game session lifecycle event.",
        "PAYMENT": "A deposit or withdrawal event touching the customer ledger.",
        "ACCOUNT": "An account status change affecting access.",
        "RESPONSIBLE_GAMBLING": "A responsible-gambling limit change (safety/compliance).",
        "RISK": "A risk signal produced by the risk engine.",
        "PLATFORM": "A platform/health event.",
        "OTHER": "Uncategorised event.",
    }

    def enrich(self, event: dict[str, Any]) -> EnrichmentRecord:
        et = event.get("event_type", "Unknown")
        category = event.get("event_category", "OTHER")
        payload = event.get("payload", {}) or {}
        desc = self._describe(et, payload)
        explanation = self._CATEGORY_EXPLAIN.get(category, self._CATEGORY_EXPLAIN["OTHER"])
        return EnrichmentRecord(
            enrichment_id=_enrichment_id(event["event_id"], self.name, self.version),
            event_id=event["event_id"],
            model_name=self.name,
            model_version=self.version,
            enrichment_type="normalized_description",
            enrichment_payload={
                "normalized_description": desc,
                "category": category,
                "category_explanation": explanation,
            },
            confidence=1.0,  # deterministic rules -> full confidence
            source_event_type=et,
        )

    def _describe(self, et: str, payload: dict[str, Any]) -> str:
        if et == "BetPlaced":
            return (
                f"Stake of {payload.get('stake_minor', 0)} minor units "
                f"({payload.get('currency', '?')}) on {payload.get('outcome_id', '?')} "
                f"at odds {payload.get('decimal_odds', '?')}."
            )
        if et == "BetSettled":
            return (
                f"Bet settled as {payload.get('result', '?')} with payout "
                f"{payload.get('payout_minor', 0)} minor units."
            )
        if et in ("DepositRecorded", "WithdrawalRecorded"):
            return (
                f"{et} of {payload.get('amount_minor', 0)} minor units via "
                f"{payload.get('method', '?')}."
            )
        if et.startswith("GameSession"):
            return f"{et} for game {payload.get('game_type', '?')}."
        return f"{et} event."


# ---------------------------------------------------------------------------
# 2) Local ML enricher (optional sklearn)
# ---------------------------------------------------------------------------


class LocalMLEnricher:
    """TF-IDF + LogisticRegression classifier over normalized descriptions.

    Trains on a small built-in, labelled seed corpus (rules-derived text) so the
    platform has a self-contained supervised model with no downloads. Used to
    predict an event category and provide a confidence.
    """

    name = "local-ml-enricher"
    version = "1.0.0"

    _SEED: list[tuple[str, str]] = [
        ("stake bet placed odds outcome", "BETTING"),
        ("bet accepted liability exposure", "BETTING"),
        ("bet rejected insufficient funds", "BETTING"),
        ("bet settled won payout", "SETTLEMENT"),
        ("bet settled lost stake", "SETTLEMENT"),
        ("odds updated market movement", "MARKET_DATA"),
        ("market opened new outcomes", "MARKET_DATA"),
        ("market suspended integrity", "MARKET_DATA"),
        ("game session started slots", "CASINO"),
        ("game session ended wagered", "CASINO"),
        ("deposit recorded card", "PAYMENT"),
        ("withdrawal recorded bank", "PAYMENT"),
        ("account status changed", "ACCOUNT"),
        ("responsible gambling limit changed", "RESPONSIBLE_GAMBLING"),
        ("risk signal velocity", "RISK"),
        ("pipeline health event degraded", "PLATFORM"),
    ]

    def __init__(self) -> None:
        self._ready = False
        self._pipeline = None
        try:
            from sklearn.feature_extraction.text import TfidfVectorizer
            from sklearn.linear_model import LogisticRegression
            from sklearn.pipeline import Pipeline as SkPipeline

            self._pipeline = SkPipeline(
                [
                    ("tfidf", TfidfVectorizer(ngram_range=(1, 2), min_df=1)),
                    ("clf", LogisticRegression(max_iter=500)),
                ]
            )
            texts = [t for t, _ in self._SEED]
            labels = [lab for _, lab in self._SEED]
            self._pipeline.fit(texts, labels)
            self._ready = True
        except Exception as exc:  # pragma: no cover - env dependent
            log.warning("local_ml.unavailable", error=str(exc))

    @property
    def available(self) -> bool:
        return self._ready

    def enrich(self, event: dict[str, Any]) -> EnrichmentRecord:
        rules = RulesEnricher()
        base = rules.enrich(event)
        text = base.enrichment_payload["normalized_description"].lower()
        if not self._ready or self._pipeline is None:
            base.model_name = self.name
            base.processing_status = "skipped"
            base.error_reason = "sklearn model unavailable"
            return base
        proba = self._pipeline.predict_proba([text])[0]
        classes = list(self._pipeline.classes_)
        idx = int(proba.argmax())
        predicted = classes[idx]
        conf = float(proba[idx])
        return EnrichmentRecord(
            enrichment_id=_enrichment_id(event["event_id"], self.name, self.version),
            event_id=event["event_id"],
            model_name=self.name,
            model_version=self.version,
            enrichment_type="category_explanation",
            enrichment_payload={
                "predicted_category": predicted,
                "rule_category": event.get("event_category", "OTHER"),
                "agreement": predicted == event.get("event_category"),
                "normalized_description": base.enrichment_payload["normalized_description"],
            },
            confidence=round(conf, 4),
            source_event_type=event.get("event_type"),
        )


# ---------------------------------------------------------------------------
# 3) Optional local LLM enricher (Ollama)
# ---------------------------------------------------------------------------


class LLMEnricher:
    """Optional Ollama-backed enricher. Never required for the platform to run.

    Applies: structured prompt, JSON-constrained output, timeout, bounded
    retries, output validation, and sanitization/redaction of untrusted event
    data before it is placed in a prompt.
    """

    name = "local-llm-enricher"

    def __init__(
        self, base_url: str, model: str, timeout_seconds: float = 20.0, max_retries: int = 2
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.model = model
        self.version = model
        self.timeout_seconds = timeout_seconds
        self.max_retries = max_retries

    def available(self) -> bool:
        import httpx

        try:
            r = httpx.get(f"{self.base_url}/api/tags", timeout=2.0)
            return r.status_code == 200
        except Exception:
            return False

    def _sanitize(self, event: dict[str, Any]) -> dict[str, Any]:
        """Redact anything that could be sensitive before prompting."""
        safe = {
            "event_id": event.get("event_id"),
            "event_type": event.get("event_type"),
            "event_category": event.get("event_category"),
            "sport_code": event.get("sport_code"),
            "is_late": event.get("is_late"),
        }
        return safe

    def enrich(self, event: dict[str, Any]) -> EnrichmentRecord:
        import httpx

        safe = self._sanitize(event)
        prompt = (
            f"PromptVersion={PROMPT_VERSION}. You are a data-quality assistant for a "
            f"synthetic sportsbook pipeline. Given this event summary, return STRICT JSON "
            f"with keys: description (string, <=160 chars), quality_issue (string or null), "
            f"remediation (string or null). Event: {json.dumps(safe)}"
        )
        last_err: str | None = None
        for attempt in range(self.max_retries + 1):
            try:
                resp = httpx.post(
                    f"{self.base_url}/api/generate",
                    json={"model": self.model, "prompt": prompt, "stream": False, "format": "json"},
                    timeout=self.timeout_seconds,
                )
                resp.raise_for_status()
                body = resp.json()
                raw = body.get("response", "{}")
                parsed = json.loads(raw)
                return EnrichmentRecord(
                    enrichment_id=_enrichment_id(event["event_id"], self.name, self.version),
                    event_id=event["event_id"],
                    model_name=self.name,
                    model_version=self.version,
                    enrichment_type="quality_issue_description",
                    enrichment_payload=parsed,
                    confidence=None,
                    prompt_version=PROMPT_VERSION,
                    source_event_type=event.get("event_type"),
                )
            except (httpx.HTTPError, json.JSONDecodeError, ValidationError) as exc:
                last_err = str(exc)
                log.warning("llm.retry", attempt=attempt, error=last_err)
        return EnrichmentRecord(
            enrichment_id=_enrichment_id(event["event_id"], self.name, self.version),
            event_id=event["event_id"],
            model_name=self.name,
            model_version=self.version,
            enrichment_type="quality_issue_description",
            enrichment_payload={},
            processing_status="failed",
            error_reason=last_err or "unknown",
            prompt_version=PROMPT_VERSION,
            source_event_type=event.get("event_type"),
        )


def build_enricher(mode: str, *, llm_base_url: str, llm_model: str, llm_timeout: float) -> Enricher:
    """Select an enricher by mode. Falls back to rules if the requested backend
    is unavailable, recording the fallback (never silently for the LLM case)."""
    if mode == "llm":
        enr = LLMEnricher(llm_base_url, llm_model, llm_timeout)
        if enr.available():
            return enr
        log.warning("llm.unavailable_fallback_to_rules", base_url=llm_base_url)
        return RulesEnricher()
    if mode == "local_ml":
        enr_ml = LocalMLEnricher()
        if enr_ml.available:
            return enr_ml
        log.warning("local_ml.unavailable_fallback_to_rules")
        return RulesEnricher()
    return RulesEnricher()


class EnrichmentRunner:
    """Runs an enricher over Silver events with idempotency + stats."""

    def __init__(self, enricher: Enricher) -> None:
        self.enricher = enricher
        self.stats = EnrichmentStats()
        self._done: set[str] = set()

    def run(
        self, events: list[dict[str, Any]], *, skip_ids: set[str] | None = None
    ) -> list[dict[str, Any]]:
        skip_ids = skip_ids or set()
        out: list[dict[str, Any]] = []
        for ev in events:
            eid = ev.get("event_id")
            if eid is None:
                continue
            if eid in self._done or eid in skip_ids:
                self.stats.record(ok=True, latency_ms=0.0, skipped=True)
                continue
            start = utcnow()
            try:
                rec = self.enricher.enrich(ev)
                ok = rec.processing_status != "failed"
            except ValidationError as exc:
                self.stats.record(ok=False, latency_ms=0.0)
                out.append(
                    _failed(
                        eid,
                        self.enricher.name,
                        self.enricher.version,
                        f"schema validation failed: {exc}",
                    )
                )
                continue
            except Exception as exc:  # pragma: no cover - defensive
                self.stats.record(ok=False, latency_ms=0.0)
                out.append(_failed(eid, self.enricher.name, self.enricher.version, str(exc)))
                continue
            latency = (utcnow() - start).total_seconds() * 1000
            self.stats.record(ok=ok, latency_ms=latency, skipped=rec.processing_status == "skipped")
            self._done.add(eid)
            out.append(rec.model_dump())
        return out


def _failed(event_id: str, model: str, version: str, reason: str) -> dict[str, Any]:
    return EnrichmentRecord(
        enrichment_id=_enrichment_id(event_id, model, version),
        event_id=event_id,
        model_name=model,
        model_version=version,
        enrichment_type="quality_issue_description",
        enrichment_payload={},
        processing_status="failed",
        error_reason=reason,
    ).model_dump()


def validate_enrichment(records: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], int]:
    """Validate enrichment records against the schema; returns (valid, invalid_count)."""
    valid: list[dict[str, Any]] = []
    invalid = 0
    for r in records:
        try:
            EnrichmentRecord.model_validate(r)
            valid.append(r)
        except ValidationError:
            invalid += 1
    return valid, invalid
