"""Silver-layer transformations.

Deterministic, reusable business logic applied after Bronze. This is the single
implementation of the "clean + normalize + enrich" rules; both the streaming
Python processor and the Spark job call into the same pure functions so the two
never diverge.

Responsibilities
----------------
* Schema enforcement + timestamp normalization (to UTC ISO).
* Identifier standardization (lowercase, trimmed).
* Monetary validation (integer minor units, non-negative, in range).
* Reference-data joins (sport/league/team/market labels).
* Deduplication (by event_id, within a window handled by the caller).
* Business-rule validation and lifecycle consistency checks.
* Late-event classification relative to a watermark.
* Quarantine routing for invalid records.
"""

from __future__ import annotations

import contextlib
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any

from apexbet.common.timeutil import ms_between, parse_iso, to_iso, utcnow
from apexbet.generator.reference import ReferenceData
from apexbet.schemas.events import EventType, validate_event

_REF = ReferenceData.build()


@dataclass
class SilverResult:
    """Outcome of transforming one raw event."""

    ok: bool
    record: dict[str, Any] | None = None
    quarantine_reason: str | None = None
    issues: list[str] = field(default_factory=list)
    is_late: bool = False
    is_duplicate: bool = False


_DERIVED_CATEGORY: dict[str, str] = {
    EventType.BET_PLACED.value: "BETTING",
    EventType.BET_ACCEPTED.value: "BETTING",
    EventType.BET_REJECTED.value: "BETTING",
    EventType.BET_SETTLED.value: "SETTLEMENT",
    EventType.ODDS_UPDATED.value: "MARKET_DATA",
    EventType.MARKET_OPENED.value: "MARKET_DATA",
    EventType.MARKET_SUSPENDED.value: "MARKET_DATA",
    EventType.GAME_SESSION_STARTED.value: "CASINO",
    EventType.GAME_SESSION_ENDED.value: "CASINO",
    EventType.DEPOSIT_RECORDED.value: "PAYMENT",
    EventType.WITHDRAWAL_RECORDED.value: "PAYMENT",
    EventType.ACCOUNT_STATUS_CHANGED.value: "ACCOUNT",
    EventType.RESPONSIBLE_GAMBLING_LIMIT_CHANGED.value: "RESPONSIBLE_GAMBLING",
    EventType.RISK_SIGNAL_GENERATED.value: "RISK",
    EventType.PIPELINE_HEALTH_EVENT.value: "PLATFORM",
}

_MONETARY_FIELDS = (
    "stake_minor",
    "accepted_stake_minor",
    "rejected_stake_minor",
    "settled_stake_minor",
    "payout_minor",
    "amount_minor",
    "potential_return_minor",
    "liability_minor",
    "starting_balance_minor",
    "total_wagered_minor",
    "total_won_minor",
    "ending_balance_minor",
    "previous_limit_minor",
    "new_limit_minor",
)


def _std_id(value: Any) -> Any:
    return value.strip().lower() if isinstance(value, str) else value


def _validate_money(payload: dict[str, Any]) -> list[str]:
    problems: list[str] = []
    for f in _MONETARY_FIELDS:
        if f in payload and payload[f] is not None:
            v = payload[f]
            if not isinstance(v, int):
                problems.append(f"non_integer_{f}")
            elif v < 0:
                problems.append(f"negative_{f}")
            elif v > 10**14:
                problems.append(f"out_of_range_{f}")
    return problems


def _enrich_reference(payload: dict[str, Any], event_type: str) -> dict[str, Any]:
    """Deterministic reference enrichment + derived attributes."""
    out: dict[str, Any] = {}
    sport_code = payload.get("sport_code")
    if sport_code and sport_code in _REF.sports:
        out["sport_name"] = _REF.sports[sport_code].name
    mid = payload.get("market_id")
    if isinstance(mid, str):
        # market ids look like mkt_match_<league>_<n>_<TYPE>_<outcome>
        parts = mid.split("_")
        if len(parts) >= 3:
            league_code = parts[2].upper()
            if league_code in _REF.leagues:
                out["league_code"] = league_code
                out["league_name"] = _REF.leagues[league_code].name
                out["sport_code_derived"] = _REF.leagues[league_code].sport_code
    result = payload.get("result")
    if event_type == EventType.BET_SETTLED.value and result in ("WON",):
        out["is_winning_settlement"] = True
    elif event_type == EventType.BET_SETTLED.value:
        out["is_winning_settlement"] = False
    return out


def transform_event(
    raw: dict[str, Any],
    *,
    watermark_iso: str | None = None,
    allowed_lateness_seconds: int = 600,
) -> SilverResult:
    """Transform a raw Bronze/stream event into a Silver record.

    Returns a ``SilverResult``; invalid records carry a ``quarantine_reason``.
    """
    issues: list[str] = []
    vr = validate_event(raw)
    if not vr.ok or vr.envelope is None:
        return SilverResult(
            ok=False,
            quarantine_reason=vr.issues[0].code if vr.issues else "invalid",
            issues=[f"{i.code}:{i.field}" for i in vr.issues],
        )
    env = vr.envelope

    payload = dict(env.payload)

    # Money validation -> explicit quarantine reason
    money_problems = _validate_money(payload)
    if money_problems:
        return SilverResult(
            ok=False,
            quarantine_reason="invalid_monetary_value",
            issues=money_problems,
        )

    # Identifier standardization
    for k in ("bet_id", "market_id", "outcome_id", "transaction_id", "game_id"):
        if k in payload:
            payload[k] = _std_id(payload[k])

    # Timestamps
    event_ts = parse_iso(env.event_timestamp)
    ingestion_ts = env.ingestion_timestamp or to_iso(utcnow())
    processing_ts = to_iso(utcnow())

    # Late-event classification
    is_late = False
    if watermark_iso is not None:
        wm = parse_iso(watermark_iso)
        lag_s = (wm - event_ts).total_seconds()
        if lag_s > allowed_lateness_seconds:
            is_late = True

    # Lifecycle consistency: settlement without accepted stake, etc.
    if env.event_type == EventType.BET_SETTLED.value and payload.get("settled_stake_minor", 0) <= 0:
        issues.append("settlement_without_stake")
    if (
        env.event_type == EventType.BET_ACCEPTED.value
        and payload.get("accepted_stake_minor", 0) <= 0
    ):
        issues.append("acceptance_without_stake")

    enrichment = _enrich_reference(payload, env.event_type)
    category = _DERIVED_CATEGORY.get(env.event_type, "OTHER")

    processing_latency_ms = None
    with contextlib.suppress(Exception):  # pragma: no cover
        processing_latency_ms = round(ms_between(parse_iso(ingestion_ts), utcnow()), 3)

    # Fixed, stable schema. Optional enrichment fields are always present (None
    # when not derivable) so every Silver parquet file shares one schema and
    # DuckDB can read them together without union_by_name surprises.
    record = {
        "event_id": env.event_id,
        "event_type": env.event_type,
        "schema_version": env.schema_version,
        "event_timestamp": to_iso(event_ts),
        "ingestion_timestamp": ingestion_ts,
        "processing_timestamp": processing_ts,
        "producer": env.producer,
        "account_id": env.account_id,
        "session_id": env.session_id,
        "correlation_id": env.correlation_id,
        "partition_key": env.partition_key,
        "trace_id": env.trace_id,
        "event_category": category,
        "is_late": is_late,
        "processing_latency_ms": processing_latency_ms,
        "event_date": to_iso(event_ts)[:10],
        "sport_code": payload.get("sport_code") or enrichment.get("sport_code_derived"),
        "sport_name": enrichment.get("sport_name"),
        "league_code": enrichment.get("league_code"),
        "league_name": enrichment.get("league_name"),
        "is_winning_settlement": enrichment.get("is_winning_settlement"),
        "payload": payload,
    }
    return SilverResult(ok=True, record=record, issues=issues, is_late=is_late)


def dedupe_events(records: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], int]:
    """Deduplicate by event_id preserving first occurrence. Returns (unique, dropped)."""
    seen: set[str] = set()
    out: list[dict[str, Any]] = []
    dropped = 0
    for r in records:
        eid = r["event_id"]
        if eid in seen:
            dropped += 1
            continue
        seen.add(eid)
        out.append(r)
    return out, dropped


def money_decimal(minor: int) -> Decimal:
    return (Decimal(minor) / 100).quantize(Decimal("0.01"))
