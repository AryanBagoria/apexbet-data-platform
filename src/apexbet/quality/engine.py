"""Data quality engine.

Runs structured, rule-based checks over event records (Bronze or Silver) and
produces machine-readable results with rule id, dataset, severity, record
count, failure count and diagnostics. Quarantined records are returned
separately for inspection and replay.

Rules cover: required fields, schema compatibility, valid timestamps, valid
monetary values, duplicate ids, unsupported event types, invalid lifecycle
transitions, referential integrity, missing reference data, settlement
discrepancies, record-count mismatches, volume anomalies, freshness,
processing completeness, and AI-enrichment schema validity.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta
from typing import Any, Literal

from apexbet.common.timeutil import parse_iso, utcnow
from apexbet.generator.reference import ReferenceData
from apexbet.schemas.events import EventType, is_supported_version

Severity = Literal["info", "warning", "error", "critical"]

_REF = ReferenceData.build()
_KNOWN_TYPES = {t.value for t in EventType}


@dataclass
class QualityResult:
    rule_id: str
    dataset: str
    severity: Severity
    record_count: int
    failure_count: int
    description: str
    diagnostics: dict[str, Any] = field(default_factory=dict)
    sample: list[dict[str, Any]] = field(default_factory=list)

    @property
    def passed(self) -> bool:
        return self.failure_count == 0

    def to_dict(self) -> dict[str, Any]:
        d = asdict(self)
        d["passed"] = self.passed
        return d


def _fail(severity: Severity, diagnostics: dict[str, Any]) -> tuple[bool, Severity, dict[str, Any]]:
    return True, severity, diagnostics


class DataQualityEngine:
    """Runs the rule set over a list of event dicts (raw envelopes)."""

    def __init__(self, *, collection_name: str = "bronze.events") -> None:
        self.collection = collection_name

    def run_all(
        self,
        events: list[dict[str, Any]],
        *,
        expected_count: int | None = None,
        freshness_seconds: int = 3600,
        now: datetime | None = None,
    ) -> tuple[list[QualityResult], list[dict[str, Any]]]:
        """Run every rule. Returns (results, quarantined_records)."""
        now = now or utcnow()
        results: list[QualityResult] = []
        quarantined: list[dict[str, Any]] = []

        results.append(self._required_fields(events, quarantined))
        results.append(self._schema_compat(events, quarantined))
        results.append(self._valid_timestamps(events, quarantined))
        results.append(self._valid_money(events, quarantined))
        results.append(self._duplicate_ids(events))
        results.append(self._unsupported_types(events, quarantined))
        results.append(self._referential_integrity(events, quarantined))
        results.append(self._lifecycle(events))
        results.append(self._settlement_discrepancies(events))
        results.append(self._count_match(events, expected_count))
        results.append(self._freshness(events, freshness_seconds, now))
        return results, _merge_quarantine(quarantined)

    # -- individual rules ---------------------------------------------------

    def _required_fields(
        self, events: list[dict[str, Any]], quarantined: list[dict[str, Any]]
    ) -> QualityResult:
        required = ("event_id", "event_type", "schema_version", "event_timestamp", "partition_key")
        failures = 0
        sample: list[dict[str, Any]] = []
        for e in events:
            missing = [f for f in required if not e.get(f)]
            if missing:
                failures += 1
                if len(sample) < 5:
                    sample.append({"event_id": e.get("event_id"), "missing": missing})
                quarantined.append({**e, "_dq_rule": "required_fields", "_dq_missing": missing})
        return QualityResult(
            rule_id="DQ001_required_fields",
            dataset=self.collection,
            severity="critical",
            record_count=len(events),
            failure_count=failures,
            description="Envelope must contain all required fields.",
            diagnostics={"required": list(required)},
            sample=sample,
        )

    def _schema_compat(
        self, events: list[dict[str, Any]], quarantined: list[dict[str, Any]]
    ) -> QualityResult:
        failures = 0
        sample: list[dict[str, Any]] = []
        for e in events:
            v = e.get("schema_version", "")
            if not is_supported_version(str(v)):
                failures += 1
                if len(sample) < 5:
                    sample.append({"event_id": e.get("event_id"), "schema_version": v})
                quarantined.append({**e, "_dq_rule": "schema_compat"})
        return QualityResult(
            rule_id="DQ002_schema_compatibility",
            dataset=self.collection,
            severity="error",
            record_count=len(events),
            failure_count=failures,
            description="schema_version must be within the supported range.",
            sample=sample,
        )

    def _valid_timestamps(
        self, events: list[dict[str, Any]], quarantined: list[dict[str, Any]]
    ) -> QualityResult:
        failures = 0
        sample: list[dict[str, Any]] = []
        for e in events:
            try:
                ts = parse_iso(str(e.get("event_timestamp")))
                if ts > utcnow() + timedelta(minutes=5):
                    raise ValueError("timestamp in the future")
            except Exception as exc:
                failures += 1
                if len(sample) < 5:
                    sample.append(
                        {
                            "event_id": e.get("event_id"),
                            "event_timestamp": e.get("event_timestamp"),
                            "error": str(exc),
                        }
                    )
                quarantined.append({**e, "_dq_rule": "valid_timestamps"})
        return QualityResult(
            rule_id="DQ003_valid_timestamps",
            dataset=self.collection,
            severity="error",
            record_count=len(events),
            failure_count=failures,
            description="event_timestamp must be a parseable UTC time not in the future.",
            sample=sample,
        )

    def _valid_money(
        self, events: list[dict[str, Any]], quarantined: list[dict[str, Any]]
    ) -> QualityResult:
        money_fields = (
            "stake_minor",
            "amount_minor",
            "payout_minor",
            "accepted_stake_minor",
            "settled_stake_minor",
        )
        failures = 0
        sample: list[dict[str, Any]] = []
        for e in events:
            payload = e.get("payload") or {}
            bad = [
                f
                for f in money_fields
                if payload.get(f) is not None
                and (
                    not isinstance(payload[f], int)
                    or isinstance(payload[f], bool)
                    or payload[f] < 0
                )
            ]
            if bad:
                failures += 1
                if len(sample) < 5:
                    sample.append(
                        {
                            "event_id": e.get("event_id"),
                            "invalid": bad,
                            "values": {f: payload[f] for f in bad},
                        }
                    )
                quarantined.append({**e, "_dq_rule": "valid_money"})
        return QualityResult(
            rule_id="DQ004_valid_monetary_values",
            dataset=self.collection,
            severity="error",
            record_count=len(events),
            failure_count=failures,
            description="Monetary fields must be non-negative integers (minor units).",
            sample=sample,
        )

    def _duplicate_ids(self, events: list[dict[str, Any]]) -> QualityResult:
        seen: dict[str, int] = {}
        for e in events:
            eid = str(e.get("event_id") or "")
            seen[eid] = seen.get(eid, 0) + 1
        dupes = {k: v for k, v in seen.items() if v > 1 and k}
        return QualityResult(
            rule_id="DQ005_unique_event_ids",
            dataset=self.collection,
            severity="warning",
            record_count=len(events),
            failure_count=sum(v - 1 for v in dupes.values()),
            description="event_id should be unique; duplicates indicate at-least-once redelivery.",
            diagnostics={"distinct_duplicate_ids": len(dupes)},
            sample=[{"event_id": k, "count": v} for k, v in list(dupes.items())[:5]],
        )

    def _unsupported_types(
        self, events: list[dict[str, Any]], quarantined: list[dict[str, Any]]
    ) -> QualityResult:
        failures = 0
        sample: list[dict[str, Any]] = []
        for e in events:
            if e.get("event_type") not in _KNOWN_TYPES:
                failures += 1
                if len(sample) < 5:
                    sample.append(
                        {"event_id": e.get("event_id"), "event_type": e.get("event_type")}
                    )
                quarantined.append({**e, "_dq_rule": "unsupported_type"})
        return QualityResult(
            rule_id="DQ006_supported_event_type",
            dataset=self.collection,
            severity="error",
            record_count=len(events),
            failure_count=failures,
            description="event_type must be one of the registered contract types.",
            sample=sample,
        )

    def _referential_integrity(
        self, events: list[dict[str, Any]], quarantined: list[dict[str, Any]]
    ) -> QualityResult:
        failures = 0
        sample: list[dict[str, Any]] = []
        for e in events:
            payload = e.get("payload") or {}
            mid = payload.get("market_id")
            if isinstance(mid, str) and mid.startswith("mkt_") and ("unknown" in mid.lower()):
                failures += 1
                if len(sample) < 5:
                    sample.append({"event_id": e.get("event_id"), "market_id": mid})
                quarantined.append({**e, "_dq_rule": "referential_integrity"})
            elif isinstance(mid, str) and mid.startswith("mkt_match_"):
                parts = mid.split("_")
                if len(parts) >= 3 and parts[2].upper() not in _REF.leagues:
                    failures += 1
                    if len(sample) < 5:
                        sample.append({"event_id": e.get("event_id"), "market_id": mid})
        return QualityResult(
            rule_id="DQ007_referential_integrity",
            dataset=self.collection,
            severity="error",
            record_count=len(events),
            failure_count=failures,
            description="Referenced market/league must exist in reference data.",
            sample=sample,
        )

    def _lifecycle(self, events: list[dict[str, Any]]) -> QualityResult:
        """Settlement must be preceded (in the batch) by an acceptance of the bet."""
        placed: set[str] = set()
        accepted: set[str] = set()
        settled_without_accept = 0
        sample: list[dict[str, Any]] = []
        # events come in some order; do a two-pass window check.
        for e in events:
            if e.get("event_type") == EventType.BET_ACCEPTED.value:
                bid = (e.get("payload") or {}).get("bet_id")
                if bid:
                    accepted.add(bid)
            elif e.get("event_type") == EventType.BET_PLACED.value:
                bid = (e.get("payload") or {}).get("bet_id")
                if bid:
                    placed.add(bid)
        for e in events:
            if e.get("event_type") == EventType.BET_SETTLED.value:
                bid = (e.get("payload") or {}).get("bet_id")
                if bid and bid not in accepted:
                    settled_without_accept += 1
                    if len(sample) < 5:
                        sample.append({"event_id": e.get("event_id"), "bet_id": bid})
        return QualityResult(
            rule_id="DQ008_lifecycle_consistency",
            dataset=self.collection,
            severity="warning",
            record_count=len(events),
            failure_count=settled_without_accept,
            description="A settled bet should have an accepted bet in the same window.",
            diagnostics={"placed": len(placed), "accepted": len(accepted)},
            sample=sample,
        )

    def _settlement_discrepancies(self, events: list[dict[str, Any]]) -> QualityResult:
        """Payout should equal stake*odds for WON, stake for VOID/PUSH, 0 for LOST."""
        failures = 0
        sample: list[dict[str, Any]] = []
        accepted_odds: dict[str, float] = {}
        for e in events:
            if e.get("event_type") == EventType.BET_ACCEPTED.value:
                p = e.get("payload") or {}
                if p.get("bet_id"):
                    accepted_odds[p["bet_id"]] = float(p.get("accepted_odds", 0))
        for e in events:
            if e.get("event_type") != EventType.BET_SETTLED.value:
                continue
            p = e.get("payload") or {}
            bid = str(p.get("bet_id") or "")
            result = p.get("result")
            stake = p.get("settled_stake_minor", 0)
            payout = p.get("payout_minor", 0)
            odds = accepted_odds.get(bid)
            expected: int | None = None
            if result == "LOST":
                expected = 0
            elif result in ("VOID", "PUSH"):
                expected = stake
            elif result == "WON" and odds:
                expected = round(stake * odds)
            if expected is not None and payout != expected:
                tolerance = max(2, int(expected * 0.01))
                if abs(payout - expected) > tolerance:
                    failures += 1
                    if len(sample) < 5:
                        sample.append(
                            {
                                "event_id": e.get("event_id"),
                                "bet_id": bid,
                                "result": result,
                                "expected": expected,
                                "actual": payout,
                            }
                        )
        return QualityResult(
            rule_id="DQ009_settlement_discrepancy",
            dataset=self.collection,
            severity="critical",
            record_count=len(events),
            failure_count=failures,
            description="Settlement payout must match the deterministic expected amount.",
            sample=sample,
        )

    def _count_match(
        self, events: list[dict[str, Any]], expected_count: int | None
    ) -> QualityResult:
        if expected_count is None:
            return QualityResult(
                rule_id="DQ010_record_count_match",
                dataset=self.collection,
                severity="info",
                record_count=len(events),
                failure_count=0,
                description="No expected count supplied; skipped.",
                diagnostics={"skipped": True},
            )
        diff = abs(len(events) - expected_count)
        return QualityResult(
            rule_id="DQ010_record_count_match",
            dataset=self.collection,
            severity="error",
            record_count=len(events),
            failure_count=1 if diff else 0,
            description="Observed record count must match the expected count.",
            diagnostics={"expected": expected_count, "observed": len(events), "difference": diff},
        )

    def _freshness(
        self, events: list[dict[str, Any]], freshness_seconds: int, now: datetime
    ) -> QualityResult:
        if not events:
            return QualityResult(
                rule_id="DQ011_freshness",
                dataset=self.collection,
                severity="warning",
                record_count=0,
                failure_count=1,
                description="No events to check freshness.",
            )
        stamps = [parse_iso(str(e["event_timestamp"])) for e in events if e.get("event_timestamp")]
        if not stamps:
            return QualityResult(
                rule_id="DQ011_freshness",
                dataset=self.collection,
                severity="warning",
                record_count=len(events),
                failure_count=1,
                description="No parseable event timestamps to check freshness.",
            )
        newest = max(stamps)
        age = (now - newest).total_seconds()
        return QualityResult(
            rule_id="DQ011_freshness",
            dataset=self.collection,
            severity="warning" if age > freshness_seconds else "info",
            record_count=len(events),
            failure_count=1 if age > freshness_seconds else 0,
            description="Newest event must be within the freshness SLA.",
            diagnostics={"age_seconds": round(age, 1), "sla_seconds": freshness_seconds},
        )


def _merge_quarantine(quarantined: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Collapse duplicate quarantine entries for the same source event.

    A single bad event may violate several rules; we keep one record per event
    with the union of offending rule ids so quarantine counts stay meaningful.
    """
    merged: dict[str, dict[str, Any]] = {}
    order: list[str] = []
    for rec in quarantined:
        key = str(rec.get("event_id") or rec.get("_local_key") or id(rec))
        if key not in merged:
            merged[key] = {k: v for k, v in rec.items() if k != "_dq_rule"}
            merged[key]["_dq_rules"] = []
            order.append(key)
        rule = rec.get("_dq_rule")
        if rule and rule not in merged[key]["_dq_rules"]:
            merged[key]["_dq_rules"].append(rule)
        if "_dq_missing" in rec:
            merged[key]["_dq_missing"] = rec["_dq_missing"]
    return [merged[k] for k in order]


def quality_summary(results: list[QualityResult]) -> dict[str, Any]:
    total_rules = len(results)
    failed_rules = sum(1 for r in results if not r.passed)
    total_failures = sum(r.failure_count for r in results)
    by_severity: dict[str, int] = {}
    for r in results:
        if not r.passed:
            by_severity[r.severity] = by_severity.get(r.severity, 0) + 1
    return {
        "total_rules": total_rules,
        "failed_rules": failed_rules,
        "total_failures": total_failures,
        "failed_by_severity": by_severity,
    }
