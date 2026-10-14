"""Financial reconciliation + event-count reconciliation.

Deterministic, Decimal-based reconciliation between source (Bronze raw events)
and target (Silver/Gold aggregates). Detects:
  * event-count mismatches (source vs target),
  * accepted-bet count mismatches,
  * settled-bet count and monetary total mismatches,
  * ledger-entry discrepancies,
  * missing settlement records (accepted but never settled),
  * duplicate business effects (same bet settled more than once),
  * invalid status transitions.

Produces structured reports and a severity per finding. This is synthetic
reconciliation against synthetic data; it is NOT a real payment integration.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from decimal import Decimal
from typing import Any

from apexbet.common.logging import get_logger
from apexbet.common.money import to_major
from apexbet.schemas.events import EventType

log = get_logger("apexbet.reconciliation")


@dataclass
class ReconciliationFinding:
    rule_id: str
    description: str
    severity: str  # info | warning | error | critical
    expected: Any
    actual: Any
    difference: Any
    missing: list[str] = field(default_factory=list)
    duplicates: list[str] = field(default_factory=list)
    resolution_status: str = "OPEN"

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class ReconciliationReport:
    findings: list[ReconciliationFinding] = field(default_factory=list)
    processed_at: str = ""

    @property
    def ok(self) -> bool:
        return all(f.severity in ("info",) or f.difference in (0, None, "0") for f in self.findings)

    def summary(self) -> dict[str, Any]:
        by_sev: dict[str, int] = {}
        for f in self.findings:
            by_sev[f.severity] = by_sev.get(f.severity, 0) + 1
        return {
            "processed_at": self.processed_at,
            "total_findings": len(self.findings),
            "by_severity": by_sev,
            "findings": [f.to_dict() for f in self.findings],
        }


def _ids(events: list[dict[str, Any]], event_type: str, key: str) -> list[str]:
    out = []
    for e in events:
        if e.get("event_type") == event_type:
            v = (e.get("payload") or {}).get(key)
            if v:
                out.append(str(v))
    return out


def reconcile(
    bronze_events: list[dict[str, Any]],
    silver_events: list[dict[str, Any]],
    *,
    processed_at: str = "",
    known_quarantined: int = 0,
    known_duplicates: int = 0,
    quarantined_event_ids: set[str] | None = None,
) -> ReconciliationReport:
    """Reconcile raw (bronze) events against processed (silver) events.

    ``known_quarantined`` is the number of source events the pipeline
    deliberately quarantined (invalid money, unsupported schema, etc.).
    ``quarantined_event_ids`` is the precise set, when available, used to prove
    that a missing bet id is *explained* by quarantine rather than lost.
    Differences that are fully explained are downgraded to ``info`` with
    resolution status ``EXPLAINED_BY_QUARANTINE``; unexplained ones stay
    ``critical``.
    """
    quarantined_event_ids = quarantined_event_ids or set()
    # bet_ids belonging to quarantined source events are "expected missing"
    quarantined_bets: set[str] = {
        str((e.get("payload") or {}).get("bet_id"))
        for e in bronze_events
        if e.get("event_id") in quarantined_event_ids and (e.get("payload") or {}).get("bet_id")
    }
    report = ReconciliationReport(processed_at=processed_at)

    # 1) Source-to-target event counts.
    src_total = len(bronze_events)
    tgt_total = len(silver_events)
    explained = known_quarantined + known_duplicates
    if quarantined_event_ids:
        explained = len(quarantined_event_ids) + known_duplicates
    unexplained = (src_total - tgt_total) - explained
    report.findings.append(
        ReconciliationFinding(
            rule_id="REC001_event_count",
            description="Total raw events vs processed events (dedupe/quarantine expected).",
            severity="info" if unexplained <= 0 else "error",
            expected=src_total,
            actual=tgt_total,
            difference=src_total - tgt_total,
            resolution_status="EXPLAINED_BY_DEDUP_OR_QUARANTINE" if unexplained <= 0 else "OPEN",
        )
    )

    # 2) Accepted-bet counts match (distinct bet ids; quarantine-aware).
    src_accepted = sorted(set(_ids(bronze_events, EventType.BET_ACCEPTED.value, "bet_id")))
    tgt_accepted = sorted(set(_ids(silver_events, EventType.BET_ACCEPTED.value, "bet_id")))
    missing_accepted = sorted(set(src_accepted) - set(tgt_accepted))
    unexplained_accepted = [b for b in missing_accepted if b not in quarantined_bets]
    report.findings.append(
        ReconciliationFinding(
            rule_id="REC002_accepted_bets",
            description="Distinct accepted bet ids source vs target.",
            severity="critical" if unexplained_accepted else "info",
            expected=len(src_accepted),
            actual=len(tgt_accepted),
            difference=len(src_accepted) - len(tgt_accepted),
            missing=unexplained_accepted[:50],
            resolution_status=(
                "EXPLAINED_BY_QUARANTINE"
                if not unexplained_accepted and missing_accepted
                else "OPEN"
            ),
        )
    )
    # 3) Settled-bet counts (distinct bet ids; quarantine-aware).
    src_settled = sorted(set(_ids(bronze_events, EventType.BET_SETTLED.value, "bet_id")))
    tgt_settled = sorted(set(_ids(silver_events, EventType.BET_SETTLED.value, "bet_id")))
    missing_settled = sorted(set(src_settled) - set(tgt_settled))
    unexplained_settled = [b for b in missing_settled if b not in quarantined_bets]
    report.findings.append(
        ReconciliationFinding(
            rule_id="REC003_settled_bets",
            description="Distinct settled bet ids source vs target.",
            severity="critical" if unexplained_settled else "info",
            expected=len(src_settled),
            actual=len(tgt_settled),
            difference=len(src_settled) - len(tgt_settled),
            missing=unexplained_settled[:50],
            resolution_status=(
                "EXPLAINED_BY_QUARANTINE" if not unexplained_settled and missing_settled else "OPEN"
            ),
        )
    )

    # 4) Monetary totals: settled stake, payout. Compared over the distinct bet
    # ids present in BOTH sides (like-for-like), so quarantined/deduped events
    # never masquerade as monetary discrepancies. Duplicate settlements within
    # the source are collapsed to the first occurrence.
    def _settled_totals(events: list[dict[str, Any]]) -> tuple[int, int]:
        seen: set[str] = set()
        stake_total = 0
        payout_total = 0
        for e in events:
            if e.get("event_type") != EventType.BET_SETTLED.value:
                continue
            p = e.get("payload") or {}
            bid = str(p.get("bet_id"))
            if not bid or bid in seen:
                continue
            seen.add(bid)
            stake_total += int(p.get("settled_stake_minor", 0))
            payout_total += int(p.get("payout_minor", 0))
        return stake_total, payout_total

    common_bets = set(src_settled) & set(tgt_settled)
    src_by_bet = {
        str((e.get("payload") or {}).get("bet_id")): e
        for e in bronze_events
        if e.get("event_type") == EventType.BET_SETTLED.value
    }
    tgt_by_bet = {
        str((e.get("payload") or {}).get("bet_id")): e
        for e in silver_events
        if e.get("event_type") == EventType.BET_SETTLED.value
    }
    src_stake = sum(
        int((src_by_bet[b].get("payload") or {}).get("settled_stake_minor", 0)) for b in common_bets
    )
    tgt_stake = sum(
        int((tgt_by_bet[b].get("payload") or {}).get("settled_stake_minor", 0)) for b in common_bets
    )
    src_payout = sum(
        int((src_by_bet[b].get("payload") or {}).get("payout_minor", 0)) for b in common_bets
    )
    tgt_payout = sum(
        int((tgt_by_bet[b].get("payload") or {}).get("payout_minor", 0)) for b in common_bets
    )
    report.findings.append(
        ReconciliationFinding(
            rule_id="REC004_settled_stake_total",
            description="Total settled stake (minor units) over common settled bets.",
            severity="critical" if src_stake != tgt_stake else "info",
            expected=src_stake,
            actual=tgt_stake,
            difference=src_stake - tgt_stake,
        )
    )
    report.findings.append(
        ReconciliationFinding(
            rule_id="REC005_payout_total",
            description="Total payout (minor units) over common settled bets.",
            severity="critical" if src_payout != tgt_payout else "info",
            expected=src_payout,
            actual=tgt_payout,
            difference=src_payout - tgt_payout,
        )
    )

    # 5) Missing settlement records: accepted bets never settled.
    missing = sorted(set(src_accepted) - set(src_settled))
    report.findings.append(
        ReconciliationFinding(
            rule_id="REC006_missing_settlements",
            description="Bets accepted but never settled within the window.",
            severity="warning" if missing else "info",
            expected=0,
            actual=len(missing),
            difference=len(missing),
            missing=missing[:50],
        )
    )

    # 6) Duplicate settlements (same bet settled more than once).
    from collections import Counter

    settled_counts = Counter(_ids(bronze_events, EventType.BET_SETTLED.value, "bet_id"))
    dupes = sorted([k for k, v in settled_counts.items() if v > 1])
    report.findings.append(
        ReconciliationFinding(
            rule_id="REC007_duplicate_settlements",
            description="A single bet must not be settled more than once.",
            severity="error" if dupes else "info",
            expected=0,
            actual=len(dupes),
            difference=len(dupes),
            duplicates=dupes[:50],
        )
    )

    # 7) Ledger entry validity on settlements.
    bad_ledger = 0
    for e in bronze_events:
        if e.get("event_type") == EventType.BET_SETTLED.value:
            p = e.get("payload") or {}
            if not p.get("ledger_entry_id"):
                bad_ledger += 1
    report.findings.append(
        ReconciliationFinding(
            rule_id="REC008_ledger_entries",
            description="Every settlement must reference a ledger entry.",
            severity="error" if bad_ledger else "info",
            expected=0,
            actual=bad_ledger,
            difference=bad_ledger,
        )
    )

    # 8) Invalid status transitions (settled without accepted, in-batch).
    accepted_set = set(src_accepted)
    invalid_transitions = [b for b in set(src_settled) if b not in accepted_set]
    report.findings.append(
        ReconciliationFinding(
            rule_id="REC009_invalid_status_transitions",
            description="Settlement without a prior acceptance is an invalid transition.",
            severity=(
                "info"
                if not invalid_transitions
                else "critical"
                if len(invalid_transitions) > len(src_settled) * 0.5
                else "warning"
            ),
            expected=0,
            actual=len(invalid_transitions),
            difference=len(invalid_transitions),
            missing=invalid_transitions[:50],
        )
    )

    log.info(
        "reconciliation.done",
        findings=len(report.findings),
        critical=sum(1 for f in report.findings if f.severity == "critical"),
    )
    return report


def ledger_totals(events: list[dict[str, Any]]) -> dict[str, Decimal]:
    """Compute ledger totals in major units for display."""
    totals: dict[str, int] = {}
    for e in events:
        et = e.get("event_type")
        p = e.get("payload") or {}
        if et == EventType.BET_SETTLED.value:
            totals["payout"] = totals.get("payout", 0) + int(p.get("payout_minor", 0))
            totals["settled_stake"] = totals.get("settled_stake", 0) + int(
                p.get("settled_stake_minor", 0)
            )
        elif et == EventType.DEPOSIT_RECORDED.value:
            totals["deposits"] = totals.get("deposits", 0) + int(p.get("amount_minor", 0))
        elif et == EventType.WITHDRAWAL_RECORDED.value:
            totals["withdrawals"] = totals.get("withdrawals", 0) + int(p.get("amount_minor", 0))
    return {k: to_major(v) for k, v in totals.items()}
