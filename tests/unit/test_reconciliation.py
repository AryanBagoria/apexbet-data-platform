"""Reconciliation tests, including deliberate-discrepancy detection."""

from __future__ import annotations

from apexbet.reconciliation.engine import ledger_totals, reconcile


def _settled(bet_id, stake, payout, ledger=True, event_id=None):
    return {
        "event_id": event_id or f"evt_s_{bet_id}",
        "event_type": "BetSettled",
        "payload": {
            "bet_id": bet_id,
            "market_id": "mkt_match_apl_00001_match_result_home",
            "outcome_id": "HOME",
            "result": "WON",
            "settled_stake_minor": stake,
            "payout_minor": payout,
            "currency": "USD",
            "ledger_entry_id": ("led_" + bet_id) if ledger else None,
        },
    }


def _accepted(bet_id, event_id=None):
    return {
        "event_id": event_id or f"evt_a_{bet_id}",
        "event_type": "BetAccepted",
        "payload": {
            "bet_id": bet_id,
            "market_id": "mkt_match_apl_00001_match_result_home",
            "accepted_odds": 2.0,
            "accepted_stake_minor": 1000,
            "currency": "USD",
            "liability_minor": 1000,
        },
    }


class TestReconciliation:
    def test_clean_reconciles(self):
        src = [_accepted("b1"), _settled("b1", 1000, 2000)]
        tgt = list(src)
        report = reconcile(src, tgt)
        # No critical/error findings when source == target.
        assert all(f.severity in ("info",) for f in report.findings)

    def test_event_count_mismatch_explained(self):
        src = [_accepted("b1"), _settled("b1", 1000, 2000)]
        tgt = [src[0]]  # one dropped
        report = reconcile(src, tgt, known_quarantined=1)
        rec1 = next(f for f in report.findings if f.rule_id == "REC001_event_count")
        assert rec1.severity == "info"  # explained
        assert rec1.resolution_status == "EXPLAINED_BY_DEDUP_OR_QUARANTINE"

    def test_event_count_mismatch_unexplained(self):
        src = [_accepted("b1"), _settled("b1", 1000, 2000)]
        tgt = [src[0]]
        report = reconcile(src, tgt, known_quarantined=0)
        rec1 = next(f for f in report.findings if f.rule_id == "REC001_event_count")
        assert rec1.severity == "error"

    def test_monetary_mismatch_detected(self):
        src = [_accepted("b1"), _settled("b1", 1000, 2000)]
        tgt = [_accepted("b1"), _settled("b1", 1000, 9999)]  # payout differs
        report = reconcile(src, tgt)
        rec5 = next(f for f in report.findings if f.rule_id == "REC005_payout_total")
        assert rec5.severity == "critical"
        assert rec5.difference != 0

    def test_duplicate_settlements_detected(self):
        src = [
            _accepted("b1"),
            _settled("b1", 1000, 2000),
            _settled("b1", 1000, 2000, event_id="dup"),
        ]
        report = reconcile(src, src)
        rec7 = next(f for f in report.findings if f.rule_id == "REC007_duplicate_settlements")
        assert rec7.severity == "error"
        assert rec7.actual == 1

    def test_missing_ledger_detected(self):
        src = [_accepted("b1"), _settled("b1", 1000, 2000, ledger=False)]
        report = reconcile(src, src)
        rec8 = next(f for f in report.findings if f.rule_id == "REC008_ledger_entries")
        assert rec8.severity == "error"

    def test_missing_settlement_detected(self):
        src = [_accepted("b1"), _accepted("b2"), _settled("b1", 1000, 2000)]
        report = reconcile(src, src)
        rec6 = next(f for f in report.findings if f.rule_id == "REC006_missing_settlements")
        assert rec6.actual == 1
        assert "b2" in rec6.missing

    def test_quarantine_explains_missing_accepted(self):
        src = [_accepted("b1"), _accepted("b2")]
        tgt = [_accepted("b1")]
        report = reconcile(src, tgt, quarantined_event_ids={"evt_a_b2"})
        rec2 = next(f for f in report.findings if f.rule_id == "REC002_accepted_bets")
        assert rec2.severity == "info"
        assert rec2.resolution_status == "EXPLAINED_BY_QUARANTINE"


class TestLedgerTotals:
    def test_totals(self):
        events = [
            {
                "event_type": "BetSettled",
                "payload": {"settled_stake_minor": 1000, "payout_minor": 1500},
            },
            {"event_type": "DepositRecorded", "payload": {"amount_minor": 5000}},
            {"event_type": "WithdrawalRecorded", "payload": {"amount_minor": 2000}},
        ]
        totals = ledger_totals(events)
        assert str(totals["payout"]) == "15.00"
        assert str(totals["deposits"]) == "50.00"
        assert str(totals["withdrawals"]) == "20.00"
