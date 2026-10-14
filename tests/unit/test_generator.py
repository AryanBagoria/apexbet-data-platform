"""Generator tests: determinism, coverage, anomaly injection."""

from __future__ import annotations

from collections import Counter

from apexbet.generator import AnomalyInjection, GeneratorConfig, SyntheticEventGenerator
from apexbet.schemas import validate_event
from apexbet.schemas.events import EventType


def _gen(seed=1, anomalies=None, n=2000):
    cfg = GeneratorConfig(
        seed=seed, num_accounts=200, num_matches=20, anomalies=anomalies or AnomalyInjection()
    )
    return SyntheticEventGenerator(cfg)


class TestDeterminism:
    def test_same_seed_same_stream(self):
        a = _gen(seed=7)
        b = _gen(seed=7)
        sa = [(e["event_type"], e["payload"]) for e in a.generate(300)]
        sb = [(e["event_type"], e["payload"]) for e in b.generate(300)]
        assert sa == sb

    def test_different_seed_differs(self):
        a = _gen(seed=7)
        b = _gen(seed=8)
        sa = [e["payload"].get("bet_id") for e in a.generate(100)]
        sb = [e["payload"].get("bet_id") for e in b.generate(100)]
        assert sa != sb


class TestCoverage:
    def test_all_event_types_generated(self):
        seen = {e["event_type"] for e in _gen(n=1).generate(20000)}
        assert seen == {t.value for t in EventType}

    def test_money_is_integer_minor(self):
        for e in _gen().generate(500):
            p = e["payload"]
            for k in ("stake_minor", "amount_minor", "payout_minor"):
                if k in p:
                    assert isinstance(p[k], int)


class TestLifecycle:
    def test_bets_have_accept_or_reject(self):
        events = list(_gen(seed=3).generate(4000))
        placed = {e["payload"]["bet_id"] for e in events if e["event_type"] == "BetPlaced"}
        accepted = {e["payload"]["bet_id"] for e in events if e["event_type"] == "BetAccepted"}
        rejected = {e["payload"]["bet_id"] for e in events if e["event_type"] == "BetRejected"}
        # Every accepted/rejected bet must have been placed.
        assert accepted <= placed
        assert rejected <= placed


class TestAnomalies:
    def test_clean_has_no_duplicates_or_invalid(self):
        g = _gen(anomalies=AnomalyInjection.clean())
        events = list(g.generate(1000))
        ids = Counter(e["event_id"] for e in events)
        assert all(v == 1 for v in ids.values())
        assert all(validate_event(e).ok for e in events)

    def test_dirty_injects_duplicates(self):
        g = _gen(
            anomalies=AnomalyInjection(
                duplicate_rate=0.1,
                late_rate=0,
                out_of_order_rate=0,
                invalid_amount_rate=0,
                missing_reference_rate=0,
                schema_drift_rate=0,
                settlement_discrepancy_rate=0,
            )
        )
        events = list(g.generate(2000))
        ids = Counter(e["event_id"] for e in events)
        assert sum(1 for v in ids.values() if v > 1) > 0

    def test_dirty_injects_invalid(self):
        g = _gen(
            anomalies=AnomalyInjection(
                invalid_amount_rate=0.2,
                duplicate_rate=0,
                late_rate=0,
                out_of_order_rate=0,
                missing_reference_rate=0,
                schema_drift_rate=0,
                settlement_discrepancy_rate=0,
            )
        )
        events = list(g.generate(1000))
        invalid = sum(1 for e in events if not validate_event(e).ok)
        assert invalid > 0

    def test_large_multi_type_no_crash(self):
        # ensure generator handles large type diversity
        g = _gen(anomalies=AnomalyInjection.demo())
        count = sum(1 for _ in g.generate(5000))
        assert count >= 5000
