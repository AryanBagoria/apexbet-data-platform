"""Data quality engine tests."""

from __future__ import annotations

from apexbet.quality.engine import DataQualityEngine, quality_summary


def _ev(**over):
    base = {
        "event_id": "e1",
        "event_type": "BetPlaced",
        "schema_version": "1.2.0",
        "event_timestamp": "2026-01-01T00:00:00Z",
        "partition_key": "a",
        "payload": {
            "bet_id": "b1",
            "market_id": "mkt_match_apl_00001_match_result_home",
            "outcome_id": "HOME",
            "stake_minor": 1000,
            "currency": "USD",
            "decimal_odds": 2.0,
            "potential_return_minor": 2000,
        },
    }
    base.update(over)
    return base


class TestQualityRules:
    def test_clean_events_pass_core_rules(self):
        results, quarantine = DataQualityEngine().run_all(
            [_ev(), _ev(event_id="e2", payload={**_ev()["payload"], "bet_id": "b2"})]
        )
        by_id = {r.rule_id: r for r in results}
        assert by_id["DQ001_required_fields"].passed
        assert by_id["DQ002_schema_compatibility"].passed
        assert by_id["DQ003_valid_timestamps"].passed
        assert by_id["DQ004_valid_monetary_values"].passed
        assert by_id["DQ006_supported_event_type"].passed
        assert len(quarantine) == 0

    def test_missing_required_field(self):
        bad = _ev()
        del bad["event_timestamp"]
        results, quarantine = DataQualityEngine().run_all([bad])
        assert not {r.rule_id: r for r in results}["DQ001_required_fields"].passed
        assert len(quarantine) == 1

    def test_unsupported_schema(self):
        results, _ = DataQualityEngine().run_all([_ev(schema_version="0.1.0")])
        assert not {r.rule_id: r for r in results}["DQ002_schema_compatibility"].passed

    def test_invalid_money(self):
        results, _ = DataQualityEngine().run_all(
            [_ev(payload={**_ev()["payload"], "stake_minor": -1})]
        )
        assert not {r.rule_id: r for r in results}["DQ004_valid_monetary_values"].passed

    def test_duplicate_ids(self):
        results, _ = DataQualityEngine().run_all([_ev(), _ev()])
        assert not {r.rule_id: r for r in results}["DQ005_unique_event_ids"].passed

    def test_missing_reference(self):
        results, _ = DataQualityEngine().run_all(
            [_ev(payload={**_ev()["payload"], "market_id": "mkt_UNKNOWN_000"})]
        )
        assert not {r.rule_id: r for r in results}["DQ007_referential_integrity"].passed

    def test_settlement_discrepancy_detected(self):
        accepted = _ev(
            event_id="a1",
            event_type="BetAccepted",
            payload={
                "bet_id": "b1",
                "market_id": "mkt_match_apl_00001_match_result_home",
                "accepted_odds": 2.0,
                "accepted_stake_minor": 1000,
                "currency": "USD",
                "liability_minor": 1000,
            },
        )
        settled_bad = _ev(
            event_id="s1",
            event_type="BetSettled",
            payload={
                "bet_id": "b1",
                "market_id": "mkt_match_apl_00001_match_result_home",
                "outcome_id": "HOME",
                "result": "WON",
                "settled_stake_minor": 1000,
                "payout_minor": 5000,
                "currency": "USD",
            },
        )
        results, _ = DataQualityEngine().run_all([accepted, settled_bad])
        assert not {r.rule_id: r for r in results}["DQ009_settlement_discrepancy"].passed

    def test_count_match(self):
        results, _ = DataQualityEngine().run_all([_ev()], expected_count=5)
        assert not {r.rule_id: r for r in results}["DQ010_record_count_match"].passed

    def test_summary(self):
        results, _ = DataQualityEngine().run_all([_ev()])
        s = quality_summary(results)
        assert s["total_rules"] == 11
        assert "failed_by_severity" in s
