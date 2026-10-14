"""Silver transformation tests."""

from __future__ import annotations

from apexbet.silver.transform import dedupe_events, transform_event


def _event(**over):
    base = {
        "event_id": "evt_1",
        "event_type": "BetPlaced",
        "schema_version": "1.2.0",
        "event_timestamp": "2026-01-01T00:00:00Z",
        "ingestion_timestamp": "2026-01-01T00:00:01Z",
        "producer": "sportsbook-core",
        "account_id": "acct_1",
        "partition_key": "acct_1",
        "payload": {
            "bet_id": "BET_ABC",
            "market_id": "mkt_match_apl_00001_match_result_home",
            "outcome_id": "HOME",
            "stake_minor": 1000,
            "currency": "USD",
            "decimal_odds": 2.5,
            "potential_return_minor": 2500,
            "sport_code": "SOCCER",
        },
    }
    base.update(over)
    return base


class TestTransform:
    def test_valid_transform(self):
        r = transform_event(_event())
        assert r.ok
        assert r.record["event_category"] == "BETTING"
        assert r.record["sport_name"] == "Association Football"
        assert r.record["league_code"] == "APL"

    def test_identifier_standardized(self):
        r = transform_event(_event())
        assert r.record["payload"]["bet_id"] == "bet_abc"

    def test_invalid_money_quarantined(self):
        e = _event()
        e["payload"]["stake_minor"] = -100
        r = transform_event(e)
        assert not r.ok
        # Negative stake is caught by envelope/payload validation before the
        # explicit money rule, so the reason is payload_invalid.
        assert r.quarantine_reason in ("invalid_monetary_value", "payload_invalid")

    def test_unsupported_schema_quarantined(self):
        r = transform_event(_event(schema_version="0.1.0"))
        assert not r.ok
        assert r.quarantine_reason == "unsupported_schema_version"

    def test_late_detection(self):
        # watermark far ahead of event time
        r = transform_event(
            _event(), watermark_iso="2026-01-01T05:00:00Z", allowed_lateness_seconds=600
        )
        assert r.ok
        assert r.is_late

    def test_not_late_within_window(self):
        r = transform_event(
            _event(), watermark_iso="2026-01-01T00:05:00Z", allowed_lateness_seconds=600
        )
        assert r.ok
        assert not r.is_late

    def test_settlement_without_stake_flag(self):
        e = _event(event_type="BetSettled")
        e["payload"] = {
            "bet_id": "bet_1",
            "market_id": "mkt_match_apl_00001_match_result_home",
            "outcome_id": "HOME",
            "result": "LOST",
            "settled_stake_minor": 0,
            "payout_minor": 0,
            "currency": "USD",
        }
        r = transform_event(e)
        assert r.ok  # it's a flag, not a quarantine
        assert "settlement_without_stake" in r.issues

    def test_stable_schema_keys(self):
        r = transform_event(_event())
        expected = {
            "event_id",
            "event_type",
            "schema_version",
            "event_timestamp",
            "ingestion_timestamp",
            "processing_timestamp",
            "producer",
            "account_id",
            "session_id",
            "correlation_id",
            "partition_key",
            "trace_id",
            "event_category",
            "is_late",
            "processing_latency_ms",
            "event_date",
            "sport_code",
            "sport_name",
            "league_code",
            "league_name",
            "is_winning_settlement",
            "payload",
        }
        assert set(r.record.keys()) == expected


class TestDedupe:
    def test_dedupe(self):
        rows = [{"event_id": "a"}, {"event_id": "b"}, {"event_id": "a"}]
        unique, dropped = dedupe_events(rows)
        assert len(unique) == 2
        assert dropped == 1
