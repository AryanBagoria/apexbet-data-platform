"""Event contract + schema tests."""

from __future__ import annotations

import json

from apexbet import SCHEMA_VERSION_CURRENT
from apexbet.schemas import (
    PAYLOAD_MODELS,
    EventType,
    export_json_schemas,
    is_supported_version,
    validate_event,
)

BASE = {
    "event_id": "evt_test_1",
    "event_type": "BetPlaced",
    "schema_version": "1.2.0",
    "event_timestamp": "2026-01-01T00:00:00Z",
    "partition_key": "acct_1",
    "account_id": "acct_1",
    "payload": {
        "bet_id": "bet_1",
        "market_id": "mkt_1",
        "outcome_id": "out_1",
        "stake_minor": 1000,
        "currency": "USD",
        "decimal_odds": 2.5,
        "potential_return_minor": 2500,
    },
}


class TestValidation:
    def test_valid_event(self):
        r = validate_event(BASE)
        assert r.ok
        assert r.envelope is not None
        assert r.envelope.event_type == EventType.BET_PLACED

    def test_missing_required_field(self):
        bad = dict(BASE)
        bad.pop("event_timestamp")
        r = validate_event(bad)
        assert not r.ok
        assert any(i.code == "envelope_invalid" for i in r.issues)

    def test_bad_payload(self):
        bad = dict(BASE)
        bad["payload"] = {"bet_id": "x"}  # missing required fields
        r = validate_event(bad)
        assert not r.ok
        assert any(i.code == "payload_invalid" for i in r.issues)

    def test_unsupported_schema_version(self):
        bad = dict(BASE, schema_version="0.9.0")
        r = validate_event(bad)
        assert not r.ok
        assert any(i.code == "unsupported_schema_version" for i in r.issues)

    def test_future_schema_version(self):
        assert not is_supported_version("99.0.0")
        assert is_supported_version("1.0.0")
        assert is_supported_version(SCHEMA_VERSION_CURRENT)

    def test_malformed_timestamp(self):
        bad = dict(BASE, event_timestamp="not-a-date")
        assert not validate_event(bad).ok

    def test_partition_key_defaulted(self):
        e = dict(BASE)
        e.pop("partition_key")
        r = validate_event(e)
        assert r.ok
        assert r.envelope.partition_key == "acct_1"

    def test_negative_stake_rejected(self):
        bad = json.loads(json.dumps(BASE))
        bad["payload"]["stake_minor"] = -1
        assert not validate_event(bad).ok


class TestCoverage:
    def test_all_event_types_have_payload_model(self):
        for et in EventType:
            assert et in PAYLOAD_MODELS

    def test_export_json_schemas(self, tmp_path):
        out = tmp_path / "schemas.json"
        schemas = export_json_schemas(str(out))
        assert out.exists()
        assert "EventEnvelope" in schemas
        assert len(schemas) == len(PAYLOAD_MODELS) + 1


class TestSchemaEvolution:
    def test_extra_fields_allowed_in_payload(self):
        e = json.loads(json.dumps(BASE))
        e["payload"]["new_optional_field"] = "value"
        assert validate_event(e).ok

    def test_extra_envelope_fields_rejected(self):
        e = json.loads(json.dumps(BASE))
        e["unexpected_field"] = 1
        assert not validate_event(e).ok
