"""Event-contract tests.

These lock down the *wire contract*: the common envelope, the 15 typed
payloads, schema-version policing, and the guarantee that the synthetic
generator only ever emits events that validate against the current contract.

They also assert the JSON Schema export stays consistent with the Pydantic
models so the published ``docs/data-engineering/event-schemas.json`` cannot
silently drift.
"""

from __future__ import annotations

import json

import pytest

from apexbet import SCHEMA_VERSION_CURRENT, SCHEMA_VERSION_MIN_SUPPORTED
from apexbet.generator import AnomalyInjection, GeneratorConfig, SyntheticEventGenerator
from apexbet.schemas.events import (
    PAYLOAD_MODELS,
    EventEnvelope,
    EventType,
    export_json_schemas,
    is_supported_version,
    validate_event,
)


class TestEnvelopeContract:
    def test_every_event_type_has_a_payload_model(self):
        for et in EventType:
            assert et in PAYLOAD_MODELS, f"{et} has no payload model"

    def test_generator_emits_all_15_event_types(self):
        gen = SyntheticEventGenerator(
            GeneratorConfig(
                seed=7, num_accounts=400, num_matches=40, anomalies=AnomalyInjection.clean()
            )
        )
        seen = set()
        for env in gen.generate(20000):
            seen.add(env["event_type"])
        assert seen == {et.value for et in EventType}, (
            "missing types: {et.value for et in EventType} - seen"
        )

    def test_clean_generated_events_all_validate(self):
        gen = SyntheticEventGenerator(
            GeneratorConfig(
                seed=11, num_accounts=300, num_matches=30, anomalies=AnomalyInjection.clean()
            )
        )
        checked = 0
        for env in gen.generate(4000):
            res = validate_event(env)
            assert res.ok, f"clean event failed validation: {res.issues}"
            checked += 1
        assert checked == 4000

    def test_dirty_generated_events_are_mostly_valid(self):
        """Dirty mode injects *deliberate* defects, so some events must fail --
        but the majority must still be valid (defects are a small %)."""
        gen = SyntheticEventGenerator(
            GeneratorConfig(
                seed=13, num_accounts=300, num_matches=30, anomalies=AnomalyInjection.demo()
            )
        )
        total = 0
        invalid = 0
        for env in gen.generate(4000):
            total += 1
            if not validate_event(env).ok:
                invalid += 1
        assert 0 < invalid < total * 0.5

    def test_unsupported_schema_version_is_rejected(self):
        bad = {
            "event_id": "evt_1",
            "event_type": "OddsUpdated",
            "schema_version": "0.9.0",
            "event_timestamp": "2026-01-01T00:00:00Z",
            "partition_key": "p",
            "payload": {
                "market_id": "m",
                "outcome_id": "HOME",
                "previous_odds": 2.0,
                "new_odds": 2.1,
            },
        }
        res = validate_event(bad)
        assert not res.ok
        assert any(i.code == "unsupported_schema_version" for i in res.issues)

    def test_malformed_timestamp_is_rejected(self):
        bad = {
            "event_id": "evt_1",
            "event_type": "OddsUpdated",
            "schema_version": SCHEMA_VERSION_CURRENT,
            "event_timestamp": "not-a-timestamp",
            "partition_key": "p",
            "payload": {
                "market_id": "m",
                "outcome_id": "HOME",
                "previous_odds": 2.0,
                "new_odds": 2.1,
            },
        }
        assert not validate_event(bad).ok

    def test_payload_type_violation_is_rejected(self):
        bad = {
            "event_id": "evt_1",
            "event_type": "BetPlaced",
            "schema_version": SCHEMA_VERSION_CURRENT,
            "event_timestamp": "2026-01-01T00:00:00Z",
            "partition_key": "p",
            "payload": {
                "bet_id": "b",
                "market_id": "m",
                "outcome_id": "o",
                "stake_minor": -1,
                "currency": "USD",
                "decimal_odds": 2.0,
                "potential_return_minor": 0,
            },
        }
        res = validate_event(bad)
        assert not res.ok
        assert any(i.code == "payload_invalid" for i in res.issues)

    def test_partition_key_defaults_to_account_id(self):
        env = EventEnvelope(
            event_type="OddsUpdated",
            event_timestamp="2026-01-01T00:00:00Z",
            account_id="acct_1",
            payload={},
        )
        assert env.partition_key == "acct_1"


class TestSchemaVersionPolicing:
    @pytest.mark.parametrize(
        "version,expected",
        [
            ("0.9.0", False),
            (SCHEMA_VERSION_MIN_SUPPORTED, True),
            (SCHEMA_VERSION_CURRENT, True),
            ("1.1.0", True),
            ("2.0.0", False),
            ("garbage", False),
        ],
    )
    def test_is_supported_version(self, version, expected):
        assert is_supported_version(version) is expected


class TestJsonSchemaExport:
    def test_export_covers_envelope_and_all_payloads(self, tmp_path):
        out = tmp_path / "schemas.json"
        schemas = export_json_schemas(str(out))
        assert "EventEnvelope" in schemas
        for et, _model in PAYLOAD_MODELS.items():
            assert f"{et.value}Payload" in schemas
        on_disk = json.loads(out.read_text())
        assert on_disk == schemas

    def test_envelope_json_schema_has_required_fields(self):
        schema = EventEnvelope.model_json_schema()
        required = set(schema.get("required", []))
        # event_id / schema_version / producer have defaults, so they are not
        # "required" at the JSON-Schema level; event_type + event_timestamp are.
        for field in ("event_type", "event_timestamp"):
            assert field in required
        # ...but every core identifier must still be *declared* as a property.
        props = set(schema.get("properties", {}))
        for field in ("event_id", "schema_version", "partition_key", "payload"):
            assert field in props
