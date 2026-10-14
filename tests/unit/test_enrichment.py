"""Enrichment tests: schema validation, modes, idempotency, failure handling."""

from __future__ import annotations

from apexbet.enrichment.enricher import (
    EnrichmentRecord,
    EnrichmentRunner,
    LLMEnricher,
    LocalMLEnricher,
    RulesEnricher,
    build_enricher,
    validate_enrichment,
)


def _silver_event(event_id="evt_1", event_type="BetPlaced", category="BETTING"):
    return {
        "event_id": event_id,
        "event_type": event_type,
        "event_category": category,
        "payload": {
            "stake_minor": 1000,
            "currency": "USD",
            "outcome_id": "HOME",
            "decimal_odds": 2.5,
            "payout_minor": 2500,
            "amount_minor": 5000,
            "method": "CARD",
            "game_type": "SLOTS",
        },
    }


class TestRulesEnricher:
    def test_basic(self):
        rec = RulesEnricher().enrich(_silver_event())
        assert isinstance(rec, EnrichmentRecord)
        assert rec.processing_status == "success"
        assert rec.confidence == 1.0
        assert "normalized_description" in rec.enrichment_payload

    def test_idempotent_ids(self):
        a = RulesEnricher().enrich(_silver_event("x"))
        b = RulesEnricher().enrich(_silver_event("x"))
        assert a.enrichment_id == b.enrichment_id

    def test_description_varies_by_type(self):
        placed = RulesEnricher().enrich(_silver_event(event_type="BetPlaced"))
        deposit = RulesEnricher().enrich(_silver_event(event_type="DepositRecorded"))
        assert (
            placed.enrichment_payload["normalized_description"]
            != deposit.enrichment_payload["normalized_description"]
        )


class TestRunner:
    def test_runs_all_and_stats(self):
        runner = EnrichmentRunner(RulesEnricher())
        events = [_silver_event(f"e{i}") for i in range(10)]
        out = runner.run(events)
        assert len(out) == 10
        assert runner.stats.succeeded == 10
        assert runner.stats.success_rate == 1.0

    def test_idempotent_skip(self):
        runner = EnrichmentRunner(RulesEnricher())
        events = [_silver_event("e1")]
        runner.run(events)
        runner.run(events)  # second pass should skip
        assert runner.stats.skipped == 1

    def test_skip_ids(self):
        runner = EnrichmentRunner(RulesEnricher())
        out = runner.run([_silver_event("e1")], skip_ids={"e1"})
        assert out == []
        assert runner.stats.skipped == 1


class TestLocalML:
    def test_available(self):
        assert LocalMLEnricher().available

    def test_predicts_category(self):
        rec = LocalMLEnricher().enrich(_silver_event(event_type="BetPlaced", category="BETTING"))
        assert rec.processing_status in ("success", "skipped")
        if rec.processing_status == "success":
            assert "predicted_category" in rec.enrichment_payload
            assert rec.confidence is not None


class TestValidation:
    def test_valid_record(self):
        rec = RulesEnricher().enrich(_silver_event())
        valid, invalid = validate_enrichment([rec.model_dump()])
        assert len(valid) == 1
        assert invalid == 0

    def test_invalid_record(self):
        bad = {"enrichment_id": "x"}  # missing required fields
        _valid, invalid = validate_enrichment([bad])
        assert invalid == 1


class TestLLMFallback:
    def test_unavailable_llm_falls_back_to_rules(self):
        enr = build_enricher(
            "llm", llm_base_url="http://127.0.0.1:1", llm_model="nope", llm_timeout=0.5
        )
        assert isinstance(enr, RulesEnricher)

    def test_llm_unavailable_reports(self):
        enr = LLMEnricher("http://127.0.0.1:1", "nope", timeout_seconds=0.2, max_retries=0)
        assert enr.available() is False
        rec = enr.enrich(_silver_event())
        assert rec.processing_status == "failed"
        assert rec.error_reason is not None

    def test_rules_mode_always_works(self):
        enr = build_enricher("rules", llm_base_url="", llm_model="", llm_timeout=1.0)
        assert isinstance(enr, RulesEnricher)
