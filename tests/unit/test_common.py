"""Common utilities: money, retry, ids, time."""

from __future__ import annotations

from decimal import Decimal

import pytest

from apexbet.common.ids import deterministic_id, new_event_id
from apexbet.common.money import (
    apply_payout,
    format_money,
    is_valid_amount,
    to_major,
    to_minor,
)
from apexbet.common.retry import RetryExhausted, backoff_delay, retry_call
from apexbet.common.timeutil import parse_iso, to_iso, utcnow


class TestMoney:
    def test_to_minor_rounding(self):
        assert to_minor("12.34") == 1234
        assert to_minor(Decimal("12.345")) == 1235  # ROUND_HALF_UP
        assert to_minor(0) == 0

    def test_to_major_roundtrip(self):
        assert to_major(1234) == Decimal("12.34")
        assert to_major(to_minor("9.99")) == Decimal("9.99")

    def test_apply_payout(self):
        assert apply_payout(10000, Decimal("2.50")) == 25000
        assert apply_payout(1000, Decimal("1.90")) == 1900

    def test_is_valid_amount(self):
        assert is_valid_amount(100)
        assert not is_valid_amount(0)
        assert is_valid_amount(0, allow_zero=True)
        assert not is_valid_amount(-5)
        assert not is_valid_amount(10**20)

    def test_format_money(self):
        assert format_money(1234, "USD") == "12.34 USD"


class TestTime:
    def test_parse_z_suffix(self):
        dt = parse_iso("2026-01-01T00:00:00Z")
        assert dt.tzinfo is not None

    def test_roundtrip(self):
        now = utcnow()
        assert parse_iso(to_iso(now)).timestamp() == pytest.approx(now.timestamp(), abs=1e-6)

    def test_naive_assumed_utc(self):
        assert parse_iso("2026-01-01T00:00:00").tzinfo is not None


class TestIds:
    def test_unique(self):
        assert new_event_id() != new_event_id()

    def test_deterministic(self):
        assert deterministic_id("x", "a", 1) == deterministic_id("x", "a", 1)
        assert deterministic_id("x", "a", 1) != deterministic_id("x", "a", 2)


class TestRetry:
    def test_succeeds_after_retries(self):
        calls = {"n": 0}

        def flaky():
            calls["n"] += 1
            if calls["n"] < 3:
                raise ValueError("boom")
            return "ok"

        result = retry_call(flaky, max_attempts=5, sleep=lambda _s: None)
        assert result == "ok"
        assert calls["n"] == 3

    def test_exhausted(self):
        def always_fail():
            raise ValueError("boom")

        with pytest.raises(RetryExhausted):
            retry_call(always_fail, max_attempts=2, sleep=lambda _s: None)

    def test_backoff_bounded(self):
        for attempt in range(10):
            d = backoff_delay(attempt, base=0.1, cap=2.0)
            assert 0.0 <= d <= 2.5
