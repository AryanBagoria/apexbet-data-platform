"""Bounded retries with exponential backoff and jitter.

Used by consumers, storage writers and enrichment jobs so that transient
failures (broker blips, network hiccups) are retried a bounded number of
times before records are quarantined.
"""

from __future__ import annotations

import random
import time
from collections.abc import Callable
from typing import TypeVar

T = TypeVar("T")


class RetryExhausted(RuntimeError):
    """Raised when all retry attempts fail."""

    def __init__(self, attempts: int, last_error: BaseException) -> None:
        super().__init__(f"retry exhausted after {attempts} attempts: {last_error!r}")
        self.attempts = attempts
        self.last_error = last_error


def backoff_delay(
    attempt: int,
    base: float = 0.5,
    cap: float = 30.0,
    jitter: float = 0.25,
    rng: random.Random | None = None,
) -> float:
    """Full-jitter exponential backoff (attempt is 0-based)."""
    r = rng or random
    raw = min(cap, base * (2**attempt))
    return float(raw * (1.0 - jitter + r.random() * 2 * jitter))


def retry_call(
    fn: Callable[[], T],
    *,
    max_attempts: int = 5,
    base: float = 0.5,
    cap: float = 30.0,
    sleep: Callable[[float], None] = time.sleep,
    on_retry: Callable[[int, BaseException], None] | None = None,
    retry_on: tuple[type[BaseException], ...] = (Exception,),
) -> T:
    """Call ``fn`` with bounded retries; raise RetryExhausted on final failure."""
    last: BaseException | None = None
    for attempt in range(max_attempts):
        try:
            return fn()
        except retry_on as exc:
            last = exc
            if on_retry is not None:
                on_retry(attempt, exc)
            if attempt == max_attempts - 1:
                break
            sleep(backoff_delay(attempt, base=base, cap=cap))
    assert last is not None
    raise RetryExhausted(max_attempts, last)
