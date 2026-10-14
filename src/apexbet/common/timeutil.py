"""Time helpers. All platform timestamps are UTC ISO-8601 strings.

The platform tracks four distinct notions of time and keeps them separate:
  * event_time       -- when the business event happened (producer clock)
  * ingestion_time   -- when the API accepted the event
  * processing_time  -- when a consumer/batch job handled the event
  * processing_time_ms is derived; used for latency metrics.
"""

from __future__ import annotations

from datetime import UTC, datetime


def utcnow() -> datetime:
    return datetime.now(tz=UTC)


def to_iso(dt: datetime) -> str:
    """Render an aware/naive datetime as a UTC ISO-8601 string."""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC).isoformat()


def parse_iso(value: str) -> datetime:
    """Parse an ISO-8601 string into an aware UTC datetime.

    Accepts a trailing ``Z`` and naive datetimes (assumed UTC).
    """
    s = value.strip()
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    dt = datetime.fromisoformat(s)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)


def now_ms() -> int:
    return int(utcnow().timestamp() * 1000)


def ms_between(start: datetime, end: datetime) -> float:
    return (end - start).total_seconds() * 1000.0
