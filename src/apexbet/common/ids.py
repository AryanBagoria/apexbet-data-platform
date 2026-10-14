"""Identifier and correlation-id helpers."""

from __future__ import annotations

import uuid


def new_id(prefix: str) -> str:
    """Return a prefixed UUID4 string, e.g. ``evt_7f3c...``."""
    return f"{prefix}_{uuid.uuid4().hex}"


def new_event_id() -> str:
    return new_id("evt")


def new_correlation_id() -> str:
    return new_id("cor")


def new_trace_id() -> str:
    return uuid.uuid4().hex


def deterministic_id(prefix: str, *parts: object) -> str:
    """Deterministic id derived from inputs; used so replays are reproducible."""
    import hashlib

    h = hashlib.sha256("|".join(str(p) for p in parts).encode("utf-8")).hexdigest()
    return f"{prefix}_{h[:32]}"


def partition_key(account_id: str | None, fallback: str) -> str:
    """Stable partition key. Prefers account_id so all events for one account
    share a partition (preserving per-account ordering)."""
    return account_id or fallback
