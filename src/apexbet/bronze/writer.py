"""Bronze layer: durable, replayable, auditable raw event storage.

Layout:
    bronze/event_type=<snake_case>/event_date=YYYY-MM-DD/part-<uuid>.parquet
    bronze/quarantine/event_type=<...>/event_date=<...>/part-<uuid>.parquet

Each Bronze row preserves the *original* payload alongside routing + processing
metadata. If Parquet encoding fails for a batch, the raw JSONL is written to a
``_parquet_failed`` prefix and the failure is recorded, so no event is lost.
"""

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass, field
from typing import Any

from apexbet.common.logging import get_logger
from apexbet.common.storage import ObjectStore, parquet_bytes
from apexbet.common.timeutil import to_iso, utcnow

log = get_logger("apexbet.bronze")


def snake(name: str) -> str:
    out = []
    for i, ch in enumerate(name):
        if ch.isupper() and i > 0:
            out.append("_")
        out.append(ch.lower())
    return "".join(out)


@dataclass
class BronzeWriter:
    """Buffered writer that flushes batches to Parquet objects."""

    store: ObjectStore
    event_date: str = field(default_factory=lambda: to_iso(utcnow())[:10])
    prefix: str = "bronze"
    flush_records: int = 5000
    flush_seconds: float = 5.0
    _buffer: list[dict[str, Any]] = field(default_factory=list)
    _last_flush: float = field(default_factory=lambda: utcnow().timestamp())
    counts: dict[str, int] = field(default_factory=dict)
    failed_batches: int = 0
    written_objects: int = 0

    def add(self, raw: dict[str, Any]) -> None:
        event_type = str(raw.get("event_type", "Unknown"))
        row = {
            "event_id": raw.get("event_id"),
            "event_type": event_type,
            "schema_version": raw.get("schema_version"),
            "source": raw.get("producer"),
            "event_timestamp": raw.get("event_timestamp"),
            "ingestion_timestamp": raw.get("ingestion_timestamp"),
            "correlation_id": raw.get("correlation_id"),
            "partition_key": raw.get("partition_key"),
            "account_id": raw.get("account_id"),
            "trace_id": raw.get("trace_id"),
            "bronze_ingested_at": to_iso(utcnow()),
            # Preserve the original payload verbatim for audit + replay.
            "original_payload": json.dumps(raw.get("payload", {}), separators=(",", ":")),
            "original_event_json": json.dumps(raw, separators=(",", ":")),
        }
        self._buffer.append(row)
        self.counts[event_type] = self.counts.get(event_type, 0) + 1

    def should_flush(self, now: float | None = None) -> bool:
        now = now if now is not None else utcnow().timestamp()
        return bool(
            len(self._buffer) >= self.flush_records
            or (self._buffer and (now - self._last_flush) >= self.flush_seconds)
        )

    def flush(self, force: bool = False) -> int:
        if not self._buffer:
            return 0
        if not force and not self.should_flush():
            return 0
        # Group by (event_type) for partition-aligned files.
        by_type: dict[str, list[dict[str, Any]]] = {}
        for row in self._buffer:
            by_type.setdefault(snake(str(row["event_type"])), []).append(row)

        written = 0
        for et, rows in by_type.items():
            key = (
                f"{self.prefix}/event_type={et}/event_date={self.event_date}/"
                f"part-{uuid.uuid4().hex[:12]}.parquet"
            )
            try:
                data = parquet_bytes(rows)
                if data:
                    self.store.put_bytes(key, data, content_type="application/vnd.apache.parquet")
                    self.written_objects += 1
                    written += len(rows)
            except Exception as exc:
                # Preserve the raw records so nothing is lost.
                self.failed_batches += 1
                log.error("bronze.parquet_failed", error=str(exc), count=len(rows), key=key)
                fallback = key.replace(".parquet", ".jsonl").replace(
                    self.prefix, f"{self.prefix}/_parquet_failed"
                )
                self.store.put_bytes(
                    fallback,
                    ("\n".join(json.dumps(r) for r in rows)).encode("utf-8"),
                    content_type="application/x-ndjson",
                )
        self._buffer.clear()
        self._last_flush = utcnow().timestamp()
        return written

    def write_quarantine(self, event_type: str, rows: list[dict[str, Any]]) -> None:
        if not rows:
            return
        key = (
            f"{self.prefix}/quarantine/event_type={snake(event_type)}/"
            f"event_date={self.event_date}/part-{uuid.uuid4().hex[:12]}.parquet"
        )
        try:
            self.store.put_bytes(
                key, parquet_bytes(rows), content_type="application/vnd.apache.parquet"
            )
        except Exception as exc:  # pragma: no cover
            log.error("bronze.quarantine_failed", error=str(exc))
            self.store.put_bytes(
                key.replace(".parquet", ".jsonl"),
                ("\n".join(json.dumps(r) for r in rows)).encode("utf-8"),
            )

    @property
    def buffered(self) -> int:
        return len(self._buffer)

    def summary(self) -> dict[str, Any]:
        return {
            "buffered": len(self._buffer),
            "counts": dict(self.counts),
            "total": sum(self.counts.values()),
            "written_objects": self.written_objects,
            "failed_batches": self.failed_batches,
        }
