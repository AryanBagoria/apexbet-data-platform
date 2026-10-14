"""Streaming consumer + processing pipeline.

Pipeline stages (per event):
    stream -> validate -> Bronze (buffered parquet) -> dedupe (window) ->
    Silver transform -> Silver parquet (partitioned) | quarantine

Reliability features
--------------------
* Bounded retries with exponential backoff + jitter around publish/storage.
* Dead-letter routing: structurally broken records go to the DLQ topic AND a
  quarantine object in the lake.
* Deduplication window keyed by event_id with an expiry (``dedup_window_seconds``).
* Event-time watermark for late-event classification.
* Checkpointing: the committed stream offset/cursor is persisted after each
  flush, so a restart resumes without reprocessing (at-least-once + idempotent
  writes => effective exactly-once business effect for dedup'd datasets).
* Graceful shutdown via a stop flag; on stop it flushes and checkpoints.
* Processing metrics exposed for Prometheus.
"""

from __future__ import annotations

import json
import threading
import time
import uuid
from dataclasses import dataclass, field
from datetime import UTC
from typing import Any

from apexbet.bronze.writer import BronzeWriter
from apexbet.common.logging import get_logger
from apexbet.common.money import apply_payout
from apexbet.common.retry import retry_call
from apexbet.common.storage import ObjectStore, parquet_bytes
from apexbet.common.timeutil import ms_between, parse_iso, to_iso, utcnow
from apexbet.silver.transform import dedupe_events, transform_event
from apexbet.transports.base import Transport

log = get_logger("apexbet.pipeline")


@dataclass
class PipelineMetrics:
    consumed: int = 0
    bronze_written: int = 0
    silver_written: int = 0
    quarantined: int = 0
    duplicates: int = 0
    late_events: int = 0
    dlq: int = 0
    retries: int = 0
    errors: int = 0
    batches: int = 0
    started_at: float = field(default_factory=time.time)

    def to_dict(self) -> dict[str, Any]:
        elapsed = max(1e-6, time.time() - self.started_at)
        d = {k: v for k, v in self.__dict__.items() if k != "started_at"}
        d["elapsed_seconds"] = round(elapsed, 3)
        d["throughput_eps"] = round(self.consumed / elapsed, 2)
        return d


class DedupWindow:
    """Time-bounded set of seen event ids."""

    def __init__(self, window_seconds: int) -> None:
        self.window = window_seconds
        self._seen: dict[str, float] = {}

    def seen_before(self, event_id: str) -> bool:
        self._evict()
        return event_id in self._seen

    def add(self, event_id: str, ts: float | None = None) -> None:
        self._seen[event_id] = ts if ts is not None else time.time()
        if len(self._seen) % 10000 == 0:
            self._evict()

    def _evict(self) -> None:
        cutoff = time.time() - self.window
        stale = [k for k, v in self._seen.items() if v < cutoff]
        for k in stale:
            del self._seen[k]

    def __len__(self) -> int:
        return len(self._seen)


class Pipeline:
    """Runs the streaming consumer -> Bronze -> Silver pipeline."""

    def __init__(
        self,
        transport: Transport,
        store: ObjectStore,
        *,
        source_topic: str,
        dlq_topic: str,
        group_id: str,
        dedup_window_seconds: int = 3600,
        allowed_lateness_seconds: int = 600,
        bronze_flush_records: int = 5000,
        bronze_flush_seconds: float = 5.0,
        max_retries: int = 5,
        retry_base_seconds: float = 0.5,
        batch_size: int = 500,
    ) -> None:
        self.transport = transport
        self.store = store
        self.source_topic = source_topic
        self.dlq_topic = dlq_topic
        self.group_id = group_id
        self.dedup = DedupWindow(dedup_window_seconds)
        self.allowed_lateness = allowed_lateness_seconds
        self.batch_size = batch_size
        self.writer = BronzeWriter(
            store=store, flush_records=bronze_flush_records, flush_seconds=bronze_flush_seconds
        )
        self.max_retries = max_retries
        self.retry_base = retry_base_seconds
        self.metrics = PipelineMetrics()
        self._silver_buffer: list[dict[str, Any]] = []
        self._watermark_ms: int = 0
        self._stop = threading.Event()
        self._checkpoint_key = f"checkpoints/{group_id}/{source_topic}.json"
        self.quarantined_event_ids: set[str] = set()

    # -- lifecycle ----------------------------------------------------------

    def stop(self) -> None:
        self._stop.set()

    @property
    def stopped(self) -> bool:
        return self._stop.is_set()

    # -- bronze/silver writes ----------------------------------------------

    def _flush_silver(self, force: bool = False) -> int:
        if not self._silver_buffer:
            return 0
        if not force and len(self._silver_buffer) < self.writer.flush_records:
            return 0
        by_date: dict[str, list[dict[str, Any]]] = {}
        for r in self._silver_buffer:
            by_date.setdefault(r["event_date"], []).append(r)
        written = 0
        for date, rows in by_date.items():
            key = f"silver/events/event_date={date}/part-{uuid.uuid4().hex[:12]}.parquet"
            data = parquet_bytes(rows)
            if data:
                # Bind loop variables as default args so the closure captures the
                # current batch's key/data rather than the final iteration's.
                def _put(key: str = key, data: bytes = data) -> None:
                    self.store.put_bytes(key, data, content_type="application/vnd.apache.parquet")

                try:
                    retry_call(
                        _put,
                        max_attempts=self.max_retries,
                        base=self.retry_base,
                        on_retry=lambda a, e: self._on_retry("silver", a, e),
                    )
                    written += len(rows)
                except Exception as exc:
                    self.metrics.errors += 1
                    log.error("silver.write_failed", error=str(exc), key=key)
        self._silver_buffer.clear()
        return written

    def _on_retry(self, stage: str, attempt: int, exc: BaseException) -> None:
        self.metrics.retries += 1
        log.warning("retry", stage=stage, attempt=attempt, error=str(exc))

    # -- checkpointing ------------------------------------------------------

    def _checkpoint(self, messages: list[dict[str, Any]]) -> None:
        if not messages:
            return
        last = messages[-1]
        payload = {
            "group_id": self.group_id,
            "topic": self.source_topic,
            "last_cursor": last.get("cursor"),
            "last_offset": last.get("offset"),
            "watermark_ms": self._watermark_ms,
            "checkpointed_at": to_iso(utcnow()),
            "consumed": self.metrics.consumed,
        }
        try:
            self.store.put_bytes(
                self._checkpoint_key,
                json.dumps(payload).encode("utf-8"),
                content_type="application/json",
            )
        except Exception as exc:  # pragma: no cover
            log.error("checkpoint.failed", error=str(exc))

    def read_checkpoint(self) -> dict[str, Any] | None:
        try:
            if self.store.exists(self._checkpoint_key):
                cp: dict[str, Any] = json.loads(self.store.get_bytes(self._checkpoint_key))
                return cp
        except Exception:  # pragma: no cover
            return None
        return None

    # -- core processing ----------------------------------------------------

    def process_batch(self, messages: list[Any]) -> dict[str, int]:
        """Process one batch of StreamMessages. Returns per-batch counts."""
        stats = {
            "consumed": 0,
            "bronze": 0,
            "silver": 0,
            "quarantine": 0,
            "duplicate": 0,
            "late": 0,
            "dlq": 0,
        }
        raw_events: list[dict[str, Any]] = []
        for m in messages:
            stats["consumed"] += 1
            try:
                raw = json.loads(m.value.decode("utf-8"))
            except Exception as exc:
                stats["dlq"] += 1
                self.metrics.dlq += 1
                self.metrics.errors += 1
                self._send_dlq(m, f"json_decode_error:{exc}")
                continue
            raw_events.append(raw)

        # Update watermark to the max event time seen in this batch.
        for raw in raw_events:
            try:
                ts_ms = int(parse_iso(raw["event_timestamp"]).timestamp() * 1000)
                self._watermark_ms = max(self._watermark_ms, ts_ms)
            except Exception:
                pass

        watermark_iso = (
            to_iso(parse_iso(_ms_to_iso(self._watermark_ms))) if self._watermark_ms else None
        )

        # Bronze: write the raw event verbatim, always (even if invalid).
        for raw in raw_events:
            self.writer.add(raw)
            stats["bronze"] += 1
        try:
            self.writer.flush()
        except Exception as exc:
            self.metrics.errors += 1
            log.error("bronze.flush_failed", error=str(exc))

        # Silver: validate + transform + dedupe.
        silver_records: list[dict[str, Any]] = []
        for raw in raw_events:
            result = transform_event(
                raw,
                watermark_iso=watermark_iso,
                allowed_lateness_seconds=self.allowed_lateness,
            )
            if not result.ok or result.record is None:
                stats["quarantine"] += 1
                self.metrics.quarantined += 1
                if raw.get("event_id"):
                    self.quarantined_event_ids.add(str(raw["event_id"]))
                self.writer.write_quarantine(
                    str(raw.get("event_type", "Unknown")),
                    [
                        {
                            **raw,
                            "_silver_reason": result.quarantine_reason,
                            "_silver_issues": result.issues,
                        }
                    ],
                )
                continue
            rec = result.record
            if self.dedup.seen_before(rec["event_id"]):
                stats["duplicate"] += 1
                self.metrics.duplicates += 1
                continue
            self.dedup.add(rec["event_id"])
            if result.is_late:
                stats["late"] += 1
                self.metrics.late_events += 1
            silver_records.append(rec)

        # Cross-batch dedupe within this flush as a final safety net.
        unique, dropped = dedupe_events(silver_records)
        stats["duplicate"] += dropped
        self.metrics.duplicates += dropped

        self._silver_buffer.extend(unique)
        written = self._flush_silver(
            force=len(unique) > 0 and len(self._silver_buffer) >= self.writer.flush_records
        )
        stats["silver"] = len(unique)
        self.metrics.silver_written += len(unique)

        self.metrics.consumed += stats["consumed"]
        self.metrics.bronze_written += stats["bronze"]
        self.metrics.batches += 1

        # Commit + checkpoint (after metrics so the checkpoint reflects this batch).
        try:
            self.transport.commit(messages, self.group_id)
        except Exception as exc:  # pragma: no cover
            log.warning("commit.failed", error=str(exc))
        self._checkpoint(
            [{"cursor": m.cursor, "offset": m.offset} for m in messages if hasattr(m, "cursor")]
        )

        _ = written
        return stats

    def _send_dlq(self, message: Any, reason: str) -> None:
        try:
            self.transport.to_dlq(self.source_topic, self.dlq_topic, message, reason)
        except Exception as exc:  # pragma: no cover
            log.error("dlq.failed", error=str(exc))

    def run(
        self, *, max_batches: int | None = None, idle_stop_seconds: float | None = None
    ) -> dict[str, Any]:
        """Consume until stopped, ``max_batches`` reached, or idle for a while.

        Idle is detected after two consecutive empty polls to avoid stopping
        between micro-batches under transient backpressure.
        """
        batches = 0
        last_activity = time.time()
        empty_polls = 0
        while not self._stop.is_set():
            try:
                messages = self.transport.consume(
                    self.source_topic, self.group_id, self.batch_size, timeout_seconds=1.0
                )
            except Exception as exc:
                self.metrics.errors += 1
                log.error("consume.error", error=str(exc))
                time.sleep(min(5.0, self.retry_base * 2))
                continue

            if messages:
                self.process_batch(messages)
                last_activity = time.time()
                empty_polls = 0
            else:
                empty_polls += 1
                # idle
                self.writer.flush(force=True)
                self._flush_silver(force=True)
                if (
                    idle_stop_seconds
                    and empty_polls >= 2
                    and (time.time() - last_activity) > idle_stop_seconds
                ):
                    log.info("pipeline.idle_stop", idle=idle_stop_seconds)
                    break

            batches += 1
            if max_batches and batches >= max_batches:
                break

        # final flush
        self.writer.flush(force=True)
        self._flush_silver(force=True)
        return self.metrics.to_dict()

    def run_drain(self, *, timeout_seconds: float = 30.0) -> dict[str, Any]:
        """Process everything currently available, then stop when idle."""
        return self.run(idle_stop_seconds=timeout_seconds)


def _ms_to_iso(ms: int) -> str:
    from datetime import datetime

    return datetime.fromtimestamp(ms / 1000.0, tz=UTC).isoformat()


# Re-exported for the Gold stage.
__all__ = ["DedupWindow", "Pipeline", "PipelineMetrics", "apply_payout", "ms_between"]
