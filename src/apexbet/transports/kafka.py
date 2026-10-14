"""Kafka-compatible transport backed by Redpanda (or any Kafka broker).

Delivery semantics
------------------
* Producer: idempotent if the broker supports it (Redpanda does); ``acks=all``.
* Consumer: manual offset commit. We configure ``enable.auto.commit=false`` and
  commit explicitly after a batch's business effects are durably written. This
  gives **at-least-once** delivery; end-to-end idempotency is provided
  downstream by event-id deduplication.
* Ordering: preserved per partition. Partition selection is by message key
  (``account_id`` when present), so all events for one account are ordered.

The library is ``confluent-kafka`` (librdkafka). Import is lazy so the module
can be imported without the dependency present (e.g. in CI unit runs).
"""

from __future__ import annotations

import contextlib
import time
from collections.abc import Iterable

from apexbet.transports.base import StreamMessage, Transport, TransportError


class KafkaTransport(Transport):
    name = "redpanda"

    def __init__(self, bootstrap: str, group_id: str, client_id: str = "apexbet") -> None:
        try:
            from confluent_kafka import Consumer, KafkaError, Producer  # noqa: F401
        except ImportError as exc:  # pragma: no cover - env dependent
            raise TransportError(
                "confluent-kafka is not installed; install the 'apexbet' base deps "
                "or use APEXBET_TRANSPORT=memory"
            ) from exc

        from confluent_kafka import Consumer, Producer

        self.bootstrap = bootstrap
        self.group_id = group_id
        self._Consumer = Consumer
        self._producer = Producer(
            {
                "bootstrap.servers": bootstrap,
                "client.id": client_id,
                "enable.idempotence": True,
                "acks": "all",
                "linger.ms": 5,
                "compression.type": "lz4",
            }
        )
        self._consumers: dict[str, object] = {}

    # -- producer -----------------------------------------------------------

    def publish(
        self, topic: str, key: str | None, value: bytes, headers: dict[str, str] | None = None
    ) -> None:
        hdrs: list[tuple[str, str | bytes | None]] | None = [
            (k, v.encode("utf-8")) for k, v in (headers or {}).items()
        ] or None
        try:
            self._producer.produce(
                topic, key=(key.encode("utf-8") if key else None), value=value, headers=hdrs
            )
        except BufferError:  # local queue full -> flush and retry once
            self._producer.poll(1.0)
            self._producer.produce(
                topic, key=(key.encode("utf-8") if key else None), value=value, headers=hdrs
            )
        self._producer.poll(0)

    def publish_many(self, topic: str, items: Iterable[tuple[str | None, bytes]]) -> int:
        n = 0
        for key, value in items:
            self.publish(topic, key, value)
            n += 1
        self._producer.flush(10)
        return n

    def flush(self, timeout: float = 10.0) -> int:
        return int(self._producer.flush(timeout))

    # -- consumer -----------------------------------------------------------

    def _consumer_for(self, group_id: str) -> object:
        if group_id not in self._consumers:
            self._consumers[group_id] = self._Consumer(
                {
                    "bootstrap.servers": self.bootstrap,
                    "group.id": group_id,
                    "enable.auto.commit": False,
                    "auto.offset.reset": "earliest",
                    "enable.partition.eof": False,
                    "session.timeout.ms": 20000,
                    "max.poll.interval.ms": 300000,
                }
            )
        return self._consumers[group_id]

    def consume(
        self, topic: str, group_id: str, max_messages: int = 500, timeout_seconds: float = 1.0
    ) -> list[StreamMessage]:
        consumer = self._consumer_for(group_id)
        consumer.subscribe([topic])  # type: ignore[attr-defined]
        deadline = time.time() + timeout_seconds
        out: list[StreamMessage] = []
        while len(out) < max_messages and time.time() < deadline:
            msg = consumer.poll(0.2)  # type: ignore[attr-defined]
            if msg is None:
                continue
            if msg.error():
                continue
            out.append(
                StreamMessage(
                    key=msg.key().decode("utf-8") if msg.key() else None,
                    value=msg.value(),
                    partition=msg.partition(),
                    offset=msg.offset(),
                    topic=msg.topic(),
                    cursor={"partition": msg.partition(), "offset": msg.offset()},
                    headers={k: (v.decode("utf-8") if v else "") for k, v in (msg.headers() or [])},
                )
            )
        return out

    def commit(self, messages: list[StreamMessage], group_id: str = "") -> None:
        consumer = self._consumer_for(group_id or self.group_id)
        # Commit the highest offset per partition.
        for m in messages:
            from confluent_kafka import TopicPartition

            consumer.commit(  # type: ignore[attr-defined]
                offsets=[TopicPartition(m.topic, m.partition, m.offset + 1)], asynchronous=False
            )

    def to_dlq(self, topic: str, dlq_topic: str, message: StreamMessage, reason: str) -> None:
        self.publish(
            dlq_topic,
            message.key,
            message.value,
            headers={**(message.headers or {}), "dlq.reason": reason, "dlq.source_topic": topic},
        )

    def health(self) -> dict[str, object]:
        try:
            md = self._producer.list_topics(timeout=3)
            return {
                "transport": self.name,
                "status": "OK",
                "bootstrap": self.bootstrap,
                "topics": len(md.topics),
            }
        except Exception as exc:  # pragma: no cover - env dependent
            return {
                "transport": self.name,
                "status": "DOWN",
                "bootstrap": self.bootstrap,
                "error": str(exc),
            }

    def close(self) -> None:
        with contextlib.suppress(Exception):  # pragma: no cover
            self._producer.flush(5)
        for c in self._consumers.values():
            with contextlib.suppress(Exception):  # pragma: no cover
                c.close()  # type: ignore[attr-defined]
        self._consumers.clear()
