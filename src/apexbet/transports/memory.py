"""In-process transport. Thread-safe, deterministic, used by unit tests and the
offline demo when Docker is unavailable. Not a distributed transport.

Offsets are tracked per (group_id, topic) so consumer-group semantics are
observable in tests. Duplicate/redelivery behaviour is driven by the caller.
"""

from __future__ import annotations

import threading
from collections import defaultdict
from collections.abc import Iterable

from apexbet.transports.base import StreamMessage, Transport


class MemoryTransport(Transport):
    name = "memory"

    def __init__(self) -> None:
        self._topics: dict[str, list[StreamMessage]] = defaultdict(list)
        self._offsets: dict[tuple[str, str], int] = defaultdict(int)
        self._lock = threading.RLock()
        self._dlq: list[StreamMessage] = []

    def publish(
        self, topic: str, key: str | None, value: bytes, headers: dict[str, str] | None = None
    ) -> None:
        with self._lock:
            log = self._topics[topic]
            offset = len(log)
            log.append(
                StreamMessage(
                    key=key,
                    value=value,
                    partition=0,
                    offset=offset,
                    topic=topic,
                    cursor={"offset": offset},
                    headers=headers,
                )
            )

    def publish_many(self, topic: str, items: Iterable[tuple[str | None, bytes]]) -> int:
        n = 0
        for key, value in items:
            self.publish(topic, key, value)
            n += 1
        return n

    def consume(
        self, topic: str, group_id: str, max_messages: int = 500, timeout_seconds: float = 1.0
    ) -> list[StreamMessage]:
        with self._lock:
            log = self._topics.get(topic, [])
            pos = self._offsets[(group_id, topic)]
            return list(log[pos : pos + max_messages])

    def commit(self, messages: list[StreamMessage], group_id: str = "") -> None:
        with self._lock:
            for m in messages:
                key = (group_id, m.topic)
                if m.offset + 1 > self._offsets[key]:
                    self._offsets[key] = m.offset + 1

    def to_dlq(self, topic: str, dlq_topic: str, message: StreamMessage, reason: str) -> None:
        with self._lock:
            self._dlq.append(message)
            self.publish(dlq_topic, message.key, message.value, headers={"dlq.reason": reason})

    def health(self) -> dict[str, object]:
        return {
            "transport": self.name,
            "status": "OK",
            "topics": {t: len(v) for t, v in self._topics.items()},
        }

    def close(self) -> None:
        return None

    # -- test / demo helpers (explicit group-aware commit) --
    def seed(self, topic: str, messages: list[tuple[str | None, bytes]]) -> None:
        self.publish_many(topic, messages)

    def message_count(self, topic: str) -> int:
        return len(self._topics.get(topic, []))

    def dlq_count(self) -> int:
        return len(self._dlq)

    def committed_offset(self, group_id: str, topic: str) -> int:
        return self._offsets[(group_id, topic)]
