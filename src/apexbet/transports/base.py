"""Transport abstraction for streaming event delivery.

A `Transport` is a producer + consumer factory over a topic/stream. Two real
implementations exist (Redpanda/Kafka and Kinesis) plus an in-memory one for
unit tests and offline demos. There is **no silent fallback** between them:
`build_transport` raises on an unknown or unavailable backend.

Delivery semantics differ and are documented per adapter:
  * Kafka/Redpanda: at-least-once by default; explicit offset commit.
  * Kinesis: at-least-once; per-shard sequence-number checkpointing.
Neither is exactly-once end-to-end; business-effect idempotency is handled
downstream by event-id deduplication and idempotent writes.
"""

from __future__ import annotations

from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from typing import Protocol, runtime_checkable


@dataclass
class StreamMessage:
    """A single record pulled from a transport."""

    key: str | None
    value: bytes
    partition: int
    offset: int
    topic: str
    # transport-specific cursor used to acknowledge/commit this record
    cursor: dict[str, object] | None = None
    headers: dict[str, str] | None = None


@runtime_checkable
class Transport(Protocol):
    name: str

    def publish(
        self, topic: str, key: str | None, value: bytes, headers: dict[str, str] | None = None
    ) -> None: ...

    def publish_many(self, topic: str, items: Iterable[tuple[str | None, bytes]]) -> int: ...

    def consume(
        self, topic: str, group_id: str, max_messages: int = 500, timeout_seconds: float = 1.0
    ) -> list[StreamMessage]: ...

    def commit(self, messages: list[StreamMessage], group_id: str = "") -> None: ...

    def to_dlq(self, topic: str, dlq_topic: str, message: StreamMessage, reason: str) -> None: ...

    def health(self) -> dict[str, object]: ...

    def close(self) -> None: ...


class TransportError(RuntimeError):
    """Raised when a transport cannot be built or operated."""


def build_transport(name: str, *, settings=None) -> Transport:
    """Construct a transport by name. Raises ``TransportError`` on failure.

    Names: ``redpanda`` (Kafka protocol), ``kinesis`` (AWS/LocalStack),
    ``memory`` (in-process, tests/demo).
    """
    from apexbet.config import get_settings

    settings = settings or get_settings()

    if name == "memory":
        from apexbet.transports.memory import MemoryTransport

        return MemoryTransport()

    if name == "redpanda":
        from apexbet.transports.kafka import KafkaTransport

        return KafkaTransport(
            bootstrap=settings.kafka_bootstrap,
            group_id=settings.kafka_group_id,
            client_id="apexbet",
        )

    if name == "kinesis":
        from apexbet.transports.kinesis import KinesisTransport

        return KinesisTransport(
            stream=settings.kinesis_stream,
            region=settings.aws_region,
            endpoint_url=None if settings.use_real_aws else settings.aws_endpoint_url,
            access_key=settings.aws_access_key_id,
            secret_key=settings.aws_secret_access_key,
            shards=settings.kinesis_shards,
            auto_create=True,
        )

    raise TransportError(f"unknown transport: {name!r}")


def iter_transport(
    transport: Transport,
    topic: str,
    group_id: str,
    *,
    batch_size: int = 500,
    timeout_seconds: float = 1.0,
) -> Iterator[StreamMessage]:
    """Convenience generator that yields messages forever, committing as it goes."""
    while True:
        msgs = transport.consume(topic, group_id, batch_size, timeout_seconds)
        yield from msgs
        if msgs:
            transport.commit(msgs, group_id)
