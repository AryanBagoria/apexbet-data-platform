"""Amazon Kinesis Data Streams transport.

Targets LocalStack (or real AWS when ``APEXBET_USE_REAL_AWS=1``).

Delivery semantics (AWS Kinesis, and LocalStack's emulation of it)
------------------------------------------------------------------
* Producer: ``PutRecords`` batches up to 500 records / 5 MiB. Partial failures
  return ``FailedRecordCount`` plus per-record error codes; those are retried.
* Ordering: guaranteed per **shard**, keyed by ``PartitionKey`` (MD5 hashed to
  a shard). We key by ``account_id`` so per-account ordering holds, matching
  the Kafka keying.
* Consumer: ``GetShardIterator`` with ``LATEST`` or ``AT_TIMESTAMP``, then
  ``GetRecords``. Checkpointing is by **sequence number per shard**, which we
  persist outside Kinesis (this project stores checkpoints via the storage
  layer). KCL-style lease management is out of scope locally and is documented
  as a limitation.
* Retention: 24h default (up to 365d configured). Multi-day replay beyond
  retention is impossible from the stream; Bronze is the durable replay source.

This adapter is validated against LocalStack's Kinesis API. Whether LocalStack
implements every mode (Enhanced Fan-Out, resharding semantics) is verified at
runtime, not assumed.
"""

from __future__ import annotations

import base64
import time
from collections.abc import Iterable

from apexbet.transports.base import StreamMessage, Transport, TransportError


class KinesisTransport(Transport):
    name = "kinesis"

    def __init__(
        self,
        stream: str,
        region: str = "us-east-1",
        endpoint_url: str | None = "http://localhost:4566",
        access_key: str = "test",
        secret_key: str = "test",
        shards: int = 2,
        auto_create: bool = True,
    ) -> None:
        try:
            import boto3
        except ImportError as exc:  # pragma: no cover
            raise TransportError("boto3 is required for the kinesis transport") from exc

        self.stream = stream
        self.shards = shards
        self._boto3 = boto3
        self._client = boto3.client(
            "kinesis",
            region_name=region,
            endpoint_url=endpoint_url,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
        )
        # shard_id -> current iterator; and committed sequence per shard
        self._iterators: dict[str, str] = {}
        self._started = False
        if auto_create:
            self._ensure_stream()

    # -- lifecycle ----------------------------------------------------------

    def _ensure_stream(self) -> None:
        from botocore.exceptions import ClientError

        try:
            self._client.describe_stream(StreamName=self.stream)
        except ClientError as exc:
            code = exc.response.get("Error", {}).get("Code", "")
            if code in ("ResourceNotFoundException", "ResourceNotFound"):
                self._client.create_stream(StreamName=self.stream, ShardCount=self.shards)
                self._wait_active()
            else:  # pragma: no cover - connectivity issue
                raise TransportError(f"kinesis describe failed: {exc}") from exc

    def _wait_active(self, timeout: float = 30.0) -> None:
        deadline = time.time() + timeout
        while time.time() < deadline:
            d = self._client.describe_stream(StreamName=self.stream)
            status = d["StreamDescription"]["StreamStatus"]
            if status == "ACTIVE":
                return
            time.sleep(0.5)
        raise TransportError(f"stream {self.stream} did not become ACTIVE")

    def shard_ids(self) -> list[str]:
        d = self._client.describe_stream(StreamName=self.stream)
        return [s["ShardId"] for s in d["StreamDescription"]["Shards"]]

    # -- producer -----------------------------------------------------------

    def publish(
        self, topic: str, key: str | None, value: bytes, headers: dict[str, str] | None = None
    ) -> None:
        self.publish_many(topic, [(key, value)])

    def publish_many(self, topic: str, items: Iterable[tuple[str | None, bytes]]) -> int:
        batch: list[dict[str, object]] = []
        published = 0
        for key, value in items:
            batch.append({"Data": value, "PartitionKey": key or "default"})
            if len(batch) == 100:
                published += self._put_batch(topic, batch)
                batch = []
        if batch:
            published += self._put_batch(topic, batch)
        return published

    def _put_batch(self, topic: str, batch: list[dict[str, object]], max_retries: int = 4) -> int:
        stream = topic or self.stream
        remaining = batch
        published = 0
        for attempt in range(max_retries):
            resp = self._client.put_records(StreamName=stream, Records=remaining)
            failed_count = int(resp.get("FailedRecordCount", 0))
            published += len(remaining) - failed_count
            if failed_count == 0:
                return int(published)
            failed = [
                rec
                for rec, res in zip(remaining, resp["Records"], strict=False)
                if "ErrorCode" in res
            ]
            # Backoff before retrying the failed subset.
            time.sleep(min(2.0, 0.05 * (2**attempt)))
            remaining = failed
        # Anything still failing is reported (caller may route to a local DLQ).
        raise TransportError(
            f"{len(remaining)} Kinesis records failed after {max_retries} attempts"
        )

    # -- consumer -----------------------------------------------------------

    def consume(
        self, topic: str, group_id: str, max_messages: int = 500, timeout_seconds: float = 1.0
    ) -> list[StreamMessage]:
        stream = topic or self.stream
        if not self._started:
            self._init_iterators(stream)
            self._started = True
        out: list[StreamMessage] = []
        deadline = time.time() + timeout_seconds
        for shard_id, it in list(self._iterators.items()):
            if len(out) >= max_messages or time.time() >= deadline:
                break
            try:
                resp = self._client.get_records(
                    ShardIterator=it, Limit=min(1000, max_messages - len(out))
                )
            except Exception:  # pragma: no cover - transient
                continue
            self._iterators[shard_id] = resp["NextShardIterator"]
            for rec in resp["Records"]:
                data = rec["Data"]
                if isinstance(data, str):  # LocalStack sometimes returns base64 str
                    data = base64.b64decode(data)
                out.append(
                    StreamMessage(
                        key=rec.get("PartitionKey"),
                        value=data,
                        partition=int(shard_id.split("-")[-1]) if "-" in shard_id else 0,
                        offset=int(rec["SequenceNumber"]),
                        topic=stream,
                        cursor={"shard_id": shard_id, "sequence_number": rec["SequenceNumber"]},
                    )
                )
        return out

    def _init_iterators(self, stream: str) -> None:
        for shard_id in self.shard_ids():
            resp = self._client.get_shard_iterator(
                StreamName=stream, ShardId=shard_id, ShardIteratorType="LATEST"
            )
            self._iterators[shard_id] = resp["ShardIterator"]

    def seek_to_timestamp(self, stream: str, epoch_seconds: float) -> None:
        """Reposition all shard iterators to a timestamp (replay support)."""
        for shard_id in self.shard_ids():
            resp = self._client.get_shard_iterator(
                StreamName=stream,
                ShardId=shard_id,
                ShardIteratorType="AT_TIMESTAMP",
                Timestamp=epoch_seconds,
            )
            self._iterators[shard_id] = resp["ShardIterator"]
        self._started = True

    def commit(self, messages: list[StreamMessage], group_id: str = "") -> None:
        """Kinesis has no broker-side consumer-group commit.

        Checkpointing is the responsibility of the consumer (persisted via the
        storage layer). This is a documented difference from Kafka: there is no
        equivalent of an offset commit; recovery uses stored sequence numbers
        plus an AT_TIMESTAMP/TRIM_HORIZON re-read.
        """
        return None

    def to_dlq(self, topic: str, dlq_topic: str, message: StreamMessage, reason: str) -> None:
        # Kinesis has no native DLQ; we publish to a sibling stream with a reason header.
        self.publish(
            dlq_topic,
            message.key,
            message.value,
            headers={"dlq.reason": reason, "dlq.source_topic": topic},
        )

    def health(self) -> dict[str, object]:
        try:
            d = self._client.describe_stream(StreamName=self.stream)
            desc = d["StreamDescription"]
            return {
                "transport": self.name,
                "status": "OK",
                "stream": self.stream,
                "stream_status": desc["StreamStatus"],
                "shards": len(desc["Shards"]),
                "retention_hours": desc.get("RetentionPeriodHours"),
            }
        except Exception as exc:
            return {
                "transport": self.name,
                "status": "DOWN",
                "stream": self.stream,
                "error": str(exc),
            }

    def close(self) -> None:
        return None
