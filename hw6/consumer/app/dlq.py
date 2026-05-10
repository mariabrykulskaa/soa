"""Dead Letter Queue producer.

Sends failed events to the DLQ topic with error metadata attached.
The DLQ message is plain JSON (no Avro) so it can be inspected without
a schema registry.
"""
from __future__ import annotations

import json
import logging
import time
from datetime import datetime, timezone
from typing import Any

from confluent_kafka import Producer

log = logging.getLogger("consumer.dlq")


class DLQProducer:
    def __init__(self, bootstrap: str, dlq_topic: str) -> None:
        self._topic = dlq_topic
        self._producer = Producer(
            {
                "bootstrap.servers": bootstrap,
                "acks": "all",
                "retries": 5,
                "retry.backoff.ms": 200,
                "client.id": "warehouse-dlq-producer",
            }
        )

    def send(
        self,
        *,
        original_event: dict[str, Any] | None,
        raw_value: bytes | None,
        error_reason: str,
        error_code: str,
        partition: int,
        offset: int,
    ) -> None:
        """Publish a DLQ record."""
        record = {
            "original_event": original_event,
            "raw_value": raw_value.hex() if raw_value else None,
            "error_reason": error_reason,
            "error_code": error_code,
            "failed_at": datetime.now(timezone.utc).isoformat(),
            "kafka_metadata": {
                "partition": partition,
                "offset": offset,
            },
        }
        try:
            self._producer.produce(
                topic=self._topic,
                value=json.dumps(record).encode(),
                on_delivery=self._delivery_cb,
            )
            self._producer.poll(0)
            log.warning(
                "DLQ: partition=%d offset=%d error_code=%s reason=%.120s",
                partition,
                offset,
                error_code,
                error_reason,
            )
        except Exception as exc:  # noqa: BLE001
            log.error("Failed to send event to DLQ: %s", exc)

    @staticmethod
    def _delivery_cb(err: Any, msg: Any) -> None:
        if err:
            log.error("DLQ delivery failed: %s", err)

    def flush(self) -> None:
        self._producer.flush()
