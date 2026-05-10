"""Kafka Avro producer for warehouse events.

Connects to Schema Registry, fetches the latest schema version,
and publishes events to the configured topic with Avro serialization.
"""
from __future__ import annotations

import logging
import time
from pathlib import Path
from typing import Any

from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext, StringSerializer

log = logging.getLogger("producer.kafka")

_SCHEMA_CANDIDATES = [
    Path("/schemas/warehouse_event_v2.avsc"),
    Path(__file__).resolve().parent.parent / "schemas" / "warehouse_event_v2.avsc",
]


def _load_schema() -> str:
    for p in _SCHEMA_CANDIDATES:
        if p.is_file():
            return p.read_text()
    raise FileNotFoundError("warehouse_event_v2.avsc not found")


class WarehouseEventProducer:
    """Thread-safe Avro producer for warehouse events."""

    def __init__(self, bootstrap: str, schema_registry_url: str, topic: str) -> None:
        self.topic = topic
        self._bootstrap = bootstrap
        self._sr_url = schema_registry_url
        self._producer: Producer | None = None
        self._value_ser: AvroSerializer | None = None
        self._key_ser = StringSerializer("utf_8")

    # ------------------------------------------------------------------
    def start(self) -> None:
        schema_str = _load_schema()
        sr = SchemaRegistryClient({"url": self._sr_url})
        self._wait_sr(sr)

        self._value_ser = AvroSerializer(
            schema_registry_client=sr,
            schema_str=schema_str,
            # Use the latest registered version (V2); V1 events still fit
            # because all V2 fields added have default=null.
            conf={"auto.register.schemas": False, "use.latest.version": True},
        )
        self._producer = Producer(
            {
                "bootstrap.servers": self._bootstrap,
                "acks": "all",
                "enable.idempotence": True,
                "retries": 10,
                "retry.backoff.ms": 200,
                "linger.ms": 10,
                "compression.type": "zstd",
                "client.id": "warehouse-events-producer",
            }
        )
        log.info("Producer started: topic=%s bootstrap=%s", self.topic, self._bootstrap)

    # ------------------------------------------------------------------
    @staticmethod
    def _wait_sr(sr: SchemaRegistryClient, attempts: int = 60, delay: float = 2.0) -> None:
        for i in range(attempts):
            try:
                sr.get_subjects()
                log.info("Schema Registry ready.")
                return
            except Exception as exc:  # noqa: BLE001
                log.info("Waiting for Schema Registry (%d/%d): %s", i + 1, attempts, exc)
                time.sleep(delay)
        raise RuntimeError("Schema Registry not reachable after retries")

    # ------------------------------------------------------------------
    def _delivery_cb(self, err: Any, msg: Any) -> None:
        if err is not None:
            log.error("Delivery failed key=%s err=%s", msg.key(), err)
        else:
            key = msg.key().decode() if msg.key() else None
            log.debug(
                "Delivered event_id=%s topic=%s partition=%d offset=%d",
                key,
                msg.topic(),
                msg.partition(),
                msg.offset(),
            )

    # ------------------------------------------------------------------
    def produce(self, event: dict[str, Any]) -> None:
        """Publish a warehouse event dict. event_id is used as message key."""
        if self._producer is None or self._value_ser is None:
            raise RuntimeError("Producer not started. Call start() first.")

        key = event.get("event_id", "")
        # Use product_id as Kafka key so all events for one product go to
        # the same partition (preserves ordering within a product).
        partition_key = event.get("product_id") or event.get("order_id") or key

        self._producer.produce(
            topic=self.topic,
            key=self._key_ser(partition_key, SerializationContext(self.topic, MessageField.KEY)),
            value=self._value_ser(event, SerializationContext(self.topic, MessageField.VALUE)),
            on_delivery=self._delivery_cb,
        )
        self._producer.poll(0)

    # ------------------------------------------------------------------
    def flush(self) -> None:
        if self._producer:
            self._producer.flush()
