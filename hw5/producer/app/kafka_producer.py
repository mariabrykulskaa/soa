"""Kafka Avro producer bound to the Schema Registry."""
from __future__ import annotations

import json
import logging
import time
from pathlib import Path
from typing import Any

from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext, StringSerializer

log = logging.getLogger("producer.kafka")

SCHEMA_PATH = Path(__file__).resolve().parent.parent / "schema" / "movie_event.avsc"


def _load_schema() -> str:
    # Allow running both inside container (schema copied next to app) and in dev.
    candidates = [
        Path("/schemas/movie_event.avsc"),
        SCHEMA_PATH,
        Path(__file__).resolve().parent / "movie_event.avsc",
    ]
    for p in candidates:
        if p.is_file():
            return p.read_text()
    raise FileNotFoundError("movie_event.avsc not found in any known location")


class MovieEventProducer:
    def __init__(self, bootstrap: str, schema_registry_url: str, topic: str) -> None:
        self.topic = topic
        self.bootstrap = bootstrap
        self.schema_registry_url = schema_registry_url
        self._producer: Producer | None = None
        self._value_ser: AvroSerializer | None = None
        self._key_ser = StringSerializer("utf_8")

    def start(self) -> None:
        schema_str = _load_schema()
        sr = SchemaRegistryClient({"url": self.schema_registry_url})
        # Wait for Schema Registry to be ready.
        self._wait_sr(sr)
        self._value_ser = AvroSerializer(
            schema_registry_client=sr,
            schema_str=schema_str,
            conf={"auto.register.schemas": False, "use.latest.version": True},
        )
        self._producer = Producer({
            "bootstrap.servers": self.bootstrap,
            "acks": "all",
            "enable.idempotence": True,
            "retries": 10,
            "retry.backoff.ms": 200,
            "linger.ms": 20,
            "compression.type": "zstd",
            "client.id": "movie-events-producer",
        })
        log.info("producer started: topic=%s bootstrap=%s", self.topic, self.bootstrap)

    @staticmethod
    def _wait_sr(sr: SchemaRegistryClient, attempts: int = 30, delay: float = 1.0) -> None:
        for i in range(attempts):
            try:
                sr.get_subjects()
                return
            except Exception as e:  # noqa: BLE001
                log.info("waiting for schema registry (%s/%s): %s", i + 1, attempts, e)
                time.sleep(delay)
        raise RuntimeError("schema registry not reachable")

    def _delivery(self, err, msg) -> None:  # noqa: ANN001
        if err is not None:
            log.error("delivery failed key=%s err=%s", msg.key(), err)
            return
        try:
            key = msg.key().decode() if msg.key() else None
        except Exception:  # noqa: BLE001
            key = None
        log.info(
            "delivered topic=%s partition=%s offset=%s key=%s",
            msg.topic(), msg.partition(), msg.offset(), key,
        )

    def send(self, event: dict[str, Any]) -> None:
        assert self._producer is not None and self._value_ser is not None
        # Normalize timestamp to ms epoch int as required by Avro logicalType timestamp-millis.
        ts = event["timestamp"]
        if hasattr(ts, "timestamp"):
            event = {**event, "timestamp": int(ts.timestamp() * 1000)}
        key = event["user_id"]  # partitioning by user_id to preserve per-user event order
        value_ctx = SerializationContext(self.topic, MessageField.VALUE)
        value_bytes = self._value_ser(event, value_ctx)
        # Retry loop for BufferError (local queue full).
        for attempt in range(5):
            try:
                self._producer.produce(
                    topic=self.topic,
                    key=self._key_ser(key),
                    value=value_bytes,
                    on_delivery=self._delivery,
                )
                break
            except BufferError:
                log.warning("local queue full, polling (attempt=%s)", attempt)
                self._producer.poll(0.5)
        else:
            raise RuntimeError("producer local queue stays full")
        self._producer.poll(0)
        log.info(
            "published event_id=%s type=%s ts=%s key=%s",
            event["event_id"], event["event_type"], event["timestamp"], key,
        )

    def flush(self, timeout: float = 5.0) -> int:
        assert self._producer is not None
        return self._producer.flush(timeout)

    def stop(self) -> None:
        if self._producer is not None:
            self._producer.flush(10)
            self._producer = None
