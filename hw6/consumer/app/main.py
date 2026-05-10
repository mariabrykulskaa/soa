"""Warehouse consumer main: Kafka consumer loop + FastAPI HTTP server.

Architecture:
  - FastAPI runs in the main thread (lifespan manages resources).
  - Kafka consumer loop runs in a dedicated background thread.
  - Consumer lag is updated periodically in a third thread.
  - All state is shared via prometheus_client thread-safe primitives.

At-least-once semantics:
  - enable.auto.commit = false
  - Offset is committed only after successful processing OR after sending
    the event to the DLQ. This means we never lose an event but may
    reprocess duplicates (handled by idempotency in handlers.py).
"""
from __future__ import annotations

import logging
import os
import threading
import time
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any

import uvicorn
from confluent_kafka import Consumer, KafkaError, KafkaException, Producer, TopicPartition
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext
from fastapi import FastAPI, Response
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from .cassandra_db import CassandraClient
from .dlq import DLQProducer
from .handlers import handle_event
from . import metrics as m

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s :: %(message)s",
)
log = logging.getLogger("consumer")

# ---------------------------------------------------------------------------
# Settings
# ---------------------------------------------------------------------------
BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
SR_URL = os.environ.get("SCHEMA_REGISTRY_URL", "http://localhost:8081")
TOPIC = os.environ.get("KAFKA_TOPIC", "warehouse-events")
DLQ_TOPIC = os.environ.get("KAFKA_DLQ_TOPIC", "warehouse-events-dlq")
GROUP_ID = os.environ.get("KAFKA_GROUP_ID", "warehouse-state-consumer")
CASSANDRA_HOSTS = os.environ.get("CASSANDRA_HOSTS", "cassandra-1,cassandra-2,cassandra-3").split(",")
CASSANDRA_KEYSPACE = os.environ.get("CASSANDRA_KEYSPACE", "warehouse")
CASSANDRA_CL_WRITE = os.environ.get("CASSANDRA_CONSISTENCY_WRITE", "QUORUM")
CASSANDRA_CL_READ = os.environ.get("CASSANDRA_CONSISTENCY_READ", "ONE")
HTTP_PORT = int(os.environ.get("HTTP_PORT", "8082"))

# ---------------------------------------------------------------------------
# Shared state
# ---------------------------------------------------------------------------
_db: CassandraClient | None = None
_dlq: DLQProducer | None = None
_consumer_thread: threading.Thread | None = None
_kafka_ok: threading.Event = threading.Event()
_stop_event: threading.Event = threading.Event()


# ---------------------------------------------------------------------------
# Consumer thread
# ---------------------------------------------------------------------------
def _build_deserializer() -> AvroDeserializer:
    sr = SchemaRegistryClient({"url": SR_URL})
    # Use the latest schema version (V2) for deserialization.
    # V1 messages are backward-compatible: supplier_id will be None.
    return AvroDeserializer(schema_registry_client=sr)


def _update_lag(consumer: Consumer) -> None:
    """Update consumer_lag gauge for all assigned partitions."""
    try:
        partitions = consumer.assignment()
        if not partitions:
            return
        committed = consumer.committed(partitions, timeout=5.0)
        for tp in committed:
            try:
                low, high = consumer.get_watermark_offsets(
                    TopicPartition(tp.topic, tp.partition), timeout=5.0, cached=False
                )
                committed_off = tp.offset if tp.offset >= 0 else low
                lag = max(0, high - committed_off)
                m.consumer_lag.labels(topic=tp.topic, partition=tp.partition).set(lag)
            except Exception:  # noqa: BLE001
                pass
    except Exception:  # noqa: BLE001
        pass


def _run_consumer() -> None:
    """Main consumer loop. Runs in a dedicated background thread."""
    global _db, _dlq

    deserializer = _build_deserializer()

    consumer = Consumer(
        {
            "bootstrap.servers": BOOTSTRAP,
            "group.id": GROUP_ID,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,  # manual commit after processing
            "session.timeout.ms": 30000,
            "heartbeat.interval.ms": 10000,
            "max.poll.interval.ms": 300000,
            "client.id": "warehouse-consumer-1",
        }
    )
    consumer.subscribe([TOPIC])
    m.kafka_connected.set(1)
    _kafka_ok.set()

    lag_update_interval = 10  # seconds
    last_lag_update = 0.0

    log.info(
        "Consumer started: topic=%s group=%s bootstrap=%s",
        TOPIC,
        GROUP_ID,
        BOOTSTRAP,
    )

    try:
        while not _stop_event.is_set():
            msg = consumer.poll(timeout=1.0)

            # Periodic consumer lag update
            now = time.monotonic()
            if now - last_lag_update >= lag_update_interval:
                _update_lag(consumer)
                last_lag_update = now

            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                log.error("Kafka error: %s", msg.error())
                m.kafka_connected.set(0)
                continue

            m.kafka_connected.set(1)
            partition = msg.partition()
            offset = msg.offset()
            event: dict[str, Any] | None = None

            try:
                event = deserializer(
                    msg.value(),
                    SerializationContext(msg.topic(), MessageField.VALUE),
                )
            except Exception as exc:  # noqa: BLE001
                log.error(
                    "Deserialization error partition=%d offset=%d: %s",
                    partition,
                    offset,
                    exc,
                )
                _dlq.send(
                    original_event=None,
                    raw_value=msg.value(),
                    error_reason=str(exc),
                    error_code="DESERIALIZATION_ERROR",
                    partition=partition,
                    offset=offset,
                )
                m.dlq_sent_total.labels(error_code="DESERIALIZATION_ERROR").inc()
                consumer.commit(message=msg)
                continue

            event_type = str(event.get("event_type", "UNKNOWN"))
            event_id = str(event.get("event_id", ""))
            log.info(
                "Processing event_id=%s event_type=%s partition=%d offset=%d",
                event_id,
                event_type,
                partition,
                offset,
            )

            start_ts = time.perf_counter()
            try:
                handle_event(event, _db)
                elapsed = time.perf_counter() - start_ts
                m.events_processed_total.labels(event_type=event_type).inc()
                m.event_processing_duration_seconds.labels(event_type=event_type).observe(elapsed)
                log.info(
                    "OK event_id=%s event_type=%s partition=%d offset=%d elapsed=%.3fs",
                    event_id,
                    event_type,
                    partition,
                    offset,
                    elapsed,
                )
            except ValueError as exc:
                # Validation failure — send to DLQ, do NOT retry
                elapsed = time.perf_counter() - start_ts
                log.warning(
                    "VALIDATION_ERROR event_id=%s event_type=%s: %s", event_id, event_type, exc
                )
                _dlq.send(
                    original_event=event,
                    raw_value=msg.value(),
                    error_reason=str(exc),
                    error_code="VALIDATION_ERROR",
                    partition=partition,
                    offset=offset,
                )
                m.dlq_sent_total.labels(error_code="VALIDATION_ERROR").inc()

            except Exception as exc:  # noqa: BLE001
                # Unexpected / Cassandra error — send to DLQ to avoid blocking
                elapsed = time.perf_counter() - start_ts
                log.error(
                    "PROCESSING_ERROR event_id=%s event_type=%s: %s",
                    event_id,
                    event_type,
                    exc,
                    exc_info=True,
                )
                _dlq.send(
                    original_event=event,
                    raw_value=msg.value(),
                    error_reason=str(exc),
                    error_code="PROCESSING_ERROR",
                    partition=partition,
                    offset=offset,
                )
                m.dlq_sent_total.labels(error_code="PROCESSING_ERROR").inc()

            # Commit offset regardless — event was either processed or sent to DLQ.
            # at-least-once: if we crash here, the event will be reprocessed on
            # restart (idempotency guard will skip the duplicate).
            consumer.commit(message=msg)

    except KafkaException as exc:
        log.error("Fatal Kafka exception: %s", exc)
        m.kafka_connected.set(0)
    finally:
        consumer.close()
        m.kafka_connected.set(0)
        log.info("Consumer loop terminated.")


# ---------------------------------------------------------------------------
# FastAPI app
# ---------------------------------------------------------------------------
@asynccontextmanager
async def lifespan(app: FastAPI):  # noqa: ARG001
    global _db, _dlq, _consumer_thread

    # Connect to Cassandra
    _db = CassandraClient(
        hosts=CASSANDRA_HOSTS,
        keyspace=CASSANDRA_KEYSPACE,
        consistency_write=CASSANDRA_CL_WRITE,
        consistency_read=CASSANDRA_CL_READ,
    )
    _db.connect(retries=40, delay=5.0)

    # DLQ producer
    _dlq = DLQProducer(bootstrap=BOOTSTRAP, dlq_topic=DLQ_TOPIC)

    # Start consumer loop in background thread
    _consumer_thread = threading.Thread(
        target=_run_consumer, daemon=True, name="kafka-consumer"
    )
    _consumer_thread.start()

    # Wait for Kafka connection
    if not _kafka_ok.wait(timeout=60):
        log.error("Consumer failed to connect to Kafka within 60s")

    yield

    # Shutdown
    _stop_event.set()
    if _consumer_thread:
        _consumer_thread.join(timeout=10)
    _dlq.flush()
    if _db:
        _db.close()


app = FastAPI(title="Warehouse Consumer", lifespan=lifespan)


@app.get("/health")
def health() -> Response:
    """Liveness / readiness probe.

    Returns 200 if consumer is connected to both Kafka and Cassandra.
    Returns 503 otherwise.
    """
    cassandra_ok = _db.is_healthy() if _db else False
    kafka_ok = _kafka_ok.is_set() and not _stop_event.is_set()

    if cassandra_ok and kafka_ok:
        m.cassandra_connected.set(1)
        return Response(
            content='{"status":"ok","kafka":"connected","cassandra":"connected"}',
            media_type="application/json",
            status_code=200,
        )

    body_parts = []
    if not kafka_ok:
        body_parts.append('"kafka":"disconnected"')
        m.kafka_connected.set(0)
    if not cassandra_ok:
        body_parts.append('"cassandra":"disconnected"')
        m.cassandra_connected.set(0)

    return Response(
        content='{{"status":"degraded",{}}}'.format(",".join(body_parts)),
        media_type="application/json",
        status_code=503,
    )


@app.get("/metrics")
def metrics() -> Response:
    """Prometheus-compatible metrics endpoint."""
    return Response(
        content=generate_latest(),
        media_type=CONTENT_TYPE_LATEST,
    )


if __name__ == "__main__":
    uvicorn.run("app.main:app", host="0.0.0.0", port=HTTP_PORT, log_level="info")
