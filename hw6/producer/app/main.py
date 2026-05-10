"""WMS Producer service — HTTP API + background synthetic event generator.

Endpoints:
  POST /events        — send a specific warehouse event
  POST /generate/start  — start background generator
  POST /generate/stop   — stop background generator
  GET  /health        — liveness probe
"""
from __future__ import annotations

import logging
import os
import threading
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any, Optional
import uuid

import uvicorn
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field, model_validator

from .generator import GeneratorWorker
from .kafka_producer import WarehouseEventProducer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s :: %(message)s",
)
log = logging.getLogger("producer")

# ---------------------------------------------------------------------------
# Settings
# ---------------------------------------------------------------------------
BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
SR_URL = os.environ.get("SCHEMA_REGISTRY_URL", "http://localhost:8081")
TOPIC = os.environ.get("KAFKA_TOPIC", "warehouse-events")
INTERVAL_MS = int(os.environ.get("GENERATE_INTERVAL_MS", "2000"))
AUTO_GENERATE = os.environ.get("AUTO_GENERATE", "true").lower() == "true"
HTTP_PORT = int(os.environ.get("HTTP_PORT", "8080"))

# ---------------------------------------------------------------------------
# Globals
# ---------------------------------------------------------------------------
_producer: WarehouseEventProducer
_generator: Optional[GeneratorWorker] = None
_gen_lock = threading.Lock()


# ---------------------------------------------------------------------------
# Lifespan
# ---------------------------------------------------------------------------
@asynccontextmanager
async def lifespan(app: FastAPI):  # noqa: ARG001
    global _producer, _generator

    _producer = WarehouseEventProducer(BOOTSTRAP, SR_URL, TOPIC)
    _producer.start()

    if AUTO_GENERATE:
        _generator = GeneratorWorker(_producer, INTERVAL_MS)
        _generator.start()
        log.info("Auto-generator started (interval=%dms)", INTERVAL_MS)

    yield

    if _generator:
        _generator.stop()
    _producer.flush()


app = FastAPI(title="Warehouse WMS Producer", lifespan=lifespan)


# ---------------------------------------------------------------------------
# Request models
# ---------------------------------------------------------------------------
class OrderItemIn(BaseModel):
    product_id: str
    zone_id: str
    quantity: int = Field(gt=0)


class EventIn(BaseModel):
    event_id: Optional[str] = None
    event_type: str
    product_id: Optional[str] = None
    zone_id: Optional[str] = None
    from_zone_id: Optional[str] = None
    to_zone_id: Optional[str] = None
    quantity: Optional[int] = None
    counted_quantity: Optional[int] = None
    order_id: Optional[str] = None
    order_items: Optional[list[OrderItemIn]] = None
    supplier_id: Optional[str] = None
    # Allow custom timestamp (epoch ms) for testing out-of-order scenarios
    timestamp: Optional[int] = None

    @model_validator(mode="after")
    def _validate(self) -> "EventIn":
        valid = {
            "PRODUCT_RECEIVED", "PRODUCT_SHIPPED", "PRODUCT_MOVED",
            "PRODUCT_RESERVED", "PRODUCT_RELEASED", "INVENTORY_COUNTED",
            "ORDER_CREATED", "ORDER_COMPLETED",
        }
        if self.event_type not in valid:
            raise ValueError(f"Unknown event_type: {self.event_type}")
        return self


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------
@app.post("/events", status_code=202)
def send_event(body: EventIn) -> dict[str, Any]:
    event: dict[str, Any] = {
        "event_id": body.event_id or str(uuid.uuid4()),
        "event_type": body.event_type,
        "timestamp": body.timestamp or int(datetime.now(timezone.utc).timestamp() * 1000),
        "version": 1,
        "product_id": body.product_id,
        "zone_id": body.zone_id,
        "from_zone_id": body.from_zone_id,
        "to_zone_id": body.to_zone_id,
        "quantity": body.quantity,
        "counted_quantity": body.counted_quantity,
        "order_id": body.order_id,
        "order_items": (
            [i.model_dump() for i in body.order_items] if body.order_items else None
        ),
        "supplier_id": body.supplier_id,
    }
    _producer.produce(event)
    log.info("Manual event sent: event_type=%s event_id=%s", event["event_type"], event["event_id"])
    return {"event_id": event["event_id"], "status": "accepted"}


@app.post("/generate/start", status_code=200)
def start_generator() -> dict[str, str]:
    global _generator
    with _gen_lock:
        if _generator is not None:
            return {"status": "already running"}
        _generator = GeneratorWorker(_producer, INTERVAL_MS)
        _generator.start()
    return {"status": "started"}


@app.post("/generate/stop", status_code=200)
def stop_generator() -> dict[str, str]:
    global _generator
    with _gen_lock:
        if _generator is None:
            return {"status": "not running"}
        _generator.stop()
        _generator = None
    return {"status": "stopped"}


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


if __name__ == "__main__":
    uvicorn.run("app.main:app", host="0.0.0.0", port=HTTP_PORT, log_level="info")
