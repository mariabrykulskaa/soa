"""Movie events producer: HTTP API + synthetic generator."""
from __future__ import annotations

import logging
import os
import threading
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Literal, Optional

import uvicorn
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field, field_validator

from .kafka_producer import MovieEventProducer
from .generator import GeneratorWorker

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s :: %(message)s",
)
log = logging.getLogger("producer")

EVENT_TYPES = {"VIEW_STARTED", "VIEW_FINISHED", "VIEW_PAUSED", "VIEW_RESUMED", "LIKED", "SEARCHED"}
DEVICE_TYPES = {"MOBILE", "DESKTOP", "TV", "TABLET"}


class EventIn(BaseModel):
    event_id: Optional[str] = None
    user_id: str = Field(min_length=1)
    movie_id: str = Field(min_length=1)
    event_type: Literal["VIEW_STARTED", "VIEW_FINISHED", "VIEW_PAUSED", "VIEW_RESUMED", "LIKED", "SEARCHED"]
    timestamp: Optional[datetime] = None
    device_type: Literal["MOBILE", "DESKTOP", "TV", "TABLET"]
    session_id: str = Field(min_length=1)
    progress_seconds: int = 0

    @field_validator("progress_seconds")
    @classmethod
    def _nonneg(cls, v: int) -> int:
        if v < 0:
            raise ValueError("progress_seconds must be >= 0")
        return v


producer: MovieEventProducer
generator: Optional[GeneratorWorker] = None


@asynccontextmanager
async def lifespan(app: FastAPI):  # noqa: ARG001
    global producer, generator
    producer = MovieEventProducer(
        bootstrap=os.environ["KAFKA_BOOTSTRAP"],
        schema_registry_url=os.environ["SCHEMA_REGISTRY_URL"],
        topic=os.environ.get("TOPIC", "movie-events"),
    )
    producer.start()
    if os.environ.get("GENERATOR_ENABLED", "false").lower() == "true":
        eps = float(os.environ.get("GENERATOR_EPS", "5"))
        generator = GeneratorWorker(producer, events_per_second=eps)
        threading.Thread(target=generator.run, daemon=True, name="generator").start()
        log.info("generator started (eps=%s)", eps)
    try:
        yield
    finally:
        if generator:
            generator.stop()
        producer.stop()


app = FastAPI(title="movie-events producer", lifespan=lifespan)


@app.get("/health")
def health() -> dict:
    return {"status": "ok"}


@app.post("/events", status_code=202)
def publish_event(ev: EventIn) -> dict:
    event = {
        "event_id": ev.event_id or str(uuid.uuid4()),
        "user_id": ev.user_id,
        "movie_id": ev.movie_id,
        "event_type": ev.event_type,
        "timestamp": ev.timestamp or datetime.now(timezone.utc),
        "device_type": ev.device_type,
        "session_id": ev.session_id,
        "progress_seconds": ev.progress_seconds,
    }
    try:
        producer.send(event)
    except Exception as e:  # noqa: BLE001
        log.exception("publish failed")
        raise HTTPException(status_code=502, detail=f"kafka publish failed: {e}") from e
    return {"event_id": event["event_id"]}


@app.post("/events/flush")
def flush() -> dict:
    remaining = producer.flush(timeout=5.0)
    return {"remaining": remaining}


if __name__ == "__main__":
    uvicorn.run("app.main:app", host="0.0.0.0", port=8000, log_level="info")
