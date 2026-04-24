"""Synthetic traffic generator producing realistic user sessions."""
from __future__ import annotations

import logging
import random
import threading
import time
import uuid
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .kafka_producer import MovieEventProducer

log = logging.getLogger("producer.generator")

DEVICES = ["MOBILE", "DESKTOP", "TV", "TABLET"]
MOVIES = [f"movie-{i:04d}" for i in range(50)]
USERS = [f"user-{i:04d}" for i in range(200)]


class GeneratorWorker:
    """Runs forever, emitting coherent user sessions at a target rate."""

    def __init__(self, producer: "MovieEventProducer", events_per_second: float) -> None:
        self.producer = producer
        self.eps = max(0.1, events_per_second)
        self._stop = threading.Event()

    def stop(self) -> None:
        self._stop.set()

    def run(self) -> None:
        interval = 1.0 / self.eps
        while not self._stop.is_set():
            try:
                self._emit_session()
            except Exception:  # noqa: BLE001
                log.exception("generator cycle failed")
            time.sleep(interval)

    def _now(self) -> datetime:
        # Spread historical data: 80% recent, 20% up to 8 days back (for retention signal).
        now = datetime.now(timezone.utc)
        if random.random() < 0.2:
            return now - timedelta(days=random.randint(1, 8), seconds=random.randint(0, 86400))
        return now

    def _emit(self, user: str, movie: str, session: str, device: str,
              event_type: str, ts: datetime, progress: int) -> None:
        self.producer.send({
            "event_id": str(uuid.uuid4()),
            "user_id": user,
            "movie_id": movie,
            "event_type": event_type,
            "timestamp": ts,
            "device_type": device,
            "session_id": session,
            "progress_seconds": progress,
        })

    def _emit_session(self) -> None:
        user = random.choice(USERS)
        movie = random.choice(MOVIES)
        device = random.choice(DEVICES)
        session = f"s-{uuid.uuid4().hex[:12]}"
        start_ts = self._now()

        roll = random.random()
        if roll < 0.08:
            # Search-only interaction
            self._emit(user, movie, session, device, "SEARCHED", start_ts, 0)
            return
        if roll < 0.15:
            # Like without view
            self._emit(user, movie, session, device, "LIKED", start_ts, 0)
            return

        # Watch session
        self._emit(user, movie, session, device, "VIEW_STARTED", start_ts, 0)
        progress = 0
        ts = start_ts
        # 0..3 pause/resume cycles
        for _ in range(random.randint(0, 3)):
            watch = random.randint(60, 900)
            progress += watch
            ts += timedelta(seconds=watch)
            self._emit(user, movie, session, device, "VIEW_PAUSED", ts, progress)
            pause = random.randint(10, 120)
            ts += timedelta(seconds=pause)
            self._emit(user, movie, session, device, "VIEW_RESUMED", ts, progress)

        # 70% finish, 30% abandon
        if random.random() < 0.7:
            watch = random.randint(300, 1800)
            progress += watch
            ts += timedelta(seconds=watch)
            self._emit(user, movie, session, device, "VIEW_FINISHED", ts, progress)
            if random.random() < 0.4:
                self._emit(user, movie, session, device, "LIKED", ts + timedelta(seconds=2), progress)
