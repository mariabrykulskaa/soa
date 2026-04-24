import os
import time
import uuid
from datetime import date, datetime, timezone

import boto3
import clickhouse_connect
import psycopg
import pytest
import requests

PRODUCER_URL = os.environ.get("PRODUCER_URL", "http://producer:8000")
AGGREGATOR_URL = os.environ.get("AGGREGATOR_URL", "http://aggregator:8010")
CH_HOST = os.environ.get("CLICKHOUSE_HOST", "clickhouse")
CH_PORT = int(os.environ.get("CLICKHOUSE_PORT", "8123"))
PG_DSN = os.environ.get("PG_DSN", "postgresql://analytics:analytics@postgres:5432/analytics")
S3_ENDPOINT = os.environ.get("S3_ENDPOINT", "http://minio:9000")
S3_BUCKET = os.environ.get("S3_BUCKET", "movie-analytics")


def _wait(url, timeout=120):
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        try:
            r = requests.get(url, timeout=2)
            if r.status_code == 200:
                return
        except Exception as e:  # noqa: BLE001
            last = e
        time.sleep(1)
    raise TimeoutError(f"{url} not ready: {last}")


@pytest.fixture(scope="session", autouse=True)
def wait_services():
    _wait(f"{PRODUCER_URL}/health")
    _wait(f"{AGGREGATOR_URL}/health")


@pytest.fixture()
def ch():
    client = clickhouse_connect.get_client(host=CH_HOST, port=CH_PORT, database="cinema")
    yield client
    client.close()


def _poll_ch(ch, event_id: str, attempts: int = 60, delay: float = 1.0):
    for _ in range(attempts):
        rows = ch.query(
            "SELECT event_id, user_id, movie_id, event_type, device_type, progress_seconds "
            "FROM cinema.movie_events WHERE event_id = %(eid)s",
            parameters={"eid": event_id},
        ).result_rows
        if rows:
            return rows[0]
        time.sleep(delay)
    return None


def test_pipeline_event_reaches_clickhouse(ch):
    """Block 1-4.4: producer -> Kafka -> ClickHouse Kafka engine -> MergeTree."""
    session_id = f"s-test-{uuid.uuid4().hex[:8]}"
    user_id = f"user-test-{uuid.uuid4().hex[:6]}"
    movie_id = f"movie-test-{uuid.uuid4().hex[:6]}"

    payload = {
        "user_id": user_id,
        "movie_id": movie_id,
        "event_type": "VIEW_STARTED",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "device_type": "DESKTOP",
        "session_id": session_id,
        "progress_seconds": 0,
    }
    r = requests.post(f"{PRODUCER_URL}/events", json=payload, timeout=5)
    assert r.status_code == 202, r.text
    event_id = r.json()["event_id"]
    assert uuid.UUID(event_id)

    requests.post(f"{PRODUCER_URL}/events/flush", timeout=10)

    row = _poll_ch(ch, event_id)
    assert row is not None, f"event {event_id} did not land in ClickHouse"
    got_id, got_user, got_movie, got_type, got_device, got_progress = row
    assert got_id == event_id
    assert got_user == user_id
    assert got_movie == movie_id
    assert got_type == "VIEW_STARTED"
    assert got_device == "DESKTOP"
    assert got_progress == 0


def test_invalid_event_rejected():
    bad = {"user_id": "", "movie_id": "m", "event_type": "NOT_A_TYPE",
           "device_type": "DESKTOP", "session_id": "s"}
    r = requests.post(f"{PRODUCER_URL}/events", json=bad, timeout=5)
    assert r.status_code == 422


def _seed_and_wait(ch, n=60):
    """Ensure there is enough data today for the aggregator to produce non-empty metrics."""
    session_id = f"s-bulk-{uuid.uuid4().hex[:8]}"
    user = f"user-bulk-{uuid.uuid4().hex[:6]}"
    movie = f"movie-bulk-{uuid.uuid4().hex[:6]}"
    for i in range(n):
        etype = "VIEW_STARTED" if i % 2 == 0 else "VIEW_FINISHED"
        progress = 0 if etype == "VIEW_STARTED" else 600
        requests.post(f"{PRODUCER_URL}/events", json={
            "user_id": f"{user}-{i % 5}",
            "movie_id": movie,
            "event_type": etype,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "device_type": "MOBILE",
            "session_id": f"{session_id}-{i}",
            "progress_seconds": progress,
        }, timeout=5).raise_for_status()
    requests.post(f"{PRODUCER_URL}/events/flush", timeout=10)
    # Wait until at least some rows visible for today.
    for _ in range(60):
        total = ch.query(
            "SELECT count() FROM cinema.movie_events WHERE event_date = today()"
        ).result_rows[0][0]
        if total >= n:
            return
        time.sleep(1)
    raise AssertionError("seed data did not materialize in time")


def test_aggregator_writes_metrics_to_postgres(ch):
    """Block 5: recompute -> ClickHouse aggregates -> Postgres upsert (idempotent)."""
    _seed_and_wait(ch, n=40)

    today = date.today().isoformat()
    r = requests.post(f"{AGGREGATOR_URL}/recompute", json={"date": today}, timeout=60)
    assert r.status_code == 200, r.text
    # Second call must be idempotent (same PK).
    r2 = requests.post(f"{AGGREGATOR_URL}/recompute", json={"date": today}, timeout=60)
    assert r2.status_code == 200

    with psycopg.connect(PG_DSN) as conn, conn.cursor() as cur:
        cur.execute("SELECT metric_name, value FROM metrics WHERE metric_date = %s", (today,))
        rows = {name: val for name, val in cur.fetchall()}
    assert "dau" in rows and rows["dau"] >= 1
    assert "view_conversion" in rows
    assert "views_started" in rows and rows["views_started"] >= 1


def test_s3_export_creates_object(ch):
    """Block 7: export -> S3 object with all metrics for the day."""
    _seed_and_wait(ch, n=20)
    today = date.today().isoformat()
    requests.post(f"{AGGREGATOR_URL}/recompute", json={"date": today}, timeout=60).raise_for_status()
    r = requests.post(f"{AGGREGATOR_URL}/export", json={"date": today}, timeout=30)
    assert r.status_code == 200, r.text
    key = r.json()["s3_key"]

    from botocore.config import Config as _BotoConfig
    s3 = boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=os.environ.get("S3_ACCESS_KEY", "minio"),
        aws_secret_access_key=os.environ.get("S3_SECRET_KEY", "minio12345"),
        region_name="us-east-1",
        config=_BotoConfig(signature_version="s3v4", s3={"addressing_style": "path"}),
    )
    obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
    body = obj["Body"].read()
    assert len(body) > 0
    import json
    doc = json.loads(body)
    assert doc["date"] == today
    assert len(doc["metrics"]) >= 1
