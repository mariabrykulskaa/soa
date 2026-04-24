"""PostgreSQL sink: idempotent upserts + aggregation-run log."""
from __future__ import annotations

import logging
import time
from contextlib import contextmanager
from typing import Iterable

import psycopg
from psycopg import sql

from .metrics import CohortRow, MetricRow

log = logging.getLogger("aggregator.pg")


class PgSink:
    def __init__(self, dsn: str) -> None:
        self.dsn = dsn

    @contextmanager
    def _conn(self):
        last_exc: Exception | None = None
        for attempt in range(5):
            try:
                with psycopg.connect(self.dsn, autocommit=False) as conn:
                    yield conn
                    return
            except psycopg.OperationalError as e:
                last_exc = e
                delay = 0.5 * (2 ** attempt)
                log.warning("pg connect failed (attempt=%s), retrying in %.1fs: %s", attempt + 1, delay, e)
                time.sleep(delay)
        raise RuntimeError(f"postgres unreachable: {last_exc}")

    def upsert_metrics(self, rows: Iterable[MetricRow]) -> int:
        rows = list(rows)
        if not rows:
            return 0
        q = """
        INSERT INTO metrics (metric_date, metric_name, dimension, value, computed_at)
        VALUES (%s, %s, %s, %s, now())
        ON CONFLICT (metric_date, metric_name, dimension)
        DO UPDATE SET value = EXCLUDED.value, computed_at = EXCLUDED.computed_at
        """
        with self._conn() as conn, conn.cursor() as cur:
            cur.executemany(q, [(r.metric_date, r.metric_name, r.dimension, r.value) for r in rows])
            conn.commit()
        return len(rows)

    def upsert_cohort(self, rows: Iterable[CohortRow]) -> int:
        rows = list(rows)
        if not rows:
            return 0
        q = """
        INSERT INTO retention_cohort (cohort_date, day_n, users, share, computed_at)
        VALUES (%s, %s, %s, %s, now())
        ON CONFLICT (cohort_date, day_n)
        DO UPDATE SET users = EXCLUDED.users,
                      share = EXCLUDED.share,
                      computed_at = EXCLUDED.computed_at
        """
        with self._conn() as conn, conn.cursor() as cur:
            cur.executemany(q, [(r.cohort_date, r.day_n, r.users, r.share) for r in rows])
            conn.commit()
        return len(rows)

    def log_run_start(self, target_date) -> int:
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute(
                "INSERT INTO aggregation_runs (target_date, status) VALUES (%s, 'running') RETURNING run_id",
                (target_date,),
            )
            row = cur.fetchone()
            conn.commit()
            return int(row[0])

    def log_run_finish(self, run_id: int, records: int, status: str, error: str | None = None) -> None:
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute(
                """UPDATE aggregation_runs
                   SET finished_at = now(), records_out = %s, status = %s, error_message = %s
                   WHERE run_id = %s""",
                (records, status, error, run_id),
            )
            conn.commit()

    def fetch_metrics_for_date(self, target_date) -> list[dict]:
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute(
                """SELECT metric_date, metric_name, dimension, value, computed_at
                   FROM metrics WHERE metric_date = %s
                   ORDER BY metric_name, dimension""",
                (target_date,),
            )
            cols = [c.name for c in cur.description]
            return [dict(zip(cols, row)) for row in cur.fetchall()]

    def fetch_cohort_for_date(self, target_date) -> list[dict]:
        """Fetch cohorts up to 14 days back anchored around target_date."""
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute(
                """SELECT cohort_date, day_n, users, share, computed_at
                   FROM retention_cohort
                   WHERE cohort_date BETWEEN %s::date - INTERVAL '14 days' AND %s::date
                   ORDER BY cohort_date, day_n""",
                (target_date, target_date),
            )
            cols = [c.name for c in cur.description]
            return [dict(zip(cols, row)) for row in cur.fetchall()]
