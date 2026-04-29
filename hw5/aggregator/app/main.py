"""Aggregator service: scheduler + HTTP API (manual recompute + manual export)."""
from __future__ import annotations

import logging
import os
import time
from contextlib import asynccontextmanager
from datetime import date, datetime, timedelta, timezone
from typing import Optional

import uvicorn
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from .metrics import ch_client, compute_metrics
from .pg_sink import PgSink
from .s3_exporter import S3Config, S3Exporter

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s :: %(message)s",
)
log = logging.getLogger("aggregator")


class Settings:
    def __init__(self) -> None:
        self.ch_host = os.environ["CLICKHOUSE_HOST"]
        self.ch_port = int(os.environ.get("CLICKHOUSE_PORT", "8123"))
        self.ch_db = os.environ.get("CLICKHOUSE_DB", "cinema")
        self.pg_dsn = os.environ["PG_DSN"]
        self.aggregation_cron = os.environ.get("AGGREGATION_CRON", "*/5 * * * *")
        self.export_cron = os.environ.get("EXPORT_CRON", "5 0 * * *")
        self.s3 = S3Config(
            endpoint=os.environ["S3_ENDPOINT"],
            access_key=os.environ["S3_ACCESS_KEY"],
            secret_key=os.environ["S3_SECRET_KEY"],
            bucket=os.environ["S3_BUCKET"],
            prefix=os.environ.get("S3_PREFIX", "daily"),
        )


settings: Settings
pg: PgSink
s3: S3Exporter
scheduler: BackgroundScheduler


def _today_utc() -> date:
    return datetime.now(timezone.utc).date()


def run_aggregation(target: date) -> dict:
    log.info("aggregation cycle start target=%s", target)
    t0 = time.perf_counter()
    ch = ch_client(settings.ch_host, settings.ch_port, settings.ch_db)
    run_id = pg.log_run_start(target)
    try:
        metric_rows, cohort_rows = compute_metrics(ch, target)
        n_m = pg.upsert_metrics(metric_rows)
        n_c = pg.upsert_cohort(cohort_rows)
        total = n_m + n_c
        pg.log_run_finish(run_id, total, "ok")
        elapsed = time.perf_counter() - t0
        log.info(
            "aggregation cycle done target=%s metrics=%s cohort=%s took=%.2fs",
            target, n_m, n_c, elapsed,
        )
        return {"target_date": target.isoformat(), "metrics": n_m, "cohort_rows": n_c, "elapsed_sec": elapsed}
    except Exception as e:  # noqa: BLE001
        pg.log_run_finish(run_id, 0, "failed", error=str(e))
        log.exception("aggregation cycle failed target=%s", target)
        raise
    finally:
        ch.close()


def run_export(target: date) -> dict:
    log.info("export start target=%s", target)
    metrics = pg.fetch_metrics_for_date(target)
    cohort = pg.fetch_cohort_for_date(target)
    key = s3.export(target, metrics, cohort)
    log.info("export done target=%s key=%s metrics=%s cohort=%s", target, key, len(metrics), len(cohort))
    return {"target_date": target.isoformat(), "s3_key": key, "metrics": len(metrics), "cohort_rows": len(cohort)}


@asynccontextmanager
async def lifespan(app: FastAPI):  # noqa: ARG001
    global settings, pg, s3, scheduler
    settings = Settings()
    pg = PgSink(settings.pg_dsn)
    s3 = S3Exporter(settings.s3)
    scheduler = BackgroundScheduler(timezone="UTC")

    scheduler.add_job(
        lambda: run_aggregation(_today_utc()),
        CronTrigger.from_crontab(settings.aggregation_cron, timezone="UTC"),
        id="aggregate_today",
        max_instances=1,
        coalesce=True,
    )
    scheduler.add_job(
        lambda: run_export(_today_utc() - timedelta(days=1)),
        CronTrigger.from_crontab(settings.export_cron, timezone="UTC"),
        id="export_yesterday",
        max_instances=1,
        coalesce=True,
    )
    scheduler.start()
    log.info("scheduler started: aggregation='%s' export='%s'",
             settings.aggregation_cron, settings.export_cron)
    try:
        yield
    finally:
        scheduler.shutdown(wait=False)


app = FastAPI(title="cinema aggregator", lifespan=lifespan)


class DateBody(BaseModel):
    date: Optional[date] = None


@app.get("/health")
def health() -> dict:
    return {"status": "ok"}


@app.post("/recompute")
def recompute(body: DateBody) -> dict:
    target = body.date or _today_utc()
    try:
        return run_aggregation(target)
    except Exception as e:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(e)) from e


@app.post("/export")
def export(body: DateBody) -> dict:
    target = body.date or (_today_utc() - timedelta(days=1))
    try:
        return run_export(target)
    except Exception as e:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(e)) from e


@app.get("/metrics/{target_date}")
def metrics_by_date(target_date: date) -> dict:
    return {
        "metrics": pg.fetch_metrics_for_date(target_date),
        "cohort": pg.fetch_cohort_for_date(target_date),
    }


if __name__ == "__main__":
    uvicorn.run("app.main:app", host="0.0.0.0", port=8010, log_level="info")
