"""S3/MinIO exporter: dumps daily aggregates as JSON and uploads under daily/YYYY-MM-DD/."""
from __future__ import annotations

import io
import json
import logging
import time
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal

import boto3
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError

log = logging.getLogger("aggregator.s3")


@dataclass
class S3Config:
    endpoint: str
    access_key: str
    secret_key: str
    bucket: str
    prefix: str = "daily"


def _json_default(o):  # noqa: ANN001
    if isinstance(o, (datetime, date)):
        return o.isoformat()
    if isinstance(o, Decimal):
        return float(o)
    raise TypeError(f"unserializable: {type(o).__name__}")


class S3Exporter:
    def __init__(self, cfg: S3Config) -> None:
        self.cfg = cfg
        self._client = boto3.client(
            "s3",
            endpoint_url=cfg.endpoint,
            aws_access_key_id=cfg.access_key,
            aws_secret_access_key=cfg.secret_key,
            config=Config(
                signature_version="s3v4",
                s3={"addressing_style": "path"},
                retries={"max_attempts": 3, "mode": "standard"},
            ),
            region_name="us-east-1",
        )

    def export(self, target: date, metrics: list[dict], cohort: list[dict]) -> str:
        payload = {
            "date": target.isoformat(),
            "exported_at": datetime.utcnow().isoformat() + "Z",
            "metrics": metrics,
            "retention_cohort": cohort,
        }
        body = json.dumps(payload, default=_json_default, ensure_ascii=False, indent=2).encode("utf-8")
        key = f"{self.cfg.prefix}/{target.isoformat()}/aggregates.json"
        last_err: Exception | None = None
        for attempt in range(5):
            try:
                self._client.put_object(
                    Bucket=self.cfg.bucket,
                    Key=key,
                    Body=io.BytesIO(body),
                    ContentType="application/json",
                )
                log.info("exported %s bytes to s3://%s/%s", len(body), self.cfg.bucket, key)
                return key
            except (BotoCoreError, ClientError) as e:
                last_err = e
                delay = 0.5 * (2 ** attempt)
                log.warning("s3 put failed (attempt=%s): %s — retrying in %.1fs", attempt + 1, e, delay)
                time.sleep(delay)
        raise RuntimeError(f"s3 export failed after retries: {last_err}")
