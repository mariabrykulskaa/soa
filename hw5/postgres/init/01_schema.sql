-- Store of computed business metrics. Idempotent upsert via composite PK.
CREATE TABLE IF NOT EXISTS metrics (
    metric_date  DATE             NOT NULL,
    metric_name  VARCHAR(64)      NOT NULL,
    dimension    VARCHAR(128)     NOT NULL DEFAULT '',
    value        DOUBLE PRECISION NOT NULL,
    computed_at  TIMESTAMPTZ      NOT NULL DEFAULT now(),
    PRIMARY KEY (metric_date, metric_name, dimension)
);

CREATE INDEX IF NOT EXISTS metrics_name_date_idx ON metrics (metric_name, metric_date DESC);

-- Retention cohort: day of first activity -> day_n -> share.
CREATE TABLE IF NOT EXISTS retention_cohort (
    cohort_date  DATE             NOT NULL,
    day_n        SMALLINT         NOT NULL,
    users        INTEGER          NOT NULL,
    share        DOUBLE PRECISION NOT NULL,
    computed_at  TIMESTAMPTZ      NOT NULL DEFAULT now(),
    PRIMARY KEY (cohort_date, day_n)
);

-- Log of aggregation runs for observability / debugging.
CREATE TABLE IF NOT EXISTS aggregation_runs (
    run_id        BIGSERIAL PRIMARY KEY,
    started_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at   TIMESTAMPTZ,
    target_date   DATE,
    records_out   INTEGER,
    status        VARCHAR(32),
    error_message TEXT
);
