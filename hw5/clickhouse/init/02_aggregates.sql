-- Materialized daily DAU aggregate in ClickHouse. This satisfies the requirement
-- "результаты материализованы в ClickHouse" and is used by Grafana's DAU panel.
CREATE TABLE IF NOT EXISTS cinema.dau_daily
(
    event_date Date,
    users      AggregateFunction(uniq, String)
)
ENGINE = AggregatingMergeTree
PARTITION BY toYYYYMM(event_date)
ORDER BY event_date;

CREATE MATERIALIZED VIEW IF NOT EXISTS cinema.dau_daily_mv
TO cinema.dau_daily AS
SELECT
    toDate(`timestamp`) AS event_date,
    uniqState(user_id)  AS users
FROM cinema.movie_events
GROUP BY event_date;

-- Daily top-movies aggregate (views per movie).
CREATE TABLE IF NOT EXISTS cinema.top_movies_daily
(
    event_date Date,
    movie_id   String,
    views      AggregateFunction(count)
)
ENGINE = AggregatingMergeTree
PARTITION BY toYYYYMM(event_date)
ORDER BY (event_date, movie_id);

CREATE MATERIALIZED VIEW IF NOT EXISTS cinema.top_movies_daily_mv
TO cinema.top_movies_daily AS
SELECT
    toDate(`timestamp`) AS event_date,
    movie_id,
    countState()        AS views
FROM cinema.movie_events
WHERE event_type = 'VIEW_STARTED'
GROUP BY event_date, movie_id;
