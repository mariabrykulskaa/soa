-- Runs once on first ClickHouse container startup.
CREATE DATABASE IF NOT EXISTS cinema;

-- Kafka source table. Uses AvroConfluent because producer serializes values with the Confluent
-- Schema Registry. ClickHouse looks up the schema by the magic byte/schema-id in each message.
CREATE TABLE IF NOT EXISTS cinema.movie_events_kafka
(
    event_id         String,
    user_id          String,
    movie_id         String,
    event_type       LowCardinality(String),
    `timestamp`      DateTime64(3, 'UTC'),
    device_type      LowCardinality(String),
    session_id       String,
    progress_seconds Int32
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka1:29092,kafka2:29092',
    kafka_topic_list = 'movie-events',
    kafka_group_name = 'clickhouse-movie-events',
    kafka_format = 'AvroConfluent',
    kafka_num_consumers = 1,
    kafka_thread_per_consumer = 0,
    kafka_max_block_size = 1048576,
    format_avro_schema_registry_url = 'http://schema-registry:8081';

-- Permanent raw event storage. Partition by day, primary key optimized for time-series queries
-- and aggregate-by-event-type lookups used by the retention / DAU / top-movie queries.
CREATE TABLE IF NOT EXISTS cinema.movie_events
(
    event_id         String,
    user_id          String,
    movie_id         String,
    event_type       LowCardinality(String),
    `timestamp`      DateTime64(3, 'UTC'),
    device_type      LowCardinality(String),
    session_id       String,
    progress_seconds Int32,
    event_date       Date MATERIALIZED toDate(`timestamp`)
)
ENGINE = MergeTree
PARTITION BY event_date
ORDER BY (event_date, event_type, user_id, `timestamp`)
TTL event_date + INTERVAL 180 DAY;

-- Pump messages from Kafka into MergeTree.
CREATE MATERIALIZED VIEW IF NOT EXISTS cinema.movie_events_mv
TO cinema.movie_events AS
SELECT
    event_id,
    user_id,
    movie_id,
    event_type,
    `timestamp`,
    device_type,
    session_id,
    progress_seconds
FROM cinema.movie_events_kafka;
