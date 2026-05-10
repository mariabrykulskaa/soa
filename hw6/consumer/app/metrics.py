"""Prometheus metrics for the warehouse consumer."""
from __future__ import annotations

from prometheus_client import Counter, Gauge, Histogram

# ---------------------------------------------------------------------------
# Counters
# ---------------------------------------------------------------------------
events_processed_total = Counter(
    "events_processed_total",
    "Total number of successfully processed warehouse events.",
    ["event_type"],
)

events_skipped_total = Counter(
    "events_skipped_total",
    "Total number of skipped events (duplicate or out-of-order).",
    ["reason"],
)

dlq_sent_total = Counter(
    "dlq_sent_total",
    "Total number of events sent to the Dead Letter Queue.",
    ["error_code"],
)

cassandra_write_errors_total = Counter(
    "cassandra_write_errors_total",
    "Total number of errors during Cassandra write operations.",
    ["operation"],
)

# ---------------------------------------------------------------------------
# Gauges
# ---------------------------------------------------------------------------
consumer_lag = Gauge(
    "consumer_lag",
    "Current consumer lag (latest_offset - committed_offset) per partition.",
    ["topic", "partition"],
)

cassandra_connected = Gauge(
    "cassandra_connected",
    "1 if consumer is connected to Cassandra, 0 otherwise.",
)

kafka_connected = Gauge(
    "kafka_connected",
    "1 if consumer is connected to Kafka, 0 otherwise.",
)

# ---------------------------------------------------------------------------
# Histograms
# ---------------------------------------------------------------------------
event_processing_duration_seconds = Histogram(
    "event_processing_duration_seconds",
    "Time spent processing a single warehouse event (end-to-end, including Cassandra write).",
    ["event_type"],
    buckets=(0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5),
)
