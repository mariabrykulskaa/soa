"""Cassandra client: connection management, schema queries, and BATCH writes.

Design decisions:
- Consistency level for writes: QUORUM (majority of 3 nodes must acknowledge).
  Guarantees durability even if one node fails.
- Consistency level for reads: ONE (configurable via env).
  Trade-off: faster reads at the cost of possibly reading stale data.
  For inventory queries, eventual consistency is acceptable — a short lag is
  tolerable because the consumer processes events sequentially per product.
- LOGGED BATCH is used to atomically update all denormalized tables for a
  single event. If the coordinator fails mid-batch, Cassandra replays it from
  the batchlog, guaranteeing all statements eventually apply.
"""
from __future__ import annotations

import logging
import time
from datetime import datetime, timezone
from typing import Any

from cassandra import ConsistencyLevel
from cassandra.cluster import Cluster, Session
from cassandra.policies import DCAwareRoundRobinPolicy
from cassandra.query import BatchStatement, BatchType, SimpleStatement

from . import metrics as m

log = logging.getLogger("consumer.cassandra")

_CL_MAP = {
    "ONE": ConsistencyLevel.ONE,
    "QUORUM": ConsistencyLevel.QUORUM,
    "ALL": ConsistencyLevel.ALL,
    "LOCAL_QUORUM": ConsistencyLevel.LOCAL_QUORUM,
}


class CassandraClient:
    """Thin wrapper around cassandra-driver Session with pre-prepared statements."""

    def __init__(
        self,
        hosts: list[str],
        keyspace: str,
        consistency_write: str = "QUORUM",
        consistency_read: str = "ONE",
    ) -> None:
        self._hosts = hosts
        self._keyspace = keyspace
        self._cl_write = _CL_MAP.get(consistency_write.upper(), ConsistencyLevel.QUORUM)
        self._cl_read = _CL_MAP.get(consistency_read.upper(), ConsistencyLevel.ONE)
        self._cluster: Cluster | None = None
        self._session: Session | None = None
        self._prepared: dict[str, Any] = {}

    # ------------------------------------------------------------------
    def connect(self, retries: int = 30, delay: float = 5.0) -> None:
        """Connect to the cluster, retrying until ready."""
        for attempt in range(1, retries + 1):
            try:
                self._cluster = Cluster(
                    contact_points=self._hosts,
                    load_balancing_policy=DCAwareRoundRobinPolicy(local_dc="dc1"),
                    protocol_version=4,
                )
                self._session = self._cluster.connect(self._keyspace)
                self._prepare_statements()
                m.cassandra_connected.set(1)
                log.info(
                    "Connected to Cassandra keyspace=%s hosts=%s CL_write=%s CL_read=%s",
                    self._keyspace,
                    self._hosts,
                    self._cl_write,
                    self._cl_read,
                )
                return
            except Exception as exc:  # noqa: BLE001
                m.cassandra_connected.set(0)
                log.warning("Cassandra not ready (%d/%d): %s", attempt, retries, exc)
                time.sleep(delay)
        raise RuntimeError("Cannot connect to Cassandra after retries")

    # ------------------------------------------------------------------
    def is_healthy(self) -> bool:
        try:
            self._session.execute("SELECT now() FROM system.local")
            return True
        except Exception:  # noqa: BLE001
            return False

    # ------------------------------------------------------------------
    def _s(self, name: str) -> Any:
        """Return a prepared statement by name."""
        return self._prepared[name]

    # ------------------------------------------------------------------
    def _prepare_statements(self) -> None:
        sess = self._session

        def p(name: str, cql: str) -> None:
            stmt = sess.prepare(cql)
            stmt.consistency_level = self._cl_write
            self._prepared[name] = stmt

        def pr(name: str, cql: str) -> None:
            """Read-consistency prepared statement."""
            stmt = sess.prepare(cql)
            stmt.consistency_level = self._cl_read
            self._prepared[name] = stmt

        # ---- idempotency checks ----
        pr(
            "check_processed",
            "SELECT event_id FROM processed_events WHERE event_id = ?",
        )
        p(
            "insert_processed",
            "INSERT INTO processed_events (event_id, processed_at) VALUES (?, ?)",
        )

        # ---- inventory reads ----
        pr(
            "get_inv_pz",
            "SELECT available_quantity, reserved_quantity, last_event_timestamp "
            "FROM inventory_by_product_zone WHERE product_id = ? AND zone_id = ?",
        )
        pr(
            "get_inv_z",
            "SELECT available_quantity, reserved_quantity, last_event_timestamp "
            "FROM inventory_by_zone WHERE zone_id = ? AND product_id = ?",
        )

        # ---- inventory upserts (used inside BATCH) ----
        p(
            "upsert_inv_pz",
            "INSERT INTO inventory_by_product_zone "
            "(product_id, zone_id, available_quantity, reserved_quantity, "
            " last_event_timestamp, last_updated, supplier_id) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
        )
        p(
            "upsert_inv_z",
            "INSERT INTO inventory_by_zone "
            "(zone_id, product_id, available_quantity, reserved_quantity, "
            " last_event_timestamp, last_updated) "
            "VALUES (?, ?, ?, ?, ?, ?)",
        )

        # ---- event history ----
        p(
            "insert_history",
            "INSERT INTO event_history "
            "(product_id, event_timestamp, event_id, event_type, zone_id, "
            " from_zone_id, to_zone_id, quantity, order_id, details) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )

        # ---- orders ----
        pr("get_order", "SELECT status FROM orders WHERE order_id = ?")
        p(
            "upsert_order",
            "INSERT INTO orders (order_id, status, created_at, updated_at) VALUES (?, ?, ?, ?)",
        )

        # ---- order items ----
        p(
            "insert_order_item",
            "INSERT INTO order_items (order_id, product_id, zone_id, quantity) VALUES (?, ?, ?, ?)",
        )
        pr(
            "get_order_items",
            "SELECT product_id, zone_id, quantity FROM order_items WHERE order_id = ?",
        )

    # ------------------------------------------------------------------
    # Public helpers
    # ------------------------------------------------------------------

    def is_processed(self, event_id: str) -> bool:
        rows = self._session.execute(self._s("check_processed"), (event_id,))
        return rows.one() is not None

    def get_inventory_pz(
        self, product_id: str, zone_id: str
    ) -> tuple[int, int, datetime | None]:
        """Returns (available, reserved, last_event_timestamp)."""
        row = self._session.execute(self._s("get_inv_pz"), (product_id, zone_id)).one()
        if row is None:
            return 0, 0, None
        return row.available_quantity or 0, row.reserved_quantity or 0, row.last_event_timestamp

    def get_inventory_z(
        self, zone_id: str, product_id: str
    ) -> tuple[int, int, datetime | None]:
        row = self._session.execute(self._s("get_inv_z"), (zone_id, product_id)).one()
        if row is None:
            return 0, 0, None
        return row.available_quantity or 0, row.reserved_quantity or 0, row.last_event_timestamp

    def get_order_items(self, order_id: str) -> list[dict]:
        rows = self._session.execute(self._s("get_order_items"), (order_id,))
        return [{"product_id": r.product_id, "zone_id": r.zone_id, "quantity": r.quantity} for r in rows]

    def get_order_status(self, order_id: str) -> str | None:
        row = self._session.execute(self._s("get_order"), (order_id,)).one()
        return row.status if row else None

    # ------------------------------------------------------------------
    def write_inventory_batch(
        self,
        *,
        event_id: str,
        product_id: str,
        zone_id: str,
        available: int,
        reserved: int,
        event_ts: datetime,
        now: datetime,
        supplier_id: str | None = None,
        history_kwargs: dict | None = None,
    ) -> None:
        """Atomically update both denormalized inventory tables + mark event processed.

        Uses a LOGGED BATCH to guarantee all writes are eventually applied even if
        the coordinator crashes mid-batch (Cassandra replays from batchlog).
        """
        batch = BatchStatement(batch_type=BatchType.LOGGED)
        batch.consistency_level = self._cl_write

        batch.add(
            self._s("upsert_inv_pz"),
            (product_id, zone_id, available, reserved, event_ts, now, supplier_id),
        )
        batch.add(
            self._s("upsert_inv_z"),
            (zone_id, product_id, available, reserved, event_ts, now),
        )
        batch.add(self._s("insert_processed"), (event_id, now))

        if history_kwargs:
            batch.add(self._s("insert_history"), (
                history_kwargs.get("product_id", product_id),
                history_kwargs.get("event_timestamp", event_ts),
                history_kwargs.get("event_id", event_id),
                history_kwargs.get("event_type", ""),
                history_kwargs.get("zone_id"),
                history_kwargs.get("from_zone_id"),
                history_kwargs.get("to_zone_id"),
                history_kwargs.get("quantity"),
                history_kwargs.get("order_id"),
                history_kwargs.get("details"),
            ))

        try:
            self._session.execute(batch)
        except Exception as exc:
            m.cassandra_write_errors_total.labels(operation="inventory_batch").inc()
            raise RuntimeError(f"Cassandra BATCH write failed: {exc}") from exc

    # ------------------------------------------------------------------
    def write_order(self, order_id: str, status: str, items: list[dict], event_id: str) -> None:
        now = datetime.now(timezone.utc)
        batch = BatchStatement(batch_type=BatchType.LOGGED)
        batch.consistency_level = self._cl_write

        batch.add(self._s("upsert_order"), (order_id, status, now, now))
        for item in items:
            batch.add(
                self._s("insert_order_item"),
                (order_id, item["product_id"], item["zone_id"], item["quantity"]),
            )
        batch.add(self._s("insert_processed"), (event_id, now))

        try:
            self._session.execute(batch)
        except Exception as exc:
            m.cassandra_write_errors_total.labels(operation="order_batch").inc()
            raise RuntimeError(f"Cassandra order BATCH failed: {exc}") from exc

    def update_order_status(self, order_id: str, status: str, event_id: str) -> None:
        now = datetime.now(timezone.utc)
        batch = BatchStatement(batch_type=BatchType.LOGGED)
        batch.consistency_level = self._cl_write
        batch.add(self._s("upsert_order"), (order_id, status, now, now))
        batch.add(self._s("insert_processed"), (event_id, now))
        try:
            self._session.execute(batch)
        except Exception as exc:
            m.cassandra_write_errors_total.labels(operation="order_status_update").inc()
            raise RuntimeError(f"Cassandra order status update failed: {exc}") from exc

    # ------------------------------------------------------------------
    def close(self) -> None:
        if self._cluster:
            self._cluster.shutdown()
        m.cassandra_connected.set(0)
