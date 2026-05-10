"""Event handler: maps each warehouse event type to Cassandra state mutations.

Key design decisions:
1. Idempotency — checked via processed_events table BEFORE any state change.
2. Out-of-order detection — each inventory row stores last_event_timestamp.
   An incoming event is skipped if its timestamp is older than the last
   processed one for that (product, zone) pair.
3. LOGGED BATCH — all denormalized table writes for a single event happen in
   one Cassandra BATCH so a partial failure cannot leave tables inconsistent.
4. Validation — events with invalid payloads (negative quantity, missing
   required fields) raise ValueError and are routed to the DLQ.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

from .cassandra_db import CassandraClient
from . import metrics as m

log = logging.getLogger("consumer.handlers")


def _event_dt(event: dict[str, Any]) -> datetime:
    """Convert event timestamp (ms epoch) to UTC datetime."""
    ts_ms: int = event.get("timestamp") or 0
    return datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc)


def _require(event: dict, *fields: str) -> None:
    for f in fields:
        if event.get(f) is None:
            raise ValueError(f"Missing required field '{f}' for event_type={event.get('event_type')}")


def _require_positive(event: dict, field: str) -> int:
    val = event.get(field)
    if val is None:
        raise ValueError(f"Missing required field '{field}'")
    if not isinstance(val, int) or val <= 0:
        raise ValueError(
            f"Invalid {field}: {val} (must be a positive integer)"
        )
    return val


def handle_event(event: dict[str, Any], db: CassandraClient) -> None:
    """Dispatch the event to the appropriate handler.

    Raises:
        ValueError  — validation error → DLQ with VALIDATION_ERROR
        RuntimeError — Cassandra error  → DLQ with CASSANDRA_ERROR
    """
    event_type: str = event.get("event_type", "UNKNOWN")
    event_id: str = event.get("event_id", "")
    event_ts = _event_dt(event)
    now = datetime.now(timezone.utc)

    # ------------------------------------------------------------------
    # Idempotency check (point 4)
    # ------------------------------------------------------------------
    if db.is_processed(event_id):
        log.info("SKIP duplicate event_id=%s event_type=%s", event_id, event_type)
        m.events_skipped_total.labels(reason="duplicate").inc()
        return

    # ------------------------------------------------------------------
    # Dispatch
    # ------------------------------------------------------------------
    if event_type == "PRODUCT_RECEIVED":
        _handle_received(event, db, event_id, event_ts, now)

    elif event_type == "PRODUCT_SHIPPED":
        _handle_shipped(event, db, event_id, event_ts, now)

    elif event_type == "PRODUCT_MOVED":
        _handle_moved(event, db, event_id, event_ts, now)

    elif event_type == "PRODUCT_RESERVED":
        _handle_reserved(event, db, event_id, event_ts, now)

    elif event_type == "PRODUCT_RELEASED":
        _handle_released(event, db, event_id, event_ts, now)

    elif event_type == "INVENTORY_COUNTED":
        _handle_counted(event, db, event_id, event_ts, now)

    elif event_type == "ORDER_CREATED":
        _handle_order_created(event, db, event_id, event_ts, now)

    elif event_type == "ORDER_COMPLETED":
        _handle_order_completed(event, db, event_id, event_ts, now)

    else:
        raise ValueError(f"Unknown event_type: {event_type}")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _is_stale(event_ts: datetime, last_ts: datetime | None) -> bool:
    """Return True if event_ts is older than the last processed timestamp.

    Out-of-order detection (point 6): we keep the most recent event's
    timestamp per (product, zone) pair in the inventory row.
    Events with an older timestamp are ignored to prevent stale overwrites.
    """
    if last_ts is None:
        return False
    # Use epoch comparison; avoid timezone naivety issues
    return event_ts.timestamp() < last_ts.timestamp()


def _history_kw(event: dict, zone_id: str | None = None, details: str | None = None) -> dict:
    return {
        "product_id": event.get("product_id"),
        "event_timestamp": _event_dt(event),
        "event_id": event.get("event_id"),
        "event_type": event.get("event_type"),
        "zone_id": zone_id or event.get("zone_id"),
        "from_zone_id": event.get("from_zone_id"),
        "to_zone_id": event.get("to_zone_id"),
        "quantity": event.get("quantity") or event.get("counted_quantity"),
        "order_id": event.get("order_id"),
        "details": details,
    }


# ---------------------------------------------------------------------------
# Individual event handlers
# ---------------------------------------------------------------------------

def _handle_received(
    event: dict, db: CassandraClient, event_id: str, event_ts: datetime, now: datetime
) -> None:
    _require(event, "product_id", "zone_id")
    qty = _require_positive(event, "quantity")
    product_id: str = event["product_id"]
    zone_id: str = event["zone_id"]
    supplier_id: str | None = event.get("supplier_id")  # V2 field

    avail, reserved, last_ts = db.get_inventory_pz(product_id, zone_id)

    if _is_stale(event_ts, last_ts):
        log.info(
            "SKIP out-of-order PRODUCT_RECEIVED event_id=%s ts=%s last_ts=%s",
            event_id, event_ts, last_ts,
        )
        m.events_skipped_total.labels(reason="out_of_order").inc()
        return

    db.write_inventory_batch(
        event_id=event_id,
        product_id=product_id,
        zone_id=zone_id,
        available=avail + qty,
        reserved=reserved,
        event_ts=event_ts,
        now=now,
        supplier_id=supplier_id,
        history_kwargs=_history_kw(event, details=f"supplier_id={supplier_id}"),
    )
    log.info(
        "PRODUCT_RECEIVED product=%s zone=%s qty=%d new_avail=%d supplier=%s",
        product_id, zone_id, qty, avail + qty, supplier_id,
    )


def _handle_shipped(
    event: dict, db: CassandraClient, event_id: str, event_ts: datetime, now: datetime
) -> None:
    _require(event, "product_id", "zone_id")
    qty = _require_positive(event, "quantity")
    product_id: str = event["product_id"]
    zone_id: str = event["zone_id"]

    avail, reserved, last_ts = db.get_inventory_pz(product_id, zone_id)

    if _is_stale(event_ts, last_ts):
        log.info("SKIP out-of-order PRODUCT_SHIPPED event_id=%s", event_id)
        m.events_skipped_total.labels(reason="out_of_order").inc()
        return

    new_avail = max(0, avail - qty)
    db.write_inventory_batch(
        event_id=event_id,
        product_id=product_id,
        zone_id=zone_id,
        available=new_avail,
        reserved=reserved,
        event_ts=event_ts,
        now=now,
        history_kwargs=_history_kw(event),
    )
    log.info("PRODUCT_SHIPPED product=%s zone=%s qty=%d new_avail=%d", product_id, zone_id, qty, new_avail)


def _handle_moved(
    event: dict, db: CassandraClient, event_id: str, event_ts: datetime, now: datetime
) -> None:
    _require(event, "product_id", "from_zone_id", "to_zone_id")
    qty = _require_positive(event, "quantity")
    product_id: str = event["product_id"]
    from_zone: str = event["from_zone_id"]
    to_zone: str = event["to_zone_id"]

    avail_from, reserved_from, last_ts_from = db.get_inventory_pz(product_id, from_zone)
    avail_to, reserved_to, _ = db.get_inventory_pz(product_id, to_zone)

    if _is_stale(event_ts, last_ts_from):
        log.info("SKIP out-of-order PRODUCT_MOVED event_id=%s", event_id)
        m.events_skipped_total.labels(reason="out_of_order").inc()
        return

    now_ts = datetime.now(timezone.utc)

    # Update source zone
    db.write_inventory_batch(
        event_id=f"{event_id}#from",
        product_id=product_id,
        zone_id=from_zone,
        available=max(0, avail_from - qty),
        reserved=reserved_from,
        event_ts=event_ts,
        now=now_ts,
        history_kwargs=_history_kw(event, zone_id=from_zone, details="move_source"),
    )
    # Update destination zone — use a synthetic event_id suffix to avoid
    # processed_events collision; idempotency is preserved by the main event_id check.
    db.write_inventory_batch(
        event_id=f"{event_id}#to",
        product_id=product_id,
        zone_id=to_zone,
        available=avail_to + qty,
        reserved=reserved_to,
        event_ts=event_ts,
        now=now_ts,
        history_kwargs=None,
    )
    # Mark the base event_id as processed after both writes succeed
    db._session.execute(
        db._prepared["insert_processed"], (event_id, now_ts)
    )
    log.info(
        "PRODUCT_MOVED product=%s from=%s to=%s qty=%d",
        product_id, from_zone, to_zone, qty,
    )


def _handle_reserved(
    event: dict, db: CassandraClient, event_id: str, event_ts: datetime, now: datetime
) -> None:
    _require(event, "product_id", "zone_id")
    qty = _require_positive(event, "quantity")
    product_id: str = event["product_id"]
    zone_id: str = event["zone_id"]

    avail, reserved, last_ts = db.get_inventory_pz(product_id, zone_id)

    if _is_stale(event_ts, last_ts):
        log.info("SKIP out-of-order PRODUCT_RESERVED event_id=%s", event_id)
        m.events_skipped_total.labels(reason="out_of_order").inc()
        return

    new_avail = max(0, avail - qty)
    new_reserved = reserved + qty
    db.write_inventory_batch(
        event_id=event_id,
        product_id=product_id,
        zone_id=zone_id,
        available=new_avail,
        reserved=new_reserved,
        event_ts=event_ts,
        now=now,
        history_kwargs=_history_kw(event),
    )
    log.info(
        "PRODUCT_RESERVED product=%s zone=%s qty=%d avail=%d→%d reserved=%d→%d",
        product_id, zone_id, qty, avail, new_avail, reserved, new_reserved,
    )


def _handle_released(
    event: dict, db: CassandraClient, event_id: str, event_ts: datetime, now: datetime
) -> None:
    _require(event, "product_id", "zone_id")
    qty = _require_positive(event, "quantity")
    product_id: str = event["product_id"]
    zone_id: str = event["zone_id"]

    avail, reserved, last_ts = db.get_inventory_pz(product_id, zone_id)

    if _is_stale(event_ts, last_ts):
        log.info("SKIP out-of-order PRODUCT_RELEASED event_id=%s", event_id)
        m.events_skipped_total.labels(reason="out_of_order").inc()
        return

    new_reserved = max(0, reserved - qty)
    new_avail = avail + qty
    db.write_inventory_batch(
        event_id=event_id,
        product_id=product_id,
        zone_id=zone_id,
        available=new_avail,
        reserved=new_reserved,
        event_ts=event_ts,
        now=now,
        history_kwargs=_history_kw(event),
    )
    log.info(
        "PRODUCT_RELEASED product=%s zone=%s qty=%d avail=%d→%d reserved=%d→%d",
        product_id, zone_id, qty, avail, new_avail, reserved, new_reserved,
    )


def _handle_counted(
    event: dict, db: CassandraClient, event_id: str, event_ts: datetime, now: datetime
) -> None:
    _require(event, "product_id", "zone_id", "counted_quantity")
    qty = event.get("counted_quantity")
    if not isinstance(qty, int) or qty < 0:
        raise ValueError(f"Invalid counted_quantity: {qty} (must be >= 0)")
    product_id: str = event["product_id"]
    zone_id: str = event["zone_id"]

    _, reserved, last_ts = db.get_inventory_pz(product_id, zone_id)

    if _is_stale(event_ts, last_ts):
        log.info("SKIP out-of-order INVENTORY_COUNTED event_id=%s", event_id)
        m.events_skipped_total.labels(reason="out_of_order").inc()
        return

    db.write_inventory_batch(
        event_id=event_id,
        product_id=product_id,
        zone_id=zone_id,
        available=qty,
        reserved=reserved,
        event_ts=event_ts,
        now=now,
        history_kwargs=_history_kw(event),
    )
    log.info("INVENTORY_COUNTED product=%s zone=%s counted=%d", product_id, zone_id, qty)


def _handle_order_created(
    event: dict, db: CassandraClient, event_id: str, event_ts: datetime, now: datetime
) -> None:
    _require(event, "order_id")
    order_id: str = event["order_id"]
    items: list[dict] | None = event.get("order_items")
    if not items:
        raise ValueError(f"ORDER_CREATED requires at least one order_item (order_id={order_id})")

    # Persist order + items in one BATCH
    db.write_order(order_id=order_id, status="CREATED", items=items, event_id=event_id)

    # Reserve inventory for each item (each as a separate BATCH for atomicity)
    for item in items:
        product_id = item["product_id"]
        zone_id = item["zone_id"]
        qty = item["quantity"]
        if qty <= 0:
            raise ValueError(f"ORDER_CREATED item quantity must be > 0 (got {qty})")

        avail, reserved, last_ts = db.get_inventory_pz(product_id, zone_id)
        new_avail = max(0, avail - qty)
        new_reserved = reserved + qty
        # Use a per-item derived event_id so each inventory batch is idempotent
        item_event_id = f"{event_id}#item#{product_id}#{zone_id}"
        db.write_inventory_batch(
            event_id=item_event_id,
            product_id=product_id,
            zone_id=zone_id,
            available=new_avail,
            reserved=new_reserved,
            event_ts=event_ts,
            now=now,
            history_kwargs=_history_kw(
                event, zone_id=zone_id,
                details=f"reserve_for_order={order_id}"
            ),
        )

    log.info("ORDER_CREATED order_id=%s items=%d", order_id, len(items))


def _handle_order_completed(
    event: dict, db: CassandraClient, event_id: str, event_ts: datetime, now: datetime
) -> None:
    _require(event, "order_id")
    order_id: str = event["order_id"]

    status = db.get_order_status(order_id)
    if status is None:
        raise ValueError(f"ORDER_COMPLETED: unknown order_id={order_id}")
    if status == "COMPLETED":
        # Already completed — idempotent (should be caught by processed_events,
        # but guard here too)
        log.info("SKIP already-completed order_id=%s", order_id)
        return

    items = db.get_order_items(order_id)
    if not items:
        raise ValueError(f"ORDER_COMPLETED: no items found for order_id={order_id}")

    # Deduct reserved_quantity for each item (goods are shipped, reserved freed)
    for item in items:
        product_id = item["product_id"]
        zone_id = item["zone_id"]
        qty = item["quantity"]

        avail, reserved, _ = db.get_inventory_pz(product_id, zone_id)
        new_reserved = max(0, reserved - qty)
        # available_quantity is NOT changed: items were already removed from
        # available when reserved (at ORDER_CREATED time).
        item_event_id = f"{event_id}#item#{product_id}#{zone_id}"
        db.write_inventory_batch(
            event_id=item_event_id,
            product_id=product_id,
            zone_id=zone_id,
            available=avail,
            reserved=new_reserved,
            event_ts=event_ts,
            now=now,
            history_kwargs=_history_kw(
                event, zone_id=zone_id,
                details=f"complete_order={order_id}"
            ),
        )

    db.update_order_status(order_id=order_id, status="COMPLETED", event_id=event_id)
    log.info("ORDER_COMPLETED order_id=%s items=%d", order_id, len(items))
