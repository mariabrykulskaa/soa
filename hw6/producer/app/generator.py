"""Synthetic warehouse event generator.

Maintains a small in-memory catalogue of products and zones and generates
realistic sequences of events. Runs in a background thread.
"""
from __future__ import annotations

import logging
import random
import threading
import time
import uuid
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .kafka_producer import WarehouseEventProducer

log = logging.getLogger("producer.generator")

PRODUCTS = [f"SKU-{i:03d}" for i in range(1, 11)]
ZONES = ["ZONE-A", "ZONE-B", "ZONE-C", "ZONE-D"]

# In-memory state: {(product_id, zone_id): available_qty}
_inventory: dict[tuple[str, str], int] = {}
_orders: list[str] = []  # open order ids
_lock = threading.Lock()


def _now_ms() -> int:
    return int(datetime.now(timezone.utc).timestamp() * 1000)


def _make_event(event_type: str, **kwargs: Any) -> dict[str, Any]:
    event: dict[str, Any] = {
        "event_id": str(uuid.uuid4()),
        "event_type": event_type,
        "timestamp": _now_ms(),
        "version": 1,
        "product_id": None,
        "zone_id": None,
        "from_zone_id": None,
        "to_zone_id": None,
        "quantity": None,
        "counted_quantity": None,
        "order_id": None,
        "order_items": None,
        "supplier_id": None,
    }
    event.update(kwargs)
    return event


class GeneratorWorker:
    """Background thread that periodically emits random warehouse events."""

    def __init__(self, producer: "WarehouseEventProducer", interval_ms: int) -> None:
        self._producer = producer
        self._interval = interval_ms / 1000.0
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True, name="generator")

    def start(self) -> None:
        self._thread.start()
        log.info("Generator started, interval=%.1fs", self._interval)

    def stop(self) -> None:
        self._stop.set()
        self._thread.join(timeout=5)

    # ------------------------------------------------------------------
    def _run(self) -> None:
        # Seed initial inventory
        with _lock:
            for p in PRODUCTS[:3]:
                for z in ZONES[:2]:
                    _inventory[(p, z)] = random.randint(50, 200)

        while not self._stop.is_set():
            try:
                event = self._generate_event()
                if event:
                    self._producer.produce(event)
                    log.info(
                        "Generated event_type=%s event_id=%s",
                        event["event_type"],
                        event["event_id"],
                    )
            except Exception:  # noqa: BLE001
                log.exception("Generator error")
            time.sleep(self._interval)

    # ------------------------------------------------------------------
    def _generate_event(self) -> dict[str, Any] | None:
        """Pick a random plausible event given the current inventory state."""
        choice = random.random()

        if choice < 0.25:
            # PRODUCT_RECEIVED — always possible
            product = random.choice(PRODUCTS)
            zone = random.choice(ZONES)
            qty = random.randint(10, 100)
            with _lock:
                _inventory[(product, zone)] = _inventory.get((product, zone), 0) + qty
            # 30% chance to use V2 event (with supplier_id)
            supplier = f"SUP-{random.randint(1, 5):03d}" if random.random() < 0.3 else None
            return _make_event(
                "PRODUCT_RECEIVED",
                product_id=product,
                zone_id=zone,
                quantity=qty,
                supplier_id=supplier,
            )

        elif choice < 0.45:
            # PRODUCT_SHIPPED — needs available stock
            with _lock:
                candidates = [(p, z, q) for (p, z), q in _inventory.items() if q >= 10]
            if not candidates:
                return None
            product, zone, avail = random.choice(candidates)
            qty = random.randint(1, min(avail, 30))
            with _lock:
                _inventory[(product, zone)] = max(0, _inventory.get((product, zone), 0) - qty)
            return _make_event(
                "PRODUCT_SHIPPED", product_id=product, zone_id=zone, quantity=qty
            )

        elif choice < 0.60:
            # PRODUCT_MOVED
            with _lock:
                candidates = [(p, z, q) for (p, z), q in _inventory.items() if q >= 5]
            if not candidates:
                return None
            product, from_zone, avail = random.choice(candidates)
            to_zone = random.choice([z for z in ZONES if z != from_zone])
            qty = random.randint(1, min(avail, 20))
            with _lock:
                _inventory[(product, from_zone)] = max(
                    0, _inventory.get((product, from_zone), 0) - qty
                )
                _inventory[(product, to_zone)] = _inventory.get((product, to_zone), 0) + qty
            return _make_event(
                "PRODUCT_MOVED",
                product_id=product,
                from_zone_id=from_zone,
                to_zone_id=to_zone,
                quantity=qty,
            )

        elif choice < 0.70:
            # PRODUCT_RESERVED
            with _lock:
                candidates = [(p, z, q) for (p, z), q in _inventory.items() if q >= 5]
            if not candidates:
                return None
            product, zone, avail = random.choice(candidates)
            qty = random.randint(1, min(avail, 15))
            return _make_event(
                "PRODUCT_RESERVED", product_id=product, zone_id=zone, quantity=qty
            )

        elif choice < 0.78:
            # INVENTORY_COUNTED — set absolute quantity
            product = random.choice(PRODUCTS)
            zone = random.choice(ZONES)
            qty = random.randint(0, 200)
            with _lock:
                _inventory[(product, zone)] = qty
            return _make_event(
                "INVENTORY_COUNTED",
                product_id=product,
                zone_id=zone,
                counted_quantity=qty,
            )

        elif choice < 0.88:
            # ORDER_CREATED
            order_id = f"ORD-{uuid.uuid4().hex[:8].upper()}"
            with _lock:
                candidates = [(p, z, q) for (p, z), q in _inventory.items() if q >= 5]
            if len(candidates) < 1:
                return None
            items_raw = random.sample(candidates, min(len(candidates), random.randint(1, 3)))
            items = [
                {"product_id": p, "zone_id": z, "quantity": random.randint(1, min(q, 10))}
                for p, z, q in items_raw
            ]
            with _lock:
                _orders.append(order_id)
            return _make_event("ORDER_CREATED", order_id=order_id, order_items=items)

        else:
            # ORDER_COMPLETED
            with _lock:
                if not _orders:
                    return None
                order_id = _orders.pop(0)
            return _make_event("ORDER_COMPLETED", order_id=order_id)
