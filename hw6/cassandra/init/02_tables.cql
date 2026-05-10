USE warehouse;

-- =============================================================================
-- Table 1: inventory_by_product_zone
--   Partition key : product_id  — all data for one product on one node (efficient per-product scan)
--   Clustering key: zone_id     — rows ordered by zone; allows range scans
--
-- Supported queries:
--   SELECT * WHERE product_id = ? AND zone_id = ?   (exact zone lookup)
--   SELECT * WHERE product_id = ?                   (all zones for a product)
-- =============================================================================
CREATE TABLE IF NOT EXISTS inventory_by_product_zone (
    product_id            TEXT,
    zone_id               TEXT,
    available_quantity    INT,
    reserved_quantity     INT,
    last_event_timestamp  TIMESTAMP,   -- for out-of-order detection
    last_updated          TIMESTAMP,
    supplier_id           TEXT,        -- V2 schema evolution field (null for V1 events)
    PRIMARY KEY (product_id, zone_id)
) WITH CLUSTERING ORDER BY (zone_id ASC)
   AND comment = 'Inventory per product per zone. Partition by product_id for efficient product-level queries.';

-- =============================================================================
-- Table 2: inventory_by_zone
--   Partition key : zone_id    — all data for one zone on one node (efficient zone scan)
--   Clustering key: product_id — rows ordered by product
--
-- Supported queries:
--   SELECT * WHERE zone_id = ?                      (all products in a zone)
--   SELECT * WHERE zone_id = ? AND product_id = ?   (specific product in zone)
-- =============================================================================
CREATE TABLE IF NOT EXISTS inventory_by_zone (
    zone_id               TEXT,
    product_id            TEXT,
    available_quantity    INT,
    reserved_quantity     INT,
    last_event_timestamp  TIMESTAMP,
    last_updated          TIMESTAMP,
    PRIMARY KEY (zone_id, product_id)
) WITH CLUSTERING ORDER BY (product_id ASC)
   AND comment = 'Inventory per zone per product. Partition by zone_id for efficient zone-level queries.';

-- =============================================================================
-- Table 3: event_history  (audit / history)
--   Partition key : product_id         — all events for a product on one node
--   Clustering keys: event_timestamp DESC, event_id ASC
--                     — ordered newest-first; event_id breaks timestamp ties
--
-- Supported queries:
--   SELECT * WHERE product_id = ?                              (full history)
--   SELECT * WHERE product_id = ? ORDER BY event_timestamp DESC LIMIT 10  (recent events)
-- =============================================================================
CREATE TABLE IF NOT EXISTS event_history (
    product_id       TEXT,
    event_timestamp  TIMESTAMP,
    event_id         TEXT,
    event_type       TEXT,
    zone_id          TEXT,
    from_zone_id     TEXT,
    to_zone_id       TEXT,
    quantity         INT,
    order_id         TEXT,
    details          TEXT,
    PRIMARY KEY (product_id, event_timestamp, event_id)
) WITH CLUSTERING ORDER BY (event_timestamp DESC, event_id ASC)
   AND default_time_to_live = 2592000  -- 30 days TTL for history
   AND comment = 'Append-only audit log of all events per product.';

-- =============================================================================
-- Table 4: orders
--   Partition key: order_id — single-row partition for fast order lookup
--
-- Supported queries:
--   SELECT * WHERE order_id = ?
-- =============================================================================
CREATE TABLE IF NOT EXISTS orders (
    order_id    TEXT PRIMARY KEY,
    status      TEXT,
    created_at  TIMESTAMP,
    updated_at  TIMESTAMP
) WITH comment = 'Order state. Simple lookup by order_id.';

-- =============================================================================
-- Table 5: order_items
--   Partition key : order_id   — all items of an order on one node
--   Clustering key: product_id — individual item lookup
--
-- Supported queries:
--   SELECT * WHERE order_id = ?
--   SELECT * WHERE order_id = ? AND product_id = ?
-- =============================================================================
CREATE TABLE IF NOT EXISTS order_items (
    order_id    TEXT,
    product_id  TEXT,
    zone_id     TEXT,
    quantity    INT,
    PRIMARY KEY (order_id, product_id)
) WITH CLUSTERING ORDER BY (product_id ASC)
   AND comment = 'Order line items. Needed for ORDER_COMPLETED to know which products/zones to deduct.';

-- =============================================================================
-- Table 6: processed_events  (idempotency guard)
--   Partition key: event_id — O(1) dedup check
--   TTL: 7 days (keeps memory bounded; duplicate events are rare beyond a few minutes)
--
-- Supported queries:
--   SELECT event_id WHERE event_id = ?   (check if already processed)
-- =============================================================================
CREATE TABLE IF NOT EXISTS processed_events (
    event_id      TEXT PRIMARY KEY,
    processed_at  TIMESTAMP
) WITH default_time_to_live = 604800  -- 7 days
   AND comment = 'Idempotency store. Prevents duplicate processing of the same event_id.';
