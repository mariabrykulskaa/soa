# Smart Warehouse: Event-Driven State Management with Cassandra

Система управления складом на базе Kafka + Cassandra с полной поддержкой event-driven архитектуры.

## Быстрый старт

```bash
cd hw6
docker-compose up --build
```

Все сервисы поднимаются одной командой. Cassandra-кластер инициализируется автоматически (~3–5 минут из-за поочерёдного старта нод).

Сервисы:

| Сервис | URL |
|---|---|
| Producer API | http://localhost:8080 |
| Consumer /health | http://localhost:8082/health |
| Consumer /metrics | http://localhost:8082/metrics |
| Schema Registry | http://localhost:8081 |
| Prometheus | http://localhost:9090 |
| Grafana | http://localhost:3000 (admin/admin) |
| Cassandra | localhost:9042 |

---

## Архитектура

```
WMS Producer ──► Kafka (warehouse-events) ──► Consumer ──► Cassandra (3 нод)
                        │
                        └──► DLQ (warehouse-events-dlq)
                
Consumer /metrics ──► Prometheus ──► Grafana
```

### Компоненты

- **Producer** — FastAPI-сервис, генерирует события склада автоматически и принимает ручные POST-запросы
- **Consumer** — читает события из Kafka, обновляет состояние в Cassandra, публикует проблемные события в DLQ
- **Cassandra** — 3-нодовый кластер (cassandra-1, cassandra-2, cassandra-3) с RF=3
- **Schema Registry** — Confluent Schema Registry, хранит V1 и V2 Avro-схем событий
- **Prometheus + Grafana** — мониторинг consumer-сервиса

---

## Модель данных Cassandra

### Обоснование выбора ключей

#### `inventory_by_product_zone`
```cql
PRIMARY KEY (product_id, zone_id)
```
- **Partition key `product_id`**: все данные по одному товару хранятся на одной ноде → эффективный скан по товару
- **Clustering key `zone_id`**: строки отсортированы по зоне → эффективный поиск по конкретной зоне
- Поддерживает запросы:
  - `SELECT * WHERE product_id = ? AND zone_id = ?` — остаток в конкретной зоне
  - `SELECT * WHERE product_id = ?` — все зоны для товара (агрегированный вид)

#### `inventory_by_zone`
```cql
PRIMARY KEY (zone_id, product_id)
```
- **Partition key `zone_id`**: все товары одной зоны на одной ноде → эффективный скан по зоне
- **Clustering key `product_id`**: сортировка по товару
- Поддерживает запросы:
  - `SELECT * WHERE zone_id = ?` — все товары в зоне
- Это **денормализованная копия** `inventory_by_product_zone`, обновляется атомарно через BATCH

#### `event_history`
```cql
PRIMARY KEY (product_id, event_timestamp, event_id)
WITH CLUSTERING ORDER BY (event_timestamp DESC, event_id ASC)
```
- **Partition key `product_id`**: вся история товара на одной ноде
- **Clustering key `event_timestamp DESC`**: новые события первые → эффективный `LIMIT`
- **TTL 30 дней**: история не накапливается бесконечно

#### `orders` + `order_items`
```cql
orders:     PRIMARY KEY (order_id)
order_items: PRIMARY KEY (order_id, product_id)
```
- Заказ всегда ищется по `order_id` — простой lookup без JOIN
- `order_items` co-located с `orders` (тот же partition key) → все данные заказа на одной ноде

#### `processed_events`
```cql
PRIMARY KEY (event_id)
WITH default_time_to_live = 604800  -- 7 дней
```
- Идемпотентность: O(1) проверка дубликатов
- TTL 7 дней: память не расходуется бесконечно (дубликаты реальны только в первые минуты)

### Нет JOIN, нет нормализации ради нормализации
- `inventory_by_product_zone` и `inventory_by_zone` — намеренная денормализация
- Все запросы обращаются только к одной таблице по partition key
- Консистентность поддерживается через LOGGED BATCH

---

## Consistency Levels

| Операция | Уровень | Обоснование |
|---|---|---|
| Запись в Cassandra | **QUORUM** | При RF=3 гарантирует запись на 2 из 3 нод. Данные не потеряются при отказе одной ноды. |
| Чтение из Cassandra | **ONE** | Чтение у одной ноды быстрее. Приемлемо для складского состояния: кратковременная staleness не критична. Consumer обрабатывает события последовательно — следующее событие всегда будет новее. |

### Демонстрация при остановке одной ноды

```bash
# Проверить кластер
docker exec cassandra-1 nodetool status

# Остановить одну ноду
docker stop cassandra-2

# Система продолжает работать: QUORUM из 2 оставшихся нод достаточен для RF=3
# (нужно 2 из 3, а у нас 2/3 живых)

# Запустить обратно
docker start cassandra-2
```

**CL=ONE vs CL=QUORUM vs CL=ALL при убитой ноде:**
- `CL=ONE` — работает всегда при 1 живой ноде
- `CL=QUORUM` (наш выбор) — работает при 2+ живых нодах из 3
- `CL=ALL` — требует все 3 ноды, отказ одной = ошибки

---

## Семантика at-least-once

1. `enable.auto.commit = false`
2. Offset коммитится **после** успешной обработки события в Cassandra (или после отправки в DLQ)
3. При рестарте consumer продолжает с последнего закоммиченного offset
4. Повторная обработка дубликатов блокируется таблицей `processed_events` (идемпотентность)

---

## Идемпотентность (пункт 4)

Перед обработкой каждого события consumer проверяет наличие `event_id` в таблице `processed_events`.  
Если запись найдена — событие пропускается (логируется как duplicate).  
`INSERT INTO processed_events` включается в тот же LOGGED BATCH, что и обновление инвентаря — атомарная отметка.

---

## Обработка событий вне порядка (пункт 6)

Использован подход **Timestamp с проверкой**:
- Каждая строка инвентаря хранит `last_event_timestamp`
- Перед обработкой: если `event.timestamp < row.last_event_timestamp` → событие пропускается
- Пример:
  ```
  Event 1: RECEIVED qty=100 ts=12:00 → avail=100, last_ts=12:00
  Event 2: SHIPPED  qty=20  ts=12:05 → avail=80,  last_ts=12:05
  Event 3: RECEIVED qty=50  ts=12:02 → IGNORED (12:02 < 12:05)
  ```
- Метрика `events_skipped_total{reason="out_of_order"}` считает такие события

---

## Dead Letter Queue (пункт 7)

Проблемные события отправляются в топик `warehouse-events-dlq` в формате JSON:

```json
{
  "original_event": { "event_id": "...", "event_type": "PRODUCT_SHIPPED", ... },
  "error_reason": "Invalid quantity: -5 (must be a positive integer)",
  "error_code": "VALIDATION_ERROR",
  "failed_at": "2026-05-10T12:00:00Z",
  "kafka_metadata": { "partition": 2, "offset": 12345 }
}
```

Коды ошибок:
- `VALIDATION_ERROR` — невалидное поле (отрицательное количество, отсутствующий order_id)
- `DESERIALIZATION_ERROR` — не удалось десериализовать Avro
- `PROCESSING_ERROR` — ошибка Cassandra или непредвиденная ошибка

**Consumer не останавливается** — offset коммитится и для DLQ-событий.

Тест DLQ:
```bash
# Отправить событие с отрицательным quantity
curl -s -X POST http://localhost:8080/events \
  -H "Content-Type: application/json" \
  -d '{"event_type":"PRODUCT_SHIPPED","product_id":"SKU-001","zone_id":"ZONE-A","quantity":-5}'

# Проверить DLQ топик
docker exec kafka kafka-console-consumer \
  --bootstrap-server localhost:29092 \
  --topic warehouse-events-dlq \
  --from-beginning --max-messages 1
```

---

## Schema Evolution (пункт 10)

### Стратегия совместимости: BACKWARD

При **BACKWARD** совместимости:
- Новый consumer (читающий V2-схему) может читать старые V1-сообщения
- V1-сообщения не содержат `supplier_id` → поле получает значение по умолчанию `null`

### Версии схем

**V1** — исходная схема:
```json
{ "name": "event_id", "type": "string" },
...
{ "name": "order_items", "type": ["null", {...}], "default": null }
```

**V2** — добавлено поле `supplier_id` с `"default": null`:
```json
{ "name": "supplier_id", "type": ["null", "string"], "default": null }
```

Поле добавлено в **конец** и имеет `default` — это требование BACKWARD совместимости.

### Consumer обрабатывает обе версии

- V2 с `supplier_id`: поле записывается в колонку `supplier_id` таблицы `inventory_by_product_zone`
- V1 без `supplier_id`: `event.get("supplier_id")` возвращает `None`, в Cassandra записывается `null`

### Пошаговая инструкция добавления новой версии события

1. Разработать новую схему: добавить поле с `"default"` в конец `fields`
2. Проверить BACKWARD совместимость локально:
   ```bash
   # Проверить через Schema Registry API
   curl -X POST http://localhost:8081/compatibility/subjects/warehouse-events-value/versions/latest \
     -H "Content-Type: application/vnd.schemaregistry.v1+json" \
     -d "$(jq -Rs '{schemaType:"AVRO", schema: .}' < schemas/warehouse_event_v3.avsc)"
   ```
3. Зарегистрировать новую схему:
   ```bash
   curl -X POST http://localhost:8081/subjects/warehouse-events-value/versions \
     -H "Content-Type: application/vnd.schemaregistry.v1+json" \
     -d "$(jq -Rs '{schemaType:"AVRO", schema: .}' < schemas/warehouse_event_v3.avsc)"
   ```
4. Добавить колонку в Cassandra (если нужно):
   ```cql
   ALTER TABLE warehouse.inventory_by_product_zone ADD new_field TEXT;
   ```
5. Обновить handler в `consumer/app/handlers.py` для чтения нового поля (с `None` как дефолтом)
6. Обновить Producer для отправки нового поля в V3-событиях

---

## E2E сценарии

### Сценарий 1: Базовый цикл склада

```bash
# 1. Поднять систему
docker-compose up --build

# 2. PRODUCT_RECEIVED
curl -s -X POST http://localhost:8080/events \
  -H "Content-Type: application/json" \
  -d '{"event_type":"PRODUCT_RECEIVED","product_id":"SKU-001","zone_id":"ZONE-A","quantity":100}'

# 3. Проверить в Cassandra
docker exec cassandra-1 cqlsh -e \
  "SELECT * FROM warehouse.inventory_by_product_zone WHERE product_id='SKU-001';"

# 4. PRODUCT_RESERVED
curl -s -X POST http://localhost:8080/events \
  -H "Content-Type: application/json" \
  -d '{"event_type":"PRODUCT_RESERVED","product_id":"SKU-001","zone_id":"ZONE-A","quantity":30}'

# 5. PRODUCT_MOVED
curl -s -X POST http://localhost:8080/events \
  -H "Content-Type: application/json" \
  -d '{"event_type":"PRODUCT_MOVED","product_id":"SKU-001","from_zone_id":"ZONE-A","to_zone_id":"ZONE-B","quantity":20}'
```

### Сценарий 2: Идемпотентность

```bash
# Отправить событие с фиксированным event_id
curl -s -X POST http://localhost:8080/events \
  -H "Content-Type: application/json" \
  -d '{"event_id":"test-idem-001","event_type":"PRODUCT_RECEIVED","product_id":"SKU-002","zone_id":"ZONE-A","quantity":50}'

# Повторно отправить то же событие
curl -s -X POST http://localhost:8080/events \
  -H "Content-Type: application/json" \
  -d '{"event_id":"test-idem-001","event_type":"PRODUCT_RECEIVED","product_id":"SKU-002","zone_id":"ZONE-A","quantity":50}'

# Проверить: available = 50 (не 100)
docker exec cassandra-1 cqlsh -e \
  "SELECT * FROM warehouse.inventory_by_product_zone WHERE product_id='SKU-002' AND zone_id='ZONE-A';"
```

### Сценарий 4: События вне порядка

```bash
NOW_MS=$(date +%s%3N)
T1=$((NOW_MS - 300000))  # 5 минут назад
T2=$((NOW_MS - 0))       # сейчас
T3=$((NOW_MS - 180000))  # 3 минуты назад (между T1 и T2, но отправляем ПОСЛЕ T2)

curl -s -X POST http://localhost:8080/events -H "Content-Type: application/json" \
  -d "{\"event_type\":\"PRODUCT_RECEIVED\",\"product_id\":\"SKU-004\",\"zone_id\":\"ZONE-A\",\"quantity\":100,\"timestamp\":$T1}"

curl -s -X POST http://localhost:8080/events -H "Content-Type: application/json" \
  -d "{\"event_type\":\"PRODUCT_SHIPPED\",\"product_id\":\"SKU-004\",\"zone_id\":\"ZONE-A\",\"quantity\":20,\"timestamp\":$T2}"

# Это событие должно быть проигнорировано (timestamp T3 < T2)
curl -s -X POST http://localhost:8080/events -H "Content-Type: application/json" \
  -d "{\"event_type\":\"PRODUCT_RECEIVED\",\"product_id\":\"SKU-004\",\"zone_id\":\"ZONE-A\",\"quantity\":50,\"timestamp\":$T3}"

# Проверить: available = 80 (не 130)
docker exec cassandra-1 cqlsh -e \
  "SELECT available_quantity FROM warehouse.inventory_by_product_zone WHERE product_id='SKU-004' AND zone_id='ZONE-A';"
```

### Сценарий 8: Schema Evolution

```bash
# V1-событие (без supplier_id)
curl -s -X POST http://localhost:8080/events \
  -H "Content-Type: application/json" \
  -d '{"event_type":"PRODUCT_RECEIVED","product_id":"SKU-010","zone_id":"ZONE-A","quantity":50}'

# V2-событие (с supplier_id)
curl -s -X POST http://localhost:8080/events \
  -H "Content-Type: application/json" \
  -d '{"event_type":"PRODUCT_RECEIVED","product_id":"SKU-011","zone_id":"ZONE-A","quantity":50,"supplier_id":"SUP-001"}'

# Проверить обе записи
docker exec cassandra-1 cqlsh -e \
  "SELECT product_id, zone_id, available_quantity, supplier_id FROM warehouse.inventory_by_product_zone WHERE product_id IN ('SKU-010','SKU-011');"
# SKU-010: supplier_id = null
# SKU-011: supplier_id = 'SUP-001'

# Просмотреть версии схем в Schema Registry
curl -s http://localhost:8081/subjects/warehouse-events-value/versions
curl -s http://localhost:8081/subjects/warehouse-events-value/versions/1
curl -s http://localhost:8081/subjects/warehouse-events-value/versions/2
```

---

## Мониторинг

### Метрики (http://localhost:8082/metrics)

| Метрика | Тип | Описание |
|---|---|---|
| `consumer_lag{topic,partition}` | Gauge | Отставание consumer от HEAD топика |
| `events_processed_total{event_type}` | Counter | Счётчик обработанных событий по типу |
| `event_processing_duration_seconds{event_type}` | Histogram | Время обработки одного события |
| `cassandra_write_errors_total{operation}` | Counter | Ошибки записи в Cassandra |
| `dlq_sent_total{error_code}` | Counter | События, отправленные в DLQ |
| `events_skipped_total{reason}` | Counter | Пропущенные события (дубликаты / out-of-order) |
| `cassandra_connected` | Gauge | Статус подключения к Cassandra |
| `kafka_connected` | Gauge | Статус подключения к Kafka |

### Grafana дашборд (http://localhost:3000)

Дашборд **Warehouse Consumer** содержит 10 панелей:
1. Consumer lag по партициям
2. Throughput (events/sec) по типам событий
3. Cassandra write errors
4. Event processing duration (p50/p95/p99)
5. Суммарные счётчики (обработано / DLQ / статусы подключений)
6. Skipped events (дубликаты и out-of-order)
7. DLQ events rate

---

## Структура проекта

```
hw6/
├── docker-compose.yml
├── Makefile
├── README.md
├── schemas/
│   ├── warehouse_event_v1.avsc  ← исходная Avro-схема
│   └── warehouse_event_v2.avsc  ← V2 с supplier_id (backward compatible)
├── cassandra/
│   └── init/
│       ├── 01_keyspace.cql      ← NetworkTopologyStrategy, RF=3
│       └── 02_tables.cql        ← 6 таблиц с обоснованными ключами
├── producer/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── app/
│       ├── main.py              ← FastAPI: POST /events, GET /health
│       ├── kafka_producer.py    ← Avro producer с Schema Registry
│       └── generator.py         ← Синтетический генератор событий
├── consumer/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── app/
│       ├── main.py              ← FastAPI: /health, /metrics + consumer thread
│       ├── handlers.py          ← Диспетчер событий (все 8 типов)
│       ├── cassandra_db.py      ← CassandraClient + BATCH-записи
│       ├── dlq.py               ← DLQ producer
│       └── metrics.py           ← Prometheus метрики
├── prometheus/
│   └── prometheus.yml
└── grafana/
    ├── dashboards/
    │   └── warehouse.json       ← Grafana dashboard (10 панелей)
    └── provisioning/
        ├── dashboards/dashboards.yml
        └── datasources/datasources.yml
```
