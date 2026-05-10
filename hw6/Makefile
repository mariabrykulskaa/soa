.PHONY: up down build logs ps clean cqlsh nodetool status-cassandra

# ---------------------------------------------------------------------------
# Lifecycle
# ---------------------------------------------------------------------------

## Поднять всю систему (build + up)
up:
	docker-compose up --build -d

## Поднять и смотреть логи в терминале
up-fg:
	docker-compose up --build

## Остановить и удалить контейнеры (данные Cassandra сохраняются в volumes)
down:
	docker-compose down

## Пересобрать образы
build:
	docker-compose build --no-cache

## Удалить контейнеры И volumes (полный сброс)
clean:
	docker-compose down -v --remove-orphans

# ---------------------------------------------------------------------------
# Observability
# ---------------------------------------------------------------------------

## Логи всех сервисов
logs:
	docker-compose logs -f

## Логи только consumer
logs-consumer:
	docker-compose logs -f consumer

## Логи только producer
logs-producer:
	docker-compose logs -f producer

## Статус контейнеров
ps:
	docker-compose ps

# ---------------------------------------------------------------------------
# Cassandra
# ---------------------------------------------------------------------------

## Подключиться к cqlsh на cassandra-1
cqlsh:
	docker exec -it cassandra-1 cqlsh

## Статус кластера Cassandra
nodetool:
	docker exec cassandra-1 nodetool status

## Проверить остатки в инвентаре
check-inventory:
	docker exec cassandra-1 cqlsh -e \
		"SELECT product_id, zone_id, available_quantity, reserved_quantity, last_event_timestamp \
		 FROM warehouse.inventory_by_product_zone LIMIT 20;"

## Проверить инвентарь по зонам
check-zones:
	docker exec cassandra-1 cqlsh -e \
		"SELECT zone_id, product_id, available_quantity, reserved_quantity \
		 FROM warehouse.inventory_by_zone LIMIT 20;"

## Проверить заказы
check-orders:
	docker exec cassandra-1 cqlsh -e \
		"SELECT order_id, status, created_at FROM warehouse.orders LIMIT 10;"

# ---------------------------------------------------------------------------
# Kafka
# ---------------------------------------------------------------------------

## Просмотр DLQ топика
dlq-watch:
	docker exec kafka kafka-console-consumer \
		--bootstrap-server localhost:29092 \
		--topic warehouse-events-dlq \
		--from-beginning

## Просмотр основного топика (raw bytes, для отладки)
events-watch:
	docker exec kafka kafka-console-consumer \
		--bootstrap-server localhost:29092 \
		--topic warehouse-events \
		--from-beginning \
		--max-messages 5

## Список топиков
topics:
	docker exec kafka kafka-topics --bootstrap-server localhost:29092 --list

## Consumer group offsets
lag:
	docker exec kafka kafka-consumer-groups \
		--bootstrap-server localhost:29092 \
		--describe --group warehouse-state-consumer

# ---------------------------------------------------------------------------
# Schema Registry
# ---------------------------------------------------------------------------

## Показать зарегистрированные схемы
schemas:
	@echo "=== Subjects ==="
	@curl -s http://localhost:8081/subjects | python3 -m json.tool
	@echo ""
	@echo "=== warehouse-events-value versions ==="
	@curl -s http://localhost:8081/subjects/warehouse-events-value/versions | python3 -m json.tool

# ---------------------------------------------------------------------------
# E2E тесты (ручные curl-сценарии)
# ---------------------------------------------------------------------------

## Сценарий 1: базовый цикл склада
scenario1:
	@echo "==> PRODUCT_RECEIVED SKU-001 ZONE-A qty=100"
	@curl -s -X POST http://localhost:8080/events \
		-H "Content-Type: application/json" \
		-d '{"event_type":"PRODUCT_RECEIVED","product_id":"SKU-001","zone_id":"ZONE-A","quantity":100}' | python3 -m json.tool
	@sleep 2
	@echo ""
	@echo "==> Cassandra state:"
	@docker exec cassandra-1 cqlsh -e \
		"SELECT product_id, zone_id, available_quantity, reserved_quantity FROM warehouse.inventory_by_product_zone WHERE product_id='SKU-001';"

## Сценарий 2: идемпотентность
scenario2:
	@echo "==> Sending event with fixed event_id=test-idem-001"
	@curl -s -X POST http://localhost:8080/events \
		-H "Content-Type: application/json" \
		-d '{"event_id":"test-idem-001","event_type":"PRODUCT_RECEIVED","product_id":"SKU-002","zone_id":"ZONE-A","quantity":50}' | python3 -m json.tool
	@sleep 2
	@echo "==> Sending SAME event again"
	@curl -s -X POST http://localhost:8080/events \
		-H "Content-Type: application/json" \
		-d '{"event_id":"test-idem-001","event_type":"PRODUCT_RECEIVED","product_id":"SKU-002","zone_id":"ZONE-A","quantity":50}' | python3 -m json.tool
	@sleep 2
	@echo "==> Cassandra state (should be 50, not 100):"
	@docker exec cassandra-1 cqlsh -e \
		"SELECT available_quantity FROM warehouse.inventory_by_product_zone WHERE product_id='SKU-002' AND zone_id='ZONE-A';"

## Сценарий 5: DLQ
scenario5:
	@echo "==> Sending INVALID event (quantity=-5)"
	@curl -s -X POST http://localhost:8080/events \
		-H "Content-Type: application/json" \
		-d '{"event_type":"PRODUCT_SHIPPED","product_id":"SKU-001","zone_id":"ZONE-A","quantity":-5}' | python3 -m json.tool
	@sleep 2
	@echo "==> DLQ contents:"
	@docker exec kafka kafka-console-consumer \
		--bootstrap-server localhost:29092 \
		--topic warehouse-events-dlq \
		--from-beginning --max-messages 1 --timeout-ms 5000 || true

## Сценарий 8: schema evolution
scenario8:
	@echo "==> V1 event (no supplier_id)"
	@curl -s -X POST http://localhost:8080/events \
		-H "Content-Type: application/json" \
		-d '{"event_type":"PRODUCT_RECEIVED","product_id":"SKU-010","zone_id":"ZONE-A","quantity":50}' | python3 -m json.tool
	@echo "==> V2 event (with supplier_id=SUP-001)"
	@curl -s -X POST http://localhost:8080/events \
		-H "Content-Type: application/json" \
		-d '{"event_type":"PRODUCT_RECEIVED","product_id":"SKU-011","zone_id":"ZONE-A","quantity":50,"supplier_id":"SUP-001"}' | python3 -m json.tool
	@sleep 3
	@echo "==> Cassandra: check supplier_id column"
	@docker exec cassandra-1 cqlsh -e \
		"SELECT product_id, available_quantity, supplier_id FROM warehouse.inventory_by_product_zone WHERE product_id IN ('SKU-010','SKU-011');"
