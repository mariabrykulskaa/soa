.PHONY: up down logs test clean

up:
	docker compose up -d --build

down:
	docker compose down

logs:
	docker compose logs -f --tail=200

test:
	docker compose -f docker-compose.yml -f docker-compose.tests.yml build tests
	docker compose -f docker-compose.yml -f docker-compose.tests.yml run --rm tests

clean:
	docker compose down -v
