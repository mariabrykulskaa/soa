# Online Cinema — Event Streaming + Analytics Pipeline (HW5)

Полный event-streaming pipeline:

```
Movie Service (Producer)  ──►  Kafka (2 брокера, RF=2)  ──►  ClickHouse (raw + aggregates)
                                                                    │
                                            ┌───────────────────────┼──────────────────────────┐
                                            ▼                       ▼                          ▼
                                   Aggregation Service        Grafana (dashboards)       S3 (MinIO, daily export)
                                            │
                                            ▼
                                    PostgreSQL (aggregates)
```

## Быстрый старт

```bash
cd hw5
make up          # docker compose up -d --build
# подождать ~60-90с пока CH/Kafka/SR поднимутся
make test        # интеграционные тесты
make logs        # логи
make clean       # снять всё вместе с томами
```

Или напрямую: `docker compose up -d --build`.

## Endpoints (после старта)

| Компонент        | URL                                  |
| ---------------- | ------------------------------------ |
| Producer API     | http://localhost:8000/docs           |
| Aggregator API   | http://localhost:8010/docs           |
| ClickHouse HTTP  | http://localhost:8123                |
| Postgres         | `postgresql://analytics:analytics@localhost:5432/analytics` |
| Schema Registry  | http://localhost:8081                |
| Grafana          | http://localhost:3000 (admin/admin)  |
| MinIO console    | http://localhost:9101 (minio/minio12345) |

Dashboard: **Cinema → Online Cinema — Analytics** (включает Retention Cohort Heatmap, DAU, конверсию, Top Movies, распределение по устройствам, среднее время просмотра).

## Блок 1–4

### 1. Kafka topic + Avro схема

- Схема: [schemas/movie_event.avsc](schemas/movie_event.avsc) — Avro record `cinema.events.MovieEvent` с enum-типами `EventType`/`DeviceType`, `timestamp-millis` и UUID logical type.
- Регистрируется в Schema Registry под subject `movie-events-value` контейнером `schema-init`.
- Топик создаётся контейнером `kafka-init`: **3 партиции, replication.factor=2, min.insync.replicas=1**.
- **Ключ партиционирования: `user_id`**. Обоснование: основные бизнес-инварианты привязаны к пользователю (`VIEW_STARTED → VIEW_PAUSED → VIEW_RESUMED → VIEW_FINISHED` в рамках одной сессии), а Kafka гарантирует порядок только внутри партиции. Партиционирование по `user_id` держит все события одного пользователя в одной партиции и сохраняет порядок. Распределение нагрузки остаётся равномерным (много пользователей, каждый — небольшой трафик).

### 2. Producer ([producer/](producer/))

- FastAPI на `:8000`.
- `POST /events` — принимает JSON, валидирует через Pydantic, публикует в Kafka Avro-сериализатором из `confluent-kafka`. Возвращает `event_id`.
- Фоновый **генератор** (`GENERATOR_ENABLED=true`) эмитит реалистичные сессии: `VIEW_STARTED → [PAUSED → RESUMED]* → VIEW_FINISHED [→ LIKED]`, + отдельные `SEARCHED` / `LIKED`. `progress_seconds` монотонно растёт, таймстэмпы тоже. 20% событий сдвинуты в прошлое (до 8 дней назад) — для наполнения retention-когорт.
- Producer: `acks=all`, `enable.idempotence=true`, `retries=10` с exponential backoff, валидация по схеме через `AvroSerializer(use.latest.version=True, auto.register.schemas=False)`, логирование факта публикации (event_id, event_type, timestamp, key).

### 3. ClickHouse ([clickhouse/init/01_schema.sql](clickhouse/init/01_schema.sql))

- `cinema.movie_events_kafka` — **Kafka Engine** c форматом `AvroConfluent` и `format_avro_schema_registry_url=http://schema-registry:8081`, подписан на оба брокера.
- `cinema.movie_events` — **MergeTree**, `PARTITION BY event_date`, `ORDER BY (event_date, event_type, user_id, timestamp)` (оптимизировано под фильтры по дню/типу события, которые используют все наши метрики), TTL 180 дней.
- `cinema.movie_events_mv` — **Materialized View** перекладывает данные из Kafka-таблицы в MergeTree.
- Доп. MV для демонстрации материализованных агрегатов в CH: [clickhouse/init/02_aggregates.sql](clickhouse/init/02_aggregates.sql) (`dau_daily`, `top_movies_daily` на `AggregatingMergeTree`).

### 4. Интеграционный тест ([tests/test_pipeline.py](tests/test_pipeline.py))

- `test_pipeline_event_reaches_clickhouse` — публикует событие через HTTP producer и опрашивает ClickHouse пока оно не появится (до 60с), сверяя все поля.
- `test_invalid_event_rejected` — проверяет отказ при неверном `event_type`.
- Каждый тест использует уникальные `event_id`/`session_id`/`user_id` (изоляция без чистки таблиц).
- Запуск: `make test`.

## Блок 5–7

### 5. Aggregation Service ([aggregator/](aggregator/))

- Отдельный контейнер, не связан с producer/ingestion pipeline.
- Читает **raw-события из ClickHouse** (не из Kafka) через HTTP-интерфейс `clickhouse-connect`.
- **Cron-расписание** через APScheduler: `AGGREGATION_CRON` (по умолчанию `*/2 * * * *`), `EXPORT_CRON` (`5 0 * * *`). Оба настраиваемы через env.
- HTTP API:
  - `POST /recompute  {"date": "YYYY-MM-DD"}` — ручной пересчёт.
  - `POST /export     {"date": "YYYY-MM-DD"}` — ручной экспорт в S3.
  - `GET  /metrics/YYYY-MM-DD` — метрики из Postgres за дату.
- Каждый цикл логирует старт, число записей и длительность; одновременно пишет строку в `aggregation_runs` (audit log).
- **Метрики** (см. [aggregator/app/metrics.py](aggregator/app/metrics.py)):

  | Метрика                    | Запрос                                                        |
  | -------------------------- | ------------------------------------------------------------- |
  | DAU                        | `uniqExact(user_id)` по событиям за день                       |
  | Среднее время просмотра    | `avg(progress_seconds) WHERE event_type='VIEW_FINISHED'`      |
  | Топ фильмов                | Оконная `row_number() OVER (ORDER BY count() DESC)`, top-10   |
  | Конверсия просмотра        | `sumIf(VIEW_FINISHED)/sumIf(VIEW_STARTED)`                    |
  | Retention D1/D7            | Когорта пользователей с первой активностью = target, доля вернувшихся через N дней |
  | Device share               | `count() / sum(count()) OVER ()` (оконная функция)            |
  | Retention cohort matrix    | JOIN first-event-date × активности, day_n 0..7, rolling 14д    |

  Все запросы используют агрегатные / оконные функции ClickHouse (`uniq`, `uniqExact`, `avg`, `countIf`, `sumIf`, `row_number() OVER`), простого `count(*)` нет.

- **PostgreSQL схема** ([postgres/init/01_schema.sql](postgres/init/01_schema.sql)):
  - `metrics(metric_date, metric_name, dimension, value, computed_at)` — PK на `(metric_date, metric_name, dimension)`, upsert через `ON CONFLICT ... DO UPDATE`. **Идемпотентно**: повторный пересчёт за ту же дату обновляет, а не дублирует.
  - `retention_cohort(cohort_date, day_n, users, share, computed_at)` — матрица для heatmap.
  - `aggregation_runs` — журнал запусков.
- Ошибки записи: retry с exponential backoff в `PgSink._conn`, логирование в `aggregation_runs` со статусом `failed` и `error_message`.

### 6. Grafana ([grafana/](grafana/))

- Поднимается через compose с автоматической установкой плагина `grafana-clickhouse-datasource` и provisioning datasources + dashboards.
- **Dashboard `Cinema → Online Cinema — Analytics`**:
  - **Retention Cohort Heatmap** (обязательная панель) — строится из `retention_cohort`, `cohort_date × day_n × share`, 14-дневное окно когорт.
  - DAU (time series)
  - View conversion (time series, %)
  - Top movies (bar chart, за последний обработанный день)
  - Device distribution (pie chart)
  - Average watch time (time series, секунды)
- Панели используют агрегаты из п.5 (таблицы `metrics`/`retention_cohort`), не raw-события.

### 7. S3 export ([aggregator/app/s3_exporter.py](aggregator/app/s3_exporter.py))

- MinIO поднимается в compose (`:9100` API, `:9101` console), бакет `movie-analytics` создаётся init-контейнером `minio-init`.
- Экспорт читает из Postgres агрегаты за дату + cohort rows и пишет **JSON** с полями `date`, `exported_at`, `metrics[]`, `retention_cohort[]`.
- Ключ: `s3://movie-analytics/daily/YYYY-MM-DD/aggregates.json`. `put_object` перезаписывает существующий объект (повторный экспорт идемпотентен).
- Scheduler запускает экспорт за **вчерашний** день по расписанию `EXPORT_CRON`; `POST /export` позволяет вручную запустить за любую дату.
- Retry: до 5 попыток с exponential backoff при `BotoCoreError`/`ClientError`; в случае тотального отказа ошибка логируется и следующий запуск попробует снова.

## Блок 8

### 8. Fault-tolerant Kafka

- **Два брокера** (`kafka1`, `kafka2`) + Zookeeper в [docker-compose.yml](docker-compose.yml).
- Топик `movie-events` создан с `replication.factor=2` и `min.insync.replicas=1` (см. `kafka-init`).
- `__consumer_offsets` / `_schemas` / transaction state logs тоже с RF=2.
- **Schema Registry** поднят и использует оба брокера как bootstrap.
- **Health-checks** настроены для всех компонентов: `zookeeper`, `kafka1`, `kafka2`, `schema-registry`, `clickhouse`, `postgres`, `minio`, `producer`, `aggregator`, `grafana`. Зависимости через `depends_on: condition: service_healthy` гарантируют корректный порядок старта.

## Структура

```
hw5/
├── docker-compose.yml            # main stack
├── docker-compose.tests.yml      # тестовый сервис
├── Makefile                      # up / down / test / clean
├── schemas/movie_event.avsc      # Avro schema (source of truth)
├── producer/                     # FastAPI + Avro producer + generator
├── aggregator/                   # FastAPI + APScheduler + metrics + Postgres + S3
├── clickhouse/init/              # DDL: Kafka engine, MergeTree, MVs, aggregates
├── postgres/init/                # DDL: metrics / retention_cohort / aggregation_runs
├── grafana/                      # provisioning + cohort dashboard
└── tests/                        # интеграционные pytest-тесты
```

## Проверка вручную

```bash
# 1) Отправить одно событие
curl -s -X POST http://localhost:8000/events \
  -H 'Content-Type: application/json' \
  -d '{"user_id":"u1","movie_id":"m1","event_type":"VIEW_STARTED",
       "device_type":"DESKTOP","session_id":"s1","progress_seconds":0}'

# 2) Посмотреть в ClickHouse
curl -s 'http://localhost:8123/?query=SELECT%20count()%20FROM%20cinema.movie_events'

# 3) Посчитать агрегаты за сегодня
curl -s -X POST http://localhost:8010/recompute -H 'Content-Type: application/json' -d '{}'

# 4) Посмотреть метрики из Postgres
curl -s http://localhost:8010/metrics/$(date -u +%F) | jq

# 5) Экспорт в S3
curl -s -X POST http://localhost:8010/export -H 'Content-Type: application/json' -d '{}'
docker compose run --rm minio-init mc ls local/movie-analytics/daily/ --recursive
```
