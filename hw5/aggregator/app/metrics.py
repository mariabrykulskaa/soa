"""Business metric queries executed against ClickHouse."""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date
from typing import Iterable

import clickhouse_connect
from clickhouse_connect.driver import Client

log = logging.getLogger("aggregator.metrics")


@dataclass
class MetricRow:
    metric_date: date
    metric_name: str
    dimension: str
    value: float


@dataclass
class CohortRow:
    cohort_date: date
    day_n: int
    users: int
    share: float


def ch_client(host: str, port: int, database: str) -> Client:
    return clickhouse_connect.get_client(host=host, port=port, database=database)


def compute_metrics(ch: Client, target: date) -> tuple[list[MetricRow], list[CohortRow]]:
    """Compute all daily metrics for `target` date plus the cohort retention matrix."""
    rows: list[MetricRow] = []

    # DAU — unique users with any event on target day.
    r = ch.query(
        "SELECT uniqExact(user_id) FROM cinema.movie_events WHERE event_date = %(d)s",
        parameters={"d": target},
    ).result_rows
    dau = int(r[0][0]) if r else 0
    rows.append(MetricRow(target, "dau", "", float(dau)))

    # Average view time for completed views.
    r = ch.query(
        """
        SELECT avg(progress_seconds)
        FROM cinema.movie_events
        WHERE event_date = %(d)s AND event_type = 'VIEW_FINISHED'
        """,
        parameters={"d": target},
    ).result_rows
    avg_view = float(r[0][0]) if r and r[0][0] is not None else 0.0
    rows.append(MetricRow(target, "avg_view_seconds", "", avg_view))

    # Top 10 movies by VIEW_STARTED count (window rank via ClickHouse window function).
    r = ch.query(
        """
        SELECT movie_id, views FROM (
            SELECT
                movie_id,
                count() AS views,
                row_number() OVER (ORDER BY count() DESC) AS rn
            FROM cinema.movie_events
            WHERE event_date = %(d)s AND event_type = 'VIEW_STARTED'
            GROUP BY movie_id
        )
        WHERE rn <= 10
        ORDER BY views DESC
        """,
        parameters={"d": target},
    ).result_rows
    for movie_id, views in r:
        rows.append(MetricRow(target, "top_movie_views", str(movie_id), float(views)))

    # View-finish conversion = VIEW_FINISHED / VIEW_STARTED.
    r = ch.query(
        """
        SELECT
            sumIf(1, event_type = 'VIEW_STARTED')  AS started,
            sumIf(1, event_type = 'VIEW_FINISHED') AS finished
        FROM cinema.movie_events
        WHERE event_date = %(d)s
        """,
        parameters={"d": target},
    ).result_rows
    started, finished = (int(r[0][0]), int(r[0][1])) if r else (0, 0)
    conversion = (finished / started) if started else 0.0
    rows.append(MetricRow(target, "view_conversion", "", conversion))
    rows.append(MetricRow(target, "views_started", "", float(started)))
    rows.append(MetricRow(target, "views_finished", "", float(finished)))

    # Device distribution (share per device for the target day).
    r = ch.query(
        """
        SELECT
            device_type,
            count() / sum(count()) OVER () AS share
        FROM cinema.movie_events
        WHERE event_date = %(d)s
        GROUP BY device_type
        """,
        parameters={"d": target},
    ).result_rows
    for device, share in r:
        rows.append(MetricRow(target, "device_share", str(device), float(share)))

    # Retention D1 / D7 anchored on users whose first ever activity was `target`.
    for day_n in (1, 7):
        r = ch.query(
            """
            WITH first AS (
                SELECT user_id
                FROM cinema.movie_events
                GROUP BY user_id
                HAVING toDate(min(`timestamp`)) = %(d)s
            )
            SELECT
                count(DISTINCT f.user_id) AS cohort,
                count(DISTINCT if(e.event_date = addDays(%(d)s, %(n)s), e.user_id, NULL)) AS returned
            FROM first AS f
            LEFT JOIN cinema.movie_events AS e USING (user_id)
            """,
            parameters={"d": target, "n": day_n},
        ).result_rows
        cohort, returned = (int(r[0][0]), int(r[0][1])) if r else (0, 0)
        share = (returned / cohort) if cohort else 0.0
        rows.append(MetricRow(target, f"retention_d{day_n}", "", share))

    # Cohort retention matrix: rolling 14-day cohorts, day_n 0..7.
    cohort_rows = _cohort_matrix(ch, target)
    return rows, cohort_rows


def _cohort_matrix(ch: Client, target: date) -> list[CohortRow]:
    sql = """
    WITH first_event AS (
        SELECT user_id, toDate(min(`timestamp`)) AS cohort_date
        FROM cinema.movie_events
        GROUP BY user_id
    ),
    daily_users AS (
        SELECT DISTINCT user_id, toDate(`timestamp`) AS active_date
        FROM cinema.movie_events
    ),
    cohort_size AS (
        SELECT cohort_date, count() AS users
        FROM first_event
        GROUP BY cohort_date
    )
    SELECT
        f.cohort_date,
        toInt32(dateDiff('day', f.cohort_date, d.active_date)) AS day_n,
        uniqExact(d.user_id) AS users,
        uniqExact(d.user_id) / any(cs.users) AS share
    FROM first_event AS f
    INNER JOIN daily_users AS d USING (user_id)
    INNER JOIN cohort_size AS cs USING (cohort_date)
    WHERE f.cohort_date BETWEEN addDays(%(d)s, -14) AND %(d)s
      AND dateDiff('day', f.cohort_date, d.active_date) BETWEEN 0 AND 7
    GROUP BY f.cohort_date, day_n
    ORDER BY f.cohort_date, day_n
    """
    r = ch.query(sql, parameters={"d": target}).result_rows
    return [CohortRow(row[0], int(row[1]), int(row[2]), float(row[3])) for row in r]
