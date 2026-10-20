"""
The Latency page's numbers, read from ops.request_timings.

The backend writes one row per request (backend/services/request_timings.py,
table from backend migration 0038): its route, status, total, and `parts`, the
total split into buckets that add up to it. Percentiles are computed by
Postgres; a route's parts are averaged per request, so they add up to its mean.
Each page's queries share one REPEATABLE READ snapshot, so rows the backend
writes meanwhile can't land in one aggregate and miss the next.
Blocking: call these through `run_in_db_thread`.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Iterable, Optional

from db.base import db
from schemas.latency import (
    LatencyBin,
    LatencyOverview,
    LatencyStats,
    LatencyWindow,
    RouteLatency,
    RouteLatencyDetail,
    SlowRequest,
)

# Each window and the width of one slice of its time series.
WINDOWS: dict[str, tuple[timedelta, int]] = {
    "1h": (timedelta(hours=1), 300),
    "24h": (timedelta(hours=24), 3600),
    "7d": (timedelta(days=7), 6 * 3600),
    "30d": (timedelta(days=30), 86400),
}
SLOWEST = 20

_STATS = """
    count(*) AS requests,
    count(*) FILTER (WHERE status_code >= 500) AS errors,
    percentile_cont(0.5) WITHIN GROUP (ORDER BY duration_ms) AS p50,
    percentile_cont(0.95) WITHIN GROUP (ORDER BY duration_ms) AS p95,
    percentile_cont(0.99) WITHIN GROUP (ORDER BY duration_ms) AS p99,
    avg(duration_ms) AS mean,
    max(duration_ms) AS max
"""

_TOTALS_SQL = f"SELECT {_STATS} FROM ops.request_timings WHERE service = %s AND started_at >= %s AND started_at < %s"

_ROUTES_SQL = f"""
SELECT method, route, {_STATS}
FROM ops.request_timings
WHERE service = %s AND started_at >= %s AND started_at < %s {{only}}
GROUP BY method, route
"""

# Sum of each bucket per route; divided by the route's requests for the mean.
_MAP_SUMS_SQL = """
SELECT t.method, t.route, kv.key, sum(kv.value::double precision)
FROM ops.request_timings t CROSS JOIN LATERAL jsonb_each_text(t.{column}) kv
WHERE t.service = %s AND t.started_at >= %s AND t.started_at < %s {only}
GROUP BY 1, 2, 3
"""

_ONLY_ROUTE = "AND method = %s AND route = %s"

_SERIES_SQL = """
SELECT floor(extract(epoch FROM started_at) / %s)::bigint AS slice,
    count(*) AS requests,
    count(*) FILTER (WHERE status_code >= 500) AS errors,
    percentile_cont(0.5) WITHIN GROUP (ORDER BY duration_ms) AS p50,
    percentile_cont(0.95) WITHIN GROUP (ORDER BY duration_ms) AS p95
FROM ops.request_timings
WHERE service = %s AND started_at >= %s AND started_at < %s AND method = %s AND route = %s
GROUP BY slice
"""

_SERIES_PARTS_SQL = """
SELECT floor(extract(epoch FROM t.started_at) / %s)::bigint AS slice, kv.key, sum(kv.value::double precision)
FROM ops.request_timings t CROSS JOIN LATERAL jsonb_each_text(t.parts) kv
WHERE t.service = %s AND t.started_at >= %s AND t.started_at < %s AND t.method = %s AND t.route = %s
GROUP BY 1, 2
"""

_SLOWEST_SQL = """
SELECT started_at, duration_ms, status_code, parts, calls, trace_id, version
FROM ops.request_timings
WHERE service = %s AND started_at >= %s AND started_at < %s AND method = %s AND route = %s
ORDER BY duration_ms DESC
LIMIT %s
"""


def window_bounds(window: LatencyWindow, now: Optional[datetime] = None) -> tuple[datetime, datetime]:
    """The window as [since, until): every query counts rows from `since` up to, not including, `until`."""
    until = now or datetime.now(timezone.utc)
    return until - WINDOWS[window][0], until


def _available() -> bool:
    row = db.execute_sql("SELECT to_regclass('ops.request_timings')").fetchone()
    return row is not None and row[0] is not None


def _ms(value) -> Optional[float]:
    return None if value is None else round(float(value), 1)


def stats(row: Iterable) -> LatencyStats:
    """A LatencyStats from the `_STATS` columns, in their order."""
    requests, errors, p50, p95, p99, mean, max_ = row
    return LatencyStats(
        requests=int(requests or 0), errors=int(errors or 0),
        p50_ms=_ms(p50), p95_ms=_ms(p95), p99_ms=_ms(p99), mean_ms=_ms(mean), max_ms=_ms(max_),
    )


def per_request(sums: dict[str, float], requests: int, digits: int = 1) -> dict[str, float]:
    """Bucket sums over `requests` requests -> the mean per request (a request without the bucket counts as 0)."""
    if requests <= 0:
        return {}
    return {key: round(total / requests, digits) for key, total in sums.items() if total > 0}


def total_time(route: RouteLatency) -> float:
    """What the page orders routes by: the time they cost the service in the window."""
    return route.requests * (route.mean_ms or 0.0)


def _snapshot():
    """One snapshot for a page's several queries: a route's parts, calls and stats count the same rows."""
    return db.atomic(isolation_level="REPEATABLE READ")


def _routes(
    service: str,
    since: datetime,
    until: datetime,
    only: Optional[tuple[str, str]] = None,
) -> list[RouteLatency]:
    clause = _ONLY_ROUTE if only else ""
    params = (service, since, until, *(only or ()))
    sums: dict[str, dict[tuple[str, str], dict[str, float]]] = {"parts": {}, "calls": {}}
    for column in sums:
        for method, route, key, total in db.execute_sql(_MAP_SUMS_SQL.format(column=column, only=clause), params):
            sums[column].setdefault((method, route), {})[key] = float(total or 0)

    routes = []
    for method, route, *columns in db.execute_sql(_ROUTES_SQL.format(only=clause), params):
        base = stats(columns)
        routes.append(RouteLatency(
            **base.model_dump(), method=method, route=route,
            parts=per_request(sums["parts"].get((method, route), {}), base.requests),
            calls=per_request(sums["calls"].get((method, route), {}), base.requests, digits=2),
        ))
    routes.sort(key=lambda r: (-total_time(r), r.route, r.method))
    return routes


def build_overview(window: LatencyWindow, service: str = "api", now: Optional[datetime] = None) -> LatencyOverview:
    since, until = window_bounds(window, now)
    if not _available():
        return LatencyOverview(service=service, window=window, since=since, until=until, available=False,
                               totals=LatencyStats(), routes=[])
    with _snapshot():
        totals = stats(db.execute_sql(_TOTALS_SQL, (service, since, until)).fetchone())
        routes = _routes(service, since, until)
    return LatencyOverview(service=service, window=window, since=since, until=until, available=True,
                           totals=totals, routes=routes)


def fill_series(
    since: datetime,
    until: datetime,
    bin_seconds: int,
    rows: dict[int, tuple],
    parts: dict[int, dict[str, float]],
) -> list[LatencyBin]:
    """Every slice from `since` to `until`, the empty ones included, oldest first.

    `rows` maps a slice number (epoch seconds // bin_seconds) to its
    (requests, errors, p50, p95); `parts` to its bucket sums.
    """
    first = int(since.timestamp()) // bin_seconds
    last = int(until.timestamp()) // bin_seconds
    series = []
    for slice_ in range(first, last + 1):
        requests, errors, p50, p95 = rows.get(slice_, (0, 0, None, None))
        series.append(LatencyBin(
            at=datetime.fromtimestamp(slice_ * bin_seconds, tz=timezone.utc),
            requests=int(requests), errors=int(errors), p50_ms=_ms(p50), p95_ms=_ms(p95),
            parts=per_request(parts.get(slice_, {}), int(requests)),
        ))
    return series


def build_route(
    method: str,
    route: str,
    window: LatencyWindow,
    service: str = "api",
    now: Optional[datetime] = None,
) -> RouteLatencyDetail:
    since, until = window_bounds(window, now)
    bin_seconds = WINDOWS[window][1]
    if not _available():
        return RouteLatencyDetail(service=service, window=window, since=since, until=until, available=False,
                                  bin_seconds=bin_seconds, route=None, series=[], slowest=[])

    params = (service, since, until, method, route)
    with _snapshot():
        found = _routes(service, since, until, only=(method, route))
        rows = {int(s): tuple(rest) for s, *rest in db.execute_sql(_SERIES_SQL, (bin_seconds, *params))}
        parts: dict[int, dict[str, float]] = {}
        for slice_, key, total in db.execute_sql(_SERIES_PARTS_SQL, (bin_seconds, *params)):
            parts.setdefault(int(slice_), {})[key] = float(total or 0)
        slowest = [
            SlowRequest(started_at=started_at, duration_ms=round(duration, 1), status_code=status,
                        parts=dict(p or {}), calls=dict(c or {}), trace_id=trace_id, version=version)
            for started_at, duration, status, p, c, trace_id, version
            in db.execute_sql(_SLOWEST_SQL, (*params, SLOWEST))
        ]
    return RouteLatencyDetail(
        service=service, window=window, since=since, until=until, available=True, bin_seconds=bin_seconds,
        route=found[0] if found else None,
        series=fill_series(since, until, bin_seconds, rows, parts),
        slowest=slowest,
    )
