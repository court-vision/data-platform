"""
services/latency_service.py's arithmetic: means per request, empty slices,
and the order routes are listed in. The SQL itself runs in
tests/integration/test_latency_queries.py.
"""

from datetime import datetime, timedelta, timezone

import pytest

from schemas.latency import RouteLatency
from services.latency_service import WINDOWS, fill_series, per_request, stats, total_time, window_bounds

NOW = datetime(2026, 10, 20, 18, 30, tzinfo=timezone.utc)


@pytest.mark.unit
def test_bucket_sums_become_means_per_request_and_empty_buckets_drop_out():
    assert per_request({"espn": 300.0, "postgres": 45.0, "redis": 0.0}, 3) == {"espn": 100.0, "postgres": 15.0}
    assert per_request({"espn": 5.0}, 0) == {}
    assert per_request({"postgres": 7.0}, 3, digits=2) == {"postgres": 2.33}


@pytest.mark.unit
def test_stats_round_to_a_tenth_and_keep_none_for_no_requests():
    full = stats((10, 1, 12.345, 80.0, 99.99, 20.04, 140.0))
    assert (full.requests, full.errors, full.p50_ms, full.p99_ms, full.mean_ms) == (10, 1, 12.3, 100.0, 20.0)
    empty = stats((0, 0, None, None, None, None, None))
    assert empty.requests == 0 and empty.p95_ms is None


@pytest.mark.unit
def test_routes_are_ordered_by_the_time_they_cost():
    busy = RouteLatency(method="GET", route="/v1/rankings", requests=1000, mean_ms=20.0)
    slow = RouteLatency(method="GET", route="/v1/matchups/{team_id}", requests=50, mean_ms=900.0)
    idle = RouteLatency(method="GET", route="/v1/rare", requests=0)
    assert sorted([idle, busy, slow], key=lambda r: -total_time(r)) == [slow, busy, idle]


@pytest.mark.unit
def test_the_series_covers_every_slice_of_the_window_oldest_first():
    since, until = window_bounds("24h", NOW)
    width = WINDOWS["24h"][1]
    busy = int(NOW.timestamp()) // width
    series = fill_series(since, until, width, {busy: (4, 1, 100.0, 250.0)}, {busy: {"espn": 600.0, "app": 40.0}})

    assert len(series) == 25  # 24 hours plus the hour now in progress
    assert series[0].at <= since < series[0].at + timedelta(seconds=width)
    assert all(a.at < b.at for a, b in zip(series, series[1:]))
    last = series[-1]
    assert (last.requests, last.errors, last.p50_ms, last.p95_ms) == (4, 1, 100.0, 250.0)
    assert last.parts == {"espn": 150.0, "app": 10.0}
    assert series[0].requests == 0 and series[0].p50_ms is None and series[0].parts == {}
