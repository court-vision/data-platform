"""
/v1/dashboard/latency: token-gated, the window and route passed through to
the service, and an honest message when the backend has not created the table
yet. The service is stubbed; its SQL runs in tests/integration.
"""

import os
from datetime import datetime, timezone

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from api.v1 import latency
from schemas.latency import LatencyOverview, LatencyStats, RouteLatency, RouteLatencyDetail

pytestmark = pytest.mark.api

_AUTH = {"Authorization": f"Bearer {os.environ.get('PIPELINE_API_TOKEN', 'test-token')}"}
SINCE = datetime(2026, 10, 19, 18, 0, tzinfo=timezone.utc)
UNTIL = datetime(2026, 10, 20, 18, 0, tzinfo=timezone.utc)
ROUTE = RouteLatency(
    method="GET", route="/v1/matchups/live/{team_id}", requests=40, errors=1,
    p50_ms=180.0, p95_ms=900.0, p99_ms=1400.0, mean_ms=260.0, max_ms=1500.0,
    parts={"espn": 200.0, "postgres": 20.0, "app": 40.0}, calls={"espn": 1.0, "postgres": 3.0},
)


@pytest.fixture
def client(monkeypatch):
    calls = []

    async def direct(fn, *args, **kwargs):
        return fn(*args, **kwargs)

    def overview(window, service):
        calls.append(("overview", window, service))
        return LatencyOverview(service=service, window=window, since=SINCE, until=UNTIL, available=True,
                               totals=LatencyStats(requests=40, errors=1, p50_ms=180.0), routes=[ROUTE])

    def route(method, path, window, service):
        calls.append(("route", method, path, window, service))
        found = ROUTE if path == ROUTE.route else None
        return RouteLatencyDetail(service=service, window=window, since=SINCE, until=UNTIL, available=True,
                                  bin_seconds=3600, route=found, series=[], slowest=[])

    monkeypatch.setattr(latency, "run_in_db_thread", direct)
    monkeypatch.setattr(latency, "build_overview", overview)
    monkeypatch.setattr(latency, "build_route", route)
    app = FastAPI()
    app.include_router(latency.router, prefix="/v1")
    test_client = TestClient(app)
    test_client.calls = calls
    return test_client


def test_needs_the_pipeline_token(client):
    assert client.get("/v1/dashboard/latency").status_code in (401, 403)
    assert client.get("/v1/dashboard/latency/route", params={"route": "/v1/x"}).status_code in (401, 403)


def test_overview_defaults_to_a_day_of_the_api(client):
    response = client.get("/v1/dashboard/latency", headers=_AUTH)

    assert response.status_code == 200
    body = response.json()
    assert body["message"] == "40 requests over 1 routes in 24h"
    assert body["data"]["routes"][0]["parts"] == {"espn": 200.0, "postgres": 20.0, "app": 40.0}
    assert client.calls == [("overview", "24h", "api")]


def test_an_unknown_window_is_refused(client):
    assert client.get("/v1/dashboard/latency", params={"window": "2d"}, headers=_AUTH).status_code == 422


def test_route_detail_takes_the_template_as_a_query_parameter(client):
    response = client.get("/v1/dashboard/latency/route", headers=_AUTH,
                          params={"route": "/v1/matchups/live/{team_id}", "method": "get", "window": "7d"})

    assert response.status_code == 200
    assert response.json()["data"]["route"]["route"] == "/v1/matchups/live/{team_id}"
    assert client.calls == [("route", "GET", "/v1/matchups/live/{team_id}", "7d", "api")]


def test_a_route_with_no_requests_says_so(client):
    body = client.get("/v1/dashboard/latency/route", params={"route": "/v1/quiet"}, headers=_AUTH).json()

    assert body["data"]["route"] is None
    assert body["message"] == "No GET /v1/quiet requests in 24h"


def test_before_the_backend_migration_the_page_is_told_why_it_is_empty(client, monkeypatch):
    monkeypatch.setattr(latency, "build_overview", lambda window, service: LatencyOverview(
        service=service, window=window, since=SINCE, until=UNTIL, available=False, totals=LatencyStats(), routes=[]))

    body = client.get("/v1/dashboard/latency", headers=_AUTH).json()

    assert body["data"]["available"] is False
    assert "ops.request_timings" in body["message"]
