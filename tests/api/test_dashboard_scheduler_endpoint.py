"""
GET /v1/dashboard/scheduler: the cron-runner's job runs over a chosen window,
for the Overview's range selector. Token-gated, bounded in hours and in rows.
The query is stubbed here (`CronJobRun` faked): no database.
"""

import os
from datetime import datetime, timedelta, timezone

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from api.v1 import dashboard
from core.middleware import setup_middleware

_AUTH = {"Authorization": f"Bearer {os.environ.get('PIPELINE_API_TOKEN', 'test-token')}"}
NOW = datetime(2026, 3, 5, 13, 0, tzinfo=timezone.utc)


def _make_app() -> FastAPI:
    app = FastAPI()
    setup_middleware(app)
    app.include_router(dashboard.router, prefix="/v1")
    return app


class _Row:
    def __init__(self, i: int):
        self.id = f"run-{i}"
        self.job_name = "live-stats"
        self.triggered_at = NOW - timedelta(minutes=i)
        self.completed_at = self.triggered_at + timedelta(seconds=2)
        self.duration_ms = 2000
        self.duration_seconds = 2.0
        self.result = "success"
        self.http_status = 200
        self.attempts = 1
        self.error_message = None
        self.response_snippet = '{"status": "success"}'


class _Query:
    """Enough of a peewee query for `_build_cron_runs`: where, order_by, limit, iterate."""

    def __init__(self, rows):
        self.rows, self.window_start, self.limited = rows, None, None

    def where(self, expression):
        self.window_start = expression.rhs
        return self

    def order_by(self, *_):
        return self

    def limit(self, n):
        self.limited = n
        return self

    def __iter__(self):
        return iter(self.rows if self.limited is None else self.rows[: self.limited])


@pytest.fixture
def cron_rows(monkeypatch):
    """Fake `CronJobRun.select()`; returns the query so a test can read what was asked."""
    query = _Query([_Row(i) for i in range(8)])

    class FakeCronJobRun:
        triggered_at = dashboard.CronJobRun.triggered_at

        @classmethod
        def select(cls):
            return query

    async def run_inline(fn, *args, **kwargs):
        return fn(*args, **kwargs)

    monkeypatch.setattr(dashboard, "CronJobRun", FakeCronJobRun)
    monkeypatch.setattr(dashboard, "run_in_db_thread", run_inline)
    return query


@pytest.mark.api
def test_scheduler_requires_bearer_token() -> None:
    assert TestClient(_make_app()).get("/v1/dashboard/scheduler").status_code in (401, 403)


@pytest.mark.api
@pytest.mark.parametrize("hours", ["0", "169", "soon"])
def test_hours_are_bounded_to_a_week(hours) -> None:
    response = TestClient(_make_app()).get(f"/v1/dashboard/scheduler?hours={hours}", headers=_AUTH)
    assert response.status_code == 422


@pytest.mark.api
def test_runs_come_newest_first_without_their_response_bodies(cron_rows) -> None:
    response = TestClient(_make_app()).get("/v1/dashboard/scheduler?hours=72", headers=_AUTH)
    assert response.status_code == 200
    data = response.json()["data"]
    assert (data["hours"], data["truncated"]) == (72, False)
    assert [run["id"] for run in data["runs"]] == [f"run-{i}" for i in range(8)]
    # A week of 60-second polls is thousands of rows: the bodies stay behind.
    assert all(run["response_snippet"] is None for run in data["runs"])
    # The window asked of the database is the hours asked of the endpoint.
    age = datetime.now(timezone.utc) - cron_rows.window_start
    assert timedelta(hours=72) <= age < timedelta(hours=72, minutes=1)


@pytest.mark.api
def test_a_window_holding_more_than_the_cap_says_it_was_cut(cron_rows, monkeypatch) -> None:
    monkeypatch.setattr(dashboard, "SCHEDULER_MAX_RUNS", 5)
    data = TestClient(_make_app()).get("/v1/dashboard/scheduler", headers=_AUTH).json()["data"]
    assert data["hours"] == 24  # the default
    assert (len(data["runs"]), data["truncated"]) == (5, True)
    assert cron_rows.limited == 6  # one over the cap is how a cut page is told from a full one


@pytest.mark.api
def test_a_window_that_fits_is_not_marked_cut(cron_rows, monkeypatch) -> None:
    monkeypatch.setattr(dashboard, "SCHEDULER_MAX_RUNS", 8)
    data = TestClient(_make_app()).get("/v1/dashboard/scheduler", headers=_AUTH).json()["data"]
    assert (len(data["runs"]), data["truncated"]) == (8, False)


@pytest.mark.api
def test_the_status_payload_still_carries_six_hours_with_bodies(cron_rows) -> None:
    runs = dashboard._build_cron_runs()
    assert cron_rows.limited is None
    assert runs[0].response_snippet == '{"status": "success"}'
    age = datetime.now(timezone.utc) - cron_rows.window_start
    assert timedelta(hours=6) <= age < timedelta(hours=6, minutes=1)
