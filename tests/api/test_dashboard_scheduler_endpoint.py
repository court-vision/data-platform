"""
GET /v1/dashboard/scheduler: the cron-runner's job runs over a chosen window,
for the Overview's range selector. Token-gated, bounded in hours and in rows.
The query is stubbed here (`CronJobRun` faked): no database. What a window
past a day counts, against real rows at cron-runner's cadence, is in
tests/integration/test_scheduler_window.py.
"""

import os
from datetime import datetime, timedelta, timezone

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from api.v1 import dashboard
from core.middleware import setup_middleware
from schemas.dashboard import SchedulerBucket, SchedulerRunsData

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
    response = TestClient(_make_app()).get("/v1/dashboard/scheduler?hours=12", headers=_AUTH)
    assert response.status_code == 200
    data = response.json()["data"]
    assert (data["hours"], data["truncated"]) == (12, False)
    assert [run["id"] for run in data["runs"]] == [f"run-{i}" for i in range(8)]
    # A day of 30-second polls is two thousand rows: the bodies stay behind.
    assert all(run["response_snippet"] is None for run in data["runs"])
    # Every run is in hand, so nothing is counted for the timeline.
    assert (data["buckets"], data["bucket_seconds"]) == (None, None)
    # The window asked of the database is the hours asked of the endpoint.
    age = datetime.now(timezone.utc) - cron_rows.window_start
    assert timedelta(hours=12) <= age < timedelta(hours=12, minutes=1)


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


@pytest.fixture
def counted(monkeypatch):
    """Stub the counting query; returns the hours it was asked for."""
    asked = []

    def build(hours):
        asked.append(hours)
        at = NOW.replace(tzinfo=None)
        return SchedulerRunsData(
            hours=hours,
            runs=dashboard._build_cron_runs(limit=1, snippets=False),
            buckets=[SchedulerBucket(
                job_name="live-stats", start=at - timedelta(minutes=45), runs=90, failed=2, retried=1,
                first_triggered_at=at - timedelta(minutes=45), last_triggered_at=at,
            )],
            bucket_seconds=hours * 3600 // 96,
            truncated=False,
            fetched_at=NOW,
        )

    monkeypatch.setattr(dashboard, "_build_scheduler_counts", build)
    return asked


@pytest.mark.api
@pytest.mark.parametrize("hours, column_seconds", [(25, 937), (72, 2700), (168, 6300)])
def test_past_a_day_the_runs_are_counted_not_carried(cron_rows, counted, hours, column_seconds) -> None:
    # cron-runner reports some 2,000 runs a day: three days of rows was over
    # the cap, so 3d and 7d came back as the same newest 57 hours.
    response = TestClient(_make_app()).get(f"/v1/dashboard/scheduler?hours={hours}", headers=_AUTH)
    assert response.status_code == 200
    body = response.json()
    data = body["data"]
    assert counted == [hours]
    assert (data["hours"], data["truncated"], data["bucket_seconds"]) == (hours, False, column_seconds)
    assert data["buckets"] == [{
        "job_name": "live-stats", "start": "2026-03-05T12:15:00", "runs": 90, "failed": 2, "retried": 1,
        "first_triggered_at": "2026-03-05T12:15:00", "last_triggered_at": "2026-03-05T13:00:00",
    }]
    assert [run["id"] for run in data["runs"]] == ["run-0"]
    assert body["message"] == f"90 cron runs in the last {hours} hours"  # the count, not the rows carried


@pytest.mark.api
def test_a_day_is_still_every_run(cron_rows, counted) -> None:
    data = TestClient(_make_app()).get("/v1/dashboard/scheduler?hours=24", headers=_AUTH).json()["data"]
    assert counted == [] and data["buckets"] is None
    assert len(data["runs"]) == 8
