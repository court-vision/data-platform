"""
POST /v1/internal/cron/job-runs: consecutive failures of a job trigger one
`cron_failure_streak` alert exactly when the streak reaches the job's
threshold (live-stats: 3, unknown jobs: 2, deploy: 1), nothing on the runs
after it, and a "recovered" note when a success follows a streak that alerted.
`CronJobRun` and the DB thread helper are stubbed — no database.
"""

import os
import uuid
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from api.v1 import cron as cron_module
from api.v1.cron import consecutive_failures

BASE = datetime(2026, 8, 26, 1, 0, tzinfo=timezone.utc)


class FakeCronJobRun:
    rows = []

    @classmethod
    def create(cls, **fields):
        row = SimpleNamespace(id=uuid.uuid4(), **fields)
        cls.rows.append(row)
        return row

    @classmethod
    def recent_for_job(cls, job_name, limit=50):
        rows = [r for r in cls.rows if r.job_name == job_name]
        return sorted(rows, key=lambda r: r.triggered_at, reverse=True)[:limit]

    @classmethod
    def recent(cls, limit=200):
        return sorted(cls.rows, key=lambda r: r.triggered_at, reverse=True)[:limit]


@pytest.fixture
def client(monkeypatch):
    FakeCronJobRun.rows = []
    monkeypatch.setattr(cron_module, "CronJobRun", FakeCronJobRun)

    async def same_thread(fn, *args, **kwargs):
        return fn(*args, **kwargs)

    monkeypatch.setattr(cron_module, "run_in_db_thread", same_thread)
    app = FastAPI()
    app.include_router(cron_module.router, prefix="/v1/internal")
    return TestClient(app)


def _headers():
    return {"Authorization": f"Bearer {os.environ.get('PIPELINE_API_TOKEN', 'test-token')}"}


def _report(client, job, result, n, **extra):
    triggered = BASE + timedelta(minutes=n)
    body = {
        "job_name": job,
        "triggered_at": triggered.isoformat(),
        "completed_at": (triggered + timedelta(seconds=2)).isoformat(),
        "duration_ms": 2000,
        "result": result,
        "http_status": 200 if result == "success" else 502,
        "attempts": 1,
        "error_message": None if result == "success" else "HTTP 502 from data-platform",
        **extra,
    }
    res = client.post("/v1/internal/cron/job-runs", json=body, headers=_headers())
    assert res.status_code == 201, res.text
    return res


@pytest.mark.api
def test_consecutive_failures_counts_from_the_newest_row():
    assert consecutive_failures([]) == 0
    assert consecutive_failures(["failure", "failure", "success", "failure"]) == 2
    assert consecutive_failures(["success", "failure"]) == 0


@pytest.mark.api
def test_live_stats_alerts_at_three_then_once_and_recovers(client, alerts):
    _report(client, "live-stats", "failure", 0)
    _report(client, "live-stats", "failure", 1)
    assert alerts.events == []

    _report(client, "live-stats", "failure", 2)
    assert alerts.keys() == ["cron_failure_streak:live-stats"]
    event = alerts.events[0]
    assert event.severity == "critical"
    assert event.title == "Cron job failing: live-stats (3 in a row)"
    assert event.body == "HTTP 502 from data-platform"
    assert event.fields["streak"] == 3 and event.fields["threshold"] == 3
    assert event.fields["http_status"] == 502
    assert event.dedupe == timedelta(hours=6)

    _report(client, "live-stats", "failure", 3)  # a fourth: no new alert
    assert alerts.keys() == ["cron_failure_streak:live-stats"]

    _report(client, "live-stats", "success", 4)
    assert alerts.keys() == ["cron_failure_streak:live-stats", "cron_failure_streak:live-stats:recovered"]
    recovered = alerts.events[1]
    assert recovered.severity == "info"
    assert recovered.title == "Cron job recovered: live-stats"
    assert recovered.fields["failures_before"] == 4
    assert alerts.last_sent_at("cron_failure_streak:live-stats") is None  # the next streak alerts again

    assert len(FakeCronJobRun.rows) == 5  # every report was still recorded


@pytest.mark.api
def test_unknown_job_uses_the_default_threshold_of_two(client, alerts):
    """The production drill: three failures -> one embed, a fourth -> none, a success -> recovered."""
    _report(client, "alert-test", "failure", 0)
    assert alerts.events == []
    _report(client, "alert-test", "failure", 1)
    _report(client, "alert-test", "failure", 2)
    _report(client, "alert-test", "failure", 3)
    assert alerts.keys() == ["cron_failure_streak:alert-test"]
    assert alerts.events[0].fields["threshold"] == 2

    _report(client, "alert-test", "success", 4)
    assert alerts.keys()[-1] == "cron_failure_streak:alert-test:recovered"


@pytest.mark.api
def test_deploy_alerts_on_the_first_failure(client, alerts):
    _report(client, "deploy", "failure", 0, error_message="dispatch failed: data_platform")
    assert alerts.keys() == ["cron_failure_streak:deploy"]
    assert alerts.events[0].title == "Cron job failing: deploy (1 in a row)"


@pytest.mark.api
def test_success_after_a_short_streak_is_silent(client, alerts):
    _report(client, "live-stats", "failure", 0)
    _report(client, "live-stats", "success", 1)
    assert alerts.events == []


@pytest.mark.api
def test_streaks_are_per_job_and_a_success_resets_them(client, alerts):
    _report(client, "pre-game", "failure", 0)
    _report(client, "post-game", "failure", 1)
    _report(client, "pre-game", "success", 2)
    _report(client, "pre-game", "failure", 3)
    assert alerts.events == []  # pre-game: 1 (reset) 1; post-game: 1 — nobody reached 2

    _report(client, "post-game", "failure", 4)
    assert alerts.keys() == ["cron_failure_streak:post-game"]


@pytest.mark.api
def test_ingest_still_succeeds_when_alerting_blows_up(client, alerts, monkeypatch):
    def boom(*args, **kwargs):
        raise RuntimeError("alerting broke")

    monkeypatch.setattr(cron_module, "_alert_on_streak", boom)
    res = _report(client, "live-stats", "failure", 0)
    assert res.json() == {"status": "ok"}
    assert len(FakeCronJobRun.rows) == 1


@pytest.mark.api
def test_job_runs_require_the_pipeline_token(client):
    res = client.post("/v1/internal/cron/job-runs", json={})
    assert res.status_code in (401, 403)
