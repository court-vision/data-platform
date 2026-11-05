"""POST /pipelines/dime-checkins gates on a switched-on team and runs the pipeline only then."""

import os

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from api.v1 import pipelines as pipelines_api
from schemas.common import ApiStatus
from schemas.pipeline import PipelineResult

PATH = "/v1/internal/pipelines/dime-checkins"


def _client() -> TestClient:
    app = FastAPI()
    app.include_router(pipelines_api.router, prefix="/v1/internal")
    return TestClient(app)


def _auth() -> dict:
    return {"Authorization": f"Bearer {os.environ.get('PIPELINE_API_TOKEN', 'test-token')}"}


@pytest.fixture
def run_calls(monkeypatch):
    calls = []

    async def fake_run_pipeline(name, date_override=None, options=None, nba_date=None):
        calls.append(name)
        return PipelineResult(status=ApiStatus.SUCCESS, message="1 check-in started",
                              started_at="2026-11-10T14:00:00", records_processed=1)

    monkeypatch.setattr(pipelines_api, "run_pipeline", fake_run_pipeline)
    return calls


def _on(monkeypatch, value):
    async def on():
        return value
    monkeypatch.setattr(pipelines_api, "_autopilot_on", on)


@pytest.mark.api
def test_no_team_switched_on_is_skipped_without_a_run(run_calls, monkeypatch):
    _on(monkeypatch, False)
    res = _client().post(PATH, headers=_auth())
    assert res.status_code == 200
    assert res.json() == {"status": "skipped", "message": "No team has Dime's check-ins switched on", "data": None}
    assert run_calls == []


@pytest.mark.api
def test_a_switched_on_team_runs_the_pipeline(run_calls, monkeypatch):
    _on(monkeypatch, True)
    res = _client().post(PATH, headers=_auth())
    assert res.status_code == 200 and res.json()["status"] == "success"
    assert run_calls == ["dime_checkins"]


@pytest.mark.api
def test_force_runs_it_regardless(run_calls, monkeypatch):
    _on(monkeypatch, False)
    res = _client().post(f"{PATH}?force=true", headers=_auth())
    assert res.status_code == 200 and run_calls == ["dime_checkins"]


@pytest.mark.api
def test_requires_bearer_token(run_calls, monkeypatch):
    _on(monkeypatch, True)
    assert _client().post(PATH).status_code in (401, 403)
    assert run_calls == []


@pytest.mark.api
def test_the_gate_reads_switched_on_rows_once_the_table_exists():
    assert "to_regclass('usr.autopilot')" in pipelines_api.AUTOPILOT_TABLE_SQL
    assert pipelines_api.AUTOPILOT_ON_SQL == "SELECT EXISTS (SELECT 1 FROM usr.autopilot WHERE paused_at IS NULL)"
