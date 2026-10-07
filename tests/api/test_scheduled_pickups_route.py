"""POST /pipelines/scheduled-pickups gates on a due row and runs the pipeline only then."""

import os

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from api.v1 import pipelines as pipelines_api
from schemas.common import ApiStatus
from schemas.pipeline import PipelineResult

PATH = "/v1/internal/pipelines/scheduled-pickups"


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
        return PipelineResult(status=ApiStatus.SUCCESS, message="2 pickup(s) attempted",
                              started_at="2026-10-22T00:00:00", records_processed=1, records_skipped=1)

    monkeypatch.setattr(pipelines_api, "run_pipeline", fake_run_pipeline)
    return calls


def _due(monkeypatch, value):
    async def exists():
        return value
    monkeypatch.setattr(pipelines_api, "_due_pickups_exist", exists)


@pytest.mark.api
def test_nothing_due_is_skipped_without_a_run(run_calls, monkeypatch):
    _due(monkeypatch, False)
    res = _client().post(PATH, headers=_auth())
    assert res.status_code == 200
    assert res.json() == {"status": "skipped", "message": "No scheduled pickups are due", "data": None}
    assert run_calls == []


@pytest.mark.api
def test_a_due_row_runs_the_pipeline(run_calls, monkeypatch):
    _due(monkeypatch, True)
    res = _client().post(PATH, headers=_auth())
    assert res.status_code == 200 and res.json()["status"] == "success"
    assert res.json()["data"]["records_processed"] == 1
    assert run_calls == ["scheduled_pickups"]


@pytest.mark.api
def test_force_runs_it_regardless(run_calls, monkeypatch):
    _due(monkeypatch, False)
    res = _client().post(f"{PATH}?force=true", headers=_auth())
    assert res.status_code == 200 and run_calls == ["scheduled_pickups"]


@pytest.mark.api
def test_requires_bearer_token(run_calls, monkeypatch):
    _due(monkeypatch, True)
    assert _client().post(PATH).status_code in (401, 403)
    assert run_calls == []


@pytest.mark.api
def test_the_gate_query_reads_only_due_pending_rows():
    sql = pipelines_api.DUE_PICKUPS_SQL
    assert "status = 'pending'" in sql and "not_before_at <= now()" in sql and "next_attempt_at IS NULL" in sql
