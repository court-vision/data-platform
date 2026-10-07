"""POST /pipelines/lineup-snapshots runs the pipeline; ?date= becomes the backfill date."""

import os
from datetime import date

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from api.v1 import pipelines as pipelines_api
from schemas.common import ApiStatus
from schemas.pipeline import PipelineResult

PATH = "/v1/internal/pipelines/lineup-snapshots"


def _client() -> TestClient:
    app = FastAPI()
    app.include_router(pipelines_api.router, prefix="/v1/internal")
    return TestClient(app)


def _auth() -> dict:
    return {"Authorization": f"Bearer {os.environ.get('PIPELINE_API_TOKEN', 'test-token')}"}


@pytest.fixture
def run_calls(monkeypatch):
    calls = []

    async def fake_run_pipeline(name, date_override=None, options=None):
        calls.append((name, date_override, options))
        return PipelineResult(
            status=ApiStatus.SUCCESS, message="ok", started_at="2026-10-21T08:00:00", records_processed=0
        )

    monkeypatch.setattr(pipelines_api, "run_pipeline", fake_run_pipeline)
    return calls


@pytest.mark.api
def test_runs_the_nightly_catch_up(run_calls):
    res = _client().post(PATH, headers=_auth())
    assert res.status_code == 200 and res.json()["status"] == "success"
    assert run_calls == [("lineup_snapshots", None, None)]


@pytest.mark.api
def test_date_is_the_backfill_day(run_calls):
    res = _client().post(f"{PATH}?date=2026-10-21", headers=_auth())
    assert res.status_code == 200
    assert run_calls == [("lineup_snapshots", date(2026, 10, 21), None)]


@pytest.mark.api
def test_requires_the_pipeline_token(run_calls):
    res = _client().post(PATH)
    assert res.status_code in (401, 403)
    assert run_calls == []
