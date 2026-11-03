"""POST /pipelines/availability polls only the sources that are due, and skips the tick when none is."""

import os

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from api.v1 import pipelines as pipelines_api
from pipelines.availability import Due
from schemas.common import ApiStatus
from schemas.pipeline import PipelineResult

PATH = "/v1/internal/pipelines/availability"


def _client() -> TestClient:
    app = FastAPI()
    app.include_router(pipelines_api.router, prefix="/v1/internal")
    return TestClient(app)


def _auth() -> dict:
    return {"Authorization": f"Bearer {os.environ.get('PIPELINE_API_TOKEN', 'test-token')}"}


@pytest.fixture
def runs(monkeypatch):
    calls = []

    async def fake_run_pipeline(name, date_override=None, options=None, nba_date=None):
        calls.append((name, options))
        return PipelineResult(status=ApiStatus.SUCCESS, message="availability completed successfully",
                              started_at="2026-10-27T18:00:00", records_processed=3, records_skipped=1)

    monkeypatch.setattr(pipelines_api, "run_pipeline", fake_run_pipeline)
    return calls


@pytest.fixture
def due(monkeypatch):
    """What the route's due check returns, and the `force` it was asked with."""
    asked = []
    state = {"due": Due()}

    def due_now(force=False, now=None):
        asked.append(force)
        return state["due"]

    async def thread_hop(fn, *args, **kwargs):
        return fn(*args, **kwargs)

    monkeypatch.setattr(pipelines_api, "availability_due_now", due_now)
    monkeypatch.setattr(pipelines_api, "run_in_db_thread", thread_hop)
    state["asked"] = asked
    return state


@pytest.mark.api
def test_nothing_due_is_skipped_without_a_run(runs, due):
    res = _client().post(PATH, headers=_auth())
    assert res.status_code == 200
    assert res.json() == {"status": "skipped", "message": "No availability source is due", "data": None}
    assert runs == [] and due["asked"] == [False]


@pytest.mark.api
def test_a_due_source_runs_the_pipeline_with_just_that_source(runs, due):
    due["due"] = Due(espn_injuries=True, boxscore_games=("0022600101",))
    res = _client().post(PATH, headers=_auth())
    assert res.status_code == 200 and res.json()["status"] == "success"
    assert res.json()["data"]["records_processed"] == 3
    assert runs == [("availability", {"sources": ["espn_injuries"], "boxscore_games": ["0022600101"]})]


@pytest.mark.api
def test_force_asks_for_every_source(runs, due):
    due["due"] = Due(espn_injuries=True, espn_fantasy=True)
    res = _client().post(f"{PATH}?force=true", headers=_auth())
    assert res.status_code == 200 and due["asked"] == [True]
    assert runs == [("availability", {"sources": ["espn_injuries", "espn_fantasy"], "boxscore_games": []})]


@pytest.mark.api
def test_requires_bearer_token(runs, due):
    due["due"] = Due(espn_injuries=True)
    assert _client().post(PATH).status_code in (401, 403)
    assert runs == [] and due["asked"] == []
