"""POST /pipelines/daily-rankings gates on the season and on the day being stored, and
runs the pipeline only when there is a board to store."""

import os
from datetime import date

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from api.v1 import pipelines as pipelines_api
from pipelines.daily_rankings import Stored
from schemas.common import ApiStatus
from schemas.pipeline import PipelineResult

PATH = "/v1/internal/pipelines/daily-rankings"
TODAY = date(2026, 11, 16)


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
        calls.append((name, options))
        return PipelineResult(status=ApiStatus.SUCCESS, message="daily_rankings completed successfully",
                              started_at="2026-11-16T05:00:00", records_processed=1860)

    monkeypatch.setattr(pipelines_api, "run_pipeline", fake_run_pipeline)
    return calls


@pytest.fixture
def gate(monkeypatch):
    """The gate's three reads, answered from memory: the date, the season's phase, the day's rows."""
    state = {"phase": "regular", "stored": Stored(cv=0, espn=0), "asked": []}

    async def thread_hop(fn, *args, **kwargs):
        return fn(*args, **kwargs)

    def phase(today):
        if isinstance(state["phase"], Exception):
            raise state["phase"]
        return state["phase"]

    def stored(day):
        state["asked"].append(day)
        return state["stored"]

    monkeypatch.setattr(pipelines_api, "run_in_db_thread", thread_hop)
    monkeypatch.setattr(pipelines_api, "daily_rankings_today", lambda: TODAY)
    monkeypatch.setattr(pipelines_api, "get_season_phase", phase)
    monkeypatch.setattr(pipelines_api, "daily_rankings_stored_on", stored)
    return state


@pytest.mark.api
def test_needs_the_pipeline_token(run_calls, gate):
    assert _client().post(PATH).status_code in (401, 403)
    assert _client().post(PATH, headers={"Authorization": "Bearer not-the-token"}).status_code in (401, 403)
    assert run_calls == []


@pytest.mark.api
def test_a_morning_with_nothing_stored_runs_the_pipeline(run_calls, gate):
    res = _client().post(PATH, headers=_auth())
    assert res.status_code == 200 and res.json()["status"] == "success"
    assert res.json()["data"]["records_processed"] == 1860
    assert run_calls == [("daily_rankings", None)]
    assert gate["asked"] == [TODAY]


@pytest.mark.api
def test_a_stored_day_is_skipped_without_a_run(run_calls, gate):
    gate["stored"] = Stored(cv=1040, espn=820)
    res = _client().post(PATH, headers=_auth())
    assert res.status_code == 200
    assert res.json() == {"status": "skipped", "message": "Today's rankings are already stored", "data": None}
    assert run_calls == []


@pytest.mark.api
def test_a_day_missing_espns_ranks_runs_again(run_calls, gate):
    gate["stored"] = Stored(cv=1040, espn=0)
    assert _client().post(PATH, headers=_auth()).json()["status"] == "success"
    assert run_calls == [("daily_rankings", None)]


@pytest.mark.api
@pytest.mark.parametrize("phase", ["offseason", "preseason"])
def test_outside_the_regular_season_is_skipped_without_a_query(run_calls, gate, phase):
    gate["phase"] = phase
    res = _client().post(PATH, headers=_auth())
    assert res.json() == {"status": "skipped", "message": "Outside the regular season: no rankings to store",
                          "data": None}
    assert run_calls == [] and gate["asked"] == []


@pytest.mark.api
def test_no_calendar_leaves_the_season_to_the_backend(run_calls, gate):
    gate["phase"] = FileNotFoundError("no calendar")
    assert _client().post(PATH, headers=_auth()).json()["status"] == "success"
    assert run_calls == [("daily_rankings", None)]


@pytest.mark.api
def test_force_skips_both_gates_and_tells_the_pipeline(run_calls, gate):
    gate["phase"] = "offseason"
    gate["stored"] = Stored(cv=1040, espn=820)
    res = _client().post(f"{PATH}?force=true", headers=_auth())
    assert res.status_code == 200 and run_calls == [("daily_rankings", {"force": True})]
    assert gate["asked"] == []
