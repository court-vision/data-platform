"""POST /pipelines/game-history threads seasons and season types into the
pipeline; POST /pipelines/playoffs runs the bracket, then the postseason box scores."""

import asyncio
import os

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from api.v1 import pipelines as pipelines_api
from schemas.common import ApiStatus
from schemas.pipeline import PipelineResult

PATH = "/v1/internal/pipelines/game-history"


def _client() -> TestClient:
    app = FastAPI()
    app.include_router(pipelines_api.router, prefix="/v1/internal")
    return TestClient(app)


def _auth() -> dict:
    return {"Authorization": f"Bearer {os.environ.get('PIPELINE_API_TOKEN', 'test-token')}"}


class _Calls(list):
    """The runs asked for, plus `outcomes`: a pipeline's (status, message) when not a success."""

    outcomes: dict


@pytest.fixture
def run_calls(monkeypatch):
    calls = _Calls()
    outcomes = {}

    async def fake_run_pipeline(name, date_override=None, options=None):
        calls.append((name, options))
        status, message = outcomes.get(name, (ApiStatus.SUCCESS, f"{name} ok"))
        return PipelineResult(status=status, message=message, started_at="2026-10-12T00:00:00", records_processed=0)

    monkeypatch.setattr(pipelines_api, "run_pipeline", fake_run_pipeline)
    calls.outcomes = outcomes
    return calls


@pytest.mark.api
def test_seasons_and_types_reach_the_pipeline(run_calls):
    res = _client().post(f"{PATH}?seasons=2012-13&seasons=2013-14&season_types=playoffs", headers=_auth())
    assert res.status_code == 200 and res.json()["status"] == "success"
    assert run_calls == [("game_history", {"seasons": ["2012-13", "2013-14"], "season_types": ["playoffs"]})]


@pytest.mark.api
def test_no_seasons_means_this_seasons_postseason(run_calls):
    assert _client().post(PATH, headers=_auth()).status_code == 200
    assert run_calls == [("game_history", None)]


@pytest.mark.api
@pytest.mark.parametrize("query", [
    "seasons=2012",                          # not a season
    "seasons=2012-13&season_types=finals",   # not a season type
    "season_types=playoffs",                 # a type with no season
    "seasons=2012-14",                       # not one season
    "seasons=2011-12",                       # before the history (SEA, NJN have no alias)
])
def test_bad_input_is_refused_before_any_run(run_calls, query):
    assert _client().post(f"{PATH}?{query}", headers=_auth()).status_code == 422
    assert run_calls == []


@pytest.fixture
def started(monkeypatch):
    """What /playoffs starts in the background, run here once the response is back."""
    coros = []
    monkeypatch.setattr(pipelines_api, "_start_in_background", coros.append)
    return coros


@pytest.mark.api
def test_playoffs_answers_for_the_bracket_and_starts_the_postseason_load(run_calls, started):
    res = _client().post("/v1/internal/pipelines/playoffs", headers=_auth())
    assert res.status_code == 200 and res.json()["message"] == "playoff_bracket ok"
    assert run_calls == [("playoff_bracket", None)]

    asyncio.run(started[0])   # game_history, with no options: this season's postseason

    assert run_calls == [("playoff_bracket", None), ("game_history", None)]


@pytest.mark.api
def test_a_failed_postseason_load_does_not_change_the_bracket_answer(run_calls, started):
    run_calls.outcomes["game_history"] = (ApiStatus.ERROR, "NBA API timeout")
    body = _client().post("/v1/internal/pipelines/playoffs", headers=_auth()).json()
    assert body["status"] == "success"   # its own run row and alert carry the failure
    started[0].close()
