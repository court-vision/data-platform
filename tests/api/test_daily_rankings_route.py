"""POST /pipelines/daily-rankings gates on the season and on the day being stored, and
runs the morning's chain (the live projection, then the boards built on it) only
when there is a board to store. Once they are stored it starts the analyst's
morning run in the background, on this tick or a later one, until a run has succeeded."""

import os
from datetime import date, datetime

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


RECORDS = {"live_projection": 520, "daily_rankings": 1860}
CHAIN = [("live_projection", None), ("daily_rankings", None)]


@pytest.fixture
def results():
    """What each pipeline of the chain answers; a test swaps one for a failure."""
    return {
        name: PipelineResult(status=ApiStatus.SUCCESS, message=f"{name} completed successfully",
                             started_at="2026-11-16T05:00:00", records_processed=records)
        for name, records in RECORDS.items()
    }


@pytest.fixture
def run_calls(monkeypatch, results):
    calls = []

    async def fake_run_pipeline(name, date_override=None, options=None, nba_date=None):
        calls.append((name, options))
        return results[name]

    monkeypatch.setattr(pipelines_api, "run_pipeline", fake_run_pipeline)
    monkeypatch.setattr(pipelines_api, "daily_rankings_now", lambda: datetime(2026, 11, 16, 6, 0))
    return calls


@pytest.fixture
def gate(monkeypatch):
    """The gate's three reads, answered from memory: the date, the season's phase, the day's rows."""
    state = {"phase": "regular", "stored": Stored(cv=0, espn=0), "asked": [],
             # Why the analyst is not started (None: it is due), and what was started behind the answer
             "analyst": "the analyst has run this morning", "analyst_asked": [], "background": []}

    async def thread_hop(fn, *args, **kwargs):
        return fn(*args, **kwargs)

    def phase(today):
        if isinstance(state["phase"], Exception):
            raise state["phase"]
        return state["phase"]

    def stored(day):
        state["asked"].append(day)
        return state["stored"]

    def analyst_due(season, day):
        state["analyst_asked"].append(day)
        return state["analyst"]

    def background(coro):
        # Not run: what matters here is which pipeline was started behind the answer
        state["background"].append(coro.cr_frame.f_locals["name"])
        coro.close()

    monkeypatch.setattr(pipelines_api, "run_in_db_thread", thread_hop)
    monkeypatch.setattr(pipelines_api, "daily_rankings_today", lambda: TODAY)
    monkeypatch.setattr(pipelines_api, "get_season_phase", phase)
    monkeypatch.setattr(pipelines_api, "daily_rankings_stored_on", stored)
    monkeypatch.setattr(pipelines_api, "analyst_due_reason", analyst_due)
    monkeypatch.setattr(pipelines_api, "_start_in_background", background)
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
    # The projection first, so the boards are built on it; the answer is the boards'
    assert res.json()["data"]["records_processed"] == 1860
    assert run_calls == CHAIN
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
    assert run_calls == CHAIN


def _waiting(results):
    results["live_projection"] = PipelineResult(
        status=ApiStatus.ERROR, message="2 of the games of 2026-11-15 have no box score yet",
        started_at="2026-11-16T05:00:00", error="DataNotReady")


@pytest.mark.api
def test_early_in_the_morning_the_boards_wait_for_the_projection(run_calls, gate, results):
    _waiting(results)
    res = _client().post(PATH, headers=_auth())
    assert res.status_code == 200 and res.json()["status"] == "error"
    assert "no box score yet" in res.json()["message"]
    assert run_calls == [("live_projection", None)]


@pytest.mark.api
def test_late_in_the_morning_the_boards_are_stored_on_what_there_is(run_calls, gate, results, monkeypatch):
    _waiting(results)
    monkeypatch.setattr(pipelines_api, "daily_rankings_now", lambda: datetime(2026, 11, 16, 10, 0))
    res = _client().post(PATH, headers=_auth())
    assert res.json()["status"] == "success" and res.json()["data"]["records_processed"] == 1860
    assert run_calls == CHAIN


@pytest.mark.api
def test_a_forced_run_builds_the_boards_whatever_the_projection_did(run_calls, gate, results):
    _waiting(results)
    assert _client().post(f"{PATH}?force=true", headers=_auth()).json()["status"] == "success"
    assert run_calls == [("live_projection", {"force": True}), ("daily_rankings", {"force": True})]


@pytest.mark.api
@pytest.mark.parametrize("phase", ["offseason", "preseason"])
def test_outside_the_regular_season_is_skipped_without_a_query(run_calls, gate, phase):
    gate["phase"] = phase
    res = _client().post(PATH, headers=_auth())
    assert res.json() == {"status": "skipped", "message": "Outside the regular season: no rankings to store",
                          "data": None}
    assert run_calls == [] and gate["asked"] == []
    # Nor is the analyst asked about: there is nothing for it to read against
    assert gate["analyst_asked"] == [] and gate["background"] == []


@pytest.mark.api
def test_no_calendar_leaves_the_season_to_the_backend(run_calls, gate):
    gate["phase"] = FileNotFoundError("no calendar")
    assert _client().post(PATH, headers=_auth()).json()["status"] == "success"
    assert run_calls == CHAIN


@pytest.mark.api
def test_force_skips_both_gates_and_tells_the_pipeline(run_calls, gate):
    gate["phase"] = "offseason"
    gate["stored"] = Stored(cv=1040, espn=820)
    res = _client().post(f"{PATH}?force=true", headers=_auth())
    assert res.status_code == 200
    assert run_calls == [("live_projection", {"force": True}), ("daily_rankings", {"force": True})]
    assert gate["asked"] == []


@pytest.mark.api
def test_the_projection_has_its_own_route_for_a_run_on_demand(run_calls, gate):
    path = "/v1/internal/pipelines/live-projection"
    assert _client().post(path).status_code in (401, 403)
    res = _client().post(path, headers=_auth())
    assert res.status_code == 200 and res.json()["data"]["records_processed"] == 520
    assert _client().post(f"{path}?force=true", headers=_auth()).status_code == 200
    assert run_calls == [("live_projection", None), ("live_projection", {"force": True})]
    assert gate["asked"] == []


# ---- the analyst, started behind the answer ------------------------------------------------


@pytest.mark.api
def test_the_analyst_starts_in_the_background_once_the_boards_are_stored(run_calls, gate):
    gate["analyst"] = None
    res = _client().post(PATH, headers=_auth())
    # The answer is still the boards': the reads take minutes and are not waited for
    assert res.json()["status"] == "success" and res.json()["data"]["records_processed"] == 1860
    assert run_calls == CHAIN and gate["background"] == ["analyst"]
    assert gate["analyst_asked"] == [TODAY]


@pytest.mark.api
def test_a_later_tick_starts_the_analyst_again_until_a_run_has_succeeded(run_calls, gate):
    gate["stored"] = Stored(cv=1040, espn=820)
    gate["analyst"] = None
    res = _client().post(PATH, headers=_auth())
    assert res.json()["status"] == "skipped" and run_calls == []
    assert gate["background"] == ["analyst"]

    # Once it has run, the later ticks leave it alone
    gate["analyst"] = "the analyst has run this morning"
    _client().post(PATH, headers=_auth())
    assert gate["background"] == ["analyst"]


@pytest.mark.api
def test_the_analyst_waits_for_the_projection_with_the_boards(run_calls, gate, results):
    _waiting(results)
    gate["analyst"] = None
    _client().post(PATH, headers=_auth())
    assert run_calls == [("live_projection", None)] and gate["background"] == []


@pytest.mark.api
def test_a_forced_chain_outside_the_season_starts_no_analyst(run_calls, gate):
    gate["phase"] = "offseason"
    gate["analyst"] = None
    _client().post(f"{PATH}?force=true", headers=_auth())
    assert gate["background"] == [] and gate["analyst_asked"] == []


@pytest.mark.api
def test_the_analyst_has_its_own_route_for_a_run_on_demand(run_calls, gate, results):
    path = "/v1/internal/pipelines/analyst"
    results["analyst"] = PipelineResult(status=ApiStatus.SUCCESS, message="analyst completed successfully",
                                        started_at="2026-11-16T06:00:00", records_processed=1052)
    assert _client().post(path).status_code in (401, 403)

    # Started behind the answer: the run takes minutes
    res = _client().post(path, headers=_auth())
    assert res.status_code == 200
    assert res.json() == {"status": "success", "message": "The analyst's morning run was started", "data": None}
    assert gate["background"] == ["analyst"] and run_calls == []

    # ... or waited for
    res = _client().post(f"{path}?wait=true", headers=_auth())
    assert res.json()["data"]["records_processed"] == 1052
    assert run_calls == [("analyst", None)]
    # A run on demand does not ask whether one is due
    assert gate["analyst_asked"] == []
