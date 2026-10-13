"""
POST /v1/internal/pipelines/live-stats once the night is over.

cron-runner fires live-stats every 30 s until 4 AM ET, whatever the endpoint
says. Until 2026-10 every one of those polls ran the pipeline after the last
final: it fetched every box score again and rewrote every row, 531 runs on
2026-10-12 alone and half of all DB execution time in prod. When every game of
the date has its final box score stored (`stored_final_games`, tested against
Postgres in tests/integration/test_live_stats_settled.py), the endpoint now
answers what the scoreboard check would have said, all games complete, without
running anything.

The database and the NBA are faked at the model and extractor boundary: these
are contract tests for the endpoint's decisions.
"""

from __future__ import annotations

import os
from datetime import date, time

import pytest
from fastapi import FastAPI
from fastapi.routing import iter_route_contexts
from fastapi.testclient import TestClient
from freezegun import freeze_time

from api.v1 import pipelines as pipelines_api
from db.models.nba.games import Game
from db.models.nba.live_player_stats import LivePlayerStats
from pipelines import live_game_stats
from pipelines.extractors.nba_api import NBAApiExtractor
from pipelines.live_game_stats import LiveGameStatsPipeline, StoredFinalGames
from schemas.common import ApiStatus
from schemas.pipeline import PipelineResult

PATH = "/v1/internal/pipelines/live-stats"
GAME_DATE = date(2026, 3, 4)
DURING_GAMES = "2026-03-05T01:00:00Z"  # 20:00 ET; first tip 19:00 ET
BEFORE_TIP = "2026-03-04T22:00:00Z"    # 17:00 ET

ALL_STORED = StoredFinalGames(frozenset({"g1", "g2"}), frozenset({"g1", "g2"}), False)
ONE_STILL_ON = StoredFinalGames(frozenset({"g1", "g2"}), frozenset({"g1"}), False)


@pytest.fixture
def client():
    app = FastAPI()
    app.include_router(pipelines_api.router, prefix="/v1/internal")
    # Build the routes' dependants before freeze_time swaps `date` for FakeDate.
    list(iter_route_contexts(app.routes))
    return TestClient(app)


@pytest.fixture
def night(monkeypatch):
    """Two games tonight, first tip 19:00 ET. Returns what the endpoint did."""
    did = {"stored_reads": [], "runs": [], "scoreboard": 0, "finalize_by_time": 0, "finalize_all": 0}
    state = {"stored": ONE_STILL_ON, "scoreboard_final": False}

    async def same_thread(fn, *args, **kwargs):
        return fn(*args, **kwargs)

    monkeypatch.setattr(pipelines_api, "run_in_db_thread", same_thread)
    monkeypatch.setattr(Game, "get_games_on_date", classmethod(lambda cls, d: [object(), object()]))
    monkeypatch.setattr(Game, "get_earliest_game_time_on_date", classmethod(lambda cls, d: time(19, 0)))

    def stored(game_date, settle_minutes=None):
        did["stored_reads"].append(game_date)
        return state["stored"]

    monkeypatch.setattr(live_game_stats, "stored_final_games", stored)

    async def run(self, date_override=None, options=None, nba_date=None):
        did["runs"].append(nba_date)
        return PipelineResult(
            status=ApiStatus.SUCCESS, message="live_game_stats completed successfully",
            started_at="2026-03-05T01:00:00", records_processed=40, duration_seconds=8.5,
        )

    monkeypatch.setattr(LiveGameStatsPipeline, "run", run)

    def scoreboard(self, game_date):
        did["scoreboard"] += 1
        return state["scoreboard_final"]

    monkeypatch.setattr(NBAApiExtractor, "check_all_games_final", scoreboard)

    def by_time(cls, game_date, stale_minutes=30):
        did["finalize_by_time"] += 1
        return 0

    def all_final(cls, game_date):
        did["finalize_all"] += 1
        return 0

    monkeypatch.setattr(LivePlayerStats, "finalize_stale_by_time", classmethod(by_time))
    monkeypatch.setattr(LivePlayerStats, "finalize_stale_games", classmethod(all_final))
    did["state"] = state
    return did


def _post(client):
    token = os.environ.get("PIPELINE_API_TOKEN", "test-token")
    res = client.post(PATH, headers={"Authorization": f"Bearer {token}"})
    assert res.status_code == 200, res.text
    return res.json()


@pytest.mark.api
def test_a_night_whose_games_are_all_stored_final_is_done_without_a_run(client, night):
    night["state"]["stored"] = ALL_STORED

    with freeze_time(DURING_GAMES):
        body = _post(client)

    assert body["status"] == "success"
    assert body["message"] == f"All 2 games final and stored for {GAME_DATE}"
    data = body["data"]
    # What cron-runner and the dashboard read: the night is over.
    assert data["all_games_complete"] is True and data["done"] is True
    assert data["status"] == "skipped" and data["records_processed"] == 0
    assert data["games_total"] == 2

    assert night["stored_reads"] == [GAME_DATE]
    assert night["runs"] == []
    assert night["scoreboard"] == 0
    assert night["finalize_by_time"] == night["finalize_all"] == 0


@pytest.mark.api
def test_a_game_still_to_settle_runs_the_pipeline_and_asks_the_scoreboard(client, night):
    night["state"]["stored"] = ONE_STILL_ON

    with freeze_time(DURING_GAMES):
        body = _post(client)

    assert night["runs"] == [GAME_DATE]
    assert night["scoreboard"] == 1
    assert night["finalize_by_time"] == 1
    assert night["finalize_all"] == 0
    assert body["data"]["status"] == "success" and body["data"]["records_processed"] == 40
    assert body["data"]["all_games_complete"] is False and body["data"]["done"] is False


@pytest.mark.api
def test_the_scoreboard_still_ends_a_night_the_store_has_not_caught_up_with(client, night):
    """Final on the scoreboard but inside the settle margin: the old path, unchanged."""
    night["state"]["stored"] = ONE_STILL_ON
    night["state"]["scoreboard_final"] = True

    with freeze_time(DURING_GAMES):
        body = _post(client)

    assert night["runs"] == [GAME_DATE]
    assert night["finalize_all"] == 1
    assert body["data"]["all_games_complete"] is True and body["data"]["done"] is True


@pytest.mark.api
def test_a_store_that_cannot_be_read_falls_back_to_the_full_run(client, night, monkeypatch):
    def broken(game_date, settle_minutes=None):
        raise RuntimeError("relation nba.live_game_score_snapshots does not exist")

    monkeypatch.setattr(live_game_stats, "stored_final_games", broken)
    night["state"]["scoreboard_final"] = True

    with freeze_time(DURING_GAMES):
        body = _post(client)

    assert night["runs"] == [GAME_DATE]
    assert night["scoreboard"] == 1
    assert body["data"]["all_games_complete"] is True


@pytest.mark.api
def test_before_tip_off_the_live_tables_are_not_read(client, night):
    with freeze_time(BEFORE_TIP):
        body = _post(client)

    assert body["data"]["all_games_complete"] is False
    assert night["stored_reads"] == []
    assert night["runs"] == []


@pytest.mark.unit
@pytest.mark.parametrize(
    ("stored", "complete"),
    [
        (StoredFinalGames(frozenset(), frozenset(), False), False),  # no run yet tonight
        (ONE_STILL_ON, False),
        (ALL_STORED, True),
        # Every game settled, but some row of the date is still live.
        (StoredFinalGames(frozenset({"g1"}), frozenset({"g1"}), True), False),
    ],
)
def test_complete_means_every_listed_game_settled_and_no_live_row(stored, complete):
    assert stored.complete is complete
