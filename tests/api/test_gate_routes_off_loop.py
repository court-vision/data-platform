"""
The cron gate routes keep their blocking work off the event loop.

pre-game, post-game and live-stats are `async def`, so they run on the private
process's one event loop, next to /health, the scheduled-pickups tick and
cron-run ingestion. Until 2026-10 live-stats called the NBA scoreboard (a sync
retry that sleeps 2-30 s between attempts) and Peewee inline, and the post-game
gate did the same plus an ESPN request: on game nights the loop stalled 0.3-8 s
per call, which was about 21 false /health 503s and slow pickup ticks in a week.

Two nets. Every blocking function a route calls is replaced by a fake that
records whether it ran on the event loop's thread. Under those, Peewee's
`execute_sql` and requests' `Session.request` are tripwires, so a DB or HTTP
call that nobody faked (one added to a route later) is caught the same way.
`run_in_db_thread` becomes a plain thread hop, because there is no database
here to check a connection out of.
"""

from __future__ import annotations

import asyncio
import os
from datetime import date, time

import pytest
import requests
from fastapi import FastAPI
from fastapi.routing import iter_route_contexts
from fastapi.testclient import TestClient
from freezegun import freeze_time

from api.v1 import pipelines as pipelines_api
from db.base import db
from db.models.nba.games import Game
from db.models.nba.live_player_stats import LivePlayerStats
from db.models.nba.pipeline_batch import PipelineBatch
from db.models.pipeline_run import PipelineRun
from pipelines.extractors.nba_api import NBAApiExtractor
from pipelines.gates import GateDecision
from pipelines.live_game_stats import LiveGameStatsPipeline
from schemas.common import ApiStatus
from schemas.pipeline import PipelineResult

GAME_DATE = date(2026, 3, 4)
# The real batch runner; the fixture swaps the module's copy for a no-op.
RUN_BATCH = pipelines_api._run_pipelines_background


def _on_event_loop() -> bool:
    """True on a thread that is running an event loop: where a route body runs."""
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return False
    return True


class Blocking:
    """Records each blocking call as (name, ran_on_the_event_loop)."""

    def __init__(self, monkeypatch):
        self.monkeypatch = monkeypatch
        self.calls: list[tuple[str, bool]] = []

    def record(self, name: str) -> None:
        self.calls.append((name, _on_event_loop()))

    def fake(self, owner, attr: str, result=None, *, kind: str = "function"):
        """Replace `owner.attr` with a recorder returning `result` (called if callable)."""
        name = f"{getattr(owner, '__name__', owner)}.{attr}"

        def call(*args, **kwargs):
            self.record(name)
            return result(*args, **kwargs) if callable(result) else result

        if kind == "classmethod":
            replacement = classmethod(lambda cls, *a, **kw: call(*a, **kw))
        elif kind == "method":
            replacement = lambda self_, *a, **kw: call(*a, **kw)  # noqa: E731
        else:
            replacement = call
        self.monkeypatch.setattr(owner, attr, replacement)

    def names(self) -> set[str]:
        return {name for name, _ in self.calls}

    def on_the_loop(self) -> list[str]:
        return [name for name, on_loop in self.calls if on_loop]


@pytest.fixture
def blocking(monkeypatch):
    calls = Blocking(monkeypatch)

    async def thread_hop(fn, *args, **kwargs):
        return await asyncio.to_thread(fn, *args, **kwargs)

    monkeypatch.setattr(pipelines_api, "run_in_db_thread", thread_hop)

    def tripwire(name):
        def call(*args, **kwargs):
            calls.record(name)
            raise AssertionError(f"{name} was not faked in this test")

        return call

    monkeypatch.setattr(type(db), "execute_sql", tripwire("unfaked DB query"))
    monkeypatch.setattr(requests.Session, "request", tripwire("unfaked HTTP request"))

    # Writes and dispatch every route shares.
    calls.fake(PipelineBatch, "open", type("Row", (), {"id": 1})(), kind="classmethod")

    async def no_background(job_id, **kwargs):
        return None

    monkeypatch.setattr(pipelines_api, "_run_pipelines_background", no_background)
    return calls


@pytest.fixture
def client():
    app = FastAPI()
    app.include_router(pipelines_api.router, prefix="/v1/internal")
    # Build the routes' dependants before freeze_time swaps `date` for FakeDate
    # (see tests/api/test_post_game_batch.py).
    list(iter_route_contexts(app.routes))
    return TestClient(app)


def _post(client, slug: str):
    token = os.environ.get("PIPELINE_API_TOKEN", "test-token")
    res = client.post(f"/v1/internal/pipelines/{slug}", headers={"Authorization": f"Bearer {token}"})
    assert res.status_code == 200, res.text
    return res.json()


def _assert_off_the_loop(blocking: Blocking, expected: set[str]) -> None:
    assert blocking.on_the_loop() == [], "blocking calls made on the event loop"
    # Not vacuous: the route really went through each of these.
    assert expected <= blocking.names(), expected - blocking.names()
    assert not {n for n in blocking.names() if n.startswith("unfaked")}


@pytest.mark.api
def test_pre_game_reads_the_schedule_and_run_table_off_the_loop(client, blocking):
    blocking.fake(Game, "get_games_on_date", [object()], kind="classmethod")
    blocking.fake(Game, "get_earliest_game_time_on_date", time(19, 30), kind="classmethod")
    blocking.fake(PipelineRun, "was_successful_on_date", False, kind="classmethod")
    blocking.fake(PipelineRun, "is_running", False, kind="classmethod")

    # 19:20 ET: inside every pre-game pipeline's window.
    with freeze_time("2026-03-05T00:20:00Z"):
        body = _post(client, "pre-game")

    assert "triggered" in body["message"]
    _assert_off_the_loop(
        blocking,
        {
            "Game.get_games_on_date",
            "Game.get_earliest_game_time_on_date",
            "PipelineRun.was_successful_on_date",
            "PipelineRun.is_running",
            "PipelineBatch.open",
        },
    )


@pytest.mark.api
def test_post_game_gates_run_off_the_loop(client, blocking):
    """Inside the window, past 02:00 CST, so the ESPN gate and the attempt count are read too."""
    blocking.fake(Game, "get_latest_game_time_on_date", time(22, 0), kind="classmethod")
    blocking.fake(pipelines_api, "probe_espn_watermark", None)
    blocking.fake(NBAApiExtractor, "check_all_games_final", True, kind="method")
    blocking.fake(PipelineRun, "is_running", False, kind="classmethod")
    blocking.fake(PipelineRun, "was_successful_on_date", False, kind="classmethod")
    blocking.fake(PipelineRun, "count_since", 0, kind="classmethod")
    blocking.fake(
        pipelines_api,
        "_espn_scoring_period_advanced",
        GateDecision(True, "period_advanced", {}),
    )

    # 03:30 ET / 02:30 CST: the window for a 22:00 ET last tip is 00:30-04:00 ET.
    with freeze_time("2026-03-05T08:30:00Z"):
        body = _post(client, "post-game")

    assert "triggered" in body["message"]
    _assert_off_the_loop(
        blocking,
        {
            "Game.get_latest_game_time_on_date",
            "api.v1.pipelines.probe_espn_watermark",
            "NBAApiExtractor.check_all_games_final",
            "PipelineRun.is_running",
            "PipelineRun.was_successful_on_date",
            "PipelineRun.count_since",
            "api.v1.pipelines._espn_scoring_period_advanced",
            "PipelineBatch.open",
        },
    )


@pytest.mark.api
def test_the_dispatched_batch_checks_dependencies_off_the_loop(blocking, monkeypatch):
    """The batch a gate dispatches is a task on the same loop; its run-table reads hop too."""
    blocking.fake(PipelineRun, "was_successful_on_date", True, kind="classmethod")

    async def fake_run_pipeline(name, date_override=None, nba_date=None):
        return PipelineResult(status=ApiStatus.SUCCESS, message=name, started_at="2026-03-05T06:00:00")

    monkeypatch.setattr(pipelines_api, "run_pipeline", fake_run_pipeline)

    async def batch():
        manager = pipelines_api.get_job_manager()
        job = await manager.create_job(1)
        # player_season_stats depends on the game log, which is not in this
        # batch, so the run table is asked whether it succeeded tonight.
        await RUN_BATCH(job.job_id, pipeline_names=["player_season_stats"], nba_date=GAME_DATE)

    asyncio.run(batch())

    _assert_off_the_loop(blocking, {"PipelineRun.was_successful_on_date"})


def _live_stats_fakes(blocking: Blocking, *, db_games: bool):
    if db_games:
        blocking.fake(Game, "get_games_on_date", [object(), object()], kind="classmethod")
        blocking.fake(Game, "get_earliest_game_time_on_date", time(19, 0), kind="classmethod")
    else:
        blocking.fake(Game, "get_games_on_date", [], kind="classmethod")
        blocking.fake(
            NBAApiExtractor,
            "get_scoreboard_games",
            [{"game_id": "0042500101", "game_status": 2}],
            kind="method",
        )
    blocking.fake(NBAApiExtractor, "check_all_games_final", True, kind="method")
    blocking.fake(LivePlayerStats, "finalize_stale_by_time", 0, kind="classmethod")
    blocking.fake(LivePlayerStats, "finalize_stale_games", 0, kind="classmethod")

    # The pipeline's own run hops to a thread inside BasePipeline.run; this
    # stand-in only counts that it was started.
    runs: list[date] = []

    async def run(self, date_override=None, options=None, nba_date=None):
        runs.append(nba_date)
        return PipelineResult(
            status=ApiStatus.SUCCESS, message="ok", started_at="2026-03-05T01:00:00", records_processed=2
        )

    blocking.monkeypatch.setattr(LiveGameStatsPipeline, "run", run)
    return runs


@pytest.mark.api
def test_live_stats_reads_the_schedule_scoreboard_and_finalizes_off_the_loop(client, blocking):
    runs = _live_stats_fakes(blocking, db_games=True)

    # 20:00 ET, after a 19:00 ET first tip.
    with freeze_time("2026-03-05T01:00:00Z"):
        body = _post(client, "live-stats")

    assert runs == [GAME_DATE]
    assert body["data"]["all_games_complete"] is True
    _assert_off_the_loop(
        blocking,
        {
            "Game.get_games_on_date",
            "Game.get_earliest_game_time_on_date",
            "NBAApiExtractor.check_all_games_final",
            "LivePlayerStats.finalize_stale_by_time",
            "LivePlayerStats.finalize_stale_games",
        },
    )


@pytest.mark.api
def test_live_stats_scoreboard_fallback_runs_off_the_loop(client, blocking):
    """No games in nba.games (an unscheduled playoff round): the scoreboard is asked instead."""
    runs = _live_stats_fakes(blocking, db_games=False)

    with freeze_time("2026-03-05T01:00:00Z"):
        _post(client, "live-stats")

    assert runs == [GAME_DATE]

    _assert_off_the_loop(
        blocking,
        {
            "Game.get_games_on_date",
            "NBAApiExtractor.get_scoreboard_games",
            "NBAApiExtractor.check_all_games_final",
        },
    )
