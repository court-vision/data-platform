"""
POST /v1/internal/pipelines/post-game: the durable batch record and the
end-of-night completeness sweep.

The failure this covers is the one from `docs/PENDING_PROD_CHECKS.md` #3: every
individual poll correctly decided there was nothing to do, nothing ever asked
whether the night as a whole had come up empty, and the evidence had to be
reconstructed afterwards from the shape of the data that was missing.

The database is faked at the model boundary — these are contract tests for the
endpoint's decisions, not for Peewee.
"""

import asyncio
import os
from datetime import date, datetime, time

import pytest
from fastapi import FastAPI
from fastapi.routing import iter_route_contexts
from fastapi.testclient import TestClient
from freezegun import freeze_time

from api.v1 import pipelines as pipelines_api
from db.models.nba.games import Game
from db.models.nba.pipeline_batch import PipelineBatch
from db.models.pipeline_run import PipelineRun

PATH = "/v1/internal/pipelines/post-game"
NBA_DATE = date(2026, 3, 4)

# Latest tip 22:00 ET -> window opens 00:30, closes 04:00 ET on the 5th.
AFTER_WINDOW = "2026-03-05T10:00:00Z"   # 05:00 ET
BEFORE_WINDOW = "2026-03-05T04:00:00Z"  # 23:00 ET on the 4th


@pytest.fixture
def client():
    app = FastAPI()
    app.include_router(pipelines_api.router, prefix="/v1/internal")
    # FastAPI builds an included route's dependant on first use. Under
    # freeze_time, datetime.date is freezegun's FakeDate, and pydantic cannot
    # build a schema for the real `date` query parameters. Build them now.
    list(iter_route_contexts(app.routes))
    return TestClient(app)


def _headers():
    return {"Authorization": f"Bearer {os.environ.get('PIPELINE_API_TOKEN', 'test-token')}"}


@pytest.fixture
def batches(monkeypatch):
    """Capture batch rows instead of writing them; expose the sweep latch."""

    class Written(list):
        """The batch rows, with the sweep latch hanging off them."""

        state = {"swept": False}

    written = Written()
    written.state = state = {"swept": False}

    async def same_thread(fn, *args, **kwargs):
        return fn(*args, **kwargs)

    def fake_open(cls, category, nba_date, decision, reason, pipelines=None,
                  job_id=None, forced=False, alerted=False):
        row = {
            "category": category, "nba_date": nba_date, "decision": decision,
            "reason": reason, "pipelines": pipelines or {}, "job_id": job_id,
            "forced": forced, "alerted": alerted,
        }
        written.append(row)
        return type("Row", (), {"id": len(written)})()

    monkeypatch.setattr(pipelines_api, "run_in_db_thread", same_thread)
    monkeypatch.setattr(PipelineBatch, "open", classmethod(fake_open))
    monkeypatch.setattr(
        PipelineBatch, "swept", classmethod(lambda cls, c, d: state["swept"])
    )
    monkeypatch.setattr(
        Game, "get_latest_game_time_on_date", classmethod(lambda cls, d: time(22, 0))
    )
    return written


def _ran(monkeypatch, names: set[str]):
    """Only `names` have a successful run for the date."""
    monkeypatch.setattr(
        PipelineRun,
        "was_successful_on_date",
        classmethod(lambda cls, name, d, after=None: name in names),
    )


@pytest.mark.api
class TestCompletenessSweep:
    def test_a_night_where_nothing_ran_alerts_once(self, client, batches, alerts, monkeypatch):
        _ran(monkeypatch, set())

        with freeze_time(AFTER_WINDOW):
            res = client.post(PATH, headers=_headers())

        assert res.status_code == 200
        assert "never ran" in res.json()["message"]

        row = next(r for r in batches if r["decision"] == "window_closed")
        assert row["reason"] == "incomplete"
        assert row["nba_date"] == NBA_DATE
        assert row["alerted"] is True
        # Every post-game pipeline is named, not just a count.
        assert all(p["status"] == "never_ran" for p in row["pipelines"].values())

        assert alerts.keys() == [f"post_game_incomplete:{NBA_DATE}"]
        event = alerts.events[0]
        assert event.severity == "critical"
        assert "daily_matchup_scores" in event.fields["missing"]

    def test_a_complete_night_records_but_does_not_alert(self, client, batches, alerts, monkeypatch):
        from pipelines import get_pipelines_by_category
        from pipelines.config import PipelineCategory

        everything = {
            cls.config.name
            for cls in get_pipelines_by_category(PipelineCategory.POST_GAME)
        }
        _ran(monkeypatch, everything)

        with freeze_time(AFTER_WINDOW):
            res = client.post(PATH, headers=_headers())

        assert "all pipelines ran" in res.json()["message"]
        row = next(r for r in batches if r["decision"] == "window_closed")
        assert row["reason"] == "complete" and row["alerted"] is False
        assert alerts.events == []

    def test_partial_night_names_only_what_is_missing(self, client, batches, alerts, monkeypatch):
        from pipelines import get_pipelines_by_category
        from pipelines.config import PipelineCategory

        everything = {
            cls.config.name
            for cls in get_pipelines_by_category(PipelineCategory.POST_GAME)
        }
        _ran(monkeypatch, everything - {"daily_matchup_scores"})

        with freeze_time(AFTER_WINDOW):
            client.post(PATH, headers=_headers())

        row = next(r for r in batches if r["decision"] == "window_closed")
        assert row["pipelines"]["daily_matchup_scores"]["status"] == "never_ran"
        assert alerts.events[0].fields["missing"] == "daily_matchup_scores"
        assert alerts.events[0].fields["ran"] == len(everything) - 1

    def test_the_sweep_runs_once_per_night(self, client, batches, alerts, monkeypatch):
        """The endpoint keeps being polled for hours after the window closes.

        The latch is durable (a `window_closed` row) rather than in-memory,
        because the nightly deploy lands at 03:00 CST — inside the window this
        has to survive.
        """
        _ran(monkeypatch, set())

        with freeze_time(AFTER_WINDOW):
            client.post(PATH, headers=_headers())
            batches.state["swept"] = True   # the row the first poll wrote
            res = client.post(PATH, headers=_headers())

        assert "already swept" in res.json()["message"]
        assert len([r for r in batches if r["decision"] == "window_closed"]) == 1
        assert len(alerts.events) == 1

    def test_before_the_window_nothing_is_swept_or_recorded(self, client, batches, alerts, monkeypatch):
        _ran(monkeypatch, set())

        with freeze_time(BEFORE_WINDOW):
            res = client.post(PATH, headers=_headers())

        assert res.json()["message"] == "Outside post-game window"
        assert batches == []
        assert alerts.events == []

    def test_no_games_means_no_sweep(self, client, batches, alerts, monkeypatch):
        """An off day is not an incomplete night."""
        monkeypatch.setattr(
            Game, "get_latest_game_time_on_date", classmethod(lambda cls, d: None)
        )
        _ran(monkeypatch, set())

        with freeze_time(AFTER_WINDOW):
            res = client.post(PATH, headers=_headers())

        assert "No games scheduled" in res.json()["message"]
        assert batches == []
        assert alerts.events == []


@pytest.mark.api
class TestBatchRecords:
    def test_an_all_skipped_batch_is_recorded_with_per_pipeline_reasons(
        self, client, batches, alerts, monkeypatch
    ):
        """"Nothing to do" is a decision, and it is the one worth keeping."""
        from pipelines import get_pipelines_by_category
        from pipelines.config import PipelineCategory
        from pipelines.extractors.nba_api import NBAApiExtractor

        everything = {
            cls.config.name
            for cls in get_pipelines_by_category(PipelineCategory.POST_GAME)
        }
        _ran(monkeypatch, everything)
        monkeypatch.setattr(PipelineRun, "is_running", classmethod(lambda cls, n, **kw: False))
        monkeypatch.setattr(PipelineRun, "count_since", classmethod(lambda cls, n, c: 1))
        monkeypatch.setattr(
            NBAApiExtractor, "check_all_games_final", lambda self, d: True
        )
        monkeypatch.setattr(pipelines_api, "probe_espn_watermark", lambda: None)
        monkeypatch.setattr(
            pipelines_api,
            "_espn_scoring_period_advanced",
            lambda nba_date, observed=None, succeeded_tonight=False, attempts_tonight=0: (
                pipelines_api.GateDecision(False, "period_unchanged_already_ran", {})
            ),
        )

        # 03:00 CST: past daily_matchup_scores' earliest_run_time_cst of 02:00,
        # so the ESPN gate is what decides rather than the time gate.
        with freeze_time("2026-03-05T09:00:00Z"):
            res = client.post(PATH, headers=_headers())

        assert "already completed" in res.json()["message"]
        row = next(r for r in batches if r["decision"] == "all_skipped")
        assert row["reason"] == "already_complete"
        assert row["pipelines"]["player_game_stats"]["reason"] == "already_ran"
        assert (
            row["pipelines"]["daily_matchup_scores"]["reason"]
            == "period_unchanged_already_ran"
        )


@pytest.mark.api
class TestTheProbeStillSamplesTheWholeNight:
    """`probe_espn_watermark` runs on every poll of a night that has games.

    Not just inside the post-game window. The measurement it exists for
    (`docs/PENDING_PROD_CHECKS.md` #4) is the interval between ESPN advancing
    `latestScoringPeriod` and its totals absorbing the day, and that flip has to
    be bracketed across the whole 20:00–07:59 CST span the cron polls. Narrowing
    it to the window would leave the flag flip unmeasurable while every test
    still passed.
    """

    @pytest.fixture
    def probes(self, monkeypatch):
        calls = []
        monkeypatch.setattr(
            pipelines_api, "probe_espn_watermark", lambda: calls.append(1) or None
        )
        return calls

    def test_probes_before_the_window_opens(self, client, batches, probes, monkeypatch):
        _ran(monkeypatch, set())
        with freeze_time(BEFORE_WINDOW):
            client.post(PATH, headers=_headers())
        assert len(probes) == 1

    def test_probes_after_the_window_closes(self, client, batches, probes, monkeypatch):
        _ran(monkeypatch, set())
        with freeze_time(AFTER_WINDOW):
            client.post(PATH, headers=_headers())
        assert len(probes) == 1

    def test_does_not_probe_on_a_night_with_no_games(self, client, batches, probes, monkeypatch):
        monkeypatch.setattr(
            Game, "get_latest_game_time_on_date", classmethod(lambda cls, d: None)
        )
        _ran(monkeypatch, set())
        with freeze_time(AFTER_WINDOW):
            client.post(PATH, headers=_headers())
        assert probes == []


# Window for NBA_DATE: latest tip 22:00 ET + 150 min = 00:30 ET on the 5th.
WINDOW_OPEN_UTC = datetime(2026, 3, 5, 5, 30)
IN_WINDOW = "2026-03-05T06:00:00Z"  # 01:00 ET
# The run that covered the night before: 01:00 ET on the 4th, which is after
# midnight UTC of NBA_DATE.
LAST_NIGHT_UTC = datetime(2026, 3, 4, 6, 0)


def _only_last_night_succeeded(monkeypatch):
    """Every pipeline's one successful run is last night's."""

    def was_successful(cls, name, nba_date, after=None):
        cutoff = after if after is not None else datetime.combine(nba_date, time.min)
        return LAST_NIGHT_UTC >= cutoff

    monkeypatch.setattr(
        PipelineRun, "was_successful_on_date", classmethod(was_successful)
    )


@pytest.mark.api
class TestDependenciesMeanTonight:
    """A post-game dependency is met by tonight's run, not last night's.

    The date-based cutoff is midnight UTC of the NBA date, and post-game runs
    start after midnight UTC, so last night's success sits inside tonight's
    cutoff. Through 2025-26 that let `player_season_stats` run at the first
    poll, before `player_game_stats` had the night's games: on 38 nights the
    season rows were written ahead of the game log, with the late games'
    players a game behind.
    """

    def test_last_nights_run_satisfies_the_date_cutoff_but_not_the_window(self, monkeypatch):
        from pipelines import PIPELINE_REGISTRY

        _only_last_night_succeeded(monkeypatch)
        season_stats = PIPELINE_REGISTRY["player_season_stats"]

        with freeze_time(IN_WINDOW):
            loose = pipelines_api._check_unmet_dependencies(season_stats, set())
            tonight = pipelines_api._check_unmet_dependencies(
                season_stats, set(), since=WINDOW_OPEN_UTC
            )

        assert loose == []  # the reading the pre-game batch still wants
        assert tonight == ["player_game_stats"]

    def test_a_success_earlier_in_the_batch_still_counts(self, monkeypatch):
        from pipelines import PIPELINE_REGISTRY

        _only_last_night_succeeded(monkeypatch)

        with freeze_time(IN_WINDOW):
            unmet = pipelines_api._check_unmet_dependencies(
                PIPELINE_REGISTRY["team_stats"],
                {"player_game_stats", "game_schedule"},
                since=WINDOW_OPEN_UTC,
            )

        assert unmet == []

    def test_the_batch_hands_its_window_to_the_dependency_check(
        self, client, batches, alerts, monkeypatch
    ):
        from pipelines.extractors.nba_api import NBAApiExtractor

        dispatched = []

        def fake_background(job_id, **kwargs):
            dispatched.append(kwargs)
            return asyncio.sleep(0)

        _ran(monkeypatch, set())
        monkeypatch.setattr(PipelineRun, "is_running", classmethod(lambda cls, n, **kw: False))
        monkeypatch.setattr(PipelineRun, "count_since", classmethod(lambda cls, n, c: 0))
        monkeypatch.setattr(NBAApiExtractor, "check_all_games_final", lambda self, d: True)
        monkeypatch.setattr(pipelines_api, "probe_espn_watermark", lambda: None)
        monkeypatch.setattr(pipelines_api, "_run_pipelines_background", fake_background)

        with freeze_time(IN_WINDOW):
            res = client.post(PATH, headers=_headers())

        assert "triggered" in res.json()["message"]
        assert len(dispatched) == 1
        assert dispatched[0]["dependencies_since"] == WINDOW_OPEN_UTC
        assert "player_season_stats" in dispatched[0]["pipeline_names"]

    def test_a_forced_batch_keeps_the_date_cutoff(self, client, batches, alerts, monkeypatch):
        """A backfill has no window; its dependencies are judged by the date given."""
        dispatched = []

        def fake_background(job_id, **kwargs):
            dispatched.append(kwargs)
            return asyncio.sleep(0)

        monkeypatch.setattr(pipelines_api, "_run_pipelines_background", fake_background)

        with freeze_time(IN_WINDOW):
            client.post(PATH, headers=_headers(), params={"date": "2026-03-01"})

        assert dispatched[0]["dependencies_since"] is None

    def test_a_forced_run_on_a_day_without_games_keeps_the_date_cutoff(
        self, client, batches, alerts, monkeypatch
    ):
        """No games, no window to count "tonight" from."""
        dispatched = []

        def fake_background(job_id, **kwargs):
            dispatched.append(kwargs)
            return asyncio.sleep(0)

        monkeypatch.setattr(
            Game, "get_latest_game_time_on_date", classmethod(lambda cls, d: None)
        )
        monkeypatch.setattr(pipelines_api, "_run_pipelines_background", fake_background)

        with freeze_time(IN_WINDOW):
            client.post(PATH, headers=_headers(), params={"force": "true"})

        assert dispatched[0]["dependencies_since"] is None

    def test_a_forced_rerun_of_tonights_batch_still_holds_the_dependents(
        self, client, batches, alerts, monkeypatch
    ):
        """`?force=true` skips the gates, not the dependency rule.

        The night an operator reaches for it is a night things are being held.
        On the date cutoff, a game log that failed in the forced batch left
        season, rolling and team stats running on last night's success, and
        that run then deduped them for the rest of the night.
        """
        from schemas.pipeline import PipelineResult

        run_batch = pipelines_api._run_pipelines_background
        dispatched = []

        def fake_background(job_id, **kwargs):
            dispatched.append(kwargs)
            return asyncio.sleep(0)

        _only_last_night_succeeded(monkeypatch)
        monkeypatch.setattr(pipelines_api, "_run_pipelines_background", fake_background)

        with freeze_time(IN_WINDOW):
            client.post(PATH, headers=_headers(), params={"force": "true"})

        assert len(dispatched) == 1
        assert dispatched[0]["dependencies_since"] == WINDOW_OPEN_UTC
        # Tonight's batch, not a backfill: the pipelines keep their own checks.
        assert dispatched[0]["date_override"] is None

        # The batch as the endpoint dispatched it, with the game log not ready.
        ran: list[str] = []

        async def fake_run_pipeline(name, date_override=None, nba_date=None):
            ran.append(name)
            failed = name == "player_game_stats"
            return PipelineResult(
                status="error" if failed else "success",
                message=name,
                started_at="2026-03-05T06:00:00",
                error="Data not ready yet — will retry." if failed else None,
            )

        monkeypatch.setattr(pipelines_api, "run_pipeline", fake_run_pipeline)

        async def forced_batch() -> dict:
            manager = pipelines_api.get_job_manager()
            job = await manager.create_job(len(dispatched[0]["pipeline_names"]))
            await run_batch(job.job_id, **(dispatched[0] | {"batch_id": None}))
            job = await manager.get_job(job.job_id)
            return {name: result.status for name, result in job.results.items()}

        with freeze_time(IN_WINDOW):
            statuses = asyncio.run(forced_batch())

        assert statuses["player_game_stats"] == "error"
        for name in ("player_season_stats", "player_rolling_stats", "team_stats"):
            assert statuses[name] == "skipped"
            assert name not in ran

    def test_dependents_wait_for_the_game_log_and_run_once_it_lands(self, monkeypatch):
        """Two polls of one night, through the real batch runner.

        First poll: the game log is not ready, so what depends on it is
        skipped rather than run against last night's success. Second poll: it
        lands, and they run behind it in the same batch.
        """
        from pipelines import PIPELINE_REGISTRY, get_pipelines_by_category
        from pipelines.config import PipelineCategory
        from schemas.pipeline import PipelineResult

        _only_last_night_succeeded(monkeypatch)
        names = [
            cls.config.name
            for cls in get_pipelines_by_category(PipelineCategory.POST_GAME)
            if not cls.config.espn_gated
        ]
        ran: list[str] = []
        game_log_ready = {"value": False}

        async def fake_run_pipeline(name, date_override=None, nba_date=None):
            ran.append(name)
            failed = name == "player_game_stats" and not game_log_ready["value"]
            return PipelineResult(
                status="error" if failed else "success",
                message=name,
                started_at="2026-03-05T06:00:00",
                error="Data not ready yet — will retry." if failed else None,
            )

        monkeypatch.setattr(pipelines_api, "run_pipeline", fake_run_pipeline)

        async def poll() -> dict:
            manager = pipelines_api.get_job_manager()
            job = await manager.create_job(len(names))
            await pipelines_api._run_pipelines_background(
                job.job_id,
                pipeline_names=names,
                nba_date=NBA_DATE,
                dependencies_since=WINDOW_OPEN_UTC,
            )
            job = await manager.get_job(job.job_id)
            return {name: result.status for name, result in job.results.items()}

        waits_on_game_log = {
            name for name in names
            if "player_game_stats" in PIPELINE_REGISTRY[name].config.depends_on
        }
        assert waits_on_game_log >= {"player_season_stats", "player_rolling_stats", "team_stats"}
        # lineup_snapshots waits for the ESPN-gated daily_matchup_scores instead,
        # which this batch never runs: it stays skipped on both polls.
        waits_on_espn_gate = {
            name for name in names
            if any(PIPELINE_REGISTRY[dep].config.espn_gated for dep in PIPELINE_REGISTRY[name].config.depends_on)
        }
        assert waits_on_espn_gate == {"lineup_snapshots"}

        with freeze_time(IN_WINDOW):
            first = asyncio.run(poll())

        assert first["player_game_stats"] == "error"
        assert all(first[name] == "skipped" for name in waits_on_game_log)
        assert not waits_on_game_log & set(ran)
        # What does not depend on it is not held up.
        assert first["game_schedule"] == "success"

        ran.clear()
        game_log_ready["value"] = True
        with freeze_time(IN_WINDOW):
            second = asyncio.run(poll())

        assert all(status == "success" for name, status in second.items() if name not in waits_on_espn_gate)
        assert all(second[name] == "skipped" for name in waits_on_espn_gate)
        assert not waits_on_espn_gate & set(ran)
        assert ran.index("game_schedule") < ran.index("team_stats")
