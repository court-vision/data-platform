"""
GET /v1/dashboard/pipelines/{name}/runs: one pipeline's config and its run
history. The summary is a pure function over the rows, tested here without a
database; the endpoint test stubs the query, and the builder's tests stub the
three reads it makes.
"""

import os
from datetime import datetime, timedelta, timezone

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from api.v1 import dashboard
from core.middleware import setup_middleware
from core.settings import settings
from db.models.pipeline_run import RUNNING_MAX_AGE_MINUTES, PipelineRun
from pipelines import PIPELINE_REGISTRY, PipelineCategory
from schemas.dashboard import PipelineInfo, PipelineRunEntry, PipelineRunsData

_AUTH = {"Authorization": f"Bearer {os.environ.get('PIPELINE_API_TOKEN', 'test-token')}"}


def _make_app() -> FastAPI:
    app = FastAPI()
    setup_middleware(app)  # the JSON error envelope, for the 404
    app.include_router(dashboard.router, prefix="/v1")
    return app


def _run(status="success", started=(2026, 3, 5, 7, 0), seconds=12.0, records=240, error=None, id="r1"):
    started_at = datetime(*started)
    unfinished = status in ("running", "stuck")
    completed_at = None if unfinished else started_at + timedelta(seconds=seconds)
    return PipelineRunEntry(
        id=id, started_at=started_at, completed_at=completed_at, status=status,
        duration_seconds=None if unfinished else seconds,
        records_processed=records, error_message=error,
    )


@pytest.mark.unit
class TestSummarizeRuns:
    def test_counts_rates_and_durations_over_the_window(self):
        runs = [
            _run("running", (2026, 3, 6, 7, 0), id="a"),
            _run("failed", (2026, 3, 5, 7, 0), seconds=4.0, records=0, error="boom", id="b"),
            _run("success", (2026, 3, 4, 7, 0), seconds=20.0, id="c"),
            _run("success", (2026, 3, 3, 7, 0), seconds=10.0, id="d"),
        ]
        s = dashboard.summarize_runs(runs)
        assert (s.total, s.succeeded, s.failed, s.running, s.stuck) == (4, 2, 1, 1, 0)
        assert s.success_rate == pytest.approx(2 / 3)     # over finished runs only
        assert s.median_duration_seconds == 15.0            # of 10 and 20: the runs that worked
        assert s.max_duration_seconds == 20.0
        assert s.oldest_started_at == datetime(2026, 3, 3, 7, 0)

    def test_a_run_swept_at_restart_does_not_set_the_durations(self):
        # reset_stale_runs closes an orphaned row at the next boot: 71 hours
        # of "duration" for a run that did 12 seconds of work, or none.
        swept = _run("failed", (2026, 3, 2, 7, 0), seconds=255600.0, records=0,
                     error="Interrupted by service restart", id="swept")
        runs = [_run(id=str(i), started=(2026, 3, 5, 7, i)) for i in range(5)] + [swept]
        s = dashboard.summarize_runs(runs)
        assert (s.median_duration_seconds, s.max_duration_seconds) == (12.0, 12.0)
        assert s.failed == 1                                # still counted as a failure

    def test_a_stuck_run_is_counted_apart_from_the_live_ones(self):
        runs = [_run("running", (2026, 3, 6, 7, 0), id="live"), _run("stuck", (2026, 3, 5, 7, 0), id="hung"), _run(id="ok")]
        s = dashboard.summarize_runs(runs)
        assert (s.total, s.succeeded, s.failed, s.running, s.stuck) == (3, 1, 0, 1, 1)
        assert s.success_rate == 1.0                        # stuck is not finished

    def test_last_success_is_the_callers_not_the_windows(self):
        runs = [_run("failed", seconds=4.0, error="boom", id="b"), _run("success", (2026, 3, 4, 7, 0), id="c")]
        assert dashboard.summarize_runs(runs).last_success_at is None
        earlier = datetime(2026, 2, 1, 7, 0, 12)
        assert dashboard.summarize_runs(runs, last_success_at=earlier).last_success_at == earlier

    def test_nothing_finished_means_no_rate_and_no_durations(self):
        s = dashboard.summarize_runs([_run("running")])
        assert (s.total, s.running, s.success_rate, s.median_duration_seconds) == (1, 1, None, None)

    def test_an_empty_window(self):
        s = dashboard.summarize_runs([])
        assert s.total == 0 and s.oldest_started_at is None and s.last_success_at is None


@pytest.mark.unit
class TestRunStatus:
    cutoff = datetime(2026, 3, 5, 10, 0)

    def test_a_running_row_older_than_the_cutoff_is_stuck(self):
        assert dashboard.run_status("running", self.cutoff - timedelta(seconds=1), self.cutoff) == "stuck"

    def test_a_running_row_is_running_exactly_while_is_running_counts_it(self):
        # PipelineRun.is_running counts started_at >= cutoff as live.
        assert dashboard.run_status("running", self.cutoff, self.cutoff) == "running"

    @pytest.mark.parametrize("status", ["success", "failed"])
    def test_a_finished_row_keeps_its_status_whatever_its_age(self, status):
        assert dashboard.run_status(status, self.cutoff - timedelta(days=3), self.cutoff) == status

    def test_an_aware_start_compares_with_a_naive_cutoff_and_the_reverse(self):
        # nba.pipeline_runs.started_at is timestamptz in the migrated schema,
        # so production rows are aware; a model-created table's are naive UTC.
        aware_cutoff = self.cutoff.replace(tzinfo=timezone.utc)
        central = timezone(timedelta(hours=-6))
        hung = datetime(2026, 3, 5, 3, 59, tzinfo=central)    # 09:59 UTC
        live = datetime(2026, 3, 5, 4, 0, tzinfo=central)     # 10:00 UTC
        for cutoff in (self.cutoff, aware_cutoff):
            assert dashboard.run_status("running", hung, cutoff) == "stuck"
            assert dashboard.run_status("running", live, cutoff) == "running"
        assert dashboard.run_status("running", self.cutoff - timedelta(seconds=1), aware_cutoff) == "stuck"


class _Rows(list):
    """Stands in for the peewee query _build_runs chains: .where().order_by().limit()."""

    def where(self, *_):
        return self

    def order_by(self, *_):
        return self

    def limit(self, n):
        return _Rows(self[:n])


def _row(status, started_at, seconds=12.0, **fields):
    completed_at = None if status == "running" else started_at + timedelta(seconds=seconds)
    return PipelineRun(pipeline_name="espn_injury_status", started_at=started_at,
                       completed_at=completed_at, status=status, **fields)


@pytest.fixture
def stub_runs(monkeypatch):
    """Serve _build_runs these rows (newest first) with no database."""

    def install(rows, *, is_running=False):
        successes = sorted((r for r in rows if r.status == "success"), key=lambda r: r.completed_at)
        monkeypatch.setattr(PipelineRun, "select", classmethod(lambda cls, *_: _Rows(rows)))
        monkeypatch.setattr(PipelineRun, "get_latest_successful",
                            classmethod(lambda cls, _name: successes[-1] if successes else None))
        monkeypatch.setattr(PipelineRun, "is_running", classmethod(lambda cls, _name: is_running))

    return install


@pytest.mark.api
class TestBuildRuns:
    def test_last_success_reaches_past_the_window(self, stub_runs):
        # One success, then 55 failures: a live pipeline gets there in an hour
        # of upstream outage. The page said "Last success: never".
        now = datetime.utcnow()
        failures = [_row("failed", now - timedelta(minutes=i), error_message="ESPN 503") for i in range(1, 56)]
        success = _row("success", now - timedelta(hours=2))
        stub_runs(failures + [success])

        data = dashboard._build_runs("espn_injury_status", 50)

        assert (data.summary.total, data.summary.succeeded) == (50, 0)
        assert data.summary.last_success_at == success.completed_at
        # ...and the tile does not move with the ?limit= buttons.
        assert dashboard._build_runs("espn_injury_status", 100).summary.last_success_at == success.completed_at

    def test_a_pipeline_that_never_succeeded_has_no_last_success(self, stub_runs):
        stub_runs([_row("failed", datetime.utcnow() - timedelta(minutes=5), error_message="boom")])
        assert dashboard._build_runs("espn_injury_status", 50).summary.last_success_at is None

    def test_a_row_left_running_past_the_cutoff_comes_back_stuck(self, stub_runs):
        now = datetime.utcnow()
        live = _row("running", now - timedelta(minutes=3))
        hung = _row("running", now - timedelta(minutes=RUNNING_MAX_AGE_MINUTES + 5))
        stub_runs([live, hung, _row("success", now - timedelta(hours=6))], is_running=True)

        data = dashboard._build_runs("espn_injury_status", 50)

        assert [r.status for r in data.runs] == ["running", "stuck", "success"]
        assert (data.summary.running, data.summary.stuck) == (1, 1)
        assert data.pipeline.is_running is True

    def test_rows_as_production_returns_them_timezone_aware(self, stub_runs):
        now = datetime.now(timezone.utc)
        hung = _row("running", now - timedelta(hours=5))
        success = _row("success", now - timedelta(hours=6))
        stub_runs([hung, success])

        data = dashboard._build_runs("espn_injury_status", 50)

        assert [r.status for r in data.runs] == ["stuck", "success"]
        assert data.summary.last_success_at == success.completed_at


@pytest.mark.api
class TestPipelineInfo:
    @pytest.mark.parametrize("name", sorted(PIPELINE_REGISTRY))
    def test_every_registry_entry_describes_itself(self, name):
        info = dashboard.pipeline_info(name)
        assert info.name == name and info.display_name and info.target_table
        assert info.trigger_endpoint.startswith("/v1/internal/pipelines/")

    def test_gates_come_through_as_the_page_shows_them(self):
        ownership = dashboard.pipeline_info("player_ownership")
        assert ownership.category == "post_game"
        # The wall-clock gate is a string the page can print, not a time object.
        assert ownership.earliest_run_time_cst is None or len(ownership.earliest_run_time_cst) == 5
        alerts = dashboard.pipeline_info("lineup_alerts")
        assert alerts.accepts_date is False and alerts.cron_job == "pre-game"

    def test_the_pre_game_window_is_the_one_the_gate_uses(self, monkeypatch):
        # trigger_pre_game falls back to the setting, which the environment
        # can change; the page used to print a literal 150 for it.
        monkeypatch.setattr(settings, "pre_game_window_minutes", 90)
        for name, cls in PIPELINE_REGISTRY.items():
            window = dashboard.pipeline_info(name).pre_game_window_minutes
            if cls.config.category == PipelineCategory.PRE_GAME:
                assert window == (cls.config.pre_game_window_minutes or 90), name
            else:
                assert window is None, name
        assert dashboard.pipeline_info("lineup_alerts").pre_game_window_minutes == 90
        assert dashboard.pipeline_info("espn_injury_status").pre_game_window_minutes == 120

    def test_the_timeout_nothing_enforces_is_not_presented_as_a_fact(self):
        # PipelineConfig.timeout_seconds is declared and never read: no code
        # stops a run at it. Put it back here when something does.
        assert "timeout_seconds" not in PipelineInfo.model_fields


@pytest.mark.api
def test_runs_requires_bearer_token() -> None:
    assert TestClient(_make_app()).get("/v1/dashboard/pipelines/player_game_stats/runs").status_code in (401, 403)


@pytest.mark.api
def test_an_unknown_pipeline_is_a_json_404() -> None:
    res = TestClient(_make_app()).get("/v1/dashboard/pipelines/nope/runs", headers=_AUTH)
    assert res.status_code == 404
    assert res.headers["content-type"].startswith("application/json")
    assert "nope" in res.json()["message"]


@pytest.mark.api
def test_limit_is_bounded() -> None:
    client = TestClient(_make_app())
    assert client.get("/v1/dashboard/pipelines/player_game_stats/runs?limit=0", headers=_AUTH).status_code == 422
    assert client.get("/v1/dashboard/pipelines/player_game_stats/runs?limit=201", headers=_AUTH).status_code == 422


@pytest.mark.api
def test_runs_payload(monkeypatch) -> None:
    seen = {}

    def fake_build(name, limit):
        seen["args"] = (name, limit)
        runs = [_run("failed", seconds=4.0, records=0, error="ESPN 503", id="b"), _run("success", (2026, 3, 4, 7, 0), id="c")]
        return PipelineRunsData(
            pipeline=dashboard.pipeline_info(name, is_running=False),
            runs=runs, summary=dashboard.summarize_runs(runs), limit=limit,
            fetched_at=datetime(2026, 3, 5, 13, 0, tzinfo=timezone.utc),
        )

    monkeypatch.setattr(dashboard, "_build_runs", fake_build)
    res = TestClient(_make_app()).get("/v1/dashboard/pipelines/player_game_stats/runs?limit=25", headers=_AUTH)

    assert res.status_code == 200
    assert seen["args"] == ("player_game_stats", 25)
    body = res.json()
    assert body["message"] == "2 runs of player_game_stats"
    data = body["data"]
    assert data["pipeline"]["name"] == "player_game_stats" and data["pipeline"]["accepts_date"] is True
    assert [r["status"] for r in data["runs"]] == ["failed", "success"]
    assert data["runs"][0]["error_message"] == "ESPN 503"
    assert data["summary"]["success_rate"] == 0.5 and data["limit"] == 25
