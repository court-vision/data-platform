"""
GET /v1/dashboard/pipelines/{name}/runs: one pipeline's config and its run
history. The summary is a pure function over the rows, tested here without a
database; the endpoint test stubs the query.
"""

import os
from datetime import datetime, timezone

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from api.v1 import dashboard
from core.middleware import setup_middleware
from pipelines import PIPELINE_REGISTRY
from schemas.dashboard import PipelineRunEntry, PipelineRunsData

_AUTH = {"Authorization": f"Bearer {os.environ.get('PIPELINE_API_TOKEN', 'test-token')}"}


def _make_app() -> FastAPI:
    app = FastAPI()
    setup_middleware(app)  # the JSON error envelope, for the 404
    app.include_router(dashboard.router, prefix="/v1")
    return app


def _run(status="success", started=(2026, 3, 5, 7, 0), seconds=12.0, records=240, error=None, id="r1"):
    started_at = datetime(*started)
    completed_at = None if status == "running" else started_at.replace(second=int(seconds))
    return PipelineRunEntry(
        id=id, started_at=started_at, completed_at=completed_at, status=status,
        duration_seconds=None if status == "running" else seconds,
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
        assert (s.total, s.succeeded, s.failed, s.running) == (4, 2, 1, 1)
        assert s.success_rate == pytest.approx(2 / 3)     # over finished runs only
        assert s.median_duration_seconds == 10.0            # median of 4, 10, 20
        assert s.max_duration_seconds == 20.0
        assert s.last_success_at == datetime(2026, 3, 4, 7, 0, 20)
        assert s.oldest_started_at == datetime(2026, 3, 3, 7, 0)

    def test_nothing_finished_means_no_rate_and_no_durations(self):
        s = dashboard.summarize_runs([_run("running")])
        assert (s.total, s.running, s.success_rate, s.median_duration_seconds) == (1, 1, None, None)

    def test_an_empty_window(self):
        s = dashboard.summarize_runs([])
        assert s.total == 0 and s.oldest_started_at is None and s.last_success_at is None


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
