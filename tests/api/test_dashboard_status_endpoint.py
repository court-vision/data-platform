import os
from types import SimpleNamespace

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from peewee import SqliteDatabase

from api.v1 import dashboard
from db.models.nba.cron_job_run import CronJobRun
from db.models.pipeline_run import PipelineRun


class _FakeJobManager:
    async def list_jobs(self, limit: int = 10):
        return [
            SimpleNamespace(
                job_id="job-1",
                status="completed",
                created_at="2026-03-04T10:00:00Z",
                started_at="2026-03-04T10:00:01Z",
                completed_at="2026-03-04T10:00:03Z",
                duration_seconds=2.0,
                pipelines_total=3,
                pipelines_completed=3,
                pipelines_failed=0,
                current_pipeline=None,
            )
        ]


def _make_app() -> FastAPI:
    app = FastAPI()
    app.include_router(dashboard.router, prefix="/v1")
    return app


@pytest.mark.api
def test_dashboard_root_redirects_to_the_app() -> None:
    res = TestClient(_make_app()).get("/v1/dashboard", follow_redirects=False)
    assert res.status_code == 307
    assert res.headers["location"] == "/"


@pytest.mark.api
def test_dashboard_status_requires_bearer_token() -> None:
    client = TestClient(_make_app())
    res = client.get("/v1/dashboard/status")
    assert res.status_code in (401, 403)


@pytest.mark.api
def test_dashboard_status_returns_expected_payload(monkeypatch) -> None:
    def fake_pipeline_health():
        return [
            {
                "name": "player_game_stats",
                "display_name": "Player Game Stats",
                "category": "post_game",
                "trigger_endpoint": "/v1/internal/pipelines/daily-player-stats",
                "last_run_at": None,
                "last_status": "success",
                "last_duration_seconds": 12.5,
                "last_records_processed": 530,
                "last_success_at": None,
                "is_running": False,
                "error_streak": 0,
            }
        ]

    def fake_quality_status():
        return {
            "quality_latest": {
                "run_id": "run-1",
                "status": "failed",
                "started_at": "2026-03-04T11:00:00Z",
                "completed_at": "2026-03-04T11:00:05Z",
                "duration_seconds": 5.0,
                "total_checks": 4,
                "passed_checks": 3,
                "failed_checks": 1,
                "triggered_by": "dashboard",
                "error_message": None,
            },
            "recent_quality_runs": [
                {
                    "run_id": "run-1",
                    "status": "failed",
                    "started_at": "2026-03-04T11:00:00Z",
                    "completed_at": "2026-03-04T11:00:05Z",
                    "duration_seconds": 5.0,
                    "total_checks": 4,
                    "passed_checks": 3,
                    "failed_checks": 1,
                    "triggered_by": "dashboard",
                    "error_message": None,
                }
            ],
            "quality_failed_checks": [
                {
                    "check_name": "player_game_stats_non_negative_minutes",
                    "status": "failed",
                    "severity": "critical",
                    "failures": 2,
                    "message": "negative minutes found",
                    "duration_ms": 18,
                }
            ],
        }

    monkeypatch.setattr(dashboard, "_build_pipeline_health", fake_pipeline_health)
    monkeypatch.setattr(dashboard, "_build_quality_status", fake_quality_status)
    monkeypatch.setattr(dashboard, "get_job_manager", lambda: _FakeJobManager())

    client = TestClient(_make_app())
    token = os.environ.get("PIPELINE_API_TOKEN", "test-token")
    res = client.get(
        "/v1/dashboard/status",
        headers={"Authorization": f"Bearer {token}"},
    )

    assert res.status_code == 200
    body = res.json()
    assert body["status"] == "success"
    assert "data" in body
    assert len(body["data"]["pipelines"]) == 1
    assert body["data"]["pipelines"][0]["name"] == "player_game_stats"
    assert len(body["data"]["recent_jobs"]) == 1
    assert body["data"]["recent_jobs"][0]["job_id"] == "job-1"
    assert body["data"]["quality_latest"]["run_id"] == "run-1"
    assert len(body["data"]["recent_quality_runs"]) == 1
    assert body["data"]["recent_quality_runs"][0]["failed_checks"] == 1
    assert body["data"]["quality_failed_checks"][0]["check_name"] == "player_game_stats_non_negative_minutes"


@pytest.mark.api
def test_the_overview_rows_tell_the_run_button_to_force_only_the_scheduled_pickups_tick() -> None:
    # The Overview's Run button posts what its row says. The scheduled-pickups
    # route skips a tick with no pickup due, so without ?force=true Run did nothing.
    # The run tables are bound to in-memory SQLite (schema stripped), empty.
    models = [PipelineRun, CronJobRun]
    saved = {model: model._meta.schema for model in models}
    for model in models:
        model._meta.schema = None
    db = SqliteDatabase(":memory:")
    try:
        with db.bind_ctx(models):
            db.create_tables(models)
            rows = dashboard._build_pipeline_health()
    finally:
        for model, schema in saved.items():
            model._meta.schema = schema

    assert [row.name for row in rows if row.force_on_run] == ["scheduled_pickups"]


# --- GET /v1/dashboard/services -------------------------------------------

_AUTH = {"Authorization": f"Bearer {os.environ.get('PIPELINE_API_TOKEN', 'test-token')}"}


@pytest.mark.api
def test_services_requires_bearer_token() -> None:
    assert TestClient(_make_app()).get("/v1/dashboard/services").status_code in (401, 403)


@pytest.mark.api
def test_services_reports_this_process_and_the_backend(monkeypatch) -> None:
    from schemas.dashboard import ServiceInfo

    monkeypatch.setattr(
        dashboard, "_probe_backend",
        lambda: ServiceInfo(key="backend", name="Backend", ok=True, version="abc1234",
                            environment="production", uptime_s=120),
    )
    res = TestClient(_make_app()).get("/v1/dashboard/services", headers=_AUTH)

    assert res.status_code == 200
    services = {s["key"]: s for s in res.json()["data"]["services"]}
    assert list(services) == ["data_platform", "backend"]
    own = services["data_platform"]
    assert own["ok"] is True and own["configured"] is True
    assert own["version"] and own["environment"] == "development"
    assert isinstance(own["uptime_s"], int)
    assert services["backend"]["version"] == "abc1234"


class _Response:
    def __init__(self, status_code: int, body: dict):
        self.status_code = status_code
        self._body = body

    def json(self) -> dict:
        return self._body


@pytest.mark.api
def test_probe_backend_without_a_url_is_not_configured(monkeypatch) -> None:
    monkeypatch.setattr(dashboard.settings, "backend_internal_url", None)
    info = dashboard._probe_backend()
    assert info.configured is False and info.ok is False
    assert "BACKEND_INTERNAL_URL" in (info.error or "")


@pytest.mark.api
def test_probe_backend_reads_health_and_strips_a_trailing_slash(monkeypatch) -> None:
    seen = {}

    def fake_get(url, timeout):
        seen["url"] = url
        return _Response(200, {"status": "ok", "version": "9f8e7d6", "environment": "production",
                               "uptime_s": 4242, "checks": {"database": {"ok": True}}})

    monkeypatch.setattr(dashboard.settings, "backend_internal_url", "http://backend.railway.internal:8000/")
    monkeypatch.setattr(dashboard.httpx, "get", fake_get)

    info = dashboard._probe_backend()
    assert seen["url"] == "http://backend.railway.internal:8000/health"
    assert (info.ok, info.version, info.environment, info.uptime_s, info.error) == (
        True, "9f8e7d6", "production", 4242, None)


@pytest.mark.api
def test_probe_backend_degraded_names_the_failing_checks(monkeypatch) -> None:
    monkeypatch.setattr(dashboard.settings, "backend_internal_url", "http://backend")
    monkeypatch.setattr(dashboard.httpx, "get", lambda url, timeout: _Response(503, {
        "status": "degraded", "version": "9f8e7d6", "environment": "production", "uptime_s": 7,
        "checks": {"database": {"ok": False, "error": "OperationalError"}, "calendar": {"ok": True}},
    }))
    info = dashboard._probe_backend()
    assert info.ok is False and info.version == "9f8e7d6"
    assert info.error == "degraded: database"


@pytest.mark.api
def test_probe_backend_unreachable_is_an_error_not_a_500(monkeypatch) -> None:
    import httpx

    def boom(url, timeout):
        raise httpx.ConnectError("refused")

    monkeypatch.setattr(dashboard.settings, "backend_internal_url", "http://backend")
    monkeypatch.setattr(dashboard.httpx, "get", boom)
    info = dashboard._probe_backend()
    assert info.configured is True and info.ok is False
    assert info.error == "ConnectError" and info.version is None


# Valid JSON that is not the backend's /health: whatever BACKEND_INTERNAL_URL
# points at answered, or the contract moved.
_NOT_A_HEALTH_BODY = [
    pytest.param([1, 2, 3], id="a list"),
    pytest.param("ok", id="a string"),
    pytest.param({"status": "ok", "uptime_s": 12.5}, id="fractional uptime"),
    pytest.param({"status": "ok", "version": 1234567}, id="numeric version"),
    pytest.param({"status": "degraded", "checks": ["database"]}, id="checks as a list"),
]


@pytest.mark.api
@pytest.mark.parametrize("body", _NOT_A_HEALTH_BODY)
def test_probe_backend_unreadable_body_is_an_error_not_a_500(monkeypatch, body) -> None:
    monkeypatch.setattr(dashboard.settings, "backend_internal_url", "http://backend")
    monkeypatch.setattr(dashboard.httpx, "get", lambda url, timeout: _Response(200, body))
    info = dashboard._probe_backend()
    assert info.configured is True and info.ok is False
    assert info.error and info.version is None


@pytest.mark.api
def test_probe_backend_body_that_is_not_an_object_says_so(monkeypatch) -> None:
    monkeypatch.setattr(dashboard.settings, "backend_internal_url", "http://backend")
    monkeypatch.setattr(dashboard.httpx, "get", lambda url, timeout: _Response(200, [1, 2, 3]))
    assert dashboard._probe_backend().error == "HTTP 200: not a /health body"


@pytest.mark.api
@pytest.mark.parametrize("body", _NOT_A_HEALTH_BODY)
def test_services_keeps_this_process_when_the_backend_body_is_unreadable(monkeypatch, body) -> None:
    monkeypatch.setattr(dashboard.settings, "backend_internal_url", "http://backend")
    monkeypatch.setattr(dashboard.httpx, "get", lambda url, timeout: _Response(200, body))
    res = TestClient(_make_app(), raise_server_exceptions=False).get("/v1/dashboard/services", headers=_AUTH)

    assert res.status_code == 200
    services = {s["key"]: s for s in res.json()["data"]["services"]}
    assert services["data_platform"]["ok"] is True and services["data_platform"]["version"]
    assert services["backend"]["ok"] is False and services["backend"]["error"]
