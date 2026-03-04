import os
from types import SimpleNamespace

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from api.v1 import dashboard


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
