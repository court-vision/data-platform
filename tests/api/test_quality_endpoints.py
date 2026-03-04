from __future__ import annotations

import uuid
import os

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from api.v1 import quality


class _FakeService:
    def __init__(self):
        self._run_id = str(uuid.uuid4())

    def run_checks(self, check_names=None, triggered_by: str = "manual"):
        return type("Run", (), {"id": self._run_id})

    def get_run(self, run_id: str):
        if run_id != self._run_id:
            return None
        return {
            "run_id": self._run_id,
            "status": "success",
            "started_at": "2026-03-04T10:00:00Z",
            "completed_at": "2026-03-04T10:00:01Z",
            "duration_seconds": 1.0,
            "total_checks": 2,
            "passed_checks": 2,
            "failed_checks": 0,
            "triggered_by": "manual",
            "error_message": None,
            "checks": [
                {
                    "check_name": "player_game_stats_required_fields_not_null",
                    "status": "passed",
                    "severity": "critical",
                    "failures": 0,
                    "message": None,
                    "details": None,
                    "duration_ms": 12,
                }
            ],
        }

    def list_runs(self, limit: int = 20):
        return [
            {
                "run_id": self._run_id,
                "status": "success",
                "started_at": "2026-03-04T10:00:00Z",
                "completed_at": "2026-03-04T10:00:01Z",
                "duration_seconds": 1.0,
                "total_checks": 2,
                "passed_checks": 2,
                "failed_checks": 0,
                "triggered_by": "manual",
                "error_message": None,
            }
        ]


def _make_app() -> FastAPI:
    app = FastAPI()
    app.include_router(quality.router, prefix="/v1/internal")
    return app


@pytest.mark.api
def test_quality_endpoints_require_auth() -> None:
    client = TestClient(_make_app())
    res = client.get("/v1/internal/quality/runs")
    assert res.status_code in (401, 403)


@pytest.mark.api
def test_quality_run_and_query_endpoints(monkeypatch) -> None:
    fake = _FakeService()
    monkeypatch.setattr(quality, "_service", fake)

    client = TestClient(_make_app())
    token = os.environ.get("PIPELINE_API_TOKEN", "test-token")
    headers = {"Authorization": f"Bearer {token}"}

    run_res = client.post("/v1/internal/quality/run", headers=headers)
    assert run_res.status_code == 200
    run_payload = run_res.json()
    assert run_payload["status"] == "success"
    run_id = run_payload["data"]["run_id"]

    list_res = client.get("/v1/internal/quality/runs", headers=headers)
    assert list_res.status_code == 200
    assert len(list_res.json()["data"]) == 1

    detail_res = client.get(f"/v1/internal/quality/runs/{run_id}", headers=headers)
    assert detail_res.status_code == 200
    assert detail_res.json()["data"]["run_id"] == run_id
