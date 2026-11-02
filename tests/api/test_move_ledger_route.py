"""POST /pipelines/move-ledger runs the move ledger pipeline."""

import os

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from api.v1 import pipelines as pipelines_api
from schemas.common import ApiStatus
from schemas.pipeline import PipelineResult


@pytest.mark.api
def test_runs_the_rebuild(monkeypatch):
    calls = []

    async def fake_run_pipeline(name, date_override=None, options=None):
        calls.append(name)
        return PipelineResult(status=ApiStatus.SUCCESS, message="ok", started_at="2026-11-02T08:00:00",
                              records_processed=1)

    monkeypatch.setattr(pipelines_api, "run_pipeline", fake_run_pipeline)
    app = FastAPI()
    app.include_router(pipelines_api.router, prefix="/v1/internal")
    token = os.environ.get("PIPELINE_API_TOKEN", "test-token")
    res = TestClient(app).post("/v1/internal/pipelines/move-ledger", headers={"Authorization": f"Bearer {token}"})
    assert res.status_code == 200 and res.json()["status"] == "success"
    assert calls == ["move_ledger"]


@pytest.mark.api
def test_needs_the_pipeline_token():
    app = FastAPI()
    app.include_router(pipelines_api.router, prefix="/v1/internal")
    assert TestClient(app).post("/v1/internal/pipelines/move-ledger").status_code in (401, 403)
