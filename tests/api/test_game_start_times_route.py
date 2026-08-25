"""POST /pipelines/game-start-times threads ?source / ?include_preseason into the pipeline options."""

import os

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from api.v1 import pipelines as pipelines_api
from schemas.common import ApiStatus
from schemas.pipeline import PipelineResult

PATH = "/v1/internal/pipelines/game-start-times"


def _client() -> TestClient:
    app = FastAPI()
    app.include_router(pipelines_api.router, prefix="/v1/internal")
    return TestClient(app)


def _auth() -> dict:
    return {"Authorization": f"Bearer {os.environ.get('PIPELINE_API_TOKEN', 'test-token')}"}


@pytest.fixture
def run_calls(monkeypatch):
    calls = []

    async def fake_run_pipeline(name, date_override=None, options=None):
        calls.append((name, date_override, options))
        return PipelineResult(
            status=ApiStatus.SUCCESS, message="ok", started_at="2026-08-25T00:00:00", records_processed=0
        )

    monkeypatch.setattr(pipelines_api, "run_pipeline", fake_run_pipeline)
    return calls


@pytest.mark.api
def test_defaults_to_cdn_without_preseason(run_calls):
    res = _client().post(PATH, headers=_auth())
    assert res.status_code == 200 and res.json()["status"] == "success"
    assert run_calls == [("game_start_times", None, {"source": "cdn", "include_preseason": False})]


@pytest.mark.api
def test_explicit_static_source_and_preseason(run_calls):
    res = _client().post(f"{PATH}?source=static&include_preseason=true", headers=_auth())
    assert res.status_code == 200
    assert run_calls[0][2] == {"source": "static", "include_preseason": True}


@pytest.mark.api
def test_unknown_source_is_rejected(run_calls):
    res = _client().post(f"{PATH}?source=ftp", headers=_auth())
    assert res.status_code == 422
    assert run_calls == []


@pytest.mark.api
def test_requires_bearer_token(run_calls):
    assert _client().post(PATH).status_code in (401, 403)
    assert run_calls == []
