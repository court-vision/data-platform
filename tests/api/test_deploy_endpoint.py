"""
POST /v1/internal/pipelines/deploy: both GitHub dispatches OK -> 200 success;
any dispatch failing (exception or non-2xx) -> 502 `{status: "error"}` plus one
`deploy_dispatch_failed` alert; `source=manual` still self-records the run.
GitHub is a fake `httpx.AsyncClient`; the cron_job_runs write is captured.
"""

import json
import os
from datetime import timedelta
from types import SimpleNamespace

import httpx
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from pydantic import SecretStr

from api.v1 import pipelines as pipelines_api
from core.settings import settings
from db.models.nba.cron_job_run import CronJobRun

PATH = "/v1/internal/pipelines/deploy"


class FakeResponse:
    def __init__(self, status_code, text=""):
        self.status_code = status_code
        self.text = text


class FakeAsyncClient:
    """`outcomes[repo]` is the response to return or the exception to raise."""

    outcomes: dict = {}
    calls: list = []

    def __init__(self, timeout=None):
        self.timeout = timeout

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    async def post(self, url, json=None, headers=None):
        repo = url.split("/repos/", 1)[1].rsplit("/dispatches", 1)[0]
        FakeAsyncClient.calls.append((repo, json, headers))
        outcome = self.outcomes[repo]
        if isinstance(outcome, Exception):
            raise outcome
        return outcome


@pytest.fixture
def deploy_env(monkeypatch):
    monkeypatch.setattr(settings, "github_deploy_token", SecretStr("ghp_test"))
    monkeypatch.setattr(settings, "backend_github_repo", "cv/backend")
    monkeypatch.setattr(settings, "data_platform_github_repo", "cv/data-platform")
    # Only this module's `httpx` name is swapped (the route annotates `-> httpx.Response`)
    monkeypatch.setattr(pipelines_api, "httpx", SimpleNamespace(AsyncClient=FakeAsyncClient, Response=httpx.Response))
    FakeAsyncClient.outcomes = {}
    FakeAsyncClient.calls = []

    recorded = []

    async def same_thread(fn, *args, **kwargs):
        return fn(*args, **kwargs)

    monkeypatch.setattr(pipelines_api, "run_in_db_thread", same_thread)
    monkeypatch.setattr(CronJobRun, "create", classmethod(lambda cls, **fields: recorded.append(fields)))
    return recorded


@pytest.fixture
def client():
    app = FastAPI()
    app.include_router(pipelines_api.router, prefix="/v1/internal")
    return TestClient(app)


def _headers():
    return {"Authorization": f"Bearer {os.environ.get('PIPELINE_API_TOKEN', 'test-token')}"}


@pytest.mark.api
def test_both_dispatches_ok(client, deploy_env, alerts):
    FakeAsyncClient.outcomes = {"cv/backend": FakeResponse(204), "cv/data-platform": FakeResponse(204)}

    res = client.post(PATH, headers=_headers())

    assert res.status_code == 200
    body = res.json()
    assert body["status"] == "success"
    assert body["data"] == {"backend": {"ok": True, "status": 204}, "data_platform": {"ok": True, "status": 204}}
    assert alerts.events == []
    assert deploy_env == []  # cron-triggered runs are recorded by the cron-runner, not here
    assert {call[0] for call in FakeAsyncClient.calls} == {"cv/backend", "cv/data-platform"}
    assert FakeAsyncClient.calls[0][1] == {"event_type": "nightly-deploy"}
    assert FakeAsyncClient.calls[0][2]["Authorization"] == "Bearer ghp_test"


@pytest.mark.api
def test_one_dispatch_raising_is_a_502_and_alerts(client, deploy_env, alerts):
    FakeAsyncClient.outcomes = {
        "cv/backend": FakeResponse(204),
        "cv/data-platform": ConnectionError("api.github.com unreachable"),
    }

    res = client.post(PATH, headers=_headers())

    assert res.status_code == 502
    body = res.json()
    assert body["status"] == "error"
    assert body["error_code"] == "DEPLOY_DISPATCH_FAILED"
    assert body["message"] == "Deploy dispatch failed for: data_platform"
    assert body["data"]["backend"] == {"ok": True, "status": 204}
    assert body["data"]["data_platform"] == {"ok": False, "error": "api.github.com unreachable"}

    assert alerts.keys() == ["deploy_dispatch_failed"]
    event = alerts.events[0]
    assert event.severity == "critical"
    assert event.dedupe == timedelta(hours=12)
    assert "data_platform (cv/data-platform): api.github.com unreachable" in event.body
    assert event.fields["failed"] == "data_platform" and event.fields["ok"] == "backend"
    assert event.fields["source"] == "cron" and event.fields["service"] == "both"


@pytest.mark.api
def test_non_2xx_from_github_is_a_502_and_manual_runs_self_record(client, deploy_env, alerts):
    FakeAsyncClient.outcomes = {
        "cv/backend": FakeResponse(422, '{"message":"No ref found"}'),
        "cv/data-platform": FakeResponse(204),
    }

    res = client.post(f"{PATH}?source=manual", headers=_headers())

    assert res.status_code == 502
    assert res.json()["data"]["backend"] == {"ok": False, "status": 422, "body": '{"message":"No ref found"}'}
    assert alerts.keys() == ["deploy_dispatch_failed"]

    assert len(deploy_env) == 1
    row = deploy_env[0]
    assert row["job_name"] == "deploy" and row["result"] == "failure"
    assert json.loads(row["error_message"]) == {"backend": 'HTTP 422: {"message":"No ref found"}'}
    assert json.loads(row["response_snippet"])["data_platform"]["ok"] is True


@pytest.mark.api
def test_manual_success_self_records_a_success(client, deploy_env, alerts):
    FakeAsyncClient.outcomes = {"cv/backend": FakeResponse(204)}

    res = client.post(f"{PATH}?source=manual&service=backend", headers=_headers())

    assert res.status_code == 200
    assert [call[0] for call in FakeAsyncClient.calls] == ["cv/backend"]
    assert deploy_env[0]["result"] == "success" and deploy_env[0]["error_message"] is None
    assert alerts.events == []


@pytest.mark.api
def test_repeated_dispatch_failures_alert_once_per_window(client, deploy_env, alerts):
    FakeAsyncClient.outcomes = {"cv/backend": FakeResponse(500, "boom"), "cv/data-platform": FakeResponse(204)}
    for _ in range(3):
        assert client.post(PATH, headers=_headers()).status_code == 502
    assert alerts.keys() == ["deploy_dispatch_failed"]


@pytest.mark.api
def test_missing_configuration_is_a_503(client, deploy_env, alerts, monkeypatch):
    monkeypatch.setattr(settings, "github_deploy_token", None)
    res = client.post(PATH, headers=_headers())
    assert res.status_code == 503
    assert alerts.events == []
    assert FakeAsyncClient.calls == []
