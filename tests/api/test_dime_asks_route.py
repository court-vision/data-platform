"""POST /pipelines/dime-asks gates on a due ask and runs the pipeline only then."""

import os

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from api.v1 import pipelines as pipelines_api
from schemas.common import ApiStatus
from schemas.pipeline import PipelineResult

PATH = "/v1/internal/pipelines/dime-asks"


def _client() -> TestClient:
    app = FastAPI()
    app.include_router(pipelines_api.router, prefix="/v1/internal")
    return TestClient(app)


def _auth() -> dict:
    return {"Authorization": f"Bearer {os.environ.get('PIPELINE_API_TOKEN', 'test-token')}"}


@pytest.fixture
def run_calls(monkeypatch):
    calls = []

    async def fake_run_pipeline(name, date_override=None, options=None, nba_date=None):
        calls.append(name)
        return PipelineResult(status=ApiStatus.SUCCESS, message="1 ask started",
                              started_at="2026-11-23T02:30:00", records_processed=1)

    monkeypatch.setattr(pipelines_api, "run_pipeline", fake_run_pipeline)
    return calls


def _due(monkeypatch, value):
    async def due():
        return value
    monkeypatch.setattr(pipelines_api, "_asks_due", due)


@pytest.mark.api
def test_no_ask_due_is_skipped_without_a_run(run_calls, monkeypatch):
    _due(monkeypatch, False)
    res = _client().post(PATH, headers=_auth())
    assert res.status_code == 200
    assert res.json() == {"status": "skipped", "message": "No ask is due", "data": None}
    assert run_calls == []


@pytest.mark.api
def test_a_due_ask_runs_the_pipeline(run_calls, monkeypatch):
    _due(monkeypatch, True)
    res = _client().post(PATH, headers=_auth())
    assert res.status_code == 200 and res.json()["status"] == "success"
    assert run_calls == ["dime_asks"]


@pytest.mark.api
def test_force_runs_it_regardless(run_calls, monkeypatch):
    _due(monkeypatch, False)
    res = _client().post(f"{PATH}?force=true", headers=_auth())
    assert res.status_code == 200 and run_calls == ["dime_asks"]


@pytest.mark.api
def test_requires_bearer_token(run_calls, monkeypatch):
    _due(monkeypatch, True)
    assert _client().post(PATH).status_code in (401, 403)
    assert run_calls == []


@pytest.mark.api
def test_the_gate_reads_due_asks_and_dead_ask_check_ins_once_the_table_exists():
    assert "to_regclass('usr.ai_asks')" in pipelines_api.ASKS_TABLE_SQL
    assert pipelines_api.DUE_ASKS_SQL == (
        "SELECT EXISTS (SELECT 1 FROM usr.ai_asks WHERE state = 'pending' AND due_at <= now()) "
        "OR EXISTS (SELECT 1 FROM usr.ai_checkins WHERE state = 'running' AND kind = 'ask' "
        "AND started_at < now() - interval '16 minutes')"
    )


class _Rows:
    def __init__(self, value):
        self.value = value

    def fetchone(self):
        return (self.value,)


class _FakeDb:
    def __init__(self, table, due):
        self.answers = {pipelines_api.ASKS_TABLE_SQL: table, pipelines_api.DUE_ASKS_SQL: due}
        self.sql = []

    def execute_sql(self, sql):
        self.sql.append(sql)
        return _Rows(self.answers[sql])


@pytest.mark.api
@pytest.mark.parametrize("table,due,expected,reads", [
    (False, True, False, 1),   # before backend migration 0055: nothing is due, and the asks aren't read
    (True, False, False, 2),
    (True, True, True, 2),
])
def test_the_gate_answers_from_the_two_reads(monkeypatch, table, due, expected, reads):
    import db.base
    fake = _FakeDb(table, due)
    monkeypatch.setattr(db.base, "db", fake)
    assert pipelines_api._has_due_asks() is expected
    assert len(fake.sql) == reads
