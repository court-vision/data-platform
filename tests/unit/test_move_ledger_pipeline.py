"""
The move ledger, data-platform side: `BackendClient.run_ledger` (one call a night,
POST /v1/internal/jobs/ledger/run) and `pipelines.move_ledger`, which counts each
league the backend rebuilt. The backend owns the ledger; nothing here touches the
database.
"""

import json

import pytest
import requests
import responses

from pipelines.context import PipelineContext
from pipelines.move_ledger import MoveLedgerFailed, MoveLedgerPipeline
from services.backend_client import RUN_LEDGER_PATH, BackendClient, LedgerLeague, LedgerRun

BASE = "http://api.railway.internal:8080"
URL = BASE + RUN_LEDGER_PATH
TOKEN = "pipeline-secret-token"


def _client():
    return BackendClient(base_url=BASE, token=TOKEN, timeout_seconds=45.0, ledger_timeout_seconds=300.0)


def _league(**over):
    payload = {"provider_league_id": "1001", "season": 2027, "ok": True, "days": 14, "moves": 9,
               "by_source": {"you": 3, "outside": 6}, "error": None}
    payload.update(over)
    return payload


def _envelope(*leagues):
    return {"status": "success", "message": "ok",
            "data": {"leagues": list(leagues), "rebuilt": len(leagues), "failed": 0}}


@pytest.mark.unit
def test_disabled_without_base_url():
    run = BackendClient(base_url=None, token=TOKEN, timeout_seconds=45.0).run_ledger()
    assert (run.ok, run.reason) == (False, "backend_not_configured")


@pytest.mark.unit
@responses.activate
def test_200_parses_each_league_and_sends_the_contract():
    responses.post(URL, json=_envelope(_league(), _league(provider_league_id="2002", ok=False, days=0, moves=0,
                                                          by_source={}, error="OperationalError")), status=200)
    run = _client().run_ledger(correlation_id="corr-7")
    assert run.ok
    first, second = run.leagues
    assert (first.provider_league_id, first.season, first.moves, first.by_source) == ("1001", 2027, 9,
                                                                                      {"you": 3, "outside": 6})
    assert (second.ok, second.error) == (False, "OperationalError")

    assert len(responses.calls) == 1
    request = responses.calls[0].request
    assert request.headers["Authorization"] == f"Bearer {TOKEN}"
    assert request.headers["X-Correlation-ID"] == "corr-7"
    assert json.loads(request.body) == {}


@pytest.mark.unit
@responses.activate
@pytest.mark.parametrize("status,body,reason", [
    (401, {"detail": "Invalid pipeline token"}, "unauthorized"),
    (500, {"status": "error", "message": "boom", "error_code": "INTERNAL_ERROR"}, "http_500"),
    (200, {"status": "success", "message": "ok", "data": None}, "malformed_response"),
    (200, {"status": "success", "message": "ok", "data": {"leagues": [{"provider_league_id": "1"}]}},
     "malformed_response"),
])
def test_anything_else_is_a_run_that_is_not_ok(status, body, reason):
    responses.post(URL, json=body, status=status)
    run = _client().run_ledger()
    assert (run.ok, run.reason) == (False, reason)
    assert len(responses.calls) == 1


@pytest.mark.unit
@responses.activate
def test_a_timeout_is_not_retried():
    responses.post(URL, body=requests.Timeout("read timed out"))
    run = _client().run_ledger()
    assert (run.ok, run.reason) == (False, "timeout")
    assert len(responses.calls) == 1


@pytest.mark.unit
def test_the_ledger_waits_with_its_own_read_timeout(monkeypatch):
    # A whole night's rebuild can take minutes; the general 45 s would cut it off.
    captured = {}

    def fake_post(url, **kwargs):
        captured.update(kwargs)
        raise requests.Timeout("read timed out")

    monkeypatch.setattr(requests, "post", fake_post)
    _client().run_ledger()
    timeout = captured["timeout"]
    assert (timeout[1] if isinstance(timeout, tuple) else timeout) == 300.0


class FakeBackendClient:
    def __init__(self, run):
        self.run = run
        self.calls = []

    def run_ledger(self, **kwargs):
        self.calls.append(kwargs)
        return self.run


def _pipeline(backend_run):
    pipeline = MoveLedgerPipeline()
    pipeline.backend_client = FakeBackendClient(backend_run)
    return pipeline


def _run(backend_run):
    pipeline = _pipeline(backend_run)
    ctx = PipelineContext("move_ledger")
    pipeline.execute(ctx)
    return pipeline, ctx


@pytest.mark.unit
def test_each_rebuilt_league_is_a_record_and_a_failed_one_fails_the_run():
    # A failed run, not a successful one with a failure counted: the post-game batch
    # skips a pipeline that already succeeded that night, so only this gets retried.
    pipeline = _pipeline(LedgerRun(ok=True, leagues=[
        LedgerLeague(provider_league_id="1001", season=2027, ok=True, days=14, moves=9),
        LedgerLeague(provider_league_id="2002", season=2027, ok=False, error="OperationalError"),
    ]))
    ctx = PipelineContext("move_ledger")
    with pytest.raises(MoveLedgerFailed, match="1 of 2 leagues not rebuilt: 2002/2027"):
        pipeline.execute(ctx)
    assert (ctx.records_processed, ctx.records_failed) == (1, 1)


@pytest.mark.unit
def test_a_backend_that_cannot_be_asked_fails_the_run():
    pipeline = _pipeline(LedgerRun.unavailable(reason="timeout"))
    ctx = PipelineContext("move_ledger")
    with pytest.raises(MoveLedgerFailed, match="timeout"):
        pipeline.execute(ctx)
    assert (ctx.records_processed, ctx.records_failed) == (0, 1)


@pytest.mark.unit
def test_every_league_rebuilt_is_a_successful_run():
    _, ctx = _run(LedgerRun(ok=True, leagues=[
        LedgerLeague(provider_league_id="1001", season=2027, ok=True, days=14, moves=9),
        LedgerLeague(provider_league_id="2002", season=2027, ok=True, days=3, moves=0),
    ]))
    assert (ctx.records_processed, ctx.records_failed) == (2, 0)


@pytest.mark.unit
def test_no_active_league_is_a_quiet_night():
    pipeline, ctx = _run(LedgerRun(ok=True, leagues=[]))
    assert (ctx.records_processed, ctx.records_failed) == (0, 0)
    assert len(pipeline.backend_client.calls) == 1
