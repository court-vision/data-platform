"""
Dime's asks, data-platform side (the Dime desk redesign, step 2):
`BackendClient.run_asks` (one call a tick, POST /v1/internal/jobs/asks/run) and
`pipelines.dime_asks`, which counts what the backend started. The backend owns
the asks and their check-ins; nothing here touches the database.
"""

import json

import pytest
import requests
import responses

from pipelines.context import PipelineContext
from pipelines.dime_asks import DimeAsksFailed, DimeAsksPipeline
from services.backend_client import RUN_ASKS_PATH, AskStarted, AsksRun, BackendClient

BASE = "http://api.railway.internal:8080"
URL = BASE + RUN_ASKS_PATH
TOKEN = "pipeline-secret-token"


def _client():
    return BackendClient(base_url=BASE, token=TOKEN, timeout_seconds=45.0)


def _envelope(**data):
    body = {"off": False, "started": [], "waiting": 0, "closed": 0, "stale": 0, **data}
    return {"status": "success", "message": "ok", "data": body}


@pytest.mark.unit
def test_the_path_is_the_backends():
    assert RUN_ASKS_PATH == "/v1/internal/jobs/asks/run"


@pytest.mark.unit
def test_disabled_without_base_url():
    run = BackendClient(base_url=None, token=TOKEN, timeout_seconds=45.0).run_asks()
    assert (run.ok, run.reason) == (False, "backend_not_configured")


@pytest.mark.unit
@responses.activate
def test_200_parses_what_was_started_and_sends_the_contract():
    started = {"ask_id": 4, "team_id": 28, "checkin_id": 91}
    responses.post(URL, json=_envelope(started=[started], waiting=1, closed=2, stale=1), status=200)
    run = _client().run_asks(correlation_id="corr-9")
    assert run.ok and not run.off and (run.waiting, run.closed, run.stale) == (1, 2, 1)
    assert run.started == [AskStarted(ask_id=4, team_id=28, checkin_id=91)]

    request = responses.calls[0].request
    assert request.headers["Authorization"] == f"Bearer {TOKEN}"
    assert request.headers["X-Correlation-ID"] == "corr-9"
    assert json.loads(request.body) == {}


@pytest.mark.unit
@responses.activate
def test_off_in_the_backend_parses():
    responses.post(URL, json=_envelope(off=True), status=200)
    run = _client().run_asks()
    assert run.ok and run.off and run.started == []


@pytest.mark.unit
@responses.activate
@pytest.mark.parametrize("status,body,reason", [
    (401, {"detail": "Invalid pipeline token"}, "unauthorized"),
    (500, {"status": "error", "message": "boom", "error_code": "INTERNAL_ERROR"}, "http_500"),
    (200, {"status": "success", "message": "ok", "data": None}, "malformed_response"),
    (200, {"status": "success", "message": "ok", "data": {"started": [{"ask_id": 1}]}}, "malformed_response"),
    (200, {"status": "success", "message": "ok", "data": {"started": [], "closed": "lots"}}, "malformed_response"),
])
def test_anything_else_is_a_run_that_is_not_ok(status, body, reason):
    responses.post(URL, json=body, status=status)
    run = _client().run_asks()
    assert (run.ok, run.reason) == (False, reason)
    assert len(responses.calls) == 1


@pytest.mark.unit
@responses.activate
def test_a_timeout_is_not_retried():
    responses.post(URL, body=requests.Timeout("read timed out"))
    run = _client().run_asks()
    assert (run.ok, run.reason) == (False, "timeout")
    assert len(responses.calls) == 1


class FakeBackendClient:
    def __init__(self, run):
        self.run = run
        self.calls = []

    def run_asks(self, **kwargs):
        self.calls.append(kwargs)
        return self.run


def _run(backend_run):
    pipeline = DimeAsksPipeline()
    pipeline.backend_client = FakeBackendClient(backend_run)
    ctx = PipelineContext("dime_asks")
    pipeline.execute(ctx)
    return pipeline, ctx


@pytest.mark.unit
def test_each_ask_started_is_a_record_and_closed_ones_are_skipped():
    _, ctx = _run(AsksRun(ok=True, closed=2, waiting=1, started=[
        AskStarted(ask_id=4, team_id=28, checkin_id=91),
        AskStarted(ask_id=5, team_id=23, checkin_id=92),
    ]))
    # A waiting ask is neither: the next tick takes it
    assert (ctx.records_processed, ctx.records_skipped, ctx.records_failed) == (2, 2, 0)


@pytest.mark.unit
def test_a_backend_that_cannot_be_asked_fails_the_run():
    pipeline = DimeAsksPipeline()
    pipeline.backend_client = FakeBackendClient(AsksRun.unavailable(reason="timeout"))
    ctx = PipelineContext("dime_asks")
    with pytest.raises(DimeAsksFailed, match="timeout"):
        pipeline.execute(ctx)
    assert ctx.records_failed == 1


@pytest.mark.unit
def test_asks_switched_off_in_the_backend_is_a_quiet_tick():
    pipeline, ctx = _run(AsksRun(ok=True, off=True))
    assert (ctx.records_processed, ctx.records_failed) == (0, 0)
    assert len(pipeline.backend_client.calls) == 1
