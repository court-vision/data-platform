"""
Dime's check-ins, data-platform side (the Dime autopilot plan, step 3):
`BackendClient.run_checkins` (one call a tick, POST /v1/internal/jobs/checkins/run)
and `pipelines.dime_checkins`, which counts what the backend started. The backend
owns the schedule and the check-ins; nothing here touches the database.
"""

import json

import pytest
import requests
import responses

from pipelines.context import PipelineContext
from pipelines.dime_checkins import DimeCheckinsFailed, DimeCheckinsPipeline
from services.backend_client import RUN_CHECKINS_PATH, BackendClient, CheckinsRun, CheckinStarted

BASE = "http://api.railway.internal:8080"
URL = BASE + RUN_CHECKINS_PATH
TOKEN = "pipeline-secret-token"


def _client():
    return BackendClient(base_url=BASE, token=TOKEN, timeout_seconds=45.0)


def _envelope(**data):
    body = {"off": False, "teams": 1, "started": [], "missed": 0, "skipped": 0, "stale": 0, **data}
    return {"status": "success", "message": "ok", "data": body}


@pytest.mark.unit
def test_disabled_without_base_url():
    run = BackendClient(base_url=None, token=TOKEN, timeout_seconds=45.0).run_checkins()
    assert (run.ok, run.reason) == (False, "backend_not_configured")


@pytest.mark.unit
@responses.activate
def test_200_parses_what_was_started_and_sends_the_contract():
    started = {"checkin_id": 7, "team_id": 23, "kind": "morning", "date": "2026-11-10"}
    responses.post(URL, json=_envelope(started=[started], missed=1, stale=2), status=200)
    run = _client().run_checkins(correlation_id="corr-9")
    assert run.ok and (run.teams, run.missed, run.skipped, run.stale) == (1, 1, 0, 2)
    assert run.started == [CheckinStarted(checkin_id=7, team_id=23, kind="morning", date="2026-11-10")]

    request = responses.calls[0].request
    assert request.headers["Authorization"] == f"Bearer {TOKEN}"
    assert request.headers["X-Correlation-ID"] == "corr-9"
    assert json.loads(request.body) == {}


@pytest.mark.unit
@responses.activate
@pytest.mark.parametrize("status,body,reason", [
    (401, {"detail": "Invalid pipeline token"}, "unauthorized"),
    (500, {"status": "error", "message": "boom", "error_code": "INTERNAL_ERROR"}, "http_500"),
    (200, {"status": "success", "message": "ok", "data": None}, "malformed_response"),
    (200, {"status": "success", "message": "ok", "data": {"started": [{"checkin_id": 1}]}}, "malformed_response"),
])
def test_anything_else_is_a_run_that_is_not_ok(status, body, reason):
    responses.post(URL, json=body, status=status)
    run = _client().run_checkins()
    assert (run.ok, run.reason) == (False, reason)
    assert len(responses.calls) == 1


@pytest.mark.unit
@responses.activate
def test_a_timeout_is_not_retried():
    responses.post(URL, body=requests.Timeout("read timed out"))
    run = _client().run_checkins()
    assert (run.ok, run.reason) == (False, "timeout")
    assert len(responses.calls) == 1


class FakeBackendClient:
    def __init__(self, run):
        self.run = run
        self.calls = []

    def run_checkins(self, **kwargs):
        self.calls.append(kwargs)
        return self.run


def _run(backend_run):
    pipeline = DimeCheckinsPipeline()
    pipeline.backend_client = FakeBackendClient(backend_run)
    ctx = PipelineContext("dime_checkins")
    pipeline.execute(ctx)
    return pipeline, ctx


@pytest.mark.unit
def test_each_check_in_started_is_a_record_and_missed_ones_are_skipped():
    _, ctx = _run(CheckinsRun(ok=True, teams=1, missed=1, skipped=1, started=[
        CheckinStarted(checkin_id=7, team_id=23, kind="night", date="2026-11-09"),
    ]))
    assert (ctx.records_processed, ctx.records_skipped, ctx.records_failed) == (1, 2, 0)


@pytest.mark.unit
def test_a_backend_that_cannot_be_asked_fails_the_run():
    pipeline = DimeCheckinsPipeline()
    pipeline.backend_client = FakeBackendClient(CheckinsRun.unavailable(reason="timeout"))
    ctx = PipelineContext("dime_checkins")
    with pytest.raises(DimeCheckinsFailed, match="timeout"):
        pipeline.execute(ctx)
    assert ctx.records_failed == 1


@pytest.mark.unit
def test_check_ins_switched_off_in_the_backend_is_a_quiet_tick():
    pipeline, ctx = _run(CheckinsRun(ok=True, off=True))
    assert (ctx.records_processed, ctx.records_failed) == (0, 0)
    assert len(pipeline.backend_client.calls) == 1
