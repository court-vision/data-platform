"""
`BackendClient.execute_pickups`: the one call per tick the scheduled-pickups
pipeline makes. A 200 envelope parses into a `PickupsRun` of `PickupExecution`s;
every other status, a malformed body, a timeout or a connection error is a run
that is not `ok` — and there is exactly one request, no retry. The bearer token
rides in the header; the pickups call has its own read timeout.
"""

import json

import pytest
import requests
import responses

from services.backend_client import EXECUTE_PICKUPS_PATH, BackendClient, PickupsRun

BASE = "http://api.railway.internal:8080"
URL = BASE + EXECUTE_PICKUPS_PATH
TOKEN = "pipeline-secret-token"


def _client():
    return BackendClient(base_url=BASE, token=TOKEN, timeout_seconds=45.0, pickups_timeout_seconds=120.0)


def _result(**over):
    payload = {
        "pickup_id": 5, "team_id": 21, "user_id": 11, "team_name": "GloatingSoap369",
        "outcome": "executed", "reason": None, "detail": None,
        "add": {"player_id": 6450, "name": "Kawhi Leonard", "team": "LAC", "nba_player_id": None},
        "drop": {"player_id": 4594268, "name": "Anthony Edwards", "team": "MIN", "nba_player_id": None},
        "nba_date": "2026-10-22", "scoring_period_id": 3, "seated_slot": "PG", "verified": True,
        "audit_id": 9, "next_attempt_at": None,
    }
    payload.update(over)
    return payload


def _envelope(*results, due=None):
    return {"status": "success", "message": "ok",
            "data": {"due": len(results) if due is None else due, "results": list(results)}}


@pytest.mark.unit
def test_disabled_without_base_url():
    client = BackendClient(base_url=None, token=TOKEN, timeout_seconds=45.0)
    run = client.execute_pickups()
    assert (run.ok, run.reason, run.results) == (False, "backend_not_configured", [])


@pytest.mark.unit
@responses.activate
def test_200_parses_the_results_and_sends_the_contract():
    responses.post(URL, json=_envelope(_result(), _result(pickup_id=6, outcome="deferred", reason="add_locked",
                                                           next_attempt_at="2026-10-22T06:00:00+00:00")), status=200)

    run = _client().execute_pickups(limit=3, correlation_id="corr-9")

    assert run.ok and run.due == 2 and run.reason is None
    first, second = run.results
    assert (first.pickup_id, first.outcome, first.seated_slot, first.verified, first.audit_id) == (5, "executed", "PG", True, 9)
    assert first.add["name"] == "Kawhi Leonard" and first.drop["name"] == "Anthony Edwards" and first.nba_date == "2026-10-22"
    assert (second.outcome, second.reason, second.next_attempt_at) == ("deferred", "add_locked", "2026-10-22T06:00:00+00:00")
    assert first.as_dict()["add"]["player_id"] == 6450

    assert len(responses.calls) == 1
    request = responses.calls[0].request
    assert request.headers["Authorization"] == f"Bearer {TOKEN}"
    assert request.headers["X-Correlation-ID"] == "corr-9"
    assert json.loads(request.body) == {"limit": 3}


@pytest.mark.unit
@responses.activate
def test_an_empty_tick_is_ok():
    responses.post(URL, json=_envelope(), status=200)
    run = _client().execute_pickups()
    assert run.ok and run.due == 0 and run.results == []


@pytest.mark.unit
@responses.activate
def test_a_result_without_an_outcome_makes_the_run_unavailable():
    responses.post(URL, json=_envelope(_result(outcome="done")), status=200)
    run = _client().execute_pickups()
    assert (run.ok, run.reason) == (False, "malformed_response") and "unexpected result" in run.error


@pytest.mark.unit
@responses.activate
def test_a_body_without_results_is_malformed():
    responses.post(URL, json={"status": "success", "message": "ok", "data": {"due": 1}}, status=200)
    run = _client().execute_pickups()
    assert (run.ok, run.reason) == (False, "malformed_response")


@pytest.mark.unit
@responses.activate
def test_401_and_500_are_unavailable():
    responses.post(URL, json={"detail": "Invalid token"}, status=401)
    assert _client().execute_pickups().reason == "unauthorized"
    responses.reset()
    responses.post(URL, json={"status": "error", "message": "boom", "error_code": "INTERNAL_ERROR"}, status=500)
    run = _client().execute_pickups()
    assert (run.ok, run.reason, run.error) == (False, "http_500", "boom")


@pytest.mark.unit
@responses.activate
def test_timeout_and_connection_errors_are_unavailable_after_one_request():
    responses.post(URL, body=requests.Timeout("read timed out"))
    run = _client().execute_pickups()
    assert (run.ok, run.reason) == (False, "timeout") and len(responses.calls) == 1
    responses.reset()
    responses.post(URL, body=requests.ConnectionError("refused"))
    assert _client().execute_pickups().reason == "ConnectionError"


@pytest.mark.unit
def test_pickups_timeout_falls_back_to_the_evaluate_timeout():
    assert BackendClient(base_url=BASE, token=TOKEN, timeout_seconds=45.0)._pickups_timeout == (5.0, 45.0)
    assert _client()._pickups_timeout == (5.0, 120.0)


@pytest.mark.unit
def test_unavailable_run_shape():
    run = PickupsRun.unavailable("timeout", error="x")
    assert (run.ok, run.due, run.results, run.reason, run.error) == (False, 0, [], "timeout", "x")
