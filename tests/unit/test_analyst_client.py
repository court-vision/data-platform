"""
`services.analyst_client`: the analyst pipeline's one call per read and the
month's spend. A 200 envelope parses into how the read went; anything else (no
base URL, a refused token, a provider failure, a timeout, a body that is not
the envelope) is `unavailable` with the reason, never an exception.
"""

import json

import pytest
import requests
import responses

from services.analyst_client import (
    READ_PATH,
    SPEND_PATH,
    UNAVAILABLE,
    AnalystClient,
    Month,
    ReadResult,
    analyst_client_from_settings,
)

pytestmark = pytest.mark.unit

BASE = "http://api.railway.internal:8080"
TOKEN = "pipeline-secret-token"
BODY = {"season": "2026-27", "as_of": "2026-11-16", "player_id": 7, "name": "Luka Dončić", "items": []}
PROPOSAL = {"decision": "write", "return_date": "2026-12-08", "cites": ["news:91"]}


def _read(outcome="read", **over):
    data = {
        "outcome": outcome, "proposal": PROPOSAL if outcome == "read" else None, "detail": None,
        "usage": {"model": "claude-opus-5-5", "input_tokens": 2000, "output_tokens": 500,
                  "cache_read_input_tokens": 1000, "cache_creation_input_tokens": 0, "cost_micros": 18200,
                  "fallback": False},
        "prompt_version": "analyst-1", "month_spent_micros": 18200, "month_cap_micros": 40_000_000,
    }
    data.update(over)
    return {"status": "success", "message": "ok", "data": data}


@responses.activate
def test_a_read_comes_back_as_its_proposal_and_what_it_cost():
    responses.add(responses.POST, BASE + READ_PATH, json=_read(), status=200)

    result = AnalystClient(BASE, TOKEN).read(BODY, correlation_id="corr-1")

    assert result.outcome == "read" and result.proposal == PROPOSAL
    assert (result.model, result.prompt_version) == ("claude-opus-5-5", "analyst-1")
    # Every token the call read, cached or not
    assert (result.input_tokens, result.output_tokens, result.cost_micros) == (3000, 500, 18200)
    (call,) = responses.calls
    assert call.request.headers["Authorization"] == f"Bearer {TOKEN}"
    assert call.request.headers["X-Correlation-ID"] == "corr-1"
    assert json.loads(call.request.body) == BODY


@responses.activate
@pytest.mark.parametrize("outcome", ["declined", "malformed", "budget", "off"])
def test_every_business_outcome_is_an_answer(outcome):
    responses.add(responses.POST, BASE + READ_PATH, status=200,
                  json=_read(outcome, detail="the answer was cut off" if outcome == "malformed" else None,
                             usage=None if outcome in ("budget", "off") else _read()["data"]["usage"]))

    result = AnalystClient(BASE, TOKEN).read(BODY)

    assert result.outcome == outcome and result.proposal is None
    assert result.cost_micros == (0 if outcome in ("budget", "off") else 18200)


def test_no_base_url_is_unavailable_without_a_request():
    client = AnalystClient(None, TOKEN)
    assert not client.enabled
    assert client.read(BODY) == ReadResult.unavailable("backend_not_configured")
    assert client.month() == Month.unavailable("backend_not_configured")


@responses.activate
@pytest.mark.parametrize("status, reason", [(401, "unauthorized"), (503, "http_503"), (422, "http_422")])
def test_a_failure_is_unavailable_with_its_reason(status, reason):
    responses.add(responses.POST, BASE + READ_PATH, json={"status": "error"}, status=status)
    result = AnalystClient(BASE, TOKEN).read(BODY)
    assert (result.outcome, result.detail) == (UNAVAILABLE, reason)


@responses.activate
def test_a_timeout_and_a_dropped_connection_are_unavailable():
    responses.add(responses.POST, BASE + READ_PATH, body=requests.Timeout("read timed out"))
    assert AnalystClient(BASE, TOKEN).read(BODY).detail == "timeout"
    responses.replace(responses.POST, BASE + READ_PATH, body=requests.ConnectionError("refused"))
    assert AnalystClient(BASE, TOKEN).read(BODY).detail == "ConnectionError"


@responses.activate
@pytest.mark.parametrize("payload", [
    {"status": "success", "data": None},
    {"status": "success", "data": {"outcome": "guessed"}},
    {"status": "success", "data": {"outcome": "read", "proposal": None}},
    ["not", "an", "envelope"],
])
def test_a_body_that_is_not_the_envelope_is_unavailable(payload):
    responses.add(responses.POST, BASE + READ_PATH, json=payload, status=200)
    result = AnalystClient(BASE, TOKEN).read(BODY)
    assert (result.outcome, result.detail) == (UNAVAILABLE, "malformed_response")


@responses.activate
def test_the_month_is_what_the_reads_have_spent_and_may():
    responses.add(responses.GET, BASE + SPEND_PATH, status=200, json={"status": "success", "data": {
        "enabled": True, "month_spent_micros": 39_800_000, "month_cap_micros": 40_000_000,
        "read_max_micros": 300_000, "model": "claude-opus-5-5", "prompt_version": "analyst-1"}})

    month = AnalystClient(BASE, TOKEN).month(correlation_id="corr-2")

    assert month.available and month.enabled and month.reason is None
    assert (month.spent_micros, month.cap_micros, month.read_max_micros) == (39_800_000, 40_000_000, 300_000)
    # 0.20 left is not one more read's 0.30
    assert month.room is False
    assert Month(available=True, enabled=True, spent_micros=0, cap_micros=40_000_000, read_max_micros=300_000).room
    assert responses.calls[0].request.headers["X-Correlation-ID"] == "corr-2"


@responses.activate
@pytest.mark.parametrize("status, payload, reason", [
    (401, {}, "unauthorized"), (500, {}, "http_500"), (200, {"data": {"spent": 1}}, "malformed_response"),
])
def test_a_month_that_cannot_be_read_says_why(status, payload, reason):
    responses.add(responses.GET, BASE + SPEND_PATH, json=payload, status=status)
    month = AnalystClient(BASE, TOKEN).month()
    assert (month.available, month.reason, month.room) == (False, reason, False)


def test_the_client_is_built_from_the_backends_url_and_the_shared_token():
    class Secret:
        def get_secret_value(self):
            return TOKEN

    settings = type("S", (), {"backend_internal_url": BASE + "/", "pipeline_api_token": Secret()})()
    assert analyst_client_from_settings(settings).enabled
