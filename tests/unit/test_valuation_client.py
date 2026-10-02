"""
`services.valuation_client`: the projections editor's one call to the backend
per board. A 200 envelope parses into ranks keyed by player; anything else —
no base URL, a refused token, a 5xx, a timeout, a body that is not the
envelope — is an unavailable `Valuation` with the reason, never an exception:
the editor still has its four lines without ranks. The bearer token rides in
the header and never in a log line.
"""

import pytest
import requests
import responses

from services.valuation_client import (
    VALUATION_PATH,
    Valuation,
    ValuationClient,
    valuation_client_from_settings,
)

pytestmark = pytest.mark.unit

BASE = "http://api.railway.internal:8080"
URL = BASE + VALUATION_PATH
TOKEN = "pipeline-secret-token"
PLAYERS = [{"player_id": 1, "line": {"pts": 30.0}, "games": 70, "team": "DEN", "dd_rate": 0.5, "td_rate": 0.1},
           {"player_id": 2, "line": {"pts": 12.0}, "games": 60, "team": None, "dd_rate": 0.0, "td_rate": 0.0}]


def _envelope(**overrides):
    data = {
        "league_size": 12, "rounds": 13, "playoff_weight": 2.0, "playoff_weeks": [20, 21, 22, 23],
        "players": [
            {"player_id": 1, "points_rank": 1, "points_value": 30.0, "points_season": 2100.0,
             "category_rank": 2, "category_value": 31.5, "category_score": 1.3, "games": 70.0},
            {"player_id": 2, "points_rank": 2, "points_value": 12.0, "points_season": 720.0,
             "category_rank": 1, "category_value": 33.0, "category_score": 1.6, "games": 60.0},
        ],
    }
    data.update(overrides)
    return {"status": "success", "message": "ok", "data": data}


@responses.activate
def test_a_200_envelope_becomes_ranks_by_player():
    responses.add(responses.POST, URL, json=_envelope(), status=200)

    valuation = ValuationClient(BASE, TOKEN).standard(PLAYERS, correlation_id="corr-1")

    assert valuation.available and valuation.reason is None
    assert (valuation.league_size, valuation.rounds, valuation.playoff_weight) == (12, 13, 2.0)
    assert valuation.playoff_weeks == (20, 21, 22, 23)
    assert (valuation.ranks[1].points_rank, valuation.ranks[1].category_rank) == (1, 2)
    assert (valuation.ranks[2].points_rank, valuation.ranks[2].category_rank) == (2, 1)

    (call,) = responses.calls
    assert call.request.headers["Authorization"] == f"Bearer {TOKEN}"
    assert call.request.headers["X-Correlation-ID"] == "corr-1"
    import json
    assert json.loads(call.request.body) == {"players": PLAYERS}


def test_no_base_url_is_unavailable_without_a_request():
    valuation = ValuationClient(None, TOKEN).standard(PLAYERS)
    assert not valuation.available and valuation.reason == "backend_not_configured"
    assert valuation.ranks == {}


@responses.activate
def test_an_empty_pool_asks_nothing():
    valuation = ValuationClient(BASE, TOKEN).standard([])
    assert valuation.available and valuation.ranks == {} and len(responses.calls) == 0


@responses.activate
@pytest.mark.parametrize("status, reason", [(401, "unauthorized"), (422, "http_422"), (503, "http_503")])
def test_a_refusal_is_unavailable_with_the_status(status, reason):
    responses.add(responses.POST, URL, json={"detail": "no"}, status=status)
    valuation = ValuationClient(BASE, TOKEN).standard(PLAYERS)
    assert not valuation.available and valuation.reason == reason
    assert len(responses.calls) == 1                     # one request, no retry


@responses.activate
@pytest.mark.parametrize("body", [
    {"status": "success", "data": None},
    {"status": "success", "data": {"players": [{"player_id": 1}]}},        # a rank missing
    {"status": "success", "data": {"league_size": 12, "rounds": 13, "playoff_weight": 2.0}},   # no players
    ["not", "an", "envelope"],
])
def test_a_body_that_is_not_the_envelope_is_unavailable(body):
    responses.add(responses.POST, URL, json=body, status=200)
    valuation = ValuationClient(BASE, TOKEN).standard(PLAYERS)
    assert not valuation.available and valuation.reason == "malformed_response"


@responses.activate
@pytest.mark.parametrize("error, reason", [
    (requests.Timeout("read timed out"), "timeout"),
    (requests.ConnectionError("refused"), "ConnectionError"),
])
def test_transport_failures_are_unavailable_not_raised(error, reason):
    responses.add(responses.POST, URL, body=error)
    valuation = ValuationClient(BASE, TOKEN).standard(PLAYERS)
    assert not valuation.available and valuation.reason == reason


@responses.activate
def test_the_token_never_reaches_a_log_line():
    lines = []

    class FakeLog:
        def info(self, event, **kw):
            lines.append((event, kw))

        warning = info

    client = ValuationClient(BASE, TOKEN)
    client._log = FakeLog()
    responses.add(responses.POST, URL, json=_envelope(), status=200)
    responses.add(responses.POST, URL, body="upstream exploded", status=502)
    client.standard(PLAYERS)
    client.standard(PLAYERS)

    assert [event for event, _ in lines] == ["backend_standard_valuation"] * 2
    assert lines[0][1]["available"] is True and lines[0][1]["players"] == 2
    assert lines[1][1]["reason"] == "http_502" and lines[1][1]["error"] == "upstream exploded"
    assert TOKEN not in repr(lines)


def test_from_settings_reads_the_shared_url_and_token():
    from types import SimpleNamespace

    from pydantic import SecretStr

    settings = SimpleNamespace(backend_internal_url=f"{BASE}/", pipeline_api_token=SecretStr(TOKEN))
    client = valuation_client_from_settings(settings)
    assert client.enabled and client._base_url == BASE and client._token == TOKEN
    assert not valuation_client_from_settings(
        SimpleNamespace(backend_internal_url=None, pipeline_api_token=SecretStr(TOKEN))
    ).enabled


def test_unavailable_carries_no_ranks():
    assert Valuation.unavailable("timeout") == Valuation(available=False, reason="timeout")
