"""
`services.backend_client`: the one call per team the lineup-alerts pipeline
makes to the backend. A 200 envelope parses into `LineupEvaluation`; a 404
`TEAM_NOT_FOUND` is `skipped`; every other status, a malformed body, a timeout
or a connection error is `unavailable` — and there is exactly one request, no
retry. The bearer token rides in the header and never in a log line.
"""

from datetime import date

import pytest
import requests
import responses

from services import backend_client as client_module
from services.backend_client import (
    EVALUATE_PATH,
    BackendClient,
    LineupEvaluation,
    backend_client_from_settings,
)

BASE = "http://api.railway.internal:8080"
URL = BASE + EVALUATE_PATH
TOKEN = "pipeline-secret-token"


def _client(timeout=45.0):
    return BackendClient(base_url=BASE, token=TOKEN, timeout_seconds=timeout)


def _evaluate(client, **overrides):
    kwargs = dict(team_id=21, user_id=7, nba_date=date(2026, 10, 20), apply=False, correlation_id="corr-123")
    kwargs.update(overrides)
    return client.evaluate_lineup(**kwargs)


def _envelope(**data):
    payload = {
        "outcome": "planned",
        "reason": None,
        "moves": [
            {"player_id": 1, "name": "A", "from_slot_id": 12, "from_slot": "BE", "to_slot_id": 11,
             "to_slot": "UT", "role": "start", "note": "vs LAL · 7:30 PM"},
            {"player_id": 2, "name": "B", "from_slot_id": 11, "from_slot": "UT", "to_slot_id": 12,
             "to_slot": "BE", "role": "bench", "note": "no game today"},
        ],
        "unfilled": [{"player_id": 3, "name": "C", "slot": "BE", "reason": "no eligible open slot"}],
        "verified": None,
        "scoring_period_id": 1,
        "nba_date": "2026-10-20",
        "first_game_time_et": "19:30",
        "team_name": "Test Team",
        "audit_id": None,
    }
    payload.update(data)
    return {"status": "success", "message": "ok", "data": payload}


@pytest.mark.unit
def test_disabled_without_base_url():
    client = BackendClient(base_url=None, token=TOKEN, timeout_seconds=45.0)
    assert client.enabled is False
    assert client.base_url is None
    evaluation = _evaluate(client)
    assert (evaluation.outcome, evaluation.reason) == ("unavailable", "backend_not_configured")


@pytest.mark.unit
def test_enabled_strips_trailing_slash():
    client = BackendClient(base_url=BASE + "/", token=TOKEN, timeout_seconds=45.0)
    assert client.enabled is True
    assert client.base_url == BASE


@pytest.mark.unit
@responses.activate
def test_200_parses_the_envelope_and_sends_the_contract():
    responses.post(URL, json=_envelope(), status=200)

    evaluation = _evaluate(_client(), apply=True)

    assert evaluation.outcome == "planned"
    assert evaluation.reason is None
    assert [m["role"] for m in evaluation.moves] == ["start", "bench"]
    assert evaluation.unfilled[0]["name"] == "C"
    assert evaluation.scoring_period_id == 1
    assert evaluation.nba_date == "2026-10-20"
    assert evaluation.first_game_time_et == "19:30"
    assert evaluation.team_name == "Test Team"
    assert evaluation.error is None

    assert len(responses.calls) == 1
    request = responses.calls[0].request
    assert request.headers["Authorization"] == f"Bearer {TOKEN}"
    assert request.headers["X-Correlation-ID"] == "corr-123"
    assert request.headers["Content-Type"] == "application/json"
    import json
    assert json.loads(request.body) == {"team_id": 21, "user_id": 7, "nba_date": "2026-10-20", "apply": True}


@pytest.mark.unit
@responses.activate
def test_applied_outcome_carries_verified():
    responses.post(URL, json=_envelope(outcome="applied", verified=False), status=200)
    evaluation = _evaluate(_client(), apply=True)
    assert evaluation.outcome == "applied"
    assert evaluation.verified is False


@pytest.mark.unit
@responses.activate
def test_no_correlation_header_when_none():
    responses.post(URL, json=_envelope(outcome="noop", moves=[], unfilled=[]), status=200)
    evaluation = _evaluate(_client(), correlation_id=None)
    assert evaluation.outcome == "noop"
    assert "X-Correlation-ID" not in responses.calls[0].request.headers


@pytest.mark.unit
@responses.activate
def test_404_team_not_found_is_skipped():
    responses.post(
        URL,
        json={"status": "error", "message": "Team 21 not found", "error_code": "TEAM_NOT_FOUND"},
        status=404,
    )
    evaluation = _evaluate(_client())
    assert (evaluation.outcome, evaluation.reason) == ("skipped", "team_not_found")
    assert evaluation.moves == [] and evaluation.unfilled == []


@pytest.mark.unit
@responses.activate
def test_404_without_team_not_found_code_is_unavailable():
    responses.post(URL, json={"detail": "Not Found"}, status=404)
    evaluation = _evaluate(_client())
    assert (evaluation.outcome, evaluation.reason) == ("unavailable", "http_404")


@pytest.mark.unit
@responses.activate
def test_401_is_unavailable_unauthorized():
    responses.post(URL, json={"detail": "Invalid pipeline authentication token"}, status=401)
    evaluation = _evaluate(_client())
    assert (evaluation.outcome, evaluation.reason) == ("unavailable", "unauthorized")
    assert "Invalid pipeline authentication token" in evaluation.error


@pytest.mark.unit
@responses.activate
def test_500_is_unavailable_with_one_request_only():
    responses.post(URL, json={"status": "error", "message": "boom", "error_code": "INTERNAL"}, status=500)
    evaluation = _evaluate(_client())
    assert (evaluation.outcome, evaluation.reason) == ("unavailable", "http_500")
    assert evaluation.error == "boom"
    assert len(responses.calls) == 1  # no retry


@pytest.mark.unit
@responses.activate
def test_connection_error_is_unavailable():
    responses.post(URL, body=requests.ConnectionError("connection refused"))
    evaluation = _evaluate(_client())
    assert evaluation.outcome == "unavailable"
    assert evaluation.reason == "ConnectionError"
    assert "connection refused" in evaluation.error
    assert len(responses.calls) == 1


@pytest.mark.unit
@responses.activate
def test_timeout_is_unavailable_timeout():
    responses.post(URL, body=requests.ReadTimeout("read timed out"))
    evaluation = _evaluate(_client())
    assert (evaluation.outcome, evaluation.reason) == ("unavailable", "timeout")


@pytest.mark.unit
@responses.activate
def test_200_with_unknown_outcome_is_unavailable_malformed():
    responses.post(URL, json=_envelope(outcome="exploded"), status=200)
    evaluation = _evaluate(_client())
    assert (evaluation.outcome, evaluation.reason) == ("unavailable", "malformed_response")


@pytest.mark.unit
@responses.activate
def test_200_without_data_is_unavailable_malformed():
    responses.post(URL, json={"status": "success", "message": "ok"}, status=200)
    evaluation = _evaluate(_client())
    assert (evaluation.outcome, evaluation.reason) == ("unavailable", "malformed_response")


@pytest.mark.unit
@responses.activate
def test_non_json_body_is_unavailable():
    responses.post(URL, body="<html>bad gateway</html>", status=502)
    evaluation = _evaluate(_client())
    assert (evaluation.outcome, evaluation.reason) == ("unavailable", "http_502")
    assert "bad gateway" in evaluation.error


@pytest.mark.unit
def test_timeout_is_passed_as_connect_read_tuple(monkeypatch):
    captured = {}

    class FakeResponse:
        status_code = 200
        text = ""

        @staticmethod
        def json():
            return _envelope(outcome="noop", moves=[], unfilled=[])

    def fake_post(url, **kwargs):
        captured["url"] = url
        captured.update(kwargs)
        return FakeResponse()

    monkeypatch.setattr(client_module.requests, "post", fake_post)

    evaluation = _evaluate(_client(timeout=45.0))

    assert evaluation.outcome == "noop"
    assert captured["url"] == URL
    assert captured["timeout"] == (5.0, 45.0)
    assert captured["headers"]["Authorization"] == f"Bearer {TOKEN}"
    assert captured["json"]["nba_date"] == "2026-10-20"


@pytest.mark.unit
def test_token_never_appears_in_the_log_line(monkeypatch):
    lines = []

    class FakeLog:
        def info(self, event, **kw):
            lines.append((event, kw))

        warning = info

    client = _client()
    monkeypatch.setattr(client, "_log", FakeLog())
    monkeypatch.setattr(
        client_module.requests, "post",
        lambda url, **kw: (_ for _ in ()).throw(requests.ConnectionError(f"refused for {url}")),
    )

    _evaluate(client)

    assert len(lines) == 1
    event, fields = lines[0]
    assert event == "backend_evaluate_lineup"
    assert fields["outcome"] == "unavailable" and fields["status_code"] is None
    assert fields["team_id"] == 21 and "elapsed_ms" in fields
    assert TOKEN not in repr(lines)


@pytest.mark.unit
def test_factory_reads_settings(monkeypatch):
    from pydantic import SecretStr

    from core.settings import settings

    monkeypatch.setattr(settings, "backend_internal_url", "http://api-staging.railway.internal:8080/")
    monkeypatch.setattr(settings, "pipeline_api_token", SecretStr("from-settings"))
    monkeypatch.setattr(settings, "backend_timeout_seconds", 12.5)

    client = backend_client_from_settings()

    assert client.enabled is True
    assert client.base_url == "http://api-staging.railway.internal:8080"
    assert client._token == "from-settings"
    assert client._timeout == (5.0, 12.5)


@pytest.mark.unit
def test_from_payload_tolerates_missing_lists():
    evaluation = LineupEvaluation.from_payload({"outcome": "noop", "moves": None, "unfilled": None})
    assert evaluation.moves == [] and evaluation.unfilled == []
    assert evaluation.team_name == ""
