"""
The AI spend page's backend client: the report and a cap change go out with the
pipeline token, the backend's refusals come back with its status and words, and
anything else is unavailable with a reason.
"""

import json

import pytest
import requests
import responses

from services.ai_spend_client import CAPS_PATH, SPEND_PATH, AiSpendClient, BackendRefused, BackendUnavailable

pytestmark = pytest.mark.unit

BASE = "http://backend.internal:8000"
TOKEN = "pipe-token"


@responses.activate
def test_the_report_is_asked_for_over_its_window_with_the_token():
    responses.add(responses.GET, BASE + SPEND_PATH, json={"status": "success", "data": {"days": 7}}, status=200)

    assert AiSpendClient(BASE + "/", TOKEN).report(7, correlation_id="corr-1") == {"days": 7}
    (call,) = responses.calls
    assert call.request.url == f"{BASE}{SPEND_PATH}?days=7"
    assert call.request.headers["Authorization"] == f"Bearer {TOKEN}"
    assert call.request.headers["X-Correlation-ID"] == "corr-1"


@responses.activate
def test_a_cap_change_is_sent_as_given():
    body = {"line": "desk", "user_id": 3, "monthly_usd": "25.00", "author": "James"}
    responses.add(responses.PUT, BASE + CAPS_PATH, json={"status": "success", "data": {"cap_micros": 25_000_000}})

    assert AiSpendClient(BASE, TOKEN).set_cap(body) == {"cap_micros": 25_000_000}
    assert json.loads(responses.calls[0].request.body) == body


@responses.activate
@pytest.mark.parametrize("status, payload, message, code", [
    (404, {"status": "error", "message": "No user 999", "error_code": "AI_CAP_USER_NOT_FOUND"},
     "No user 999", "AI_CAP_USER_NOT_FOUND"),
    (400, {"detail": "The analyst's reads belong to no user"}, "The analyst's reads belong to no user", None),
    (422, {"detail": [{"msg": "Input should be less than or equal to 1000"}]},
     "Input should be less than or equal to 1000", None),
])
def test_a_refusal_comes_back_in_the_backend_s_words(status, payload, message, code):
    responses.add(responses.PUT, BASE + CAPS_PATH, json=payload, status=status)

    with pytest.raises(BackendRefused) as refused:
        AiSpendClient(BASE, TOKEN).set_cap({"line": "desk"})
    assert (refused.value.status_code, refused.value.message, refused.value.error_code) == (status, message, code)


@responses.activate
@pytest.mark.parametrize("kwargs, reason", [
    ({"status": 500, "body": "boom"}, "http_500"),
    ({"status": 401, "json": {"detail": "bad token"}}, "unauthorized"),
    ({"status": 200, "body": "not json"}, "malformed_response"),
    ({"status": 200, "json": {"status": "success", "data": None}}, "malformed_response"),
    ({"body": requests.Timeout("slow")}, "timeout"),
    ({"body": requests.ConnectionError("refused")}, "ConnectionError"),
])
def test_anything_else_is_unavailable_with_a_reason(kwargs, reason):
    responses.add(responses.GET, BASE + SPEND_PATH, **kwargs)

    with pytest.raises(BackendUnavailable) as unavailable:
        AiSpendClient(BASE, TOKEN).report(30)
    assert unavailable.value.reason == reason


def test_no_base_url_is_unavailable_without_a_request():
    with pytest.raises(BackendUnavailable) as unavailable:
        AiSpendClient(None, TOKEN).report(30)
    assert unavailable.value.reason == "backend_not_configured"
