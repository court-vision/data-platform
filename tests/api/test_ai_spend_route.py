"""
/v1/dashboard/ai-spend: token-gated, the window and cap changes passed to the
backend, the backend's own refusals passed back with its status and words, and
an unreachable backend a 502. The backend client is stubbed; its HTTP handling is
in tests/unit/test_ai_spend_client.py.
"""

import os
from decimal import Decimal

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from api.v1 import ai_spend
from services.ai_spend_client import BackendRefused, BackendUnavailable

pytestmark = pytest.mark.api

_AUTH = {"Authorization": f"Bearer {os.environ.get('PIPELINE_API_TOKEN', 'test-token')}"}

CAP = {"cap_micros": 100_000_000, "default_cap_micros": 100_000_000, "cap_set": False,
       "updated_at": None, "updated_by": None}
REPORT = {
    "as_of": "2026-11-20T12:00:00Z", "month_start": "2026-11-01", "since": "2026-10-22", "days": 30,
    "turn_max_micros": 1_000_000, "read_max_micros": 300_000,
    "lines": [{"line": "desk", "spent_micros": 12_340_000, "open_micros": 1_000_000, **CAP}],
    "app": {"spent_micros": 12_410_000, "open_micros": 1_000_000, **CAP},
    "rows": [{"day": "2026-11-20", "line": "autopilot", "feature": "checkin", "detail": "morning", "user_id": 1,
              "team_id": 22, "cost_micros": 70_000, "open_micros": 0, "entries": 1}],
    "usage": [{"day": "2026-11-20", "feature": "chat", "model": "claude-opus-5-5", "records": 3, "model_calls": 5,
               "input_tokens": 1_000, "output_tokens": 200, "cache_read_input_tokens": 9_000,
               "cache_creation_input_tokens": 0, "cost_micros": 40_000}],
    "users": [{"user_id": 1, "email": "me@courtvision.dev", "allowed": True,
               "desk": {"spent_micros": 12_340_000, **CAP},
               "autopilot": {"spent_micros": 70_000, "cap_micros": None, "default_cap_micros": None,
                             "cap_set": False, "updated_at": None, "updated_by": None}}],
    "teams": [{"team_id": 22, "user_id": 1, "name": "Mine"}],
}


class FakeClient:
    def __init__(self):
        self.calls = []
        self.fail = None
        self.answer = None

    def report(self, days, correlation_id=None):
        self.calls.append(("report", days))
        if self.fail:
            raise self.fail
        return REPORT if self.answer is None else self.answer

    def set_cap(self, body, correlation_id=None):
        self.calls.append(("set_cap", body))
        if self.fail:
            raise self.fail
        if self.answer is not None:
            return self.answer
        return {"line": body["line"], "user_id": body["user_id"], "cap_micros": 25_000_000,
                "default_cap_micros": 50_000_000, "cap_set": True, "updated_at": "2026-11-20T12:00:00Z",
                "updated_by": body["author"]}


@pytest.fixture
def client(monkeypatch):
    fake = FakeClient()
    monkeypatch.setattr(ai_spend, "_client", lambda: fake)
    app = FastAPI()
    app.include_router(ai_spend.router, prefix="/v1")
    test_client = TestClient(app)
    test_client.fake = fake
    return test_client


def test_needs_the_pipeline_token(client):
    assert client.get("/v1/dashboard/ai-spend").status_code in (401, 403)
    assert client.put("/v1/dashboard/ai-spend/caps", json={"line": "desk", "monthly_usd": 5}).status_code in (401, 403)
    assert client.fake.calls == []


def test_the_report_passes_through_over_thirty_days_by_default(client):
    response = client.get("/v1/dashboard/ai-spend", headers=_AUTH)

    assert response.status_code == 200
    data = response.json()["data"]
    assert data["lines"][0]["spent_micros"] == 12_340_000
    assert data["rows"][0]["detail"] == "morning"
    assert data["users"][0]["autopilot"]["cap_micros"] is None
    assert data["app"]["spent_micros"] == 12_410_000
    assert client.fake.calls == [("report", 30)]


def test_a_backend_with_no_whole_app_cap_still_reports(client):
    client.fake.answer = {k: v for k, v in REPORT.items() if k != "app"}

    response = client.get("/v1/dashboard/ai-spend", headers=_AUTH)

    assert response.status_code == 200 and response.json()["data"]["app"] is None


@pytest.mark.parametrize("days", [0, 91])
def test_the_window_is_one_to_ninety_days(client, days):
    assert client.get("/v1/dashboard/ai-spend", params={"days": days}, headers=_AUTH).status_code == 422
    assert client.fake.calls == []


def test_an_unreachable_backend_is_a_502_that_says_why(client):
    client.fake.fail = BackendUnavailable("timeout")

    response = client.get("/v1/dashboard/ai-spend", headers=_AUTH)

    assert response.status_code == 502
    assert response.json()["detail"] == "The backend could not be asked (timeout)"


def test_a_cap_change_goes_to_the_backend_as_dollars_in_a_string(client):
    response = client.put("/v1/dashboard/ai-spend/caps", headers=_AUTH,
                          json={"line": "desk", "user_id": 1, "monthly_usd": 25, "author": "  James "})

    assert response.status_code == 200
    assert response.json()["message"] == "Set the desk cap for user 1 to $25"
    assert response.json()["data"]["updated_by"] == "James"
    assert client.fake.calls == [("set_cap", {"line": "desk", "user_id": 1, "monthly_usd": "25", "author": "James"})]


def test_the_whole_app_s_cap_goes_to_the_backend_on_line_app(client):
    response = client.put("/v1/dashboard/ai-spend/caps", headers=_AUTH,
                          json={"line": "app", "monthly_usd": 100, "author": "James"})

    assert response.status_code == 200
    assert response.json()["message"] == "Set the whole app's cap to $100"
    assert client.fake.calls == [("set_cap", {"line": "app", "user_id": None, "monthly_usd": "100", "author": "James"})]


def test_clearing_a_cap_sends_null(client):
    response = client.put("/v1/dashboard/ai-spend/caps", headers=_AUTH, json={"line": "analyst", "monthly_usd": None})

    assert response.status_code == 200 and response.json()["message"] == "Cleared the analyst cap"
    assert client.fake.calls[0][1]["monthly_usd"] is None


@pytest.mark.parametrize("body", [
    {"line": "everything", "monthly_usd": 1},
    {"line": "desk", "monthly_usd": -1},
    {"line": "desk", "monthly_usd": 1000.5},
    {"line": "desk", "monthly_usd": "1.234"},
])
def test_a_cap_out_of_bounds_never_reaches_the_backend(client, body):
    assert client.put("/v1/dashboard/ai-spend/caps", headers=_AUTH, json=body).status_code == 422
    assert client.fake.calls == []


def test_the_backend_s_refusal_comes_back_in_its_words(client):
    client.fake.fail = BackendRefused(404, "No user 999", "AI_CAP_USER_NOT_FOUND")

    response = client.put("/v1/dashboard/ai-spend/caps", headers=_AUTH,
                          json={"line": "desk", "user_id": 999, "monthly_usd": str(Decimal("5.00"))})

    assert response.status_code == 404
    assert response.json()["detail"] == "No user 999"


def test_a_backend_answer_that_is_not_a_report_is_a_502(client):
    client.fake.answer = {}

    response = client.get("/v1/dashboard/ai-spend", headers=_AUTH)

    assert response.status_code == 502
    assert response.json()["detail"] == "The backend could not be asked (malformed_response)"


def test_a_backend_answer_that_is_not_a_cap_is_a_502_and_is_not_sent_again(client):
    client.fake.answer = {"line": "everything"}

    response = client.put("/v1/dashboard/ai-spend/caps", headers=_AUTH, json={"line": "desk", "monthly_usd": 5})

    assert response.status_code == 502
    assert response.json()["detail"] == "The backend could not be asked (malformed_response)"
    assert len(client.fake.calls) == 1
