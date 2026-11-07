"""
/v1/dashboard/availability: token-gated, `days` passed through and bounded, and
a message that says what the scorecard covers. The service is stubbed; its
reads run in tests/integration/test_availability_scorecard_reads.py.
"""

import os
from datetime import date, datetime, timezone

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from api.v1 import availability
from schemas.availability import AvailabilityDay, AvailabilityScorecard, AvailabilitySummary

pytestmark = pytest.mark.api

_AUTH = {"Authorization": f"Bearer {os.environ.get('PIPELINE_API_TOKEN', 'test-token')}"}
PATH = "/v1/dashboard/availability"


def _scorecard(days: int, available: bool) -> AvailabilityScorecard:
    dates = [
        AvailabilityDay(game_date=date(2026, 11, 4), games=8, covered=True, calls=12),
        AvailabilityDay(game_date=date(2026, 11, 3), games=6, covered=True, calls=9),
        AvailabilityDay(game_date=date(2026, 11, 2), games=10, covered=False),
    ]
    return AvailabilityScorecard(
        days=days, since=date(2026, 10, 29), until=date(2026, 11, 4),
        generated_at=datetime(2026, 11, 5, 6, 0, tzinfo=timezone.utc), available=available,
        summary=AvailabilitySummary(races=7, us_first=4, false_outs=1),
        dates=dates if available else [], calls=[],
    )


@pytest.fixture
def client(monkeypatch):
    test_client = TestClient(FastAPI())
    test_client.calls = []
    test_client.available = True

    async def direct(fn, *args, **kwargs):
        return fn(*args, **kwargs)

    def build(days):
        test_client.calls.append(days)
        return _scorecard(days, test_client.available)

    monkeypatch.setattr(availability, "run_in_db_thread", direct)
    monkeypatch.setattr(availability, "build_scorecard", build)
    test_client.app.include_router(availability.router, prefix="/v1")
    return test_client


def test_needs_the_pipeline_token(client):
    assert client.get(PATH).status_code in (401, 403)


def test_defaults_to_a_week_and_says_what_it_covered(client):
    response = client.get(PATH, headers=_AUTH)

    assert response.status_code == 200
    body = response.json()
    assert body["message"] == "7 scratches raced and 1 false out over 2 covered dates"
    assert body["data"]["summary"]["us_first"] == 4
    assert client.calls == [7]


def test_days_is_bounded(client):
    assert client.get(PATH, params={"days": 0}, headers=_AUTH).status_code == 422
    assert client.get(PATH, params={"days": 31}, headers=_AUTH).status_code == 422
    assert client.get(PATH, params={"days": 30}, headers=_AUTH).status_code == 200
    assert client.calls == [30]


def test_an_empty_log_says_so(client):
    client.available = False

    body = client.get(PATH, params={"days": 3}, headers=_AUTH).json()

    assert body["message"] == "No availability reports in the last 3 days"
