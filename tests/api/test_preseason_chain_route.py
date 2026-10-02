"""
POST /pipelines/preseason-market runs a chain: player-profiles, then the
market snapshot, then cv-projection.

The order is the point. The snapshot can only attach ESPN's rank and line to a
player with an nba.players row, and a new player gets that row from
player-profiles — with profiles second, the first CV projection of 2026-27 was
published without the season's 13 rookies. Each link runs whatever the one
before it did.
"""

import os
from datetime import date

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from api.v1 import pipelines as pipelines_api
from schemas.common import ApiStatus
from schemas.pipeline import PipelineResult

pytestmark = pytest.mark.api

PATH = "/v1/internal/pipelines/preseason-market"
CHAIN = ["player_profiles", "preseason_market", "cv_projection"]


def _client() -> TestClient:
    app = FastAPI()
    app.include_router(pipelines_api.router, prefix="/v1/internal")
    return TestClient(app)


def _auth() -> dict:
    return {"Authorization": f"Bearer {os.environ.get('PIPELINE_API_TOKEN', 'test-token')}"}


@pytest.fixture
def chain(monkeypatch):
    """Fake run_pipeline: records calls, and fails the pipelines named in `chain.failing`."""

    class Chain:
        def __init__(self):
            self.calls = []
            self.failing = set()
            self.logged = []

    state = Chain()

    async def fake_run_pipeline(name, date_override=None, options=None):
        state.calls.append((name, date_override, options))
        failed = name in state.failing
        return PipelineResult(
            status=ApiStatus.ERROR if failed else ApiStatus.SUCCESS,
            message=f"{name} {'failed' if failed else 'ok'}",
            started_at="2026-10-02T00:00:00",
            records_processed=0,
        )

    class RecordingLog:
        def info(self, event, **fields):
            state.logged.append((event, fields))

    monkeypatch.setattr(pipelines_api, "run_pipeline", fake_run_pipeline)
    monkeypatch.setattr(pipelines_api, "log", RecordingLog())
    return state


def _statuses(chain):
    events = [fields for event, fields in chain.logged if event == "preseason_chain_complete"]
    assert len(events) == 1
    return events[0]


def test_profiles_run_before_the_market_snapshot(chain):
    res = _client().post(PATH, headers=_auth())
    assert res.status_code == 200
    assert [name for name, _, _ in chain.calls] == CHAIN


def test_date_and_league_reach_the_right_links(chain):
    res = _client().post(f"{PATH}?date=2026-10-01&league_id=42", headers=_auth())
    assert res.status_code == 200
    assert chain.calls == [
        ("player_profiles", None, None),
        ("preseason_market", date(2026, 10, 1), {"league_id": 42}),
        ("cv_projection", date(2026, 10, 1), None),
    ]


def test_response_is_the_market_runs(chain):
    body = _client().post(PATH, headers=_auth()).json()
    assert body["status"] == "success" and body["message"] == "preseason_market ok"


@pytest.mark.parametrize("failing", CHAIN)
def test_every_link_runs_whatever_the_others_did(chain, failing):
    chain.failing = {failing}
    res = _client().post(PATH, headers=_auth())
    assert res.status_code == 200
    assert [name for name, _, _ in chain.calls] == CHAIN
    assert _statuses(chain) == {
        name: (ApiStatus.ERROR if name == failing else ApiStatus.SUCCESS) for name in CHAIN
    }
    # The cron-runner reads the market run's status, not the chain's.
    assert res.json()["status"] == ("error" if failing == "preseason_market" else "success")
