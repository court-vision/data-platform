"""GET /v1/dashboard/freshness: token-gated, and the payload the page reads."""

import os
from datetime import date, datetime, timezone

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from api.v1 import dashboard
from schemas.dashboard import FreshnessData, TableFreshness, TableWriter

_AUTH = {"Authorization": f"Bearer {os.environ.get('PIPELINE_API_TOKEN', 'test-token')}"}


def _make_app() -> FastAPI:
    app = FastAPI()
    app.include_router(dashboard.router, prefix="/v1")
    return app


def _fake_freshness() -> FreshnessData:
    stats = TableWriter(name="player_game_stats", display_name="Player Game Stats")
    return FreshnessData(
        tables=[
            TableFreshness(
                table="nba.player_game_stats", pipelines=[stats], category="post_game",
                date_column="game_date", latest_date=date(2026, 3, 3),
                write_column="updated_at", latest_written_at=datetime(2026, 3, 4, 8, 1),
                rows_estimate=24_180, expected_date=date(2026, 3, 4), state="stale",
            ),
            TableFreshness(
                table="nba.player_profiles",
                pipelines=[TableWriter(name="player_profiles", display_name="Player Profiles")],
                category="scheduled", write_column="updated_at",
                latest_written_at=datetime(2026, 2, 1, 3, 0), rows_estimate=612, state="unjudged",
            ),
        ],
        season="2025-26", phase="regular", today=date(2026, 3, 5), settled_through=date(2026, 3, 4),
        last_game_date=date(2026, 3, 4), next_game_date=date(2026, 3, 5),
        fetched_at=datetime(2026, 3, 5, 13, 0, tzinfo=timezone.utc),
    )


@pytest.mark.api
def test_freshness_requires_bearer_token() -> None:
    assert TestClient(_make_app()).get("/v1/dashboard/freshness").status_code in (401, 403)


@pytest.mark.api
def test_freshness_returns_every_table_and_counts_the_stale_ones(monkeypatch) -> None:
    monkeypatch.setattr(dashboard, "build_freshness", _fake_freshness)

    res = TestClient(_make_app()).get("/v1/dashboard/freshness", headers=_AUTH)

    assert res.status_code == 200
    body = res.json()
    assert body["status"] == "success"
    assert body["message"] == "2 tables, 1 stale"
    data = body["data"]
    assert (data["phase"], data["last_game_date"], data["settled_through"]) == ("regular", "2026-03-04", "2026-03-04")
    stale, profiles = data["tables"]
    assert stale["state"] == "stale" and stale["expected_date"] == "2026-03-04"
    assert stale["latest_date"] == "2026-03-03" and stale["latest_written_at"] == "2026-03-04T08:01:00"
    assert stale["pipelines"] == [{"name": "player_game_stats", "display_name": "Player Game Stats"}]
    # Defaults are on the wire (ApiModel), so the generated types need no undefined checks.
    assert profiles["latest_date"] is None and profiles["expected_date"] is None and profiles["error"] is None
