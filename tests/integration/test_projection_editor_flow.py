"""
The projections editor end to end against Postgres: the page reads the same
inputs the pipeline does, a save writes one version and republishes, and the
published snapshot is then exactly what the page shows.

Only the backend's valuation is stubbed. The tables come from the models, as
in the rest of this suite; the two things the SQL migration adds that an edit
depends on (the deferred self-reference and the one-live-row index) are put on
here, so `ProjectionAdjustment.record` runs in the order production runs it.
"""

from __future__ import annotations

import os
from datetime import date, datetime

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from api.v1 import projections
from core.settings import settings
from db.base import db
from db.models.nba import (
    DraftMarket,
    PlayerHistory,
    PlayerProfile,
    PlayerProjection,
    ProjectionAdjustment,
)
from db.models.nba.players import Player
from services.projection_model import season_label, season_start
from services.valuation_client import StandardRank, Valuation

pytestmark = pytest.mark.integration

_AUTH = {"Authorization": f"Bearer {os.environ.get('PIPELINE_API_TOKEN', 'test-token')}"}
BASE = "/v1/dashboard/projections"
SEASON = settings.nba_season
LAST_SEASON = season_label(season_start(SEASON) - 1)
STAR, VET, RETIRED = 201, 202, 203
MODELS = [PlayerProfile, PlayerHistory, PlayerProjection, ProjectionAdjustment, DraftMarket]


@pytest.fixture(scope="module", autouse=True)
def editor_tables(integration_db):
    db.create_tables(MODELS, safe=True)
    db.execute_sql("""
        DO $$ BEGIN
            ALTER TABLE nba.projection_adjustments
                ALTER CONSTRAINT projection_adjustments_superseded_by_fkey DEFERRABLE INITIALLY DEFERRED;
        EXCEPTION WHEN undefined_object THEN NULL; END $$;
        CREATE UNIQUE INDEX IF NOT EXISTS projection_adjustments_active_uidx
            ON nba.projection_adjustments (player_id, season)
            WHERE superseded_by IS NULL AND retired_at IS NULL;
    """)
    yield


@pytest.fixture(autouse=True)
def seeded(editor_tables, clean_integration_tables):
    db.execute_sql("""
        TRUNCATE TABLE nba.projection_adjustments, nba.player_projections, nba.player_history,
                       nba.player_profiles, nba.draft_market
        RESTART IDENTITY CASCADE
    """)
    now = datetime.utcnow()
    for pid, name, mpg, team in ((STAR, "Star", 34, "DEN"), (VET, "Vet", 24, "PHX"), (RETIRED, "Gone", 30, None)):
        Player.create(id=pid, name=name, name_normalized=name.lower(), position="G", espn_id=pid + 1000)
        minutes = 70 * mpg
        PlayerHistory.create(
            player_id=pid, season=LAST_SEASON, player_name=name, team=team, age=27, from_year=2018,
            gp=70, min=minutes, pts=int(0.6 * minutes), reb=int(0.2 * minutes), ast=int(0.15 * minutes),
            stl=int(0.03 * minutes), blk=int(0.02 * minutes), tov=int(0.06 * minutes),
            fgm=int(0.22 * minutes), fga=int(0.46 * minutes), fg3m=int(0.07 * minutes),
            fg3a=int(0.19 * minutes), ftm=int(0.09 * minutes), fta=int(0.11 * minutes),
            oreb=int(0.04 * minutes), dreb=int(0.16 * minutes), dd2=7, td3=0,
        )
        if team:
            PlayerProfile.create(player=pid, team=team, updated_at=now)
    # ESPN projects the star, and ranks him on both boards.
    PlayerProjection.record_projection(
        player_id=STAR, season=SEASON, as_of_date=date(2026, 9, 30), projected_gp=72,
        line={"min": 35, "pts": 27, "reb": 6, "ast": 7, "stl": 1.2, "blk": 0.5, "tov": 3,
              "fgm": 9.5, "fga": 19, "fg3m": 2.5, "fg3a": 6.5, "ftm": 5.5, "fta": 6.5},
    )
    DraftMarket.create(player=STAR, season=SEASON, as_of_date=date(2026, 9, 30), overall_rank=4, roto_rank=6)
    yield


@pytest.fixture
def client(monkeypatch):
    def by_minutes(players):
        order = sorted(players, key=lambda p: -p["line"]["min"])
        return Valuation(available=True, league_size=12, rounds=13, playoff_weight=2.0,
                         ranks={p["player_id"]: StandardRank(p["player_id"], i, 30.0, i, 28.0)
                                for i, p in enumerate(order, start=1)})

    monkeypatch.setattr(projections, "_valuation", by_minutes)
    app = FastAPI()
    app.include_router(projections.router, prefix="/v1")
    return TestClient(app)


def _page(client):
    res = client.get(BASE, headers=_AUTH)
    assert res.status_code == 200, res.text
    return res.json()["data"]


def test_the_page_reads_what_the_pipeline_would_project(client):
    data = _page(client)

    # The retired player keeps his history and is not projected: no current team, no ESPN line.
    assert [(p["player_id"], p["name"], p["team"]) for p in data["players"]] == [
        (STAR, "Star", "DEN"), (VET, "Vet", "PHX")]
    assert (data["season"], data["espn_as_of"], data["published_as_of"]) == (SEASON, "2026-09-30", None)
    assert data["unpublished"] == 2                       # nothing has been published yet
    star, vet = data["players"]
    assert star["espn"]["pts"] == 27.0 and star["espn"]["games"] == 72
    assert star["espn_ranks"] == {"points": 4, "categories": 6}
    assert star["statistical"]["min"] != star["blended"]["min"]         # ESPN's 35 pulled it
    assert vet["espn"] is None and vet["blended"] == vet["statistical"] == vet["final"]
    assert vet["adjustment"] is None


def test_a_save_republishes_and_the_board_then_reads_what_the_page_shows(client):
    res = client.put(f"{BASE}/{VET}/adjustment", headers=_AUTH, json={
        "kind": "role", "minutes": 33, "games": 70, "note": "starting after the trade", "author": "jp"})

    assert res.status_code == 200, res.text
    saved = res.json()["data"]
    assert saved["published"] is True and saved["pipeline"]["records_processed"] == 2
    assert (saved["adjustment"]["minutes"], saved["adjustment"]["state"]) == (33.0, "live")

    # The published snapshot is the page's final line, to the stored decimals.
    data = _page(client)
    assert data["unpublished"] == 0 and data["published_as_of"] is not None
    vet = next(p for p in data["players"] if p["player_id"] == VET)
    assert (vet["final"]["min"], vet["final"]["games"], vet["adjustment"]["note"]) == (
        33.0, 70.0, "starting after the trade")
    stored = PlayerProjection.get(
        (PlayerProjection.player == VET) & (PlayerProjection.season == SEASON) & (PlayerProjection.source == "cv"))
    assert (float(stored.min), stored.projected_gp, float(stored.pts)) == (33.0, 70, vet["final"]["pts"])
    assert stored.raw["adjustment_id"] == saved["adjustment"]["id"]
    # With 33 minutes he now outranks nobody new, but his own rank is the stub's by minutes.
    assert vet["ranks"]["points"] == 2


def test_an_edit_supersedes_and_a_retire_withdraws_and_every_version_is_kept(client):
    first = client.put(f"{BASE}/{VET}/adjustment", headers=_AUTH,
                       json={"kind": "role", "minutes": 30, "note": "first read"}).json()["data"]["adjustment"]
    second = client.put(f"{BASE}/{VET}/adjustment", headers=_AUTH,
                        json={"kind": "role", "minutes": 36, "note": "second read"}).json()["data"]["adjustment"]
    assert second["id"] != first["id"]

    versions = client.get(f"{BASE}/{VET}/adjustments", headers=_AUTH).json()["data"]["versions"]
    assert [(v["id"], v["state"], v["minutes"]) for v in versions] == [
        (second["id"], "live", 36.0), (first["id"], "superseded", 30.0)]
    assert ProjectionAdjustment.select().count() == 2
    assert [a.id for a in ProjectionAdjustment.active_for(SEASON)] == [second["id"]]
    # 36 minutes passes the star: the page orders by the standard points rank.
    assert [p["player_id"] for p in _page(client)["players"]] == [VET, STAR]

    res = client.delete(f"{BASE}/{VET}/adjustment", headers=_AUTH)
    assert res.status_code == 200 and res.json()["data"]["published"] is True
    data = _page(client)
    vet = next(p for p in data["players"] if p["player_id"] == VET)
    assert vet["adjustment"] is None and vet["final"] == vet["blended"] and data["unpublished"] == 0
    states = [v["state"] for v in client.get(f"{BASE}/{VET}/adjustments", headers=_AUTH).json()["data"]["versions"]]
    assert states == ["retired", "superseded"]
    # Nothing left to retire.
    assert client.delete(f"{BASE}/{VET}/adjustment", headers=_AUTH).status_code == 404


def test_a_preview_changes_nothing_in_the_database(client):
    res = client.post(f"{BASE}/preview", headers=_AUTH,
                      json={"player_id": VET, "adjustment": {"minutes": 38, "return_date": "2027-01-10"}})

    assert res.status_code == 200, res.text
    data = res.json()["data"]
    assert (data["before"]["final"]["min"], data["after"]["final"]["min"]) == (pytest.approx(24.0, abs=0.5), 38.0)
    assert data["after"]["final"]["games"] < data["before"]["final"]["games"]     # capped by the return date
    assert (data["before"]["ranks"]["points"], data["after"]["ranks"]["points"]) == (2, 1)
    assert ProjectionAdjustment.select().count() == 0
    assert PlayerProjection.select().where(PlayerProjection.source == "cv").count() == 0


def test_a_save_for_an_unknown_player_writes_nothing(client):
    res = client.put(f"{BASE}/999999/adjustment", headers=_AUTH,
                     json={"kind": "role", "minutes": 30, "note": "who"})
    assert res.status_code == 404
    assert ProjectionAdjustment.select().count() == 0


def test_a_same_day_rerun_replaces_the_snapshot_rather_than_adding_to_it():
    """The published snapshot is the day's rows for exactly the players
    projected now: a second run updates them in place, and a player the first
    run projected who has since left the league is taken out."""
    import asyncio

    from pipelines import run_pipeline

    def snapshot() -> dict[int, tuple[float, str]]:
        rows = PlayerProjection.select().where(
            (PlayerProjection.season == SEASON) & (PlayerProjection.source == "cv"))
        return {r.player_id: (float(r.min), str(r.pipeline_run_id)) for r in rows}

    first = asyncio.run(run_pipeline("cv_projection", options={"force": True}))
    assert first.records_processed == 2
    before = snapshot()
    assert set(before) == {STAR, VET}

    # The vet is waived (no team in the next profiles run) and the star gets more minutes.
    PlayerProfile.delete().where(PlayerProfile.player == VET).execute()
    ProjectionAdjustment.record(STAR, SEASON, kind="role", minutes=37, note="more minutes", author="jp")

    second = asyncio.run(run_pipeline("cv_projection", options={"force": True}))
    assert second.records_processed == 1
    after = snapshot()
    assert set(after) == {STAR}
    assert after[STAR][0] == 37.0 and after[STAR][1] != before[STAR][1]       # updated, by the second run
    assert PlayerProjection.select().where(PlayerProjection.source == "cv").count() == 1
    # ESPN's own snapshot is another source and is never touched.
    assert PlayerProjection.select().where(PlayerProjection.source == "espn").count() == 1
