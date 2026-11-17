"""
Notes in season end to end against Postgres (the Live rankings plan, step 3):
the editor tries a note on the morning's live projection, a save writes one
row beside the player's preseason adjustment and rebuilds the day's snapshot,
and the page then shows the note with how it stands.

The morning is Nov 9. DEN and BOS have met four times, two days apart, the
last on the 7th, and meet six more times from the 9th. Everybody has played
all four: what is known about the star's injury is known from a note.

Only the backend's valuation is stubbed, and the clock.
"""

from __future__ import annotations

import os
from datetime import date, datetime, timedelta, timezone

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from api.v1 import projections
from core.settings import settings
from db.base import db
from db.models.nba import (
    DraftMarket,
    NewsItem,
    NewsItemPlayer,
    PlayerHistory,
    PlayerProfile,
    PlayerProjection,
    ProjectionAdjustment,
)
from db.models.nba.games import Game
from db.models.nba.player_game_stats import PlayerGameStats
from db.models.nba.players import Player
from pipelines import live_projection
from services import projection_notes as notes
from services.live_projection_inputs import load_coefficients
from services.live_projection_model import expand
from services.projection_model import season_label, season_start
from services.valuation_client import StandardRank, Valuation

pytestmark = pytest.mark.integration

_AUTH = {"Authorization": f"Bearer {os.environ.get('PIPELINE_API_TOKEN', 'test-token')}"}
BASE = "/v1/dashboard/projections"
SEASON = settings.nba_season
YEAR = season_start(SEASON)
LAST_SEASON = season_label(YEAR - 1)
PRESEASON = date(YEAR, 10, 20)
PLAYED = [date(YEAR, 11, d) for d in (1, 3, 5, 7)]
TODAY = date(YEAR, 11, 9)
AHEAD = [TODAY + timedelta(days=2 * i) for i in range(6)]

STAR, STARTER, BENCH, CELTIC = 201, 202, 203, 301
ROSTER = ((STAR, "Star", "DEN", 34), (STARTER, "Starter", "DEN", 30), (BENCH, "Bench", "DEN", 12),
          (CELTIC, "Celtic", "BOS", 32))
PER_MINUTE = {"pts": 0.66, "reb": 0.2, "ast": 0.15, "stl": 0.03, "blk": 0.02, "tov": 0.06,
              "fgm": 0.24, "fga": 0.5, "fg3m": 0.06, "fg3a": 0.16, "ftm": 0.12, "fta": 0.15}
MODELS = [PlayerProfile, PlayerHistory, PlayerProjection, DraftMarket]


@pytest.fixture(scope="module", autouse=True)
def notes_tables(integration_db, adjustments_table):
    db.create_tables(MODELS, safe=True)
    yield


def _game_id(n: int) -> str:
    return f"002{YEAR % 100:02d}{n:05d}"


@pytest.fixture(autouse=True)
def morning(notes_tables, clean_integration_tables, monkeypatch):
    db.execute_sql("""
        TRUNCATE TABLE nba.projection_adjustments, nba.player_projections, nba.player_history,
                       nba.player_profiles, nba.draft_market
        RESTART IDENTITY CASCADE
    """)
    now = datetime.utcnow()
    for pid, name, team, mpg in ROSTER:
        Player.create(id=pid, name=name, name_normalized=name.lower(), position="G")
        PlayerProfile.create(player=pid, team=team, updated_at=now)
        minutes = 70 * mpg
        PlayerHistory.create(
            player_id=pid, season=LAST_SEASON, player_name=name, team=team, age=27, from_year=2018,
            gp=70, min=minutes, dd2=0, td3=0, oreb=int(0.04 * minutes), dreb=int(0.16 * minutes),
            **{k: int(v * minutes) for k, v in PER_MINUTE.items()},
        )
        PlayerProjection.record_projection(
            player_id=pid, season=SEASON, as_of_date=PRESEASON, projected_gp=70, source="cv",
            line={k: round(v * mpg, 2) for k, v in PER_MINUTE.items()} | {"min": float(mpg)},
        )
    for n, day in enumerate(PLAYED + AHEAD):
        Game.create(game_id=_game_id(n), game_date=day, season=SEASON, home_team="DEN", away_team="BOS",
                    status="final" if day < TODAY else "scheduled")
    for n, day in enumerate(PLAYED):
        for pid, _name, team, mpg in ROSTER:
            PlayerGameStats.create(player=pid, team=team, game=_game_id(n), game_date=day, min=mpg, fpts=0,
                                   **{k: int(round(v * mpg)) for k, v in PER_MINUTE.items()})
    _clock(monkeypatch, 9)
    load_coefficients.cache_clear()
    yield


def _clock(monkeypatch, hour: int) -> None:
    at = datetime(TODAY.year, TODAY.month, TODAY.day, hour, 0)
    monkeypatch.setattr(live_projection, "now_eastern", lambda: at)
    monkeypatch.setattr(live_projection, "today_et", lambda now=None: TODAY)
    monkeypatch.setattr(notes, "today_et", lambda now=None: TODAY)
    monkeypatch.setattr(projections, "_eastern_now", lambda: at)


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


def _news(pid: int, headline: str, days_ago: float, kind: str = "injury", published: bool = True) -> NewsItem:
    when = datetime.now(timezone.utc) - timedelta(days=days_ago)
    item = NewsItem.create(source="espn_injuries", source_item_id=headline, headline=headline, kind=kind,
                           url=f"https://example.com/{headline.replace(' ', '-')}",
                           published_at=when if published else None, fetched_at=when)
    NewsItemPlayer.create(news_item=item, player=pid, linked_by="espn_id")
    return item


def _live(day: date = TODAY) -> dict[int, PlayerProjection]:
    return {
        r.player_id: r
        for r in PlayerProjection.select().where(
            (PlayerProjection.source == "cv_live") & (PlayerProjection.as_of_date == day))
    }


def _save(client, pid: int, **body):
    return client.put(f"{BASE}/{pid}/note", headers=_AUTH,
                      json={"kind": "injury_current", "note": "read off the report", "author": "jp"} | body)


def _page(client) -> dict:
    res = client.get(BASE, headers=_AUTH)
    assert res.status_code == 200, res.text
    return res.json()["data"]


def test_a_note_tried_on_prices_the_morning_twice_and_writes_nothing(client):
    res = client.post(f"{BASE}/note/preview", headers=_AUTH,
                      json={"player_id": STAR, "note": {"return_date": AHEAD[3].isoformat()}})

    assert res.status_code == 200, res.text
    data = res.json()["data"]
    assert (data["player_id"], data["as_of"]) == (STAR, TODAY.isoformat())
    before, after = data["before"], data["after"]
    # Healthy, he is one stretch of all six games; out until the fourth, he is two
    assert [(s["first"], s["last"], s["games"]) for s in before["stretches"]] == [
        (AHEAD[0].isoformat(), AHEAD[5].isoformat(), 6)]
    assert (after["stretches"][0]["last"], after["stretches"][0]["games"], after["stretches"][0]["chance"]) == (
        AHEAD[2].isoformat(), 3, 0.0)
    assert before["line"]["games"] > 5 and after["line"]["games"] < before["line"]["games"] - 3
    # What he does in a game he plays is the same: the note took games
    assert after["line"]["min"] == pytest.approx(before["line"]["min"], abs=1.0)
    assert data["standing"] == {"state": "in_force", "games": True, "role": False, "reasons": []}
    assert ProjectionAdjustment.select().count() == 0 and _live() == {}


def test_previewing_no_note_shows_him_without_the_one_he_has(client):
    assert _save(client, STAR, out_for_season=True).status_code == 200

    res = client.post(f"{BASE}/note/preview", headers=_AUTH, json={"player_id": STAR, "note": None})

    data = res.json()["data"]
    assert data["before"]["line"]["games"] == 0.0 and data["after"]["line"]["games"] > 5
    assert data["standing"] is None
    # Nobody the model does not project
    assert client.post(f"{BASE}/note/preview", headers=_AUTH,
                       json={"player_id": 999, "note": {"minutes": 30}}).status_code == 404


def test_a_save_writes_the_note_beside_the_preseason_adjustment_and_rebuilds_the_day(client):
    preseason = ProjectionAdjustment.record(STAR, SEASON, kind="role", minutes=35, note="first option", author="jp")
    report = _news(STAR, "Star (knee) out at least two weeks", 0.5)
    published = {r.id for r in PlayerProjection.select().where(PlayerProjection.source == "cv")}

    res = _save(client, STAR, return_date=AHEAD[3].isoformat(), return_earliest=AHEAD[2].isoformat(),
                return_latest=AHEAD[5].isoformat(), confidence="likely", evidence_news_ids=[report.id],
                source_url="https://example.com/report")

    assert res.status_code == 200, res.text
    body = res.json()
    assert body["message"] == "Note saved and the live projection rebuilt"
    saved = body["data"]
    assert saved["rebuilt"] is True and saved["pipeline"]["records_processed"] == 4
    note = saved["note"]
    assert (note["scope"], note["state"], note["kind"], note["author"]) == ("season", "live", "injury_current", "jp")
    assert (note["return_date"], note["return_earliest"], note["return_latest"]) == (
        AHEAD[3].isoformat(), AHEAD[2].isoformat(), AHEAD[5].isoformat())
    assert (note["confidence"], note["review_date"]) == ("likely", (TODAY + timedelta(days=14)).isoformat())
    assert note["standing"] == {"state": "in_force", "games": True, "role": False, "reasons": []}
    (cited,) = note["evidence"]
    assert (cited["type"], cited["id"], cited["source"], cited["kind"], cited["headline"], cited["url"]) == (
        "news", report.id, "espn_injuries", "injury", "Star (knee) out at least two weeks", report.url)

    # The day's snapshot was rebuilt with it
    star = _live()[STAR]
    games = expand(star.raw["timeline"], AHEAD)
    assert [g.chance for g in games[:3]] == [0.0] * 3 and games[3].chance > 0
    assert star.raw["parts"]["known"] == {"out_next": False, "out_for_season": False,
                                          "return_date": AHEAD[3].isoformat(), "note": note["id"]}
    # The bench plays a little more while he is out, and takes more of the offense
    bench = expand(_live()[BENCH].raw["timeline"], AHEAD)
    assert bench[0].minutes > bench[-1].minutes and bench[0].usage > bench[-1].usage

    # His preseason adjustment is as it was, and the preseason projection was not republished
    assert [a.id for a in ProjectionAdjustment.active_for(SEASON)] == [preseason.id]
    assert {r.id for r in PlayerProjection.select().where(PlayerProjection.source == "cv")} == published

    # The page shows both, and the line he is stored with this morning
    data = _page(client)
    assert (data["live_as_of"], data["review_days"]) == (TODAY.isoformat(), 14)
    assert data["confidences"] == ["confirmed", "likely", "possible"]
    row = next(p for p in data["players"] if p["player_id"] == STAR)
    assert (row["adjustment"]["id"], row["adjustment"]["scope"], row["adjustment"]["minutes"]) == (
        preseason.id, "preseason", 35.0)
    assert (row["note"]["id"], row["note"]["standing"]["state"]) == (note["id"], "in_force")
    assert row["live"]["games"] == round(star.raw["games"], 1) and row["live"]["min"] == float(star.min)
    assert next(p for p in data["players"] if p["player_id"] == STARTER)["note"] is None

    versions = client.get(f"{BASE}/{STAR}/adjustments", headers=_AUTH).json()["data"]["versions"]
    assert [(v["id"], v["scope"], v["state"]) for v in versions] == [
        (note["id"], "season", "live"), (preseason.id, "preseason", "live")]
    assert versions[0]["standing"]["state"] == "in_force" and versions[1]["standing"] is None


def test_minutes_until_a_teammate_is_back(client):
    assert _save(client, STAR, return_date=AHEAD[3].isoformat()).status_code == 200

    res = _save(client, BENCH, kind="role", minutes=30, usage=1.1, depends_on_player_id=STAR,
                note="starting while the star is out")

    assert res.status_code == 200, res.text
    note = res.json()["data"]["note"]
    assert (note["minutes"], note["usage"], note["depends_on_player_id"], note["depends_on_name"]) == (
        30.0, 1.1, STAR, "Star")
    assert note["standing"] == {"state": "in_force", "games": False, "role": True, "reasons": []}
    bench = _live()[BENCH]
    games = expand(bench.raw["timeline"], AHEAD)
    assert [(g.minutes, g.usage) for g in games[:3]] == [(30.0, 1.1)] * 3 and games[-1].minutes < 25
    assert bench.raw["parts"]["note"]["id"] == note["id"]
    assert bench.raw["parts"]["note"]["through"] == AHEAD[2].isoformat()
    # The star's own note is still his
    assert _live()[STAR].raw["parts"]["known"]["return_date"] == AHEAD[3].isoformat()


def test_an_edit_supersedes_the_note_a_retire_withdraws_it_and_every_version_is_kept(client):
    report = _news(STAR, "Star (knee) to be re-evaluated", 1)
    first = _save(client, STAR, return_date=AHEAD[2].isoformat(), evidence_news_ids=[report.id]).json()["data"]["note"]
    second = _save(client, STAR, return_date=AHEAD[4].isoformat(), minutes=26,
                   until_date=AHEAD[5].isoformat(), kind="injury_return").json()["data"]["note"]
    assert second["id"] != first["id"]
    assert _live()[STAR].raw["parts"]["known"]["return_date"] == AHEAD[4].isoformat()

    # The item it cited is pruned; the note still says what it rested on
    NewsItem.delete().execute()
    versions = client.get(f"{BASE}/{STAR}/adjustments", headers=_AUTH).json()["data"]["versions"]
    assert [(v["id"], v["state"], v["return_date"]) for v in versions] == [
        (second["id"], "live", AHEAD[4].isoformat()), (first["id"], "superseded", AHEAD[2].isoformat())]
    assert versions[1]["evidence"][0]["headline"] == "Star (knee) to be re-evaluated"
    assert versions[1]["standing"] is None

    res = client.delete(f"{BASE}/{STAR}/note", headers=_AUTH)
    assert res.status_code == 200, res.text
    body = res.json()
    assert body["message"] == "Note retired and the live projection rebuilt"
    assert body["data"]["note"] is None and body["data"]["rebuilt"] is True
    star = _live()[STAR]
    assert "known" not in star.raw["parts"] and "note" not in star.raw["parts"] and star.raw["games"] > 5
    assert _page(client)["players"][0]["note"] is None
    states = [v["state"] for v in client.get(f"{BASE}/{STAR}/adjustments", headers=_AUTH).json()["data"]["versions"]]
    assert states == ["retired", "superseded"]
    # Nothing left to retire
    assert client.delete(f"{BASE}/{STAR}/note", headers=_AUTH).status_code == 404


@pytest.mark.parametrize("pid, body, status, says", [
    (999999, {"minutes": 30}, 404, "No player 999999"),
    (BENCH, {"minutes": 30, "depends_on_player_id": BENCH}, 422, "cannot depend on the player it is about"),
    (BENCH, {"minutes": 30, "depends_on_player_id": 999999}, 422, "No player 999999 to depend on"),
    (STAR, {"return_date": AHEAD[3].isoformat(), "evidence_news_ids": [424242]}, 422, "No stored news item 424242"),
    (STAR, {"return_date": AHEAD[3].isoformat(), "review_date": (TODAY - timedelta(days=1)).isoformat()}, 422,
     "review date has already passed"),
], ids=["nobody", "depends on himself", "no such teammate", "no such news item", "review date gone"])
def test_a_save_that_cannot_stand_writes_nothing(client, pid, body, status, says):
    res = _save(client, pid, **body)

    assert res.status_code == status and says in res.json()["detail"]
    assert ProjectionAdjustment.select().count() == 0 and _live() == {}


def test_before_the_day_turns_a_save_is_kept_and_the_morning_reads_it(client, monkeypatch):
    _clock(monkeypatch, 1)

    res = _save(client, STAR, out_for_season=True, review_date=(TODAY + timedelta(days=30)).isoformat())

    assert res.status_code == 200, res.text
    body = res.json()
    assert body["message"] == "Note saved; the live projection reads it on its next run"
    assert body["data"]["rebuilt"] is False
    assert (body["data"]["note"]["games"], body["data"]["note"]["review_date"]) == (
        0, (TODAY + timedelta(days=30)).isoformat())
    assert _live() == {}

    _clock(monkeypatch, 6)
    assert live_projection.LiveProjectionPipeline()._run_sync().records_processed == 4
    assert _live()[STAR].projected_gp == 0


def test_the_news_a_note_can_cite_is_his_and_recent(client):
    older = _news(STAR, "Star questionable Friday", 3, kind="availability")
    newest = _news(STAR, "Star (knee) out at least two weeks", 0.5)
    undated = _news(STAR, "Star seen in a walking boot", 1, kind="rumor", published=False)
    _news(STAR, "Star signs an extension", 40, kind="transaction")
    _news(CELTIC, "Celtic rests tonight", 1)

    res = client.get(f"{BASE}/{STAR}/news", headers=_AUTH)

    assert res.status_code == 200, res.text
    data = res.json()["data"]
    assert data["player_id"] == STAR
    # Newest first, an item with no time of its own last
    assert [(i["id"], i["kind"]) for i in data["items"]] == [
        (newest.id, "injury"), (older.id, "availability"), (undated.id, "rumor")]
    assert data["items"][0]["headline"] == "Star (knee) out at least two weeks" and data["items"][0]["type"] == "news"
    assert client.get(f"{BASE}/{BENCH}/news", headers=_AUTH).json()["data"]["items"] == []
