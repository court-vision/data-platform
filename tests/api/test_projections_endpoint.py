"""
/v1/dashboard/projections: token-gated, the page's payload, a preview that
writes nothing, and the two writes — each followed by a republish. The
database read, the backend call and the pipeline run are stubbed; the editor's
own arithmetic is not.

The season's notes have the same four routes of their own (the Live rankings
plan, step 3): a note tried on the live projection, a save and a retire that
rebuild the live projection and leave the preseason one alone, and the news a
note can cite.
"""

import os
from datetime import date, datetime, timedelta
from types import SimpleNamespace

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from api.v1 import projections
from db.models.nba import ProjectionAdjustment
from schemas.pipeline import PipelineResult
from schemas.projections import EvidenceItem
from services import live_projection_inputs as live_inputs
from services import projection_editor as editor
from services import projection_notes as notes
from services.live_projection_model import BoxLine, Prior
from services.projection_inputs import ProjectionInputs
from services.projection_model import LINE_KEYS, Coefficients, EspnLine, HistoryRow
from services.valuation_client import StandardRank, Valuation

pytestmark = pytest.mark.api

_AUTH = {"Authorization": f"Bearer {os.environ.get('PIPELINE_API_TOKEN', 'test-token')}"}
BASE = "/v1/dashboard/projections"
FLAT = Coefficients(curve={}, gp_beta=[0.72, 0, 0, 0, 0, 0, 0, 0, 0], weights=(7, 2, 1),
                    reg_minutes=0.0, reg_games=0.0, gp_shrink=0.0, espn_weight=0.5, version="test-1")
PER_MIN = {"pts": 0.6, "reb": 0.2, "ast": 0.15, "stl": 0.03, "blk": 0.02, "tov": 0.06,
           "fgm": 0.22, "fga": 0.46, "fg3m": 0.07, "fg3a": 0.19, "ftm": 0.09, "fta": 0.11,
           "oreb": 0.04, "dreb": 0.16}


def _history(pid, mpg):
    minutes = 70 * mpg
    return HistoryRow(player_id=pid, season=2025, gp=70, minutes=minutes,
                      totals={k: v * minutes for k, v in PER_MIN.items()} | {"dd2": 0.0, "td3": 0.0},
                      age=27.0, from_year=2018)


def _state(records=None) -> editor.EditorState:
    inputs = ProjectionInputs(
        season="2026-27", target=2026,
        history={1: [_history(1, 34.0)], 2: [_history(2, 24.0)]},
        espn={1: EspnLine(per_game={k: 1.0 for k in LINE_KEYS} | {"pts": 27.0}, minutes=34.0, games=72.0)},
        espn_as_of=date(2026, 9, 30), current={1: "DEN", 2: "PHX"}, roster={1, 2},
        per_day={}, coeffs=FLAT, records=records or {},
    )
    return editor.EditorState(inputs=inputs, players={1: ("Star", "G"), 2: ("Vet", "F")},
                              market={1: (1, 2)}, published_as_of=date(2026, 10, 1))


def _record(**fields) -> ProjectionAdjustment:
    base = dict(id=77, player=2, season="2026-27", kind="role", note="starting now", author="jp",
                created_at=datetime(2026, 10, 1, 12, 0))
    base.update(fields)
    return ProjectionAdjustment(**base)


def _ranked(players: list[dict]) -> Valuation:
    """A stand-in backend: more minutes is a better rank, in both formats."""
    order = sorted(players, key=lambda p: -p["line"]["min"])
    return Valuation(
        available=True, league_size=12, rounds=13, playoff_weight=2.0, playoff_weeks=(20, 21, 22, 23),
        ranks={p["player_id"]: StandardRank(p["player_id"], i, 30.0, i, 28.0) for i, p in enumerate(order, start=1)},
    )


@pytest.fixture
def client(monkeypatch):
    app = FastAPI()
    app.include_router(projections.router, prefix="/v1")
    state = _state()
    seen = SimpleNamespace(valued=[], runs=[], state=state)

    def valuation(players):
        seen.valued.append(players)
        return _ranked(players)

    async def run_pipeline(name, **kwargs):
        seen.runs.append((name, kwargs))
        return PipelineResult(status="success", message="ok", started_at="2026-10-01T12:00:00", records_processed=2)

    monkeypatch.setattr(editor, "load_state", lambda season: seen.state)
    monkeypatch.setattr(projections, "_valuation", valuation)
    monkeypatch.setattr(projections, "run_pipeline", run_pipeline)
    seen.http = TestClient(app)
    return seen


@pytest.mark.parametrize("method, path", [
    ("get", BASE), ("post", f"{BASE}/preview"), ("put", f"{BASE}/2/adjustment"),
    ("delete", f"{BASE}/2/adjustment"), ("get", f"{BASE}/2/adjustments"),
    ("post", f"{BASE}/note/preview"), ("put", f"{BASE}/2/note"), ("delete", f"{BASE}/2/note"),
    ("get", f"{BASE}/2/news"),
])
def test_every_route_needs_the_token(client, method, path):
    assert getattr(client.http, method)(path).status_code in (401, 403)
    assert client.valued == [] and client.runs == []


def test_the_page_lists_every_projected_player(client):
    res = client.http.get(BASE, headers=_AUTH)

    assert res.status_code == 200
    body = res.json()
    assert body["message"] == "2 players, 0 adjusted"
    data = body["data"]
    assert (data["season"], data["coefficients_version"], data["espn_as_of"]) == ("2026-27", "test-1", "2026-09-30")
    assert data["ranks_available"] is True and data["league"]["playoff_weeks"] == [20, 21, 22, 23]
    assert data["unpublished"] == 2                       # nothing is published in this fixture
    star, vet = data["players"]
    assert (star["name"], star["ranks"], star["espn_ranks"]) == (
        "Star", {"points": 1, "categories": 1}, {"points": 1, "categories": 2})
    assert star["espn"]["pts"] == 27.0 and star["statistical"]["min"] == 34.0
    # Defaults are on the wire, so the generated types need no undefined checks.
    assert vet["espn"] is None and vet["adjustment"] is None and vet["espn_ranks"] == {"points": None, "categories": None}
    # One valuation call, for the whole pool, with lines as the pipeline stores them.
    (sent,) = client.valued
    assert [p["player_id"] for p in sent] == [1, 2] and sent[1]["line"]["min"] == 24.0


def test_a_preview_values_the_pool_twice_and_writes_nothing(client, monkeypatch):
    wrote = []
    monkeypatch.setattr(ProjectionAdjustment, "record", classmethod(lambda cls, *a, **k: wrote.append(a)))

    res = client.http.post(f"{BASE}/preview", headers=_AUTH,
                           json={"player_id": 2, "adjustment": {"minutes": 36, "games": 75}})

    assert res.status_code == 200
    data = res.json()["data"]
    assert (data["before"]["final"]["min"], data["after"]["final"]["min"]) == (24.0, 36.0)
    assert data["after"]["final"]["games"] == 75.0
    # 36 minutes outranks the star's blended 34.
    assert (data["before"]["ranks"]["points"], data["after"]["ranks"]["points"]) == (2, 1)
    assert len(client.valued) == 2 and wrote == [] and client.runs == []


def test_previewing_no_adjustment_shows_the_line_without_the_live_one(client):
    client.state = _state({2: _record(minutes=36.0)})
    res = client.http.post(f"{BASE}/preview", headers=_AUTH, json={"player_id": 2, "adjustment": None})
    data = res.json()["data"]
    assert (data["before"]["final"]["min"], data["after"]["final"]["min"]) == (36.0, 24.0)
    assert (data["before"]["ranks"]["points"], data["after"]["ranks"]["points"]) == (1, 2)


def test_a_preview_of_someone_not_projected_is_a_404(client):
    res = client.http.post(f"{BASE}/preview", headers=_AUTH, json={"player_id": 999, "adjustment": {"games": 60}})
    assert res.status_code == 404


@pytest.mark.parametrize("adjustment", [
    {"minutes": 60}, {"games": 90}, {"usage": 0}, {"usage": 2.5},
    {"rates": {"vibes": 1.1}}, {"rates": {"blk": 0}}, {"rates": {"blk": 4}}, {"return_date": "soon"},
])
def test_an_impossible_change_is_refused(client, adjustment):
    res = client.http.post(f"{BASE}/preview", headers=_AUTH, json={"player_id": 2, "adjustment": adjustment})
    assert res.status_code == 422


def test_saving_writes_one_version_and_republishes(client, monkeypatch):
    calls = []

    def record(cls, player_id, season, **fields):
        calls.append((player_id, season, fields))
        return _record(minutes=fields["minutes"], games=fields["games"], rates=fields["rates"],
                       source_url=fields["source_url"], author=fields["author"])

    monkeypatch.setattr(ProjectionAdjustment, "record", classmethod(record))
    monkeypatch.setattr(editor, "player_exists", lambda pid: True)

    res = client.http.put(f"{BASE}/2/adjustment", headers=_AUTH, json={
        "kind": "role", "minutes": 32, "games": 66, "rates": {"blk": 1.1},
        "note": "  starting now  ", "source_url": " ", "author": "jp",
    })

    assert res.status_code == 200
    body = res.json()
    assert body["message"] == "Adjustment saved and the projection republished"
    assert body["data"]["published"] is True and body["data"]["pipeline"]["records_processed"] == 2
    assert (body["data"]["adjustment"]["id"], body["data"]["adjustment"]["state"]) == (77, "live")
    ((player_id, season, fields),) = calls
    assert (player_id, season) == (2, os.environ["NBA_SEASON"])
    assert fields == {
        "kind": "role", "minutes": 32.0, "games": 66, "return_date": None, "usage": None,
        "rates": {"blk": 1.1}, "note": "starting now", "source_url": None, "author": "jp",
    }
    # The republish runs outside the preseason window too: a save is deliberate.
    assert client.runs == [("cv_projection", {"options": {"force": True}})]


def test_a_failed_republish_keeps_the_save_and_says_so(client, monkeypatch):
    async def failing(name, **kwargs):
        return PipelineResult(status="server_error", message="boom", started_at="2026-10-01T12:00:00", error="boom")

    monkeypatch.setattr(projections, "run_pipeline", failing)
    monkeypatch.setattr(ProjectionAdjustment, "record", classmethod(lambda cls, *a, **k: _record(games=60)))
    monkeypatch.setattr(editor, "player_exists", lambda pid: True)

    res = client.http.put(f"{BASE}/2/adjustment", headers=_AUTH,
                          json={"kind": "injury_current", "games": 60, "note": "out six weeks"})

    assert res.status_code == 200
    body = res.json()
    assert body["message"] == "Adjustment saved; the projection was not republished"
    assert body["data"]["published"] is False and body["data"]["adjustment"]["games"] == 60
    assert body["data"]["adjustment"]["author"] == "jp"


@pytest.mark.parametrize("body", [
    {"kind": "role", "note": "no numbers at all"},                       # changes nothing
    {"kind": "role", "minutes": 30, "note": "   "},                      # no reason given
    {"kind": "role", "minutes": 30},                                     # no note
    {"kind": "vibes", "minutes": 30, "note": "why"},                     # not a kind
    {"kind": "role", "minutes": 30, "note": "why", "author": ""},        # nobody
])
def test_a_save_that_says_nothing_is_refused(client, monkeypatch, body):
    monkeypatch.setattr(editor, "player_exists", lambda pid: True)
    res = client.http.put(f"{BASE}/2/adjustment", headers=_AUTH, json=body)
    assert res.status_code == 422 and client.runs == []


def test_saving_for_a_player_who_does_not_exist_is_a_404(client, monkeypatch):
    monkeypatch.setattr(editor, "player_exists", lambda pid: False)
    res = client.http.put(f"{BASE}/999/adjustment", headers=_AUTH,
                          json={"kind": "role", "minutes": 30, "note": "why"})
    assert res.status_code == 404 and client.runs == []
    assert client.http.put(f"{BASE}/0/adjustment", headers=_AUTH,
                           json={"kind": "role", "minutes": 30, "note": "why"}).status_code == 422


def test_retiring_withdraws_the_live_one_and_republishes(client, monkeypatch):
    retired = []
    monkeypatch.setattr(ProjectionAdjustment, "retire",
                        classmethod(lambda cls, pid, season: retired.append((pid, season)) or 1))

    res = client.http.delete(f"{BASE}/2/adjustment", headers=_AUTH)

    assert res.status_code == 200
    body = res.json()
    assert body["message"] == "Adjustment retired and the projection republished"
    assert body["data"]["adjustment"] is None and body["data"]["published"] is True
    assert retired == [(2, os.environ["NBA_SEASON"])] and len(client.runs) == 1


def test_retiring_nothing_is_a_404_and_runs_nothing(client, monkeypatch):
    monkeypatch.setattr(ProjectionAdjustment, "retire", classmethod(lambda cls, pid, season: 0))
    res = client.http.delete(f"{BASE}/2/adjustment", headers=_AUTH)
    assert res.status_code == 404 and client.runs == []


def test_history_lists_every_version_newest_first(client, monkeypatch):
    versions = [editor.adjustment_entry(_record()),
                editor.adjustment_entry(_record(id=70, minutes=30.0, superseded_by=77))]
    monkeypatch.setattr(editor, "adjustment_history", lambda pid, season: versions)

    res = client.http.get(f"{BASE}/2/adjustments", headers=_AUTH)

    assert res.status_code == 200
    data = res.json()["data"]
    assert (data["player_id"], data["season"]) == (2, os.environ["NBA_SEASON"])
    assert [(v["id"], v["state"]) for v in data["versions"]] == [(77, "live"), (70, "superseded")]


# ---- notes in season -----------------------------------------------------------------------

TODAY = date(2026, 11, 16)
AHEAD = [TODAY + timedelta(days=2 * i) for i in range(8)]
LIVE_PER_MINUTE = {k: PER_MIN[k] for k in ("pts", "reb", "ast", "stl", "blk", "tov", "fgm", "fga", "fg3m", "fg3a",
                                           "ftm", "fta")}


def _morning() -> live_inputs.LiveInputs:
    """Nov 16: the star (1) and the vet (2) have played all eight of DEN's games, with eight to go."""
    lines = [
        BoxLine(player_id=pid, team="DEN", game_id=f"g{g}", game_date=TODAY - timedelta(days=16 - 2 * g),
                minutes=mpg, stats={k: v * mpg for k, v in LIVE_PER_MINUTE.items()})
        for g in range(8) for pid, mpg in ((1, 34.0), (2, 24.0))
    ]
    priors = {pid: Prior(minutes=mpg, games=70.0, per_game={k: v * mpg for k, v in LIVE_PER_MINUTE.items()})
              for pid, mpg in ((1, 34.0), (2, 24.0))}
    return live_inputs.LiveInputs(
        season="2026-27", as_of=TODAY, lines=lines, ahead={"DEN": AHEAD}, priors=priors, prior_source="cv",
        prior_as_of=date(2026, 10, 20), teams={1: "DEN", 2: "DEN"}, ages={},
    )


@pytest.fixture
def season(client, monkeypatch):
    """The note routes on the morning above, with nothing stored and every write watched."""
    client.loaded, client.recorded = [], []

    def load_inputs(season, day):
        client.loaded.append((season, day))
        return _morning()

    def record(cls, player_id, season, **fields):
        client.recorded.append((player_id, season, fields))
        return _record(id=91, player=player_id, scope="season", kind=fields["kind"], minutes=fields["minutes"],
                       return_date=fields["return_date"], review_date=fields["review_date"],
                       evidence=fields["evidence"], author=fields["author"])

    monkeypatch.setattr(live_inputs, "load_inputs", load_inputs)
    monkeypatch.setattr(notes, "today_et", lambda now=None: TODAY)
    monkeypatch.setattr(projections, "_eastern_now", lambda: datetime(2026, 11, 16, 9, 0))
    monkeypatch.setattr(ProjectionAdjustment, "record", classmethod(record))
    monkeypatch.setattr(editor, "player_exists", lambda pid: pid in (1, 2))
    monkeypatch.setattr(notes, "evidence_for", lambda ids: ([{"type": "news", "id": i, "headline": "out"} for i in ids], []))
    monkeypatch.setattr(editor, "adjustment_history",
                        lambda pid, season: [editor.adjustment_entry(_record(id=91, scope="season", minutes=20.0))])
    return client


def test_a_note_tried_on_runs_the_live_model_twice_and_writes_nothing(season):
    res = season.http.post(f"{BASE}/note/preview", headers=_AUTH,
                           json={"player_id": 1, "note": {"return_date": AHEAD[4].isoformat(), "minutes": 28,
                                                          "until_date": AHEAD[5].isoformat()}})

    assert res.status_code == 200, res.text
    body = res.json()
    assert body["message"] == "Preview computed; nothing was saved"
    data = body["data"]
    assert (data["player_id"], data["as_of"]) == (1, TODAY.isoformat())
    # Out for the four games before the date, then two at 28 minutes, then his own
    first, limit, own = data["after"]["stretches"]
    assert (first["first"], first["last"], first["games"], first["chance"]) == (
        AHEAD[0].isoformat(), AHEAD[3].isoformat(), 4, 0.0)
    assert (limit["first"], limit["last"], limit["minutes"]) == (AHEAD[4].isoformat(), AHEAD[5].isoformat(), 28.0)
    assert (own["first"], own["last"], own["minutes"]) == (AHEAD[6].isoformat(), AHEAD[7].isoformat(), 34.0)
    assert [(s["first"], s["games"], s["minutes"]) for s in data["before"]["stretches"]] == [
        (AHEAD[0].isoformat(), 8, 34.0)]
    assert data["after"]["line"]["games"] < data["before"]["line"]["games"] - 3
    assert data["standing"] == {"state": "in_force", "games": True, "role": True, "reasons": []}
    # The morning is read once, for the season and the Eastern day; it is a price, not a rank
    assert season.loaded == [(os.environ["NBA_SEASON"], TODAY)]
    assert season.recorded == [] and season.runs == [] and season.valued == []


def test_a_note_tried_on_someone_the_live_model_does_not_project_is_a_404(season):
    res = season.http.post(f"{BASE}/note/preview", headers=_AUTH, json={"player_id": 999, "note": {"minutes": 30}})
    assert res.status_code == 404 and "not in the live projection" in res.json()["detail"]


@pytest.mark.parametrize("note", [
    {},                                                                  # states nothing
    {"until_date": "2026-12-01"},                                        # an end with nothing to end
    {"depends_on_player_id": 1},
    {"out_for_season": True, "return_date": "2026-12-08"},               # gone and coming back
    {"return_earliest": "2026-12-01"},                                   # a range around nothing
    {"return_date": "2026-12-08", "return_earliest": "2026-12-09"},
    {"return_date": "2026-12-08", "return_latest": "2026-12-07"},
    {"minutes": 60}, {"usage": 0}, {"usage": 2.5}, {"return_date": "soon"},
    {"minutes": 30, "rates": {"blk": 1.1}},                              # a note sets no per-stat multiplier
    {"minutes": 30, "games": 60},                                        # nor a games target
])
def test_a_note_that_does_not_hold_together_is_refused(season, note):
    preview = season.http.post(f"{BASE}/note/preview", headers=_AUTH, json={"player_id": 2, "note": note})
    save = season.http.put(f"{BASE}/2/note", headers=_AUTH, json={"kind": "role", "note": "why"} | note)
    assert (preview.status_code, save.status_code) == (422, 422)
    assert season.recorded == [] and season.runs == []


def test_saving_a_note_writes_one_season_row_and_rebuilds_the_live_projection(season):
    res = season.http.put(f"{BASE}/2/note", headers=_AUTH, json={
        "kind": "injury_return", "return_date": "2026-11-20", "return_earliest": "2026-11-18",
        "return_latest": "2026-11-24", "minutes": 20, "usage": 0.9, "until_date": "2026-12-04",
        "depends_on_player_id": 1, "confidence": "likely", "evidence_news_ids": [5, 5, 8],
        "note": "  back on a limit  ", "source_url": " ", "author": "jp",
    })

    assert res.status_code == 200, res.text
    body = res.json()
    assert body["message"] == "Note saved and the live projection rebuilt"
    assert body["data"]["rebuilt"] is True and body["data"]["pipeline"]["records_processed"] == 2
    assert (body["data"]["note"]["id"], body["data"]["note"]["scope"]) == (91, "season")
    ((player_id, season_label, fields),) = season.recorded
    assert (player_id, season_label) == (2, os.environ["NBA_SEASON"])
    assert fields == {
        "scope": "season", "kind": "injury_return", "minutes": 20.0, "usage": 0.9, "games": None,
        "return_date": date(2026, 11, 20), "return_earliest": date(2026, 11, 18), "return_latest": date(2026, 11, 24),
        "until_date": date(2026, 12, 4), "depends_on_player": 1,
        "evidence": [{"type": "news", "id": 5, "headline": "out"}, {"type": "news", "id": 5, "headline": "out"},
                     {"type": "news", "id": 8, "headline": "out"}],
        "confidence": "likely", "review_date": TODAY + timedelta(days=14),
        "note": "back on a limit", "source_url": None, "author": "jp",
    }
    # The live projection, and never the preseason one: a note cannot rewrite the prior
    assert season.runs == [("live_projection", {"options": {"force": True}})]


def test_a_season_written_off_is_stored_as_no_games_and_keeps_the_review_date_given(season):
    res = season.http.put(f"{BASE}/2/note", headers=_AUTH, json={
        "kind": "injury_current", "out_for_season": True, "review_date": "2027-01-15", "note": "Achilles"})

    assert res.status_code == 200, res.text
    fields = season.recorded[0][2]
    assert (fields["games"], fields["return_date"], fields["review_date"], fields["evidence"]) == (
        0, None, date(2027, 1, 15), None)
    assert fields["author"] == "dashboard"


def test_a_rebuild_that_stored_nothing_keeps_the_note_and_says_so(season, monkeypatch):
    async def nothing_stored(name, **kwargs):
        return PipelineResult(status="success", message="day over", started_at="2026-11-16T05:00:00",
                              records_processed=0, records_skipped=1)

    monkeypatch.setattr(projections, "run_pipeline", nothing_stored)
    res = season.http.put(f"{BASE}/2/note", headers=_AUTH, json={"kind": "role", "minutes": 30, "note": "starting"})

    assert res.status_code == 200
    body = res.json()
    assert body["message"] == "Note saved; the live projection reads it on its next run"
    assert body["data"]["rebuilt"] is False and body["data"]["note"]["id"] == 91 and len(season.recorded) == 1

    async def failing(name, **kwargs):
        return PipelineResult(status="server_error", message="boom", started_at="2026-11-16T05:00:00", error="boom")

    monkeypatch.setattr(projections, "run_pipeline", failing)
    res = season.http.put(f"{BASE}/2/note", headers=_AUTH, json={"kind": "role", "minutes": 30, "note": "starting"})
    assert res.status_code == 200 and res.json()["data"]["rebuilt"] is False


@pytest.mark.parametrize("pid, extra, status, says", [
    (999, {}, 404, "No player 999"),
    (2, {"depends_on_player_id": 2}, 422, "cannot depend on the player it is about"),
    (2, {"depends_on_player_id": 999}, 422, "No player 999 to depend on"),
    (2, {"review_date": "2026-11-15"}, 422, "review date has already passed"),
    (0, {}, 422, None),
], ids=["nobody", "depends on himself", "no such teammate", "review date gone", "not an id"])
def test_a_note_that_cannot_be_saved_writes_nothing_and_rebuilds_nothing(season, pid, extra, status, says):
    res = season.http.put(f"{BASE}/{pid}/note", headers=_AUTH,
                          json={"kind": "role", "minutes": 30, "note": "why"} | extra)
    assert res.status_code == status and (says is None or says in res.json()["detail"])
    assert season.recorded == [] and season.runs == []


def test_a_note_citing_news_that_is_not_stored_is_refused(season, monkeypatch):
    monkeypatch.setattr(notes, "evidence_for", lambda ids: ([], [8, 9]))
    res = season.http.put(f"{BASE}/2/note", headers=_AUTH,
                          json={"kind": "role", "minutes": 30, "note": "why", "evidence_news_ids": [8, 9]})
    assert res.status_code == 422 and res.json()["detail"] == "No stored news item 8, 9"
    assert season.recorded == [] and season.runs == []
    # And no more than twenty at a time
    many = season.http.put(f"{BASE}/2/note", headers=_AUTH,
                           json={"kind": "role", "minutes": 30, "note": "why", "evidence_news_ids": list(range(21))})
    assert many.status_code == 422


def test_retiring_a_note_withdraws_the_season_row_only_and_rebuilds(season, monkeypatch):
    retired = []
    monkeypatch.setattr(ProjectionAdjustment, "retire",
                        classmethod(lambda cls, pid, season, scope: retired.append((pid, season, scope)) or 1))

    res = season.http.delete(f"{BASE}/2/note", headers=_AUTH)

    assert res.status_code == 200
    body = res.json()
    assert body["message"] == "Note retired and the live projection rebuilt"
    assert body["data"]["note"] is None and body["data"]["rebuilt"] is True
    assert retired == [(2, os.environ["NBA_SEASON"], "season")]
    assert season.runs == [("live_projection", {"options": {"force": True}})]

    monkeypatch.setattr(ProjectionAdjustment, "retire", classmethod(lambda cls, pid, season, scope: 0))
    assert season.http.delete(f"{BASE}/2/note", headers=_AUTH).status_code == 404
    assert len(season.runs) == 1


def test_the_news_route_lists_what_a_note_can_cite(season, monkeypatch):
    asked = []

    def recent(player_id):
        asked.append(player_id)
        return [EvidenceItem(id=8, source="rotowire_rss", kind="injury", headline="Out two weeks",
                             url="https://example.com/8", published_at=datetime(2026, 11, 15, 22, 5))]

    monkeypatch.setattr(notes, "recent_news", recent)
    res = season.http.get(f"{BASE}/2/news", headers=_AUTH)

    assert res.status_code == 200
    body = res.json()
    assert body["message"] == "1 item(s)" and asked == [2]
    assert body["data"] == {"player_id": 2, "items": [{
        "type": "news", "id": 8, "source": "rotowire_rss", "kind": "injury", "headline": "Out two weeks",
        "url": "https://example.com/8", "published_at": "2026-11-15T22:05:00"}]}
