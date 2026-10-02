"""
/v1/dashboard/projections: token-gated, the page's payload, a preview that
writes nothing, and the two writes — each followed by a republish. The
database read, the backend call and the pipeline run are stubbed; the editor's
own arithmetic is not.
"""

import os
from datetime import date, datetime
from types import SimpleNamespace

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from api.v1 import projections
from db.models.nba import ProjectionAdjustment
from schemas.pipeline import PipelineResult
from services import projection_editor as editor
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
