"""
The preseason-market pipeline against Postgres, with ESPN's payload stubbed:
which player each market row lands on, and which players come out of the run
holding an ESPN id they did not have.

A player nba.players has never seen in a box score (a rookie, or a veteran who
missed last season) has no ESPN id, and the draft room cannot send him to ESPN
without one. The market payload carries the id, so a player resolved by name
takes it, unless that could put the id on the wrong player.
"""

from __future__ import annotations

from datetime import date

import pytest

from core.settings import settings
from db.base import db
from db.models.nba import DraftMarket, PlayerProjection
from db.models.nba.players import Player
from pipelines.extractors.espn import parse_draft_market_players
from pipelines.preseason_market import PreseasonMarketPipeline, adopt_espn_id
from schemas.common import ApiStatus

pytestmark = pytest.mark.integration

AS_OF = date(2026, 9, 20)  # inside the preseason window
SEASON = settings.nba_season


@pytest.fixture(scope="module", autouse=True)
def market_tables(integration_db):
    db.create_tables([DraftMarket, PlayerProjection], safe=True)
    yield


@pytest.fixture(autouse=True)
def clean_market_tables(market_tables, clean_integration_tables):
    db.execute_sql("TRUNCATE TABLE nba.draft_market, nba.player_projections RESTART IDENTITY CASCADE")
    yield


def _player(pid: int, name: str, espn_id: int | None = None) -> Player:
    return Player.create(id=pid, name=name, name_normalized=name.lower(), espn_id=espn_id)


def _espn(espn_id: int, name: str, rank: int) -> dict:
    return {
        "id": espn_id,
        "fullName": name,
        "defaultPositionId": 3,
        "eligibleSlots": [2, 6, 11],
        "draftRanksByRankType": {"STANDARD": {"rank": rank, "auctionValue": 10}},
        "ownership": {"averageDraftPosition": float(rank)},
        "stats": [],
    }


def _run(payload: list[dict]):
    rows = parse_draft_market_players(payload, projected_split_id=f"10{settings.espn_year}")
    pipeline = PreseasonMarketPipeline()
    pipeline.espn_extractor.get_draft_market_data = lambda league_id=None: rows
    return pipeline._run_sync(date_override=AS_OF)


def _espn_id(pid: int) -> int | None:
    return Player.get_by_id(pid).espn_id


def _market_players() -> set[int]:
    return {m.player_id for m in DraftMarket.select().where(DraftMarket.as_of_date == AS_OF)}


PAYLOAD = [
    _espn(3112335, "Nikola Jokic", 1),
    _espn(4870562, "Tyrese Haliburton", 19),   # missed last season
    _espn(5142698, "AJ Dybantsa", 79),         # rookie
    _espn(222, "Same Name", 200),              # the name holder has another id
    _espn(333, "Twin Name", 300),              # two players share the name
    _espn(2580782, "Spencer Dinwiddie", 353),  # nba.players has no such player
]


def _seed() -> None:
    _player(203999, "Nikola Jokic", espn_id=3112335)
    _player(1630169, "Tyrese Haliburton")
    _player(1643407, "AJ Dybantsa")
    _player(500, "Same Name", espn_id=111)
    _player(601, "Twin Name")
    _player(602, "Twin Name")


def test_a_player_matched_by_name_takes_the_market_rows_espn_id():
    _seed()

    result = _run(PAYLOAD)

    assert result.status == ApiStatus.SUCCESS
    assert _espn_id(1630169) == 4870562
    assert _espn_id(1643407) == 5142698
    assert _espn_id(203999) == 3112335
    assert {1630169, 1643407, 203999} <= _market_players()


def test_an_id_already_held_is_never_overwritten_and_a_shared_name_adopts_nothing():
    _seed()

    _run(PAYLOAD)

    assert _espn_id(500) == 111
    assert _espn_id(601) is None and _espn_id(602) is None
    # Neither is a reason to drop the market row: attribution is unchanged.
    assert 500 in _market_players()


def test_a_second_run_finds_adopted_players_by_id():
    _seed()
    _run(PAYLOAD)
    # ESPN renames him; only the id can still find him.
    renamed = [_espn(5142698, "A.J. Dybantsa", 79) if p["id"] == 5142698 else p for p in PAYLOAD]

    result = _run(renamed)

    assert result.status == ApiStatus.SUCCESS
    assert _espn_id(1643407) == 5142698
    assert 1643407 in _market_players()


class _Log:
    def __init__(self):
        self.warnings: list[str] = []

    def warning(self, event, **_):
        self.warnings.append(event)


class _Ctx:
    def __init__(self):
        self.log = _Log()


def test_an_id_taken_mid_run_is_skipped_without_aborting_the_snapshot():
    """The savepoint: the unique violation rolls back the one write, and the
    surrounding transaction (the day's snapshot) carries on."""
    rookie = _player(1643407, "AJ Dybantsa")
    _player(1643408, "Darryn Peterson", espn_id=5142698)
    ctx = _Ctx()

    with db.atomic():
        assert adopt_espn_id(rookie, 5142698, ctx) is False
        DraftMarket.record_market(player_id=rookie.id, season=SEASON, as_of_date=AS_OF, overall_rank=79)

    assert ctx.log.warnings == ["espn_id_taken"]
    assert rookie.espn_id is None and _espn_id(1643407) is None
    assert _market_players() == {1643407}
