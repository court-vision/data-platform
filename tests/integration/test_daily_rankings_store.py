"""
The daily-rankings pipeline against Postgres, with the backend's boards and
ESPN's payload stubbed: what a morning stores, when it waits for last night,
and what a later tick does to a day that is already (or only half) stored.

The morning is Nov 16, 2026; the last regular-season game day before it is the 15th.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta

import pytest

from core.resilience import NetworkError
from db.base import db
from db.models.nba.daily_rankings import (
    BOARD_CATEGORIES,
    BOARD_POINTS,
    SOURCE_CV,
    SOURCE_ESPN,
    DailyRanking,
)
from db.models.nba.games import Game
from db.models.nba.players import Player
from db.models.pipeline_run import PipelineRun
from pipelines import daily_rankings
from pipelines.daily_rankings import SEASON_STATS_PIPELINE, DailyRankingsPipeline, stored_on
from pipelines.extractors.espn import parse_draft_market_players
from schemas.common import ApiStatus
from services.valuation_client import BoardPlace, DailyBoards, DailyRank

pytestmark = pytest.mark.integration

TODAY = date(2026, 11, 16)
LAST_NIGHT = date(2026, 11, 15)
SEASON = "2026-27"
JOKIC, SGA, ROOKIE = 203999, 1628983, 1642843


@pytest.fixture(scope="module", autouse=True)
def rankings_table(integration_db):
    db.create_tables([DailyRanking], safe=True)
    yield


@pytest.fixture(autouse=True)
def morning(rankings_table, clean_integration_tables, monkeypatch):
    """Three players, a game last night, and a clock at 6 AM ET."""
    db.execute_sql("TRUNCATE TABLE nba.daily_rankings RESTART IDENTITY")
    for pid, name, espn_id in ((JOKIC, "Nikola Jokic", 3112335), (SGA, "Shai Gilgeous-Alexander", 4278073),
                               (ROOKIE, "New Rookie", None)):
        Player.create(id=pid, name=name, name_normalized=name.lower(), espn_id=espn_id)
    Game.create(game_id="0022600201", game_date=LAST_NIGHT, season=SEASON, home_team="DEN", away_team="BOS",
                status="final")
    _clock(monkeypatch, 6)
    yield


def _clock(monkeypatch, hour: int, day: date = TODAY) -> None:
    monkeypatch.setattr(daily_rankings, "now_eastern", lambda: datetime(day.year, day.month, day.day, hour, 0))
    monkeypatch.setattr(daily_rankings, "today_et", lambda now=None: day)


def _season_stats_run(status: str, minutes_ago: int) -> PipelineRun:
    """A run of the batch that writes the season lines, started `minutes_ago`."""
    started = datetime.utcnow() - timedelta(minutes=minutes_ago)
    return PipelineRun.create(pipeline_name=SEASON_STATS_PIPELINE, started_at=started, status=status,
                              completed_at=None if status == "running" else started + timedelta(minutes=1))


def _boards(*, order=(JOKIC, SGA, ROOKIE), season_as_of=LAST_NIGHT, in_season=True, as_of=TODAY) -> DailyBoards:
    players = tuple(
        DailyRank(
            player_id=pid, line_source="projection" if pid != ROOKIE else "season", season_gp=12 - i,
            injury_status="OUT" if pid == SGA else None,
            points=BoardPlace(rank=i + 1, value=60.0 - 5 * i, score=3600.0 - 300 * i, games=60.0 + i),
            categories=BoardPlace(rank=len(order) - i, value=30.0 + i, score=1.0 + 0.1 * i, games=60.0 + i),
        )
        for i, pid in enumerate(order)
    )
    return DailyBoards(available=True, season=SEASON, as_of=as_of, in_season=in_season,
                       model="season-blend-1", season_as_of=season_as_of, players=players)


def _espn(espn_id, name, standard, roto=None, injury="ACTIVE"):
    ranks = {"STANDARD": {"rank": standard, "auctionValue": 70 - standard}}
    if roto is not None:
        ranks["ROTO"] = {"rank": roto, "auctionValue": 60 - roto}
    return {"id": espn_id, "fullName": name, "draftRanksByRankType": ranks, "injuryStatus": injury,
            "ownership": {}, "stats": []}


ESPN_PAYLOAD = [
    _espn(3112335, "Nikola Jokic", 1, roto=2),
    _espn(4278073, "Shai Gilgeous-Alexander", 2, roto=1, injury="OUT"),
    _espn(999, "Nobody We Know", 3, roto=3),
]


def _run(boards=None, espn=ESPN_PAYLOAD, *, force=False):
    """One run; `espn` is a payload, None (the league is not on this season) or an exception."""
    pipeline = DailyRankingsPipeline()
    pipeline.valuation_client.daily_boards = lambda correlation_id=None: boards if boards is not None else _boards()

    def market():
        if isinstance(espn, Exception):
            raise espn
        return None if espn is None else parse_draft_market_players(espn, projected_split_id="102027")

    pipeline.espn_extractor.get_draft_market_data = market
    return pipeline._run_sync(options={"force": True} if force else None)


def _ranks(source, board, day=TODAY):
    return DailyRanking.ranks_on(day, source, board)


def test_a_morning_stores_both_boards_and_espns_ranks():
    result = _run()

    assert result.status == ApiStatus.SUCCESS
    assert _ranks(SOURCE_CV, BOARD_POINTS) == {JOKIC: 1, SGA: 2, ROOKIE: 3}
    assert _ranks(SOURCE_CV, BOARD_CATEGORIES) == {JOKIC: 3, SGA: 2, ROOKIE: 1}
    # ESPN's STANDARD and ROTO, for the players nba.players has
    assert _ranks(SOURCE_ESPN, BOARD_POINTS) == {JOKIC: 1, SGA: 2}
    assert _ranks(SOURCE_ESPN, BOARD_CATEGORIES) == {SGA: 1, JOKIC: 2}
    # 6 Court Vision rows and 4 of ESPN's; the player ESPN ranks that we don't have is a skip
    assert (result.records_processed, result.records_failed, result.records_skipped) == (10, 0, 1)
    assert stored_on(TODAY).complete and not stored_on(LAST_NIGHT).complete

    row = DailyRanking.get((DailyRanking.source == SOURCE_CV) & (DailyRanking.board == BOARD_POINTS)
                           & (DailyRanking.player == SGA))
    assert (row.as_of_date, row.season, row.model, row.line_source) == (TODAY, SEASON, "season-blend-1", "projection")
    assert (float(row.value), float(row.score), float(row.games), row.season_gp) == (55.0, 3300.0, 61.0, 11)
    assert row.injury_status == "OUT" and row.pipeline_run_id is not None
    espn = DailyRanking.get((DailyRanking.source == SOURCE_ESPN) & (DailyRanking.board == BOARD_CATEGORIES)
                            & (DailyRanking.player == SGA))
    assert (float(espn.value), espn.injury_status, espn.score, espn.model) == (59.0, "OUT", None, None)


def test_it_waits_for_last_nights_box_scores_then_stops_waiting(monkeypatch):
    stale = _boards(season_as_of=date(2026, 11, 14))

    result = _run(stale)

    assert result.status == ApiStatus.ERROR and "2026-11-15" in (result.error or "")
    assert DailyRanking.select().count() == 0

    # Late in the morning the board is stored on what there is
    _clock(monkeypatch, 10)
    assert _run(stale).status == ApiStatus.SUCCESS
    assert stored_on(TODAY).complete


def test_no_game_day_behind_it_never_waits():
    Game.delete().execute()
    assert _run(_boards(season_as_of=None)).status == ApiStatus.SUCCESS
    assert stored_on(TODAY).cv == 6


def test_espn_failing_keeps_the_boards_and_the_next_tick_adds_its_ranks():
    first = _run(espn=NetworkError("ESPN connection failed"))

    assert first.status == ApiStatus.ERROR and "ESPN" in (first.error or "")
    held = stored_on(TODAY)
    assert (held.cv, held.espn, held.complete) == (6, 0, False)

    # An hour later the backend would rank them differently: the morning's board is kept
    second = _run(_boards(order=(ROOKIE, SGA, JOKIC)))

    assert second.status == ApiStatus.SUCCESS and second.records_processed == 4
    assert _ranks(SOURCE_CV, BOARD_POINTS) == {JOKIC: 1, SGA: 2, ROOKIE: 3}
    assert _ranks(SOURCE_ESPN, BOARD_POINTS) == {JOKIC: 1, SGA: 2}


@pytest.mark.parametrize("espn", [None, [], [_espn(999, "Nobody We Know", 3)]],
                         ids=["league_not_rolled", "empty_payload", "nobody_resolved"])
def test_an_espn_read_with_nothing_in_it_fails_the_run_and_leaves_yesterday_alone(espn):
    result = _run(espn=espn)
    assert result.status == ApiStatus.ERROR
    assert stored_on(TODAY).espn == 0 and stored_on(TODAY).cv == 6


def test_a_stored_day_is_left_alone_and_a_forced_run_rebuilds_it():
    _run()
    first_ids = {r.id for r in DailyRanking.select()}

    again = _run(_boards(order=(ROOKIE, SGA, JOKIC)))

    assert again.status == ApiStatus.SUCCESS and again.records_processed == 0
    assert {r.id for r in DailyRanking.select()} == first_ids

    forced = _run(_boards(order=(ROOKIE, SGA, JOKIC)), espn=[_espn(3112335, "Nikola Jokic", 5)], force=True)

    assert forced.status == ApiStatus.SUCCESS
    assert _ranks(SOURCE_CV, BOARD_POINTS) == {ROOKIE: 1, SGA: 2, JOKIC: 3}
    # The day's ESPN rows are replaced whole: the player it no longer ranks is gone
    assert _ranks(SOURCE_ESPN, BOARD_POINTS) == {JOKIC: 5}
    assert _ranks(SOURCE_ESPN, BOARD_CATEGORIES) == {}


def test_a_forced_run_does_not_wait():
    assert _run(_boards(season_as_of=date(2026, 11, 14)), force=True).status == ApiStatus.SUCCESS
    assert stored_on(TODAY).complete


def test_each_day_is_its_own_board(monkeypatch):
    _clock(monkeypatch, 6, LAST_NIGHT)
    _run(_boards(as_of=LAST_NIGHT, season_as_of=date(2026, 11, 14)), force=True)
    _clock(monkeypatch, 6)
    _run(_boards(order=(SGA, JOKIC, ROOKIE)))

    assert _ranks(SOURCE_CV, BOARD_POINTS, LAST_NIGHT) == {JOKIC: 1, SGA: 2, ROOKIE: 3}
    assert _ranks(SOURCE_CV, BOARD_POINTS) == {SGA: 1, JOKIC: 2, ROOKIE: 3}
    assert DailyRanking.latest_date(SOURCE_CV, before=TODAY) == LAST_NIGHT


@pytest.mark.parametrize("force", [False, True], ids=["tick", "forced"])
def test_a_run_before_the_backends_day_turns_leaves_yesterday_as_it_was(monkeypatch, force):
    # Yesterday morning's boards
    _clock(monkeypatch, 6, LAST_NIGHT)
    _run(_boards(as_of=LAST_NIGHT, season_as_of=date(2026, 11, 14)))
    yesterday = {r.id for r in DailyRanking.select()}

    # 1 AM ET on the 16th: the backend is still on the 15th, and its boards have seen that night's games
    _clock(monkeypatch, 1)
    result = _run(_boards(as_of=LAST_NIGHT, order=(ROOKIE, SGA, JOKIC)), espn=[_espn(3112335, "Nikola Jokic", 5)],
                  force=force)

    assert result.status == ApiStatus.SUCCESS
    assert (result.records_processed, result.records_skipped) == (0, 1)
    assert {r.id for r in DailyRanking.select()} == yesterday
    assert _ranks(SOURCE_CV, BOARD_POINTS, LAST_NIGHT) == {JOKIC: 1, SGA: 2, ROOKIE: 3}
    assert stored_on(TODAY).cv == 0


@pytest.mark.parametrize("status,minutes_ago", [("running", 2), ("failed", 20)],
                         ids=["still_writing", "stopped_midway"])
def test_the_boards_wait_for_a_season_stats_batch_that_is_not_done(monkeypatch, status, minutes_ago):
    # The newest date is on the season lines, but the batch that writes them player by player is not through
    _season_stats_run("success", minutes_ago=60 * 24)
    _season_stats_run(status, minutes_ago=minutes_ago)

    waiting = _run()

    assert waiting.status == ApiStatus.ERROR and "season-stats batch" in (waiting.error or "")
    assert DailyRanking.select().count() == 0

    # Its retry got through: the next tick stores the morning
    _season_stats_run("success", minutes_ago=1)
    PipelineRun.update(status="failed", completed_at=datetime.utcnow()).where(
        (PipelineRun.pipeline_name == SEASON_STATS_PIPELINE) & (PipelineRun.status == "running")).execute()

    assert _run().status == ApiStatus.SUCCESS and stored_on(TODAY).complete


def test_a_batch_that_never_gets_through_stops_holding_the_boards_late_in_the_morning(monkeypatch):
    _season_stats_run("failed", minutes_ago=20)
    assert _run().status == ApiStatus.ERROR

    _clock(monkeypatch, daily_rankings.WAIT_UNTIL_HOUR_ET)

    assert _run().status == ApiStatus.SUCCESS and stored_on(TODAY).complete


def test_out_of_season_stores_nothing():
    result = _run(_boards(in_season=False, season_as_of=None))
    assert result.status == ApiStatus.SUCCESS and result.records_processed == 0
    assert DailyRanking.select().count() == 0


def test_a_backend_that_cannot_be_asked_fails_the_run():
    result = _run(DailyBoards.unavailable("timeout"))
    assert result.status == ApiStatus.ERROR and "timeout" in (result.error or "")
    assert DailyRanking.select().count() == 0
