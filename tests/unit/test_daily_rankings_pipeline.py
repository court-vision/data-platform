"""
The daily rankings, data-platform side, without a database: the one call to the
backend for the day's boards (`ValuationClient.daily_boards`), and the pure
parts of `pipelines.daily_rankings`: when a morning's boards wait for last
night, and how the backend's boards and ESPN's payload become rows.
"""

from datetime import date, datetime

import pytest
import requests
import responses

from db.models.nba.daily_rankings import BOARD_CATEGORIES, BOARD_POINTS
from pipelines.daily_rankings import WAIT_UNTIL_HOUR_ET, cv_rows, espn_rows, today_et, waiting_on
from pipelines.extractors.espn import parse_draft_market_players
from services.valuation_client import (
    DAILY_RANKINGS_PATH,
    BoardPlace,
    DailyBoards,
    DailyRank,
    ValuationClient,
)

pytestmark = pytest.mark.unit

BASE = "http://api.railway.internal:8080"
URL = BASE + DAILY_RANKINGS_PATH
TOKEN = "pipeline-secret-token"


def _player(pid, points_rank, category_rank, **over):
    payload = {
        "player_id": pid, "name": f"Player {pid}", "team": "DEN", "line_source": "projection",
        "season_gp": 12, "injury_status": None,
        "points": {"rank": points_rank, "value": 50.5, "score": 3100.2, "games": 61.4},
        "categories": {"rank": category_rank, "value": 31.2, "score": 1.234, "games": 61.4},
    }
    payload.update(over)
    return payload


def _envelope(**over):
    data = {
        "season": "2026-27", "as_of": "2026-11-16", "in_season": True, "model": "season-blend-1",
        "season_as_of": "2026-11-15", "projections_as_of": "2026-10-31", "projection_source": "cv",
        "prior_games": 10, "league_size": 12, "rounds": 13, "playoff_weight": 2.0,
        "players": [_player(1, 1, 2), _player(2, 2, 1, season_gp=0, injury_status="OUT", line_source="baseline")],
    }
    data.update(over)
    return {"status": "success", "message": "ok", "data": data}


# ---- the client ---------------------------------------------------------------


@responses.activate
def test_a_200_envelope_becomes_the_days_boards():
    responses.add(responses.POST, URL, json=_envelope(), status=200)

    boards = ValuationClient(BASE, TOKEN).daily_boards(correlation_id="corr-9")

    assert boards.available and boards.reason is None
    assert (boards.season, boards.as_of, boards.in_season) == ("2026-27", date(2026, 11, 16), True)
    assert (boards.model, boards.season_as_of) == ("season-blend-1", date(2026, 11, 15))
    first, second = boards.players
    assert first.points == BoardPlace(rank=1, value=50.5, score=3100.2, games=61.4)
    assert first.categories.rank == 2 and (first.season_gp, first.injury_status) == (12, None)
    assert (second.season_gp, second.injury_status, second.line_source) == (0, "OUT", "baseline")

    (call,) = responses.calls
    assert call.request.headers["Authorization"] == f"Bearer {TOKEN}"
    assert call.request.headers["X-Correlation-ID"] == "corr-9"
    assert not call.request.body


@responses.activate
def test_out_of_season_boards_parse_with_no_box_score_date():
    out = _envelope(in_season=False, season_as_of=None, model="projection-1",
                    players=[_player(1, 1, 1, season_gp=None)])
    responses.add(responses.POST, URL, json=out, status=200)

    boards = ValuationClient(BASE, TOKEN).daily_boards()

    assert boards.available and not boards.in_season and boards.season_as_of is None
    assert boards.players[0].season_gp is None


def test_no_base_url_is_unavailable_without_a_request():
    boards = ValuationClient(None, TOKEN).daily_boards()
    assert (boards.available, boards.reason) == (False, "backend_not_configured")


@responses.activate
@pytest.mark.parametrize("status,body,reason", [
    (401, {"detail": "Invalid pipeline token"}, "unauthorized"),
    (503, {"status": "error", "message": "busy"}, "http_503"),
    (200, {"status": "success", "message": "ok", "data": None}, "malformed_response"),
    (200, _envelope(as_of="not a date"), "malformed_response"),
    (200, _envelope(players=[{"player_id": 1}]), "malformed_response"),
])
def test_anything_else_is_unavailable_with_the_reason(status, body, reason):
    responses.add(responses.POST, URL, json=body, status=status)
    boards = ValuationClient(BASE, TOKEN).daily_boards()
    assert (boards.available, boards.reason) == (False, reason)
    assert boards.players == ()


@responses.activate
def test_a_timeout_is_unavailable_not_an_exception():
    responses.add(responses.POST, URL, body=requests.Timeout("read timed out"))
    boards = ValuationClient(BASE, TOKEN).daily_boards()
    assert (boards.available, boards.reason) == (False, "timeout")


# ---- waiting for last night -----------------------------------------------------


def _boards(season_as_of):
    return DailyBoards(available=True, season="2026-27", as_of=date(2026, 11, 16), in_season=True,
                       model="season-blend-1", season_as_of=season_as_of)


EARLY = datetime(2026, 11, 16, 6, 0)
LATE = datetime(2026, 11, 16, WAIT_UNTIL_HOUR_ET, 0)


def test_the_boards_wait_until_last_nights_box_scores_are_in():
    last_night = date(2026, 11, 15)
    # The season lines carry last night: go
    assert waiting_on(_boards(date(2026, 11, 15)), last_night, EARLY) is None
    # They stop the night before: wait, and say what for
    reason = waiting_on(_boards(date(2026, 11, 14)), last_night, EARLY)
    assert "2026-11-14" in reason and "2026-11-15" in reason
    # Nothing played yet this season, with a game day behind us: wait too
    assert "no game yet" in waiting_on(_boards(None), last_night, EARLY)


def test_no_game_day_behind_or_a_late_morning_never_waits():
    # No regular-season game before today (opening day): nothing to wait for
    assert waiting_on(_boards(None), None, EARLY) is None
    # An off night: the lines already run through the last game day
    assert waiting_on(_boards(date(2026, 11, 13)), date(2026, 11, 13), EARLY) is None
    # Late in the morning the day's board matters more than last night's games
    assert waiting_on(_boards(date(2026, 11, 14)), date(2026, 11, 15), LATE) is None


def test_lines_that_carry_last_night_still_wait_for_the_batch_writing_them():
    last_night = date(2026, 11, 15)
    writing = "the season-stats batch is still writing"
    # The date alone would let the boards go; a batch that is not done holds them
    assert waiting_on(_boards(last_night), last_night, EARLY, writing) == writing
    # ... but lines that stop short say so first, and a late morning waits for neither
    assert "2026-11-14" in waiting_on(_boards(date(2026, 11, 14)), last_night, EARLY, writing)
    assert waiting_on(_boards(last_night), last_night, LATE, writing) is None
    # No game day behind us: nothing was written last night to be half-done
    assert waiting_on(_boards(None), None, EARLY, writing) is None


def test_the_gate_reads_the_eastern_calendar_date():
    import pytz
    assert today_et(pytz.utc.localize(datetime(2026, 11, 16, 10, 0))) == date(2026, 11, 16)   # 5 AM ET
    assert today_et(pytz.utc.localize(datetime(2026, 11, 16, 3, 0))) == date(2026, 11, 15)    # 10 PM ET the day before


# ---- rows -----------------------------------------------------------------------


def test_the_backends_boards_become_one_row_per_player_per_board():
    boards = DailyBoards(
        available=True, season="2026-27", as_of=date(2026, 11, 16), in_season=True, model="season-blend-1",
        players=(
            DailyRank(player_id=1, line_source="projection", season_gp=12, injury_status=None,
                      points=BoardPlace(1, 50.5, 3100.2, 61.4), categories=BoardPlace(2, 31.2, 1.234, 61.4)),
            DailyRank(player_id=2, line_source="season", season_gp=11, injury_status="OUT",
                      points=BoardPlace(2, 40.0, 2400.0, 60.0), categories=BoardPlace(1, 33.0, 1.6, 60.0)),
        ),
    )

    points, categories = cv_rows(boards, BOARD_POINTS), cv_rows(boards, BOARD_CATEGORIES)

    assert points[0] == {"player_id": 1, "rank": 1, "value": 50.5, "score": 3100.2, "games": 61.4,
                         "season_gp": 12, "injury_status": None, "line_source": "projection",
                         "model": "season-blend-1"}
    assert [(r["player_id"], r["rank"], r["value"], r["score"]) for r in categories] == [
        (1, 2, 31.2, 1.234), (2, 1, 33.0, 1.6)]
    assert categories[1]["injury_status"] == "OUT" and categories[1]["line_source"] == "season"


def _espn(espn_id, name, standard=None, roto=None, injury="ACTIVE"):
    ranks = {}
    if standard is not None:
        ranks["STANDARD"] = {"rank": standard, "auctionValue": 70 - standard}
    if roto is not None:
        ranks["ROTO"] = {"rank": roto, "auctionValue": 60 - roto}
    return {"id": espn_id, "fullName": name, "draftRanksByRankType": ranks, "injuryStatus": injury,
            "ownership": {}, "stats": []}


def test_espns_two_ranks_become_two_boards():
    market = parse_draft_market_players([
        _espn(11, "First Pick", standard=1, roto=3),
        _espn(22, "Hurt Star", standard=2, roto=1, injury="OUT"),
        _espn(33, "Points Only", standard=40),
        _espn(44, "Not Ranked"),
        _espn(55, "No Row Here", standard=7, roto=9),
        _espn(66, "Rank Zero", standard=0, roto=0),
    ], projected_split_id="102027")
    player_ids = {11: 101, 22: 102, 33: 103, 44: 104, 66: 106}

    points, missing_points = espn_rows(market, player_ids, BOARD_POINTS)
    categories, missing_categories = espn_rows(market, player_ids, BOARD_CATEGORIES)

    # STANDARD on the points board, with its auction value; healthy is no flag
    assert points == [
        {"player_id": 101, "rank": 1, "value": 69, "injury_status": None},
        {"player_id": 102, "rank": 2, "value": 68, "injury_status": "OUT"},
        {"player_id": 103, "rank": 40, "value": 30, "injury_status": None},
    ]
    # ROTO on the categories board: its own order, and no row for a player it doesn't rank
    assert [(r["player_id"], r["rank"], r["value"]) for r in categories] == [(102, 1, 59), (101, 3, 57)]
    # A ranked player nba.players doesn't have is counted, once per board he is ranked on
    assert (missing_points, missing_categories) == (1, 1)


def test_a_player_espn_lists_twice_keeps_his_better_rank():
    market = parse_draft_market_players(
        [_espn(11, "Listed Twice", standard=30), _espn(12, "Listed Twice", standard=20)],
        projected_split_id="102027",
    )
    rows, missing = espn_rows(market, {11: 101, 12: 101}, BOARD_POINTS)
    assert rows == [{"player_id": 101, "rank": 20, "value": 50, "injury_status": None}] and missing == 0
