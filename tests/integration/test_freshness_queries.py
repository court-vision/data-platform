"""
Table freshness against real rows: the query the pure-function tests in
tests/unit/test_freshness_service.py cannot reach.
"""

from __future__ import annotations

from datetime import date, datetime, timezone

import pytest

from db.models.nba.games import Game
from services.freshness_service import build_freshness

MAR_3, MAR_4 = date(2026, 3, 3), date(2026, 3, 4)
# 7:00 AM ET on Mar 5, 2026: last night (Mar 4) is settled.
MORNING_AFTER = datetime(2026, 3, 5, 12, 0, tzinfo=timezone.utc)


def _seed_game(game_id: str, game_date: date, status: str) -> None:
    Game.create(
        game_id=game_id,
        game_date=game_date,
        season="2025-26",
        home_team_id="GSW",
        away_team_id="LAL",
        status=status,
    )


def _games_row():
    return next(t for t in build_freshness(now=MORNING_AFTER).tables if t.table == "nba.games")


@pytest.mark.integration
def test_games_is_stale_until_last_nights_results_land():
    # The rest of the season is on the schedule, as game_start_times leaves it.
    _seed_game("0022500001", MAR_3, "final")
    _seed_game("0022500002", MAR_4, "scheduled")
    _seed_game("0022500003", date(2026, 4, 12), "scheduled")

    games = _games_row()
    assert (games.state, games.latest_date, games.expected_date) == ("stale", MAR_3, MAR_4)

    Game.update(status="final").where(Game.game_id == "0022500002").execute()

    games = _games_row()
    assert (games.state, games.latest_date, games.expected_date) == ("fresh", MAR_4, MAR_4)


@pytest.mark.integration
def test_a_schedule_whose_results_never_landed_is_stale():
    _seed_game("0022500002", MAR_4, "scheduled")
    _seed_game("0022500003", date(2026, 4, 12), "scheduled")

    games = _games_row()
    assert (games.state, games.latest_date, games.expected_date) == ("stale", None, MAR_4)
    assert games.latest_written_at is not None
