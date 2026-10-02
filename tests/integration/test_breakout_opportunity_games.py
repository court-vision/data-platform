"""breakout_detection: a peer who was on the floor for seconds still counts as absent."""

from __future__ import annotations

from datetime import date, timedelta

import pytest

from db.models.nba.player_game_stats import PlayerGameStats
from db.models.nba.players import Player
from pipelines.breakout_detection import BreakoutDetectionPipeline

CANDIDATE, PEER = 1, 2
AS_OF = date(2026, 2, 20)


def _line(player_id: int, game_date: date, minutes: int, fpts: int = 0) -> None:
    PlayerGameStats.upsert_game_stats(
        player_id=player_id,
        game_date=game_date,
        stats={"min": minutes, "fpts": fpts},
        team_id="NYK",
    )


def _opportunity_stats():
    return BreakoutDetectionPipeline()._get_position_validated_opportunity_stats(
        candidate_player_id=CANDIDATE,
        candidate_avg_min=18.0,
        team_id="NYK",
        position_peer_ids={PEER},
        as_of_date=AS_OF,
    )


@pytest.fixture
def two_big_nights(integration_db) -> list[date]:
    """The candidate's two high-usage games (30+ minutes against an 18-minute average)."""
    Player.upsert_player(player_id=CANDIDATE, name="Next Man Up")
    Player.upsert_player(player_id=PEER, name="Usual Starter")
    nights = [AS_OF - timedelta(days=3), AS_OF - timedelta(days=1)]
    _line(CANDIDATE, nights[0], minutes=32, fpts=40)
    _line(CANDIDATE, nights[1], minutes=30, fpts=36)
    return nights


@pytest.mark.integration
def test_a_peer_with_a_sub_minute_line_was_absent(two_big_nights) -> None:
    """A `min` 0 row is a cameo: his minutes were there for the candidate to take.

    Before sub-minute appearances were stored, these nights had no row for the
    peer at all, so this is the answer the detector has always given.
    """
    for night in two_big_nights:
        _line(PEER, night, minutes=0)

    avg_min, avg_fpts, games = _opportunity_stats()

    assert games == 2
    assert (avg_min, avg_fpts) == (31.0, 38.0)


@pytest.mark.integration
def test_a_peer_who_played_real_minutes_was_not_absent(two_big_nights) -> None:
    for night in two_big_nights:
        _line(PEER, night, minutes=28)

    assert _opportunity_stats() == (None, None, 0)
