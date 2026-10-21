"""What the live-stats pipeline keeps of a past night when the next one starts.

The first live run of a night cleared everything older: the live lines (which
the live matchup reads by date and must not double count) and the score
snapshots. The snapshots are now kept past their night, bar the pre-tip polls:
they are the margin line of each game's sheet after the fact (the backend's
GET /v1/games/{date}/{game_id}).
"""

from __future__ import annotations

from datetime import date, datetime, timedelta

import pytest
from freezegun import freeze_time

from db.models.nba.live_game_score_snapshots import LiveGameScoreSnapshot
from db.models.nba.live_player_stats import LivePlayerStats
from db.models.nba.players import Player
from pipelines.live_game_stats import LiveGameStatsPipeline
from schemas.common import ApiStatus

LAST_NIGHT = date(2026, 10, 20)
TONIGHT = date(2026, 10, 21)
OPENER = "0022600001"
TONIGHTS = "0022600004"


def _snapshot(game_id: str, game_date: date, status: int, minute: int, home: int = 0, away: int = 0) -> None:
    LiveGameScoreSnapshot.create(
        game_id=game_id, game_date=game_date, home_team="DET", away_team="BOS",
        home_score=home, away_score=away, period=None if status == 1 else 1, game_clock=None,
        game_status=status, captured_at=datetime(2026, 10, 20, 18, 45) + timedelta(minutes=minute),
    )


@pytest.mark.integration
def test_a_new_night_keeps_last_nights_game_snapshots_and_drops_its_pre_tip_polls(integration_db):
    for minute in range(3):
        _snapshot(OPENER, LAST_NIGHT, 1, minute)                      # pre-tip, 0-0
    _snapshot(OPENER, LAST_NIGHT, 2, 30, home=24, away=23)
    _snapshot(OPENER, LAST_NIGHT, 3, 200, home=121, away=113)
    _snapshot(TONIGHTS, TONIGHT, 1, 1440)                             # tonight's own pre-tip poll
    Player.upsert_player(player_id=202331, name="Paul George", espn_id=None)
    LivePlayerStats.create(player=202331, game_id=OPENER, game_date=LAST_NIGHT, game_status=3, fpts=72, pts=38)

    pipeline = LiveGameStatsPipeline()
    pipeline.nba_extractor.get_scoreboard_games = lambda _d: []
    with freeze_time("2026-10-21T22:45:00"):
        assert pipeline._run_sync(nba_date=TONIGHT).status == ApiStatus.SUCCESS

    kept = [(s.game_id, s.game_status) for s in LiveGameScoreSnapshot.select().order_by(LiveGameScoreSnapshot.captured_at)]
    assert kept == [(OPENER, 2), (OPENER, 3), (TONIGHTS, 1)]
    # The live lines still go: the live matchup reads them by date, and the game log has the night.
    assert LivePlayerStats.select().count() == 0
