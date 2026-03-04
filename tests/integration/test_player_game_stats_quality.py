from __future__ import annotations

from datetime import date

import pandas as pd
import pytest

from db.models.nba.games import Game
from db.models.nba.player_game_stats import PlayerGameStats
from pipelines.player_game_stats import PlayerGameStatsPipeline
from schemas.common import ApiStatus


@pytest.mark.quality
def test_quality_player_game_stats_has_no_null_core_fields(integration_db) -> None:
    game_date = date(2026, 2, 15)
    Game.create(
        game_id="0022500100",
        game_date=game_date,
        season="2025-26",
        home_team_id="BOS",
        away_team_id="NYK",
        status="final",
        home_score=105,
        away_score=99,
    )

    pipeline = PlayerGameStatsPipeline()
    pipeline.espn_extractor.get_player_data = lambda: {}
    pipeline.nba_extractor.get_game_logs = lambda *_: pd.DataFrame(
        [
            {
                "GAME_ID": "0022500100",
                "PLAYER_ID": 2544,
                "PLAYER_NAME": "LeBron James",
                "TEAM_ABBREVIATION": "BOS",
                "MIN": "35:00",
                "PTS": 28,
                "REB": 8,
                "AST": 9,
                "STL": 1,
                "BLK": 1,
                "TOV": 4,
                "FGM": 11,
                "FGA": 21,
                "FG3M": 3,
                "FG3A": 8,
                "FTM": 3,
                "FTA": 4,
            }
        ]
    )

    result = pipeline._run_sync(date_override=game_date)
    assert result.status == ApiStatus.SUCCESS

    null_count = (
        PlayerGameStats.select()
        .where(
            (PlayerGameStats.player.is_null(True))
            | (PlayerGameStats.game_date.is_null(True))
            | (PlayerGameStats.team.is_null(True))
        )
        .count()
    )
    assert null_count == 0

