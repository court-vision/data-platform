from __future__ import annotations

from datetime import date

import pandas as pd
import pytest

from db.models.nba.games import Game
from db.models.nba.player_game_stats import PlayerGameStats
from db.models.nba.players import Player
from pipelines.player_game_stats import PlayerGameStatsPipeline
from schemas.common import ApiStatus


def _stats_df(game_id: str = "0022500001") -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "GAME_ID": game_id,
                "PLAYER_ID": 201939,
                "PLAYER_NAME": "Stephen Curry",
                "TEAM_ABBREVIATION": "GSW",
                "MIN": "34:12",
                "PTS": 30,
                "REB": 5,
                "AST": 6,
                "STL": 2,
                "BLK": 0,
                "TOV": 3,
                "FGM": 10,
                "FGA": 20,
                "FG3M": 5,
                "FG3A": 12,
                "FTM": 5,
                "FTA": 6,
            }
        ]
    )


def _seed_expected_game(game_date: date, game_id: str) -> None:
    Game.create(
        game_id=game_id,
        game_date=game_date,
        season="2025-26",
        home_team_id="GSW",
        away_team_id="LAL",
        status="final",
        home_score=120,
        away_score=115,
    )


@pytest.mark.integration
def test_player_game_stats_pipeline_fails_on_incomplete_games(integration_db) -> None:
    game_date = date(2026, 2, 14)
    _seed_expected_game(game_date, "0022500001")
    _seed_expected_game(game_date, "0022500002")

    pipeline = PlayerGameStatsPipeline()
    pipeline.espn_extractor.get_player_data = lambda: {}
    pipeline.nba_extractor.get_game_logs = lambda *_: _stats_df(game_id="0022500001")

    result = pipeline._run_sync(date_override=game_date)

    assert result.status == ApiStatus.ERROR
    assert result.error is not None
    assert "Data not ready yet" in result.error
    assert PlayerGameStats.select().count() == 0


@pytest.mark.integration
def test_player_game_stats_pipeline_upserts_idempotently(integration_db) -> None:
    game_date = date(2026, 2, 14)
    _seed_expected_game(game_date, "0022500001")

    pipeline = PlayerGameStatsPipeline()
    pipeline.espn_extractor.get_player_data = lambda: {
        "stephen curry": {"espn_id": 3975, "rost_pct": 99.1}
    }
    pipeline.nba_extractor.get_game_logs = lambda *_: _stats_df(game_id="0022500001")

    first = pipeline._run_sync(date_override=game_date)
    second = pipeline._run_sync(date_override=game_date)

    assert first.status == ApiStatus.SUCCESS
    assert second.status == ApiStatus.SUCCESS
    assert Player.select().count() == 1
    assert PlayerGameStats.select().count() == 1

    row = PlayerGameStats.select().first()
    assert row is not None
    assert row.player_id == 201939
    assert row.team_id == "GSW"
    assert row.game_date == game_date
    assert row.fpts == 53
    # The payload's GAME_ID is stored, so readers identify the fixture by the
    # game rather than inferring it from (game_date, team).
    assert row.game_id == "0022500001"


@pytest.mark.integration
def test_a_game_the_schedule_has_not_caught_up_with_still_lands(integration_db) -> None:
    """The stats pipeline can outrun the schedule pipeline.

    `game_id` is a foreign key, so storing an id `nba.games` has never heard of
    would fail the whole row. The line is worth more than the label: it lands
    keyed by date, and a later run promotes it once the game exists.
    """
    game_date = date(2026, 2, 14)
    _seed_expected_game(game_date, "0022500001")

    pipeline = PlayerGameStatsPipeline()
    pipeline.espn_extractor.get_player_data = lambda: {
        "stephen curry": {"espn_id": 3975, "rost_pct": 99.1}
    }
    # The box score names a game the schedule does not have yet.
    pipeline.nba_extractor.get_game_logs = lambda *_: _stats_df(game_id="0022500001")
    Game.delete().execute()

    result = pipeline._run_sync(date_override=game_date)

    assert result.status == ApiStatus.SUCCESS
    row = PlayerGameStats.select().first()
    assert row is not None and row.game_id is None
    assert row.game_date == game_date, "the box score survived without its game"
