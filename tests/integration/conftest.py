from __future__ import annotations

import pytest
from peewee import OperationalError

from db.base import db
from db.models.nba.games import Game
from db.models.nba.player_game_stats import PlayerGameStats
from db.models.nba.player_rolling_stats import PlayerRollingStats
from db.models.nba.player_season_stats import PlayerSeasonStats
from db.models.nba.pipeline_batch import PipelineBatch
from db.models.nba.players import Player
from db.models.nba.teams import NBATeam
from db.models.pipeline_run import PipelineRun


INTEGRATION_MODELS = [
    PipelineRun,
    PipelineBatch,
    NBATeam,
    Player,
    Game,
    PlayerGameStats,
    PlayerSeasonStats,
    PlayerRollingStats,
]


@pytest.fixture(scope="session")
def integration_db():
    """Create schemas/tables for integration tests against Postgres."""
    try:
        db.connect(reuse_if_open=True)
    except OperationalError as exc:
        pytest.skip(f"Integration DB unavailable: {exc}")

    db.execute_sql("CREATE SCHEMA IF NOT EXISTS nba;")
    db.create_tables(INTEGRATION_MODELS, safe=True)

    # Ensure required dimension values are present for FK constraints.
    NBATeam.seed_teams()

    yield db

    if not db.is_closed():
        db.close()


@pytest.fixture(autouse=True)
def clean_integration_tables(integration_db):
    """Reset mutable integration tables between tests."""
    db.execute_sql(
        """
        TRUNCATE TABLE
          nba.player_rolling_stats,
          nba.player_season_stats,
          nba.player_game_stats,
          nba.games,
          nba.players,
          nba.pipeline_runs,
          nba.pipeline_batches
        RESTART IDENTITY CASCADE
        """
    )
    yield
