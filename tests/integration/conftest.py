from __future__ import annotations

import pytest
from peewee import OperationalError

from db.base import db
from db.models.nba.availability import AvailabilityReport, PlayerAvailability
from db.models.nba.games import Game
from db.models.nba.live_game_score_snapshots import LiveGameScoreSnapshot
from db.models.nba.live_player_stats import LivePlayerStats
from db.models.nba.news import NewsItem, NewsItemPlayer, NewsItemTeam
from db.models.nba.player_ownership import PlayerOwnership
from db.models.nba.player_game_stats import PlayerGameStats
from db.models.nba.player_injuries import PlayerInjury
from db.models.nba.player_rolling_stats import PlayerRollingStats
from db.models.nba.player_season_stats import PlayerSeasonStats
from db.models.nba.pipeline_batch import PipelineBatch
from db.models.nba.player_profiles import PlayerProfile
from db.models.nba.players import Player
from db.models.nba.team_stats import TeamStats
from db.models.nba.teams import NBATeam
from db.models.lineup_snapshots import LineupSnapshot, LineupSnapshotPlayer
from db.models.pipeline_run import PipelineRun


INTEGRATION_MODELS = [
    PipelineRun,
    PipelineBatch,
    NBATeam,
    Player,
    # Player.in_play() reads it: a profile is what makes a player current.
    PlayerProfile,
    Game,
    PlayerGameStats,
    PlayerSeasonStats,
    PlayerRollingStats,
    # Read by the consistency checks (services/consistency_checks.py).
    TeamStats,
    LivePlayerStats,
    # Read with live_player_stats to tell a settled game (tests/integration/test_live_stats_settled.py).
    LiveGameScoreSnapshot,
    # Written by the lineup_snapshots pipeline's store (tests/integration/test_lineup_snapshot_store.py).
    LineupSnapshot,
    LineupSnapshotPlayer,
    # Written by the availability pipeline, and by live-stats' hand-off to it
    # (tests/integration/test_availability_writes.py). A report may cite a news item.
    NewsItem,
    # Written by the news feed with the items (tests/integration/test_news_feed_store.py),
    # which reads player_ownership for its watch set.
    NewsItemPlayer,
    NewsItemTeam,
    PlayerOwnership,
    AvailabilityReport,
    PlayerAvailability,
    PlayerInjury,
]


@pytest.fixture(scope="session")
def integration_db():
    """Create schemas/tables for integration tests against Postgres."""
    try:
        db.connect(reuse_if_open=True)
    except OperationalError as exc:
        pytest.skip(f"Integration DB unavailable: {exc}")

    db.execute_sql("CREATE SCHEMA IF NOT EXISTS nba;")
    db.execute_sql("CREATE SCHEMA IF NOT EXISTS usr;")
    db.create_tables(INTEGRATION_MODELS, safe=True)

    # Ensure required dimension values are present for FK constraints.
    NBATeam.seed_teams()

    yield db

    if not db.is_closed():
        db.close()


@pytest.fixture(scope="session")
def adjustments_table(integration_db):
    """nba.projection_adjustments as the backend's migrations 0027 and 0052 leave it:
    the model's columns, a self-reference checked at commit, and one live row per
    player per season per scope. The local database outlives a test run, so a table
    left by an older checkout is rebuilt when it lacks the model's columns."""
    from db.models.nba import ProjectionAdjustment

    have = {
        name for (name,) in db.execute_sql(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = 'nba' AND table_name = 'projection_adjustments'"
        ).fetchall()
    }
    if have and not {f.column_name for f in ProjectionAdjustment._meta.sorted_fields} <= have:
        db.execute_sql("DROP TABLE nba.projection_adjustments CASCADE")
    db.create_tables([ProjectionAdjustment], safe=True)
    db.execute_sql("""
        DO $$ BEGIN
            ALTER TABLE nba.projection_adjustments
                ALTER CONSTRAINT projection_adjustments_superseded_by_fkey DEFERRABLE INITIALLY DEFERRED;
        EXCEPTION WHEN undefined_object THEN NULL; END $$;
        DROP INDEX IF EXISTS nba.projection_adjustments_active_uidx;
        CREATE UNIQUE INDEX IF NOT EXISTS projection_adjustments_active_scope_uidx
            ON nba.projection_adjustments (player_id, season, scope)
            WHERE superseded_by IS NULL AND retired_at IS NULL;
    """)
    yield


@pytest.fixture(autouse=True)
def clean_integration_tables(integration_db):
    """Reset mutable integration tables between tests."""
    db.execute_sql(
        """
        TRUNCATE TABLE
          usr.lineup_snapshot_players,
          usr.lineup_snapshots,
          nba.availability_reports,
          nba.player_availability,
          nba.player_injuries,
          nba.news_item_players,
          nba.news_item_teams,
          nba.news_items,
          nba.player_ownership,
          nba.player_rolling_stats,
          nba.team_stats,
          nba.live_player_stats,
          nba.live_game_score_snapshots,
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
