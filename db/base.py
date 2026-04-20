from playhouse.pool import PooledPostgresqlDatabase
from playhouse.db_url import parse
from peewee import Model, IntegrityError
import os

# Get database credentials from environment variables
DATABASE_URL = os.getenv('DATABASE_URL')
parsed_url = parse(DATABASE_URL)
db_name = parsed_url.pop('database')

db = PooledPostgresqlDatabase(
    db_name,
    max_connections=20,
    stale_timeout=300,
    **parsed_url
)

class BaseModel(Model):
    class Meta:
        database = db

# Function to initialize database connection
def init_db():
    """Initialize database connection and create tables if they don't exist."""
    db.connect()

    # Import models the data platform writes to or reads from
    from .models.pipeline_run import PipelineRun
    from .models.data_quality_run import DataQualityRun
    from .models.data_quality_check import DataQualityCheck

    # Legacy stats model (written by DailyMatchupScoresPipeline)
    from .models.stats.daily_matchup_score import DailyMatchupScore

    # NBA schema models (data platform is the writer)
    from .models.nba import (
        Player,
        NBATeam,
        PlayerGameStats,
        PlayerSeasonStats,
        PlayerOwnership,
        PlayerRollingStats,
        TeamStats,
        PlayerProfile,
        PlayerAdvancedStats,
        Game,
        PlayerInjury,
        LivePlayerStats,
        LiveGameScoreSnapshot,
        BreakoutCandidate,
        CronJobRun,
        PlayoffSeries,
    )

    # User/team models (read by lineup_alerts pipeline for notification prefs)
    from .models import User, Team, Lineup
    from .models.notifications import NotificationPreference, NotificationLog, NotificationTeamPreference

    # Create tables if they don't exist (safe=True = IF NOT EXISTS).
    # Wrapped in try/except to handle the startup race condition: main.py and
    # main_public.py both call init_db() concurrently. Both can pass the IF NOT
    # EXISTS check simultaneously, then one loses the pg_type insert. The loser
    # gets IntegrityError — but the table exists at that point, so we treat it
    # as success and continue.
    #
    # Note: Order matters for foreign key dependencies
    # 1. Dimension tables first (Player, NBATeam)
    # 2. Fact/aggregate tables second
    # 3. Extended data tables last (may reference dimension tables)
    try:
        db.create_tables([
            # NBA schema - audit
            PipelineRun,
            DataQualityRun,
            DataQualityCheck,
            # Legacy stats_s2 (DailyMatchupScoresPipeline still writes here)
            DailyMatchupScore,
            # NBA schema - dimension tables
            Player, NBATeam,
            # NBA schema - team stats (FK to NBATeam)
            TeamStats,
            # NBA schema - fact/aggregate tables
            PlayerGameStats, PlayerSeasonStats, PlayerOwnership, PlayerRollingStats,
            # NBA schema - extended data tables
            PlayerProfile, PlayerAdvancedStats, Game, PlayerInjury,
            # NBA schema - live data
            LivePlayerStats,
            LiveGameScoreSnapshot,
            # NBA schema - breakout detection
            BreakoutCandidate,
            # NBA schema - cron-runner audit
            CronJobRun,
            # NBA schema - playoff bracket
            PlayoffSeries,
            # User schema (referenced by lineup_alerts)
            User, Team, Lineup,
            # Notification tables (read/written by lineup_alerts pipeline)
            NotificationPreference, NotificationLog, NotificationTeamPreference,
        ], safe=True)
    except IntegrityError:
        # Race condition: the other process (main.py/main_public.py) won the
        # CREATE TABLE race and the tables already exist. Safe to continue.
        pass

# Function to close database connection
def close_db():
    """Close database connection."""
    if not db.is_closed():
        db.close()
        print("Database connection closed")
