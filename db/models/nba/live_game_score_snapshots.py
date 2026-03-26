"""
Live Game Score Snapshots Table

Periodic snapshots of NBA game scores, captured every ~60 seconds during
active game windows by the LiveGameStatsPipeline. Used to build score-over-time
charts showing how leads changed throughout a game.

One row is appended per game per pipeline run — this is a pure time series,
not a rolling snapshot. Records are cleaned up at the start of each day's
live pipeline run.
"""

from datetime import datetime

from peewee import (
    AutoField,
    CharField,
    DateField,
    DateTimeField,
    SmallIntegerField,
    UUIDField,
)

from db.base import BaseModel


class LiveGameScoreSnapshot(BaseModel):
    """
    Score snapshot for an NBA game at a specific point in time.

    One row per game per ~60-second pipeline poll. Records accumulate throughout
    a game night (~240 rows per game) and are deleted when the next game day starts.

    Attributes:
        id: Auto-incrementing primary key
        game_id: NBA game ID (e.g. "0022501234")
        game_date: Date of the game (ET-based)
        home_team: Home team abbreviation (e.g. "LAL")
        away_team: Away team abbreviation (e.g. "BOS")
        home_score: Home team score at time of capture
        away_score: Away team score at time of capture
        period: Current period (1-4, 5=OT, null if not started)
        game_clock: Current game clock in ISO 8601 duration format ("PT07M23.00S")
        game_status: 1=scheduled, 2=in_progress, 3=final
        captured_at: UTC timestamp when this snapshot was taken
        pipeline_run_id: Reference to the pipeline run that created this snapshot
    """

    id = AutoField(primary_key=True)
    game_id = CharField(max_length=20, index=True)
    game_date = DateField(index=True)
    home_team = CharField(max_length=10)
    away_team = CharField(max_length=10)
    home_score = SmallIntegerField(default=0)
    away_score = SmallIntegerField(default=0)
    period = SmallIntegerField(null=True)
    game_clock = CharField(max_length=20, null=True)
    game_status = SmallIntegerField(default=1)  # 1=scheduled, 2=in_progress, 3=final
    captured_at = DateTimeField(default=datetime.utcnow, index=True)
    pipeline_run_id = UUIDField(null=True)

    class Meta:
        table_name = "live_game_score_snapshots"
        schema = "nba"
        indexes = (
            # Composite index for fetching all snapshots for a game in order
            (("game_id", "captured_at"), False),
        )

    def __repr__(self) -> str:
        return (
            f"<LiveGameScoreSnapshot("
            f"game_id={self.game_id}, "
            f"home={self.home_team}:{self.home_score}, "
            f"away={self.away_team}:{self.away_score}, "
            f"period={self.period}, "
            f"status={self.game_status})>"
        )

    @classmethod
    def record_snapshot(
        cls,
        game_id: str,
        game_date,
        game_meta: dict,
        pipeline_run_id=None,
    ) -> "LiveGameScoreSnapshot":
        """
        Insert a new score snapshot for a game.

        Each call creates a new row — no deduplication. The caller is
        responsible for not inserting snapshots for the same game twice in
        the same pipeline run.

        Args:
            game_id: NBA game ID
            game_date: Date of the game
            game_meta: Dict from get_scoreboard_games() with keys:
                home_team, away_team, home_score, away_score,
                period, game_clock, game_status
            pipeline_run_id: Optional pipeline run UUID

        Returns:
            The newly created LiveGameScoreSnapshot instance
        """
        return cls.create(
            game_id=game_id,
            game_date=game_date,
            home_team=game_meta.get("home_team", ""),
            away_team=game_meta.get("away_team", ""),
            home_score=game_meta.get("home_score") or 0,
            away_score=game_meta.get("away_score") or 0,
            period=game_meta.get("period") or None,
            game_clock=game_meta.get("game_clock") or None,
            game_status=game_meta.get("game_status", 1),
            captured_at=datetime.utcnow(),
            pipeline_run_id=pipeline_run_id,
        )

    @classmethod
    def get_snapshots_for_game(cls, game_id: str) -> list["LiveGameScoreSnapshot"]:
        """
        Get all score snapshots for a game, ordered chronologically.

        Args:
            game_id: NBA game ID

        Returns:
            List of LiveGameScoreSnapshot ordered by captured_at ascending
        """
        return list(
            cls.select()
            .where(cls.game_id == game_id)
            .order_by(cls.captured_at.asc())
        )

    @classmethod
    def get_latest_for_game(cls, game_id: str) -> "LiveGameScoreSnapshot | None":
        """
        Get the most recent snapshot for a game.

        Used as a fallback when the live box score API is unavailable.

        Args:
            game_id: NBA game ID

        Returns:
            Most recent snapshot or None if no snapshots exist
        """
        return (
            cls.select()
            .where(cls.game_id == game_id)
            .order_by(cls.captured_at.desc())
            .first()
        )
