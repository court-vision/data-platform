"""
Live Game Score Snapshots Table

Periodic snapshots of NBA game scores, captured every 30 seconds during
active game windows by the LiveGameStatsPipeline. Used to build score-over-time
charts showing how leads changed throughout a game, and read by the backend as
each game's current score, clock and status.

A snapshot is the game as its live box score had it on that poll. Until
2026-11 it was the scoreboard file's word, which moves about once in ten
minutes; a poll that cannot read the box score still stores the scoreboard's,
unless that is older than what is already here (`LatestSnapshot.ahead_of`).

One row is appended per game per pipeline run — this is a pure time series,
not a rolling snapshot. A night's rows outlive it: the next night's first live
run deletes only its pre-tip ones (game_status 1), and the rest are the margin
line of each game's sheet after the fact.
"""

import re
from collections.abc import Iterable, Mapping
from datetime import datetime
from typing import NamedTuple

from peewee import (
    AutoField,
    CharField,
    DateField,
    DateTimeField,
    SmallIntegerField,
    UUIDField,
)

from db.base import BaseModel, db


_GAME_CLOCK = re.compile(r"^PT(\d+)M(\d+(?:\.\d+)?)S$")


def clock_seconds(clock) -> float | None:
    """Seconds left in the period on an NBA game clock ("PT06M30.00S"); None for anything else."""
    match = _GAME_CLOCK.match(clock) if isinstance(clock, str) else None
    return int(match[1]) * 60 + float(match[2]) if match else None


class LatestSnapshot(NamedTuple):
    """How far the newest stored snapshot of a game had it."""

    game_status: int
    period: int
    score: int  # both teams' points
    game_clock: str | None = None

    def ahead_of(self, game: Mapping) -> bool:
        """Whether `game` (a scoreboard entry, or a box score's state in that
        shape) is older than this: an earlier status, an earlier period or
        fewer points on the board. Level on all three in play, the one with
        more of the period left on its clock is the older; a clock that
        cannot be read settles nothing."""
        status = game.get("game_status") or 1
        period = game.get("period") or 0
        score = (game.get("home_score") or 0) + (game.get("away_score") or 0)
        if status < self.game_status or period < self.period or score < self.score:
            return True
        if status == self.game_status == 2 and (period, score) == (self.period, self.score):
            stored, theirs = clock_seconds(self.game_clock), clock_seconds(game.get("game_clock"))
            return stored is not None and theirs is not None and theirs > stored
        return False


# One index probe a game (game_id, captured_at), whatever the night has stored.
LATEST_SNAPSHOTS_SQL = """
    SELECT g.game_id, s.game_status, s.period, s.home_score, s.away_score, s.game_clock
    FROM unnest(%(game_ids)s::varchar[]) AS g(game_id)
    CROSS JOIN LATERAL (
        SELECT game_status, period, home_score, away_score, game_clock
        FROM nba.live_game_score_snapshots
        WHERE game_id = g.game_id
        ORDER BY captured_at DESC
        LIMIT 1
    ) s
"""


class LiveGameScoreSnapshot(BaseModel):
    """
    Score snapshot for an NBA game at a specific point in time.

    One row per game per 30-second pipeline poll. Records accumulate throughout
    a game night (about 300 in play per game, plus the pre-tip polls, which are
    deleted when the next game day starts; the rest are kept).

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
            game_meta: The game in the shape get_scoreboard_games() gives
                (pipelines.live_game_stats.game_state), with keys:
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
    def latest_by_game(cls, game_ids: Iterable[str]) -> dict[str, LatestSnapshot]:
        """
        The newest snapshot of each of `game_ids` that has one, in one query.

        Args:
            game_ids: NBA game IDs

        Returns:
            game_id -> LatestSnapshot; a game never snapshotted is absent
        """
        game_ids = list(game_ids)
        if not game_ids:
            return {}
        rows = db.execute_sql(LATEST_SNAPSHOTS_SQL, {"game_ids": game_ids}).fetchall()
        return {
            game_id: LatestSnapshot(status, period or 0, (home or 0) + (away or 0), clock)
            for game_id, status, period, home, away, clock in rows
        }

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
