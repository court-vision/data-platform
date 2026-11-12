"""
Live Player Stats Table

In-progress game statistics for NBA players, rewritten on every live-stats
poll (15 seconds apart) during live games. This is a continuously-overwritten snapshot table,
separate from the finalized player_game_stats fact table.

One row per player per game, uniquely keyed on (player_id, game_id).
"""

from collections.abc import Iterable, Mapping
from datetime import datetime, timedelta
from uuid import UUID

from peewee import (
    AutoField,
    CharField,
    DateField,
    DateTimeField,
    SmallIntegerField,
    IntegerField,
    UUIDField,
    ForeignKeyField,
)

from db.base import BaseModel
from db.models.nba.players import Player


class LivePlayerStats(BaseModel):
    """
    In-progress game statistics for a player.

    Updated on every live polling cycle (15 s). Stores current box score
    stats for all active games. Records persist until the next game day.

    Attributes:
        id: Auto-incrementing primary key
        player: Foreign key to Player dimension
        game_id: NBA game ID (e.g. "0022501234")
        game_date: Date of the game (ET)
        period: Current period (1-4, 5=OT, null if not started)
        game_clock: Current game clock in ISO 8601 duration format ("PT07M23.00S")
        game_status: 1=scheduled, 2=in_progress, 3=final
        fpts: Live fantasy points (calculated)
        pts, reb, ast, stl, blk, tov: Basic counting stats
        min: Minutes played (integer, truncated)
        fgm, fga, fg3m, fg3a, ftm, fta: Shooting stats
        last_updated: Timestamp of last poll that updated this record
        pipeline_run_id: Reference to the pipeline run that last updated this
    """

    id = AutoField(primary_key=True)
    player = ForeignKeyField(
        Player,
        backref="live_stats",
        on_delete="CASCADE",
        column_name="player_id",
    )
    game_id = CharField(max_length=20)
    game_date = DateField(index=True)
    period = SmallIntegerField(null=True)
    game_clock = CharField(max_length=20, null=True)
    game_status = SmallIntegerField(default=1)  # 1=scheduled, 2=in_progress, 3=final

    # Fantasy points (calculated)
    fpts = SmallIntegerField(default=0)

    # Basic counting stats
    pts = SmallIntegerField(default=0)
    reb = SmallIntegerField(default=0)
    ast = SmallIntegerField(default=0)
    stl = SmallIntegerField(default=0)
    blk = SmallIntegerField(default=0)
    tov = SmallIntegerField(default=0)
    min = IntegerField(default=0)

    # Shooting stats
    fgm = SmallIntegerField(default=0)
    fga = SmallIntegerField(default=0)
    fg3m = SmallIntegerField(default=0)
    fg3a = SmallIntegerField(default=0)
    ftm = SmallIntegerField(default=0)
    fta = SmallIntegerField(default=0)

    # Audit columns
    last_updated = DateTimeField(default=datetime.utcnow)
    pipeline_run_id = UUIDField(null=True, index=True)

    class Meta:
        table_name = "live_player_stats"
        schema = "nba"
        indexes = (
            # Unique constraint: one row per player per game
            (("player", "game_id"), True),
            # Index for querying all live stats for a given date
            (("game_date",), False),
        )

    def __repr__(self) -> str:
        return (
            f"<LivePlayerStats("
            f"player_id={self.player_id}, "
            f"game_id={self.game_id}, "
            f"fpts={self.fpts}, "
            f"status={self.game_status})>"
        )

    # What a poll writes on a line: everything but its key and its id.
    LINE_FIELDS = (
        "game_date", "period", "game_clock", "game_status", "fpts",
        "pts", "reb", "ast", "stl", "blk", "tov", "min",
        "fgm", "fga", "fg3m", "fg3a", "ftm", "fta",
    )

    @classmethod
    def upsert_lines(
        cls,
        game_id: str,
        lines: Iterable[Mapping],
        pipeline_run_id: UUID | None = None,
    ) -> int:
        """
        Insert or update one game's lines in a single statement.

        A poll used to cost three statements a player (the player, a read of
        his line, the write): about 600 round trips for a full night's 200
        lines. Every line is written on every poll, changed or not: the
        `pipeline_run_id` and `last_updated` it leaves are how a settled game
        is told (pipelines/live_game_stats.py) and how a stuck one is finalized
        (`finalize_stale_by_time`).

        Args:
            game_id: NBA game ID
            lines: One mapping per player with `player_id` and the LINE_FIELDS
                (a missing stat is 0; `game_status` 1; `period` and
                `game_clock` null). The last line wins for a player listed twice.
            pipeline_run_id: The run writing them

        Returns:
            The number of lines written
        """
        now = datetime.utcnow()
        absent = {"period": None, "game_clock": None, "game_status": 1}
        rows: dict[int, dict] = {}
        for line in lines:
            row = {name: line.get(name, absent.get(name, 0)) for name in cls.LINE_FIELDS}
            row.update(
                player=line["player_id"],
                game_id=game_id,
                last_updated=now,
                pipeline_run_id=pipeline_run_id,
            )
            rows[line["player_id"]] = row
        if not rows:
            return 0
        (
            cls.insert_many(list(rows.values()))
            .on_conflict(
                conflict_target=[cls.player, cls.game_id],
                preserve=[getattr(cls, name) for name in cls.LINE_FIELDS]
                + [cls.last_updated, cls.pipeline_run_id],
            )
            .execute()
        )
        return len(rows)

    @classmethod
    def finalize_stale_games(cls, game_date) -> int:
        """
        Set game_status=3 and game_clock=NULL for any records still marked
        as in-progress on the given date.

        Called when the NBA Scoreboard confirms all games are final, to
        reconcile the race where the Scoreboard updates before per-game
        BoxScores do.

        Returns:
            Number of records updated
        """
        return (
            cls.update(
                game_status=3,
                game_clock=None,
                last_updated=datetime.utcnow(),
            )
            .where(
                (cls.game_date == game_date)
                & (cls.game_status == 2)
            )
            .execute()
        )

    @classmethod
    def finalize_stale_by_time(cls, game_date, stale_minutes: int = 30) -> int:
        """
        Force-finalize records still marked in-progress whose last_updated
        timestamp is older than ``stale_minutes`` minutes ago.

        This is a time-based safety net that runs on every pipeline trigger,
        regardless of whether the scoreboard confirms all games are final.
        It catches edge cases where a game ended but the API never reported
        game_status=3 for that game.

        Args:
            game_date: Date to scope the cleanup
            stale_minutes: Minutes since last update to consider stale

        Returns:
            Number of records updated
        """
        cutoff = datetime.utcnow() - timedelta(minutes=stale_minutes)
        return (
            cls.update(
                game_status=3,
                game_clock=None,
                last_updated=datetime.utcnow(),
            )
            .where(
                (cls.game_date == game_date)
                & (cls.game_status == 2)
                & (cls.last_updated < cutoff)
            )
            .execute()
        )

    @classmethod
    def get_live_stats_for_date(cls, game_date) -> list["LivePlayerStats"]:
        """
        Get all live player stats for a specific date, ordered by fpts descending.

        Args:
            game_date: Date to query

        Returns:
            List of LivePlayerStats ordered by fantasy points descending
        """
        return list(
            cls.select()
            .where(cls.game_date == game_date)
            .order_by(cls.fpts.desc())
        )

    @classmethod
    def get_live_stats_for_players(
        cls,
        player_ids: list[int],
        game_date,
    ) -> list["LivePlayerStats"]:
        """
        Get live stats for a specific set of player IDs on a given date.

        Useful for fetching a fantasy team's current live stats.

        Args:
            player_ids: List of NBA player IDs
            game_date: Date to query

        Returns:
            List of LivePlayerStats for the given players
        """
        return list(
            cls.select()
            .where(
                (cls.player_id.in_(player_ids))
                & (cls.game_date == game_date)
            )
        )

    @classmethod
    def get_live_stats_by_names(
        cls,
        names: list[str],
        game_date,
    ) -> list["LivePlayerStats"]:
        """
        Get live stats for players matching a list of display names on a given date.

        Matches using the normalized name (lowercase, stripped) for robustness.
        Used to join ESPN/Yahoo fantasy roster players against live game stats.

        Args:
            names: List of player display names (e.g. from ESPN/Yahoo roster)
            game_date: Date to query

        Returns:
            List of LivePlayerStats with Player pre-loaded via join
        """
        normalized = [n.lower().strip() for n in names]
        return list(
            cls.select(cls, Player)
            .join(Player)
            .where(
                (Player.name_normalized.in_(normalized))
                & (cls.game_date == game_date)
            )
        )
