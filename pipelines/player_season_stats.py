"""
Cumulative Player Stats Pipeline

Updates cumulative season stats for players who played, then refreshes the
materialized rankings copy the public API reads.
"""

from datetime import date, datetime
from typing import Optional

import pytz
from peewee import fn

from core.nba_calendar import EASTERN
from core.season import season_for_date
from core.settings import settings
from db.models.nba import Player, PlayerSeasonStats
from db.models.nba.games import Game
from db.models.nba.player_game_stats import PlayerGameStats
from pipelines.base import BasePipeline
from pipelines.config import PipelineConfig, PipelineCategory
from pipelines.context import PipelineContext
from pipelines.extractors import ESPNExtractor, NBAApiExtractor
from pipelines.rankings_view import refresh_rankings
from pipelines.readiness import regular_season_game_rows
from pipelines.transformers import normalize_name, calculate_fantasy_points


class PlayerSeasonStatsPipeline(BasePipeline):
    """
    Update cumulative season stats for players who played.

    This pipeline:
    1. Fetches ESPN player data for roster percentages
    2. Fetches NBA league leaders with season totals
    3. Compares with previous records to find players who played
    4. Inserts new season stats records
    5. Refreshes the nba.rankings materialized view the API reads
    """

    config = PipelineConfig(
        name="player_season_stats",
        display_name="Player Season Stats",
        description="Updates season totals for players who played yesterday",
        target_table="nba.player_season_stats",
        category=PipelineCategory.POST_GAME,
        trigger_slug="cumulative-player-stats",
        depends_on=("player_game_stats",),
    )

    def __init__(self):
        super().__init__()
        self.espn_extractor = ESPNExtractor()
        self.nba_extractor = NBAApiExtractor()

    def _roster_percentages(self, ctx: PipelineContext) -> Optional[dict]:
        """ESPN ownership keyed by normalized name, or None when ESPN is unreachable.

        Deliberately non-fatal. This pipeline's actual product is season totals
        from the NBA API; ESPN only supplies `rost_pct`, which nothing reads at
        runtime (the ownership endpoints serve `nba.player_ownership`, written by
        its own pipeline). Letting an ownership lookup abort the run means losing
        the totals too.

        That is not hypothetical: ESPN publishes one season at a time and 404s
        the rest, so from the August rollover until it opens the new season every
        call here fails. Before this, that took the season stats with it.
        """
        try:
            data = self.espn_extractor.get_player_data()
            ctx.log.info("espn_data_fetched", player_count=len(data))
            return data
        except Exception as e:
            ctx.log.warning(
                "espn_ownership_unavailable",
                error=type(e).__name__,
                detail=str(e)[:200],
            )
            return None

    def _latest_gp(self, season: str) -> dict[int, int]:
        """Each player's games played on his newest season row this season.

        Without the season filter the first runs after rollover would compare
        against last season's totals and never detect new games.
        """
        subquery = (
            PlayerSeasonStats.select(
                PlayerSeasonStats.player_id,
                fn.MAX(PlayerSeasonStats.as_of_date).alias("max_date"),
            )
            .where(PlayerSeasonStats.season == season)
            .group_by(PlayerSeasonStats.player_id)
        )

        latest_records = (
            PlayerSeasonStats.select(PlayerSeasonStats.player_id, PlayerSeasonStats.gp)
            .join(
                subquery,
                on=(
                    (PlayerSeasonStats.player_id == subquery.c.player_id)
                    & (PlayerSeasonStats.as_of_date == subquery.c.max_date)
                ),
            )
            .where(PlayerSeasonStats.season == season)
        )
        return {record.player_id: record.gp for record in latest_records}

    def _played_on(self, game_date: date) -> set[int]:
        """Players with a regular-season game row that night."""
        rows = PlayerGameStats.select(PlayerGameStats.player_id).where(
            (PlayerGameStats.game_date == game_date)
            & regular_season_game_rows(game_date)
        )
        return {row.player_id for row in rows}

    def _written_for(self, game_date: date, season: str) -> set[int]:
        """Players who already have a season row dated that night."""
        rows = PlayerSeasonStats.select(PlayerSeasonStats.player_id).where(
            (PlayerSeasonStats.as_of_date == game_date)
            & (PlayerSeasonStats.season == season)
        )
        return {row.player_id for row in rows}

    def _gp_before_tip_off(
        self, game_date: date, season: str, player_ids: set[int]
    ) -> Optional[dict[int, int]]:
        """Each player's highest games played on a season row written before the night began.

        "Written" is `updated_at`, not `as_of_date`: a backfill run during the
        slate writes totals that already hold some of tonight's games under an
        older date. None when the schedule has no tip-off time for the night.
        """
        first_tip = Game.get_earliest_game_time_on_date(game_date)
        if first_tip is None:
            return None
        # start_time_et is Eastern; updated_at is UTC, naive.
        tip_off_utc = (
            EASTERN.localize(datetime.combine(game_date, first_tip))
            .astimezone(pytz.utc)
            .replace(tzinfo=None)
        )
        rows = (
            PlayerSeasonStats.select(
                PlayerSeasonStats.player_id,
                fn.MAX(PlayerSeasonStats.gp).alias("gp"),
            )
            .where(
                (PlayerSeasonStats.season == season)
                & PlayerSeasonStats.player_id.in_(list(player_ids))
                & (PlayerSeasonStats.updated_at < tip_off_utc)
            )
            .group_by(PlayerSeasonStats.player_id)
        )
        return {row.player_id: row.gp for row in rows}

    def _players_behind(
        self, game_date: date, season: str, incremented: set[int], api_gp: dict[int, int]
    ) -> tuple[set[int], set[int]]:
        """Who played that night, and which of them the API has not caught up on.

        The league leaders feed is a season dashboard and trails the game log:
        PlayerGameLogs can hold every game of the night while this feed is still
        missing the late ones, and the early games' increments made the run look
        like it had found the night's work. So the night's game log is the
        yardstick. Every player in it must show a new game played, either in
        this response (`incremented`) or on a season row an earlier run already
        wrote for the night.

        A new game, not the right number of them: after a night that never
        completed, a player on a back-to-back carries last night's increment
        and passes on it. Counting games since his last season row would close
        that, but a backfill writes today's totals under an old date, and from
        then on the count would never add up.

        The same backfill is why "no change since his newest row" is not the
        last word. Run during the slate, it writes a row under the old date
        that already holds tonight's early games, and from then on those
        players never change again and have no row dated tonight: the run
        would be held on every poll with the API fully caught up. So a player
        still behind is judged against the rows written before the night's
        first tip-off, which no run during the slate can have touched. On an
        ordinary night those are his newest rows and nothing changes. It is
        still a new game and not the right number: where the backfilled night
        was itself a game he played, he passes on that one, as above.
        """
        played = self._played_on(game_date)
        behind = played - incremented
        if behind:
            behind -= self._written_for(game_date, season)
        if behind:
            before_tip_off = self._gp_before_tip_off(game_date, season, behind)
            if before_tip_off is not None:
                behind = {
                    player_id
                    for player_id in behind
                    if api_gp.get(player_id, 0) <= before_tip_off.get(player_id, 0)
                }
        return played, behind

    def execute(self, ctx: PipelineContext) -> None:
        """Execute the cumulative player stats pipeline."""
        game_date = ctx.game_date()

        season = season_for_date(game_date)

        ctx.log.info("fetching_data", date=str(game_date), season=season)

        # Roster percentages are enrichment; season totals come from the NBA API
        # below. None when ESPN could not be reached -- see _roster_percentages.
        espn_data = self._roster_percentages(ctx)

        # Fetch NBA league leaders
        api_data = self.nba_extractor.get_league_leaders(season)
        ctx.log.info("nba_data_fetched", player_count=len(api_data))

        db_gp_map = self._latest_gp(season)

        # Find players who played (GP changed) and prepare entries
        entries = {}
        api_gp: dict[int, int] = {}
        for player in api_data:
            player_id = player["PLAYER_ID"]
            current_gp = player["GP"]
            api_gp[player_id] = max(current_gp, api_gp.get(player_id, 0))

            # Skip if player hasn't played new games
            if player_id in db_gp_map and current_gp == db_gp_map[player_id]:
                continue

            player_name = player["PLAYER"]
            normalized_name = normalize_name(player_name)
            # 0 means ESPN says nobody owns him; None means we could not ask.
            rost_pct = (
                None if espn_data is None
                else espn_data.get(normalized_name, {}).get("rost_pct", 0)
            )
            team_abbrev = player["TEAM"]

            player_stats = {
                "pts": player["PTS"],
                "reb": player["REB"],
                "ast": player["AST"],
                "stl": player["STL"],
                "blk": player["BLK"],
                "tov": player["TOV"],
                "fgm": player["FGM"],
                "fga": player["FGA"],
                "fg3m": player["FG3M"],
                "fg3a": player["FG3A"],
                "ftm": player["FTM"],
                "fta": player["FTA"],
            }
            fpts = calculate_fantasy_points(player_stats)

            # Keep only the entry with highest GP for each player
            if player_id not in entries or current_gp > entries[player_id]["gp"]:
                # Ensure player exists in dimension table
                Player.upsert_player(player_id=player_id, name=player_name)

                entries[player_id] = {
                    "player_id": player_id,
                    "team_id": team_abbrev,
                    "as_of_date": game_date,
                    "season": season,
                    "gp": current_gp,
                    "fpts": fpts,
                    "min": player["MIN"],
                    "rost_pct": rost_pct,
                    "pipeline_run_id": ctx.run_id,
                    **player_stats,
                }

        # Data readiness check, against the night's game log. Not on a backfill:
        # the API has no as-of date, so its totals say nothing about that night.
        if not ctx.date_override:
            played, behind = self._players_behind(
                game_date, season, set(entries), api_gp
            )
            if behind:
                ctx.log.warning(
                    "season_stats_behind_game_log",
                    date=str(game_date),
                    played_count=len(played),
                    behind_count=len(behind),
                    behind_player_ids=sorted(behind)[:10],
                )
                raise RuntimeError(
                    f"NBA API season stats not yet updated for {game_date}: "
                    f"{len(behind)} of {len(played)} players with a game that night "
                    "show no new game played. Data not ready yet — will retry."
                )

        if entries:
            # Insert new records
            for entry_data in entries.values():
                PlayerSeasonStats.upsert_season_stats(
                    player_id=entry_data["player_id"],
                    as_of_date=entry_data["as_of_date"],
                    season=entry_data["season"],
                    stats={
                        "gp": entry_data["gp"],
                        "fpts": entry_data["fpts"],
                        "pts": entry_data["pts"],
                        "reb": entry_data["reb"],
                        "ast": entry_data["ast"],
                        "stl": entry_data["stl"],
                        "blk": entry_data["blk"],
                        "tov": entry_data["tov"],
                        "min": entry_data["min"],
                        "fgm": entry_data["fgm"],
                        "fga": entry_data["fga"],
                        "fg3m": entry_data["fg3m"],
                        "fg3a": entry_data["fg3a"],
                        "ftm": entry_data["ftm"],
                        "fta": entry_data["fta"],
                        "rost_pct": entry_data["rost_pct"],
                    },
                    team_id=entry_data["team_id"],
                    pipeline_run_id=ctx.run_id,
                )
                ctx.increment_records()

            ctx.log.info("records_inserted", count=len(entries))

    def after_execute(self, ctx: PipelineContext) -> None:
        """Refresh the materialized rankings copy the public API reads.

        Runs on every successful execution, including one that found no new
        games: that is what re-syncs the copy after an earlier refresh failed.
        Never fails the pipeline — see pipelines/rankings_view.py.
        """
        try:
            refresh_rankings(ctx.log)
        except Exception as e:
            ctx.log.error("rankings_refresh_failed", error=str(e))
