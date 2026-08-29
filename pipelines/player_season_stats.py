"""
Cumulative Player Stats Pipeline

Updates cumulative season stats for players who played, then refreshes the
materialized rankings copy the public API reads.
"""

from datetime import timedelta
from typing import Optional

import pytz
from peewee import fn

from core.season import season_for_date
from core.settings import settings
from db.models.nba import Player, PlayerSeasonStats
from db.models.nba.player_game_stats import PlayerGameStats
from pipelines.base import BasePipeline
from pipelines.config import PipelineConfig, PipelineCategory
from pipelines.context import PipelineContext
from pipelines.extractors import ESPNExtractor, NBAApiExtractor
from pipelines.rankings_view import refresh_rankings
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

    def execute(self, ctx: PipelineContext) -> None:
        """Execute the cumulative player stats pipeline."""
        central_tz = pytz.timezone("US/Central")

        # Determine the game date. Use an explicit override for backfills;
        # otherwise use CST with a 6am cutoff (before 6am = previous night's games).
        if ctx.date_override:
            game_date = ctx.date_override
        else:
            now_cst = ctx.started_at  # already in CST from PipelineContext
            if now_cst.hour < 6:
                game_date = (now_cst - timedelta(days=1)).date()
            else:
                game_date = now_cst.date()

        season = season_for_date(game_date)

        ctx.log.info("fetching_data", date=str(game_date), season=season)

        # Roster percentages are enrichment; season totals come from the NBA API
        # below. None when ESPN could not be reached -- see _roster_percentages.
        espn_data = self._roster_percentages(ctx)

        # Fetch NBA league leaders
        api_data = self.nba_extractor.get_league_leaders(season)
        ctx.log.info("nba_data_fetched", player_count=len(api_data))

        # Latest GP per player *this season* — without the season filter the
        # first runs after rollover would compare against last season's totals
        # and never detect new games.
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
        db_gp_map = {record.player_id: record.gp for record in latest_records}

        # Find players who played (GP changed) and prepare entries
        entries = {}
        for player in api_data:
            player_id = player["PLAYER_ID"]
            current_gp = player["GP"]

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

        # Data readiness check: if player_game_stats already wrote records for
        # game_date but we found zero GP increments, the NBA league leaders API
        # hasn't processed tonight's games yet — raise so the pipeline retries.
        if not entries and not ctx.date_override:
            played_count = (
                PlayerGameStats.select()
                .where(PlayerGameStats.game_date == game_date)
                .count()
            )
            if played_count > 0:
                raise RuntimeError(
                    f"NBA API season stats not yet updated for {game_date}: "
                    f"0 GP increments detected but {played_count} player-game "
                    "records already exist in the DB. Data not ready yet — will retry."
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
