"""
Daily Player Stats Pipeline

Fetches yesterday's game stats from NBA API and ESPN ownership data,
then inserts into the nba schema tables.
"""

from core.season import season_for_date
from core.settings import settings
from db.models.nba import Player, PlayerGameStats
from db.models.nba.games import Game
from pipelines.base import BasePipeline
from pipelines.config import PipelineConfig, PipelineCategory
from pipelines.context import PipelineContext
from pipelines.extractors import ESPNExtractor, NBAApiExtractor
from pipelines.transformers import normalize_name, calculate_fantasy_points, minutes_to_int


class PlayerGameStatsPipeline(BasePipeline):
    """
    Fetch yesterday's game stats from NBA API and insert into player_game_stats.

    This pipeline:
    1. Fetches ESPN player data for ESPN IDs
    2. Fetches NBA game logs for yesterday
    3. Calculates fantasy points for each player
    4. Upserts player dimension records
    5. Inserts game stats into nba.player_game_stats
    """

    config = PipelineConfig(
        name="player_game_stats",
        display_name="Player Game Stats",
        description="Fetches yesterday's game stats from NBA API and ESPN ownership data",
        target_table="nba.player_game_stats",
        category=PipelineCategory.POST_GAME,
    )

    def __init__(self):
        super().__init__()
        self.espn_extractor = ESPNExtractor()
        self.nba_extractor = NBAApiExtractor()

    def execute(self, ctx: PipelineContext) -> None:
        """Execute the daily player stats pipeline."""
        import pandas as pd
        # The batch's game date: an explicit backfill date, else the date the
        # trigger endpoint gated on, else the 6 AM ET rule. See ctx.game_date().
        game_date = ctx.game_date()
        date_str = game_date.strftime("%m/%d/%Y")

        season = season_for_date(game_date)

        ctx.log.info("fetching_data", date=date_str, season=season)

        # Fetch ESPN player data for roster percentages
        espn_data = self.espn_extractor.get_player_data()
        ctx.log.info("espn_data_fetched", player_count=len(espn_data))

        # Fetch NBA game logs
        stats = self.nba_extractor.get_game_logs(date_str, season)

        if stats.empty:
            # Data readiness check: if games exist in the DB for this date but the
            # NBA API returned nothing, the player game log API hasn't updated yet.
            expected_games = Game.get_games_on_date(game_date)
            if expected_games:
                raise RuntimeError(
                    f"NBA API returned no stats for {date_str} but "
                    f"{len(expected_games)} game(s) were expected. "
                    "Data not ready yet — will retry."
                )
            ctx.log.info("no_games_found", date=date_str)
            return

        ctx.log.info("nba_data_fetched", record_count=len(stats))

        # Data completeness check: compare unique games in API response vs nba.games table.
        # The NBA Stats API (PlayerGameLogs) may lag behind the live scoreboard,
        # so late West Coast games can be missing from the response even after
        # the scoreboard shows them as Final. If we detect missing games, raise
        # an error so the pipeline is marked as failed and can be retried.
        if "GAME_ID" in stats.columns:
            api_game_ids = set(stats["GAME_ID"].unique())
            expected_games = Game.get_games_on_date(game_date)
            expected_game_ids = {g.game_id for g in expected_games}
            missing_games = expected_game_ids - api_game_ids
            if missing_games:
                ctx.log.warning(
                    "incomplete_game_data",
                    date=date_str,
                    expected_count=len(expected_game_ids),
                    received_count=len(api_game_ids),
                    missing_game_ids=list(missing_games),
                )
                raise RuntimeError(
                    f"NBA API returned stats for {len(api_game_ids)} of "
                    f"{len(expected_game_ids)} games on {date_str}. "
                    f"Missing: {missing_games}. Data not ready yet — will retry."
                )

        # Process each player
        for _, row in stats.iterrows():
            minutes_value = row["MIN"]
            if pd.isna(minutes_value) or minutes_value == "" or minutes_value is None:
                continue

            minutes_int = minutes_to_int(minutes_value)
            if minutes_int == 0:
                continue

            player_id = int(row["PLAYER_ID"])
            player_name = row["PLAYER_NAME"]
            normalized_name = normalize_name(player_name)
            team_abbrev = row["TEAM_ABBREVIATION"]
            game_id = row["GAME_ID"] if "GAME_ID" in stats.columns else None

            # Get ESPN data if available
            espn_info = espn_data.get(normalized_name)
            espn_id = espn_info["espn_id"] if espn_info else None

            # Calculate stats
            player_stats = {
                "pts": int(row["PTS"]),
                "reb": int(row["REB"]),
                "ast": int(row["AST"]),
                "stl": int(row["STL"]),
                "blk": int(row["BLK"]),
                "tov": int(row["TOV"]),
                "fgm": int(row["FGM"]),
                "fga": int(row["FGA"]),
                "fg3m": int(row["FG3M"]),
                "fg3a": int(row["FG3A"]),
                "ftm": int(row["FTM"]),
                "fta": int(row["FTA"]),
            }
            fpts = calculate_fantasy_points(player_stats)

            # Upsert player dimension record
            Player.upsert_player(
                player_id=player_id,
                name=player_name,
                espn_id=espn_id,
            )

            # Insert game stats
            PlayerGameStats.upsert_game_stats(
                player_id=player_id,
                game_date=game_date,
                stats={
                    "fpts": fpts,
                    "min": minutes_int,
                    **player_stats,
                },
                team_id=team_abbrev,
                pipeline_run_id=ctx.run_id,
                # The payload has carried this all along; storing it is what
                # lets readers stop inferring the fixture from (date, team).
                # An id nba.games has not caught up with is reconciled away by
                # the upsert rather than failing the row.
                game_id=game_id,
            )

            ctx.increment_records()
