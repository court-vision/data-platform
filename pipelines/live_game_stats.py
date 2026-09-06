"""
Live Game Stats Pipeline

Fetches in-progress box scores from NBA API live endpoints and writes
current player stats to nba.live_player_stats. Designed to run every
~60 seconds during active game windows.

Does no time-window gating itself — gating is handled by the trigger
endpoint (POST /v1/internal/pipelines/live-stats).
"""

from db.models.nba import Player, LivePlayerStats, LiveGameScoreSnapshot
from pipelines.base import BasePipeline
from pipelines.config import PipelineConfig, PipelineCategory
from pipelines.context import PipelineContext
from pipelines.extractors import NBAApiExtractor
from pipelines.transformers import calculate_fantasy_points, minutes_to_int


class LiveGameStatsPipeline(BasePipeline):
    """
    Fetch in-progress game box scores from NBA API live endpoints.

    This pipeline:
    1. Fetches the live scoreboard to get active game IDs for today
    2. For each game, fetches the live BoxScore
    3. Upserts stats for each active player into nba.live_player_stats

    Records are continuously overwritten on each poll — this table is a
    snapshot of current game state, not a historical record.
    """

    config = PipelineConfig(
        name="live_game_stats",
        display_name="Live Game Stats",
        description="Fetches in-progress game box scores from NBA API live endpoints",
        target_table="nba.live_player_stats",
        category=PipelineCategory.LIVE,
    )

    def __init__(self):
        super().__init__()
        self.nba_extractor = NBAApiExtractor()

    def execute(self, ctx: PipelineContext) -> None:
        """Execute the live game stats pipeline."""
        # The date the trigger endpoint gated on, so the rows this writes carry
        # the same date the API asks for when it reads them back.
        game_date = ctx.game_date()

        ctx.log.info("live_stats_start", game_date=str(game_date))

        # Clean up stale records from previous game days.
        # Without this, old final-game records persist and get picked up by
        # the live matchup endpoint, causing double-counting against the
        # already-settled daily_matchup_scores baseline.
        deleted = (
            LivePlayerStats
            .delete()
            .where(LivePlayerStats.game_date < game_date)
            .execute()
        )
        if deleted:
            ctx.log.info("live_stats_cleanup", deleted_count=deleted, game_date=str(game_date))

        # Clean up score snapshots from previous game days.
        LiveGameScoreSnapshot.delete().where(
            LiveGameScoreSnapshot.game_date < game_date
        ).execute()

        # Get all games on the scoreboard for today
        scoreboard_games = self.nba_extractor.get_scoreboard_games(game_date)

        if not scoreboard_games:
            ctx.log.info("live_stats_no_games", game_date=str(game_date))
            return

        ctx.log.info(
            "live_stats_games_found",
            game_count=len(scoreboard_games),
            game_date=str(game_date),
        )

        # Process each game
        for game_meta in scoreboard_games:
            game_id = game_meta["game_id"]
            game_status = game_meta["game_status"]
            period = game_meta.get("period") or None
            game_clock = game_meta.get("game_clock") or None

            # Record a score snapshot for this game using scoreboard data.
            # This appends one row per game per poll, building a time series
            # for the score-over-time chart. No extra API call — scoreboard
            # data already has home/away scores, team abbreviations, and status.
            LiveGameScoreSnapshot.record_snapshot(
                game_id=game_id,
                game_date=game_date,
                game_meta=game_meta,
                pipeline_run_id=ctx.run_id,
            )

            # Fetch live box score for this game
            game_data = self.nba_extractor.get_live_box_score(game_id)
            if not game_data:
                ctx.log.warning("live_box_score_skip", game_id=game_id, game_status=game_status)
                if game_status == 1:  # not tipped off yet: nothing to fetch
                    ctx.increment_skipped(1, "not_started")
                else:
                    ctx.increment_failed(1, "live_box_score_unavailable")
                continue

            # Combine home and away players
            home_players = game_data.get("homeTeam", {}).get("players", [])
            away_players = game_data.get("awayTeam", {}).get("players", [])
            all_players = home_players + away_players

            for player_data in all_players:
                # Only process players who are active (not DNP)
                status = player_data.get("status", "")
                if status != "ACTIVE":
                    continue

                stats_raw = player_data.get("statistics", {})
                minutes_str = stats_raw.get("minutesCalculated", "PT00M00.00S")
                min_int = minutes_to_int(minutes_str)

                if min_int == 0:
                    continue

                player_id = player_data.get("personId")
                if not player_id:
                    continue

                player_id = int(player_id)
                player_name = (
                    f"{player_data.get('firstName', '')} {player_data.get('familyName', '')}".strip()
                )

                # Build stats dict matching calculate_fantasy_points signature
                player_stats = {
                    "pts": int(stats_raw.get("points", 0)),
                    "reb": int(stats_raw.get("reboundsTotal", 0)),
                    "ast": int(stats_raw.get("assists", 0)),
                    "stl": int(stats_raw.get("steals", 0)),
                    "blk": int(stats_raw.get("blocks", 0)),
                    "tov": int(stats_raw.get("turnovers", 0)),
                    "fgm": int(stats_raw.get("fieldGoalsMade", 0)),
                    "fga": int(stats_raw.get("fieldGoalsAttempted", 0)),
                    "fg3m": int(stats_raw.get("threePointersMade", 0)),
                    "fg3a": int(stats_raw.get("threePointersAttempted", 0)),
                    "ftm": int(stats_raw.get("freeThrowsMade", 0)),
                    "fta": int(stats_raw.get("freeThrowsAttempted", 0)),
                }
                fpts = calculate_fantasy_points(player_stats)

                # Upsert Player dimension record if needed
                Player.upsert_player(
                    player_id=player_id,
                    name=player_name,
                    espn_id=None,  # Not available from live BoxScore
                )

                # Upsert live stats record
                LivePlayerStats.upsert_live_stats(
                    player_id=player_id,
                    game_id=game_id,
                    game_date=game_date,
                    stats={
                        "period": period,
                        "game_clock": game_clock,
                        "game_status": game_status,
                        "fpts": fpts,
                        "min": min_int,
                        **player_stats,
                    },
                    pipeline_run_id=ctx.run_id,
                )

                ctx.increment_records()

        ctx.log.info(
            "live_stats_complete",
            game_date=str(game_date),
            records_processed=ctx.records_processed,
        )
