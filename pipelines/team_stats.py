"""
Team Stats Pipeline

Fetches season-to-date statistics for all 30 NBA teams from NBA API,
combining base per-game counting stats and advanced efficiency metrics.

Makes two API calls per run (Base + Advanced measure types) and merges
them by team abbreviation before upserting to nba.team_stats.
"""

from collections import Counter
from datetime import date
from typing import Optional

from core.season import season_for_date
from db.models.nba.games import Game
from db.models.nba.player_game_stats import PlayerGameStats
from db.models.nba.team_stats import TeamStats
from pipelines.base import BasePipeline
from pipelines.config import PipelineConfig, PipelineCategory
from pipelines.context import PipelineContext
from pipelines.extractors import NBAApiExtractor
from pipelines.readiness import REGULAR_SEASON_PREFIX, regular_season_game_rows


class TeamStatsPipeline(BasePipeline):
    """
    Fetch and store season-to-date stats for all 30 NBA teams.

    This pipeline:
    1. Fetches LeagueDashTeamStats with MeasureType="Advanced" (ratings, pace)
    2. Fetches LeagueDashTeamStats with MeasureType="Base" (per-game counting stats)
    3. Merges both result sets by TEAM_ABBREVIATION
    4. Checks every team that played that night shows the game in its GP
    5. Upserts one record per team to nba.team_stats

    Key metrics collected:
    - Efficiency: OFF_RATING, DEF_RATING, NET_RATING
    - Pace: PACE (possessions per 48 min)
    - Shooting: TS_PCT, EFG_PCT, FG_PCT, FG3_PCT, FT_PCT
    - Per-game: PTS, REB, AST, STL, BLK, TOV
    - Record: W, L, W_PCT
    """

    config = PipelineConfig(
        name="team_stats",
        display_name="Team Stats",
        description="Daily team pace, ratings, and per-game stats for all 30 NBA teams",
        target_table="nba.team_stats",
        category=PipelineCategory.POST_GAME,
        trigger_slug="team-stats",
        # player_game_stats: the night's game log is what says who played.
        # game_schedule: marks earlier nights' games final, which is what the
        # readiness check counts a team's record from.
        depends_on=("player_game_stats", "game_schedule"),
    )

    def __init__(self):
        super().__init__()
        self.nba_extractor = NBAApiExtractor()

    def _teams_played(self, game_date: date, season: str) -> set[str]:
        """Teams with a regular-season game that night.

        From the night's game log as well as the schedule's finals. The game
        log is the one source the batch has already held to every game of the
        night; the schedule shows a game final only once the league game log
        has it, and nothing holds that feed to the night's games.
        """
        finals = Game.select(Game.home_team_id, Game.away_team_id).where(
            (Game.game_date == game_date)
            & (Game.season == season)
            & (Game.status == "final")
            & Game.game_id.startswith(REGULAR_SEASON_PREFIX)
        )
        teams = {t for g in finals for t in (g.home_team_id, g.away_team_id)}

        logged = (
            PlayerGameStats.select(PlayerGameStats.team_id)
            .where(
                (PlayerGameStats.game_date == game_date)
                & PlayerGameStats.team_id.is_null(False)
                & regular_season_game_rows(game_date)
            )
            .distinct()
        )
        return teams | {row.team_id for row in logged}

    def _final_games_before(self, game_date: date, season: str) -> Counter:
        """Each team's final regular-season games this season before that night."""
        finals = Game.select(Game.home_team_id, Game.away_team_id).where(
            (Game.season == season)
            & (Game.status == "final")
            & Game.game_id.startswith(REGULAR_SEASON_PREFIX)
            & (Game.game_date < game_date)
        )
        counts: Counter = Counter()
        for game in finals:
            counts[game.home_team_id] += 1
            counts[game.away_team_id] += 1
        return counts

    def _teams_behind(
        self, ctx: PipelineContext, game_date: date, season: str, api_gp: dict[str, Optional[int]]
    ) -> dict[str, dict]:
        """Teams that played that night whose GP in the API does not include it.

        A team that played should show its final games before the night plus
        the one it just played. Fewer and the dashboard has not caught up. More
        is the schedule missing a game, which is not a reason to hold the
        stats back, so it is logged and let through.
        """
        teams = self._teams_played(game_date, season)
        if not teams:
            return {}

        before = self._final_games_before(game_date, season)
        behind: dict[str, dict] = {}
        for team in sorted(teams):
            expected = before[team] + 1
            gp = api_gp.get(team)
            if gp is None or gp < expected:
                behind[team] = {"api_gp": gp, "expected_gp": expected}
            elif gp > expected:
                ctx.log.warning(
                    "team_gp_ahead_of_schedule",
                    team=team,
                    api_gp=gp,
                    expected_gp=expected,
                )
        return behind

    def execute(self, ctx: PipelineContext) -> None:
        """Execute the team stats pipeline."""

        as_of_date = ctx.game_date()

        season = season_for_date(as_of_date)

        ctx.log.info("fetching_team_stats", season=season, as_of_date=str(as_of_date))

        # Fetch merged advanced + base stats (two API calls internally)
        api_data = self.nba_extractor.get_team_stats(season)

        # Data readiness check, before anything is written. The dashboard is a
        # season total and trails the night's last games; a run that takes it
        # as it stands leaves those teams a game behind until tomorrow's batch.
        # Not on a backfill: the API has no as-of date.
        if not ctx.date_override:
            api_gp = {
                team["TEAM_ABBREVIATION"]: team.get("GP")
                for team in api_data or []
                if team.get("TEAM_ABBREVIATION")
            }
            behind = self._teams_behind(ctx, as_of_date, season, api_gp)
            if behind:
                ctx.log.warning(
                    "team_stats_behind_schedule",
                    date=str(as_of_date),
                    behind_count=len(behind),
                    behind=behind,
                )
                raise RuntimeError(
                    f"NBA API team stats not yet updated for {as_of_date}: "
                    f"{len(behind)} team(s) that played are missing a game "
                    f"({', '.join(behind)}). Data not ready yet — will retry."
                )

        if not api_data:
            ctx.log.info("no_data_returned")
            return

        ctx.log.info("data_fetched", team_count=len(api_data))

        for team in api_data:
            abbr = team.get("TEAM_ABBREVIATION")
            if not abbr:
                ctx.log.warning("missing_team_abbreviation", team=team.get("TEAM_NAME"))
                continue

            stats = {
                "gp": team.get("GP"),
                "w": team.get("W"),
                "l": team.get("L"),
                "w_pct": team.get("W_PCT"),
                # Per-game counting stats (from Base measure)
                "pts": team.get("PTS"),
                "reb": team.get("REB"),
                "ast": team.get("AST"),
                "stl": team.get("STL"),
                "blk": team.get("BLK"),
                "tov": team.get("TOV"),
                "fg_pct": team.get("FG_PCT"),
                "fg3_pct": team.get("FG3_PCT"),
                "ft_pct": team.get("FT_PCT"),
                # Advanced efficiency metrics (from Advanced measure)
                "off_rating": team.get("OFF_RATING"),
                "def_rating": team.get("DEF_RATING"),
                "net_rating": team.get("NET_RATING"),
                "pace": team.get("PACE"),
                "ts_pct": team.get("TS_PCT"),
                "efg_pct": team.get("EFG_PCT"),
                "ast_pct": team.get("AST_PCT"),
                "oreb_pct": team.get("OREB_PCT"),
                "dreb_pct": team.get("DREB_PCT"),
                "reb_pct": team.get("REB_PCT"),
                "tov_pct": team.get("TM_TOV_PCT"),
                "pie": team.get("PIE"),
            }

            TeamStats.upsert_team_stats(
                team_id=abbr,
                as_of_date=as_of_date,
                season=season,
                stats=stats,
                pipeline_run_id=ctx.run_id,
            )
            ctx.increment_records()

        ctx.log.info("processing_complete", records=ctx.records_processed)
