"""
Game Start Times Pipeline

Loads the NBA league schedule feed (scheduleLeagueV2_1.json) and upserts one
nba.games row per regular-season game with start_time_et populated. This is
the only writer of *future* games into nba.games: the pre-game, post-game and
live gates read start_time_et from that table and return early when a date has
no rows, so it runs weekly (cron-runner job "schedule-sync") and once before
opening night.

Source selection (ctx.options["source"]):
- "cdn"    (default) fetch the live feed from cdn.nba.com; on any failure fall
           back to the static file with a warning
- "static" read static/schedule_raw{YYYY}-{YYYY+1}.json for settings.nba_season

Preseason games (gameId prefix "001") are skipped unless
ctx.options["include_preseason"] is true, which is only honoured when
settings.development_mode is on — preseason rows must never reach production
tables. Cup-knockout placeholders (empty tricodes) and games against non-NBA
opponents are always skipped. The season written is the feed's seasonYear.
"""

from datetime import datetime

from core.settings import settings
from db.models.nba import Game, NBATeam
from pipelines.base import BasePipeline
from pipelines.config import PipelineConfig, PipelineCategory
from pipelines.context import PipelineContext
from utils import nba_cdn

SOURCES = ("cdn", "static")
PRESEASON_PREFIX = "001"

# Map gameStatus integers to our status strings
GAME_STATUS_MAP = {
    1: "scheduled",
    2: "in_progress",
    3: "final",
}


class GameStartTimesPipeline(BasePipeline):
    """
    Parse the NBA schedule feed and upsert Game records
    with start_time_et populated for all games.

    For completed games (status 3), scores are also included.
    """

    config = PipelineConfig(
        name="game_start_times",
        display_name="Game Start Times",
        description="Populates game start times from the NBA schedule feed (cdn.nba.com, static fallback)",
        target_table="nba.games",
        category=PipelineCategory.SCHEDULED,
    )

    def execute(self, ctx: PipelineContext) -> None:
        """Load the schedule feed and upsert games with start times."""
        source = ctx.options.get("source", "cdn")
        if source not in SOURCES:
            raise ValueError(f"Unknown schedule source {source!r}; expected one of {SOURCES}")

        include_preseason = bool(ctx.options.get("include_preseason", False))
        if include_preseason and not settings.development_mode:
            raise RuntimeError(
                "include_preseason is only allowed with DEVELOPMENT_MODE=true — "
                "preseason rows must never reach production tables"
            )

        schedule_data = self._load_feed(ctx, source)
        league = schedule_data["leagueSchedule"]
        game_dates = league["gameDates"]
        season = league["seasonYear"]

        # Cache valid NBA team IDs to skip global/international games
        valid_team_ids = {t.id for t in NBATeam.select(NBATeam.id)}

        ctx.log.info(
            "schedule_loaded",
            source=source,
            game_dates_count=len(game_dates),
            season=season,
            valid_teams=len(valid_team_ids),
            include_preseason=include_preseason,
        )

        skipped = {"preseason": 0, "placeholder": 0, "non_nba": 0, "no_datetime": 0}

        for game_date_entry in game_dates:
            for game in game_date_entry["games"]:
                game_id = game["gameId"]

                # Preseason games (game IDs starting with "001")
                if game_id.startswith(PRESEASON_PREFIX) and not include_preseason:
                    skipped["preseason"] += 1
                    continue

                game_status = game.get("gameStatus", 1)
                status = GAME_STATUS_MAP.get(game_status, "scheduled")

                # Parse gameDateTimeEst - this IS already ET despite the "Z" suffix
                dt_str = game.get("gameDateTimeEst", "")
                if not dt_str:
                    skipped["no_datetime"] += 1
                    continue

                try:
                    # Format: "2025-10-22T19:30:00Z"
                    dt = datetime.strptime(dt_str.rstrip("Z"), "%Y-%m-%dT%H:%M:%S")
                except ValueError:
                    ctx.log.warning("invalid_datetime", game_id=game_id, dt_str=dt_str)
                    skipped["no_datetime"] += 1
                    ctx.increment_failed(1, "invalid_datetime")
                    continue

                game_date = dt.date()
                start_time = dt.time()

                home_tricode = game.get("homeTeam", {}).get("teamTricode")
                away_tricode = game.get("awayTeam", {}).get("teamTricode")

                # Cup-knockout placeholders have no teams yet
                if not home_tricode or not away_tricode:
                    skipped["placeholder"] += 1
                    continue

                # Skip international/global games with non-NBA teams
                if home_tricode not in valid_team_ids or away_tricode not in valid_team_ids:
                    skipped["non_nba"] += 1
                    continue

                game_data = {
                    "game_date": game_date,
                    "season": season,
                    "home_team_id": home_tricode,
                    "away_team_id": away_tricode,
                    "start_time_et": start_time,
                    "status": status,
                }

                # Include scores for completed games
                if game_status == 3:
                    home_score = game.get("homeTeam", {}).get("score")
                    away_score = game.get("awayTeam", {}).get("score")
                    if home_score is not None:
                        game_data["home_score"] = home_score
                    if away_score is not None:
                        game_data["away_score"] = away_score

                Game.upsert_game(game_id, game_data)
                ctx.increment_records()

        for reason, count in skipped.items():
            if count:
                ctx.increment_skipped(count, reason)
        ctx.log.info("processing_complete", records=ctx.records_processed, skipped=skipped)

    def _load_feed(self, ctx: PipelineContext, source: str) -> dict:
        """The schedule feed for settings.nba_season from the requested source."""
        season = settings.nba_season
        if source == "cdn":
            try:
                data = nba_cdn.fetch_league_schedule(season)
                ctx.log.info("schedule_source", source="cdn", url=nba_cdn.NBA_CDN_SCHEDULE_URL, season=season)
                return data
            except Exception as e:
                ctx.log.warning(
                    "cdn_schedule_unavailable",
                    error=f"{type(e).__name__}: {e}",
                    fallback=nba_cdn.static_schedule_path(season).name,
                )
                # The run completes on the image's static copy: still a success, but partial
                ctx.increment_failed(1, "cdn_schedule_unavailable")
        data = nba_cdn.load_static_schedule(season)
        ctx.log.info("schedule_source", source="static", path=str(nba_cdn.static_schedule_path(season)), season=season)
        return data
