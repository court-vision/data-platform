"""
Pipeline API Routes

Endpoints for triggering data pipelines. Uses token-based authentication
(not Clerk) to allow cron jobs and scheduled tasks to trigger pipelines.

The /all endpoint uses a fire-and-forget pattern:
- Returns immediately with a job ID
- Pipelines run in the background
- Use /jobs/{job_id} to check status
"""

import asyncio
from datetime import date, datetime, timedelta, timezone
from typing import Literal, Optional

import httpx
from fastapi import APIRouter, Security, HTTPException, Query
from fastapi.responses import JSONResponse

from core.job_manager import (
    get_job_manager,
    PipelineJobResult as JobResultInternal,
)
from core.logging import get_logger
from core.pipeline_auth import verify_pipeline_token
from db.base import run_in_db_thread
from pipelines import (
    run_pipeline,
    run_all_pipelines,
    list_pipelines,
    PIPELINE_REGISTRY,
    get_pipelines_by_category,
)
from pipelines.config import PipelineCategory
from schemas.pipeline import (
    PipelineResponse,
    PipelineResult,
    AllPipelinesResponse,
    JobCreatedResponse,
    JobStatusResponse,
    JobListResponse,
    PipelineJobInfo,
    PipelineJobDetail,
    PipelineJobResult,
    LiveStatsResponse,
    LiveStatsData,
)
from schemas.common import ApiStatus
from services.alert_service import AlertEvent, get_alert_service

router = APIRouter(prefix="/pipelines", tags=["pipelines"])
log = get_logger("pipeline_api")

DEPLOY_DISPATCH_FAILED_CODE = "DEPLOY_DISPATCH_FAILED"
DEPLOY_ALERT_DEDUPE = timedelta(hours=12)


@router.get("/")
async def get_available_pipelines(
    _: str = Security(verify_pipeline_token),
) -> dict:
    """
    List all available pipelines.

    Returns pipeline names, descriptions, and target tables.
    """
    return {"pipelines": list_pipelines()}


@router.post("/daily-player-stats", response_model=PipelineResponse)
async def trigger_daily_player_stats(
    _: str = Security(verify_pipeline_token),
    date: Optional[date] = Query(None, description="Override game date (YYYY-MM-DD). Omit for automatic date."),
) -> PipelineResponse:
    """
    Trigger the daily player stats pipeline.

    Fetches game stats from NBA API and ESPN ownership data,
    then inserts into nba.player_game_stats table.
    Pass ?date=YYYY-MM-DD to backfill a specific date.
    """
    result = await run_pipeline("player_game_stats", date_override=date)
    return PipelineResponse(
        status=result.status,
        message=result.message,
        data=result,
    )


@router.post("/cumulative-player-stats", response_model=PipelineResponse)
async def trigger_cumulative_player_stats(
    _: str = Security(verify_pipeline_token),
    date: Optional[date] = Query(None, description="Override game date (YYYY-MM-DD). Omit for automatic date."),
) -> PipelineResponse:
    """
    Trigger the cumulative player stats pipeline.

    Updates season totals and rankings for players who played on the given date.
    Pass ?date=YYYY-MM-DD to backfill a specific date.
    """
    result = await run_pipeline("player_season_stats", date_override=date)
    return PipelineResponse(
        status=result.status,
        message=result.message,
        data=result,
    )


@router.post("/daily-matchup-scores", response_model=PipelineResponse)
async def trigger_daily_matchup_scores(
    _: str = Security(verify_pipeline_token),
    date: Optional[date] = Query(None, description="Override game date (YYYY-MM-DD). Omit for automatic date."),
    watch: bool = Query(False, description="Loop mode: skip pipeline until ESPN scoring period advances."),
) -> PipelineResponse:
    """
    Trigger the daily matchup scores pipeline.

    Fetches current matchup scores for all saved teams and records
    daily snapshots for visualization.
    Pass ?date=YYYY-MM-DD to backfill a specific date.

    Pass ?watch=true to enable flip-detection mode (for cron-runner loop job).
    In this mode the endpoint checks whether ESPN's scoring period has advanced
    beyond the latest stored baseline. If not yet flipped, it returns immediately
    with done=false so the cron-runner keeps polling. When the flip is detected
    the pipeline runs once and returns done=true, signalling the loop to exit.
    """
    if watch and not date:
        flipped = await run_in_db_thread(_espn_scoring_period_advanced)
        if not flipped:
            log.info("espn_scoring_period_unchanged_skipping")
            from datetime import datetime
            now = datetime.utcnow().isoformat()
            return PipelineResponse(
                status=ApiStatus.OK,
                message="espn_scoring_period_unchanged",
                data=PipelineResult(
                    status=ApiStatus.OK,
                    message="espn_scoring_period_unchanged",
                    started_at=now,
                    completed_at=now,
                    duration_seconds=0,
                    records_processed=0,
                    done=False,
                ),
            )
        log.info("espn_scoring_period_advanced_running_pipeline")

    result = await run_pipeline("daily_matchup_scores", date_override=date)
    return PipelineResponse(
        status=result.status,
        message=result.message,
        data=result,
    )


def _espn_scoring_period_advanced() -> bool:
    """
    Check whether ESPN's current scoring period has advanced beyond the latest
    stored baseline. Called once per post-game poll iteration (one ESPN API call).

    Returns True if the pipeline should run (flip detected, no baseline, 2:30 AM CST fallback, or ESPN API error).
    Returns False if ESPN's period matches the baseline and it's before the 2:30 AM CST fallback.
    """
    import json
    import requests
    import pytz
    from datetime import datetime
    from db.models.teams import Team
    from db.models.stats.daily_matchup_score import DailyMatchupScore
    from core.settings import settings

    # Time fallback: if past 2:30 AM CST, run regardless of scoring period.
    # Handles cases where ESPN's batch runs late or the scoring period doesn't
    # advance as expected. CST (not ET) matches the rest of the pipeline's
    # timezone convention.
    central = pytz.timezone("US/Central")
    now_cst = datetime.now(central)
    if now_cst.hour > 2 or (now_cst.hour == 2 and now_cst.minute >= 30):
        log.info("espn_scoring_period_time_fallback", time_cst=now_cst.strftime("%H:%M"))
        return True

    ESPN_ENDPOINT = (
        "https://lm-api-reads.fantasy.espn.com/apis/v3/games/fba/seasons/{}/segments/0/leagues/{}"
    )

    # Find the first ESPN team to peek at the scoring period
    teams = list(Team.select().limit(10))
    espn_team = None
    espn_league_info = None
    for team in teams:
        try:
            league_info = json.loads(team.league_info)
            if league_info.get("provider", "espn") == "espn":
                espn_team = team
                espn_league_info = league_info
                break
        except Exception:
            continue

    if not espn_team or not espn_league_info:
        # No ESPN teams registered — always run
        return True

    # Peek at ESPN's current latestScoringPeriod
    try:
        year = espn_league_info.get("year", settings.espn_year)
        league_id = espn_league_info.get("league_id")
        espn_s2 = espn_league_info.get("espn_s2", "")
        swid = espn_league_info.get("swid", "")
        endpoint = ESPN_ENDPOINT.format(year, league_id)
        resp = requests.get(
            endpoint,
            params={"view": "mSettings"},
            cookies={"espn_s2": espn_s2, "SWID": swid},
            timeout=10,
        )
        resp.raise_for_status()
        espn_period = resp.json().get("status", {}).get("latestScoringPeriod")
    except Exception as e:
        log.warning("espn_peek_failed_running_pipeline", error=str(e))
        return True  # On error, run the pipeline anyway

    if espn_period is None:
        return True

    # Compare to the latest baseline's scoring period for this team
    latest = (
        DailyMatchupScore.select()
        .where(DailyMatchupScore.team_id == espn_team.team_id)
        .order_by(DailyMatchupScore.date.desc())
        .first()
    )

    if latest is None or latest.scoring_period_id is None:
        return True  # No baseline yet — run pipeline

    return espn_period != latest.scoring_period_id


@router.post("/player-advanced-stats", response_model=PipelineResponse)
async def trigger_player_advanced_stats(
    _: str = Security(verify_pipeline_token),
    date: Optional[date] = Query(None, description="Override game date (YYYY-MM-DD). Omit for automatic date."),
) -> PipelineResponse:
    """
    Trigger the player advanced stats pipeline.

    Pass ?date=YYYY-MM-DD to backfill a specific date.
    """
    result = await run_pipeline("player_advanced_stats", date_override=date)
    return PipelineResponse(
        status=result.status,
        message=result.message,
        data=result,
    )


@router.post("/player-ownership", response_model=PipelineResponse)
async def trigger_player_ownership(
    _: str = Security(verify_pipeline_token),
    date: Optional[date] = Query(None, description="Override game date (YYYY-MM-DD). Omit for automatic date."),
) -> PipelineResponse:
    """
    Trigger the player ownership pipeline.

    Fetches ESPN ownership percentages for all players and updates
    the nba.player_ownership table.
    Pass ?date=YYYY-MM-DD to backfill a specific date.
    """
    result = await run_pipeline("player_ownership", date_override=date)
    return PipelineResponse(
        status=result.status,
        message=result.message,
        data=result,
    )


@router.post("/player-rolling-stats", response_model=PipelineResponse)
async def trigger_player_rolling_stats(
    _: str = Security(verify_pipeline_token),
    date: Optional[date] = Query(None, description="Override game date (YYYY-MM-DD). Omit for automatic date."),
) -> PipelineResponse:
    """
    Trigger the player rolling stats pipeline.

    Materializes L7, L14, and L30 rolling per-game averages from
    player_game_stats into nba.player_rolling_stats.
    Depends on player_game_stats having fresh data for the target date.
    Pass ?date=YYYY-MM-DD to backfill a specific date.
    """
    result = await run_pipeline("player_rolling_stats", date_override=date)
    return PipelineResponse(
        status=result.status,
        message=result.message,
        data=result,
    )


@router.post("/team-stats", response_model=PipelineResponse)
async def trigger_team_stats(
    _: str = Security(verify_pipeline_token),
    date: Optional[date] = Query(None, description="Override game date (YYYY-MM-DD). Omit for automatic date."),
) -> PipelineResponse:
    """
    Trigger the team stats pipeline.

    Fetches season-to-date stats for all 30 NBA teams from NBA API
    (base counting stats + advanced efficiency metrics) and upserts
    to nba.team_stats.
    Pass ?date=YYYY-MM-DD to backfill a specific date.
    """
    result = await run_pipeline("team_stats", date_override=date)
    return PipelineResponse(
        status=result.status,
        message=result.message,
        data=result,
    )


@router.post("/game-schedule", response_model=PipelineResponse)
async def trigger_game_schedule(
    _: str = Security(verify_pipeline_token),
    date: Optional[date] = Query(None, description="Override game date (YYYY-MM-DD). Omit for automatic date."),
) -> PipelineResponse:
    """
    Trigger the game schedule pipeline.

    Fetches NBA game schedule and results and upserts to nba.games.
    Pass ?date=YYYY-MM-DD to backfill a specific date.
    """
    result = await run_pipeline("game_schedule", date_override=date)
    return PipelineResponse(
        status=result.status,
        message=result.message,
        data=result,
    )


@router.post("/game-start-times", response_model=PipelineResponse)
async def trigger_game_start_times(
    _: str = Security(verify_pipeline_token),
    date: Optional[date] = Query(None, description="Override game date (YYYY-MM-DD). Omit for automatic date."),
    source: Literal["cdn", "static"] = Query(
        "cdn",
        description="Schedule feed source: 'cdn' fetches cdn.nba.com (falls back to the static file), 'static' reads static/schedule_raw{YYYY}-{YYYY+1}.json.",
    ),
    include_preseason: bool = Query(
        False,
        description="Also upsert preseason games. Only honoured with DEVELOPMENT_MODE=true.",
    ),
) -> PipelineResponse:
    """
    Trigger the game start times pipeline.

    Loads the NBA schedule feed for settings.nba_season and upserts every
    regular-season game's tip-off time to nba.games. Used by the pre-game,
    post-game and live gates. Fired weekly by the cron-runner "schedule-sync"
    job with ?source=cdn.
    """
    result = await run_pipeline(
        "game_start_times",
        date_override=date,
        options={"source": source, "include_preseason": include_preseason},
    )
    return PipelineResponse(
        status=result.status,
        message=result.message,
        data=result,
    )


@router.post("/espn-injury-status", response_model=PipelineResponse)
async def trigger_espn_injury_status(
    _: str = Security(verify_pipeline_token),
    date: Optional[date] = Query(None, description="Override report date (YYYY-MM-DD). Omit for automatic date."),
) -> PipelineResponse:
    """
    Trigger the ESPN injury status pipeline.

    Fetches player injury/availability status from ESPN Fantasy API and upserts
    to nba.player_injuries. Free alternative to the BALLDONTLIE injury endpoint.
    Pass ?date=YYYY-MM-DD to backfill a specific date.
    """
    result = await run_pipeline("espn_injury_status", date_override=date)
    return PipelineResponse(
        status=result.status,
        message=result.message,
        data=result,
    )


@router.post("/breakout-detection", response_model=PipelineResponse)
async def trigger_breakout_detection(
    _: str = Security(verify_pipeline_token),
    date: Optional[date] = Query(None, description="Override detection date (YYYY-MM-DD). Omit for automatic date."),
) -> PipelineResponse:
    """
    Trigger the breakout streamer detection pipeline.

    Analyzes current injuries to prominent starters and identifies teammates
    most likely to absorb their minutes. Results written to nba.breakout_candidates.

    Depends on espn_injury_status and player_season_stats being fresh.
    Pass ?date=YYYY-MM-DD to run detection as of a specific date.
    """
    result = await run_pipeline("breakout_detection", date_override=date)
    return PipelineResponse(
        status=result.status,
        message=result.message,
        data=result,
    )


@router.post("/player-profiles", response_model=PipelineResponse)
async def trigger_player_profiles(
    _: str = Security(verify_pipeline_token),
    date: Optional[date] = Query(None, description="Override game date (YYYY-MM-DD). Omit for automatic date."),
) -> PipelineResponse:
    """
    Trigger the player profiles pipeline.

    Fetches biographical and position data for all active players and
    upserts to nba.players. Intended to run weekly (slow — fetches all
    active players from NBA API).
    Pass ?date=YYYY-MM-DD to backfill a specific date.
    """
    result = await run_pipeline("player_profiles", date_override=date)
    return PipelineResponse(
        status=result.status,
        message=result.message,
        data=result,
    )


@router.post("/pre-game", response_model=PipelineResponse)
async def trigger_pre_game(
    _: str = Security(verify_pipeline_token),
    force: bool = Query(False, description="Bypass window and dedup gates. Use for manual triggers or backfills."),
    date: Optional[date] = Query(None, description="Override game date (YYYY-MM-DD). Implies force=true."),
) -> PipelineResponse:
    """
    Pre-game pipeline trigger with per-pipeline self-gating.

    Called every 15 minutes by the cron-runner. For each PRE_GAME pipeline:
    1. Window gate: skips if current time is before (first_game - window_minutes)
    2. Dedup gate: skips if the pipeline already ran successfully today
    3. Concurrency gate: skips if the pipeline is already running

    Each pipeline has its own window override (pre_game_window_minutes); otherwise
    the global settings.pre_game_window_minutes default applies.

    Safe to call frequently — gates return immediately with no-ops.
    Pass ?force=true to skip all gates.
    Pass ?date=YYYY-MM-DD to target a specific date (implies force=true).
    """
    import pytz
    from datetime import datetime, timedelta

    from core.settings import settings
    from db.models.nba.games import Game
    from db.models.pipeline_run import PipelineRun

    force = force or (date is not None)

    eastern = pytz.timezone("US/Eastern")
    now_et = datetime.now(eastern)
    now_et_naive = now_et.replace(tzinfo=None)

    # ET-based NBA date (before 6am = still yesterday's game date)
    if now_et.hour < 6:
        nba_date = (now_et - timedelta(days=1)).date()
    else:
        nba_date = now_et.date()

    target_date = date or nba_date

    if not force:
        # Exit early if no games today — nothing to prep for
        games_today = Game.get_games_on_date(nba_date)
        if not games_today:
            log.info("pre_game_no_games", nba_date=str(nba_date))
            return PipelineResponse(
                status=ApiStatus.SUCCESS,
                message=f"No games scheduled for NBA date {nba_date}",
            )

    first_game_time = Game.get_earliest_game_time_on_date(nba_date) if not force else None

    pre_game_pipelines = get_pipelines_by_category(PipelineCategory.PRE_GAME)
    pipelines_to_run = []

    for cls in pre_game_pipelines:
        name = cls.config.name

        if not force:
            # Window gate: skip if too early for this pipeline
            if first_game_time:
                window_minutes = cls.config.pre_game_window_minutes or settings.pre_game_window_minutes
                first_game_dt = datetime.combine(nba_date, first_game_time)
                window_opens_at = first_game_dt - timedelta(minutes=window_minutes)
                if now_et_naive < window_opens_at:
                    log.info(
                        "pre_game_window_not_open",
                        pipeline=name,
                        window_minutes=window_minutes,
                        opens_at=str(window_opens_at),
                        current_time=str(now_et_naive),
                    )
                    continue

            # Dedup gate: skip if already ran successfully today.
            # Pipelines with skip_batch_dedup manage their own internal dedup
            # and must not be blocked by an early "success with 0 records" run.
            if not cls.config.skip_batch_dedup:
                if PipelineRun.was_successful_on_date(name, nba_date):
                    log.info("pre_game_already_ran", pipeline=name, nba_date=str(nba_date))
                    continue

            # Concurrency gate: skip if already running
            if PipelineRun.is_running(name):
                log.info("pre_game_already_running", pipeline=name)
                continue

        pipelines_to_run.append(name)

    if not pipelines_to_run:
        return PipelineResponse(
            status=ApiStatus.SUCCESS,
            message=f"All pre-game pipelines skipped (window not open or already ran) for {nba_date}",
        )

    job_manager = get_job_manager()
    job = await job_manager.create_job(len(pipelines_to_run))
    asyncio.create_task(
        _run_pipelines_background(
            job.job_id,
            date_override=target_date if date else None,
            pipeline_names=pipelines_to_run,
        )
    )

    log.info(
        "pre_game_triggered",
        nba_date=str(target_date),
        job_id=job.job_id,
        pipeline_count=len(pipelines_to_run),
        pipelines=pipelines_to_run,
        forced=force,
    )

    return PipelineResponse(
        status=ApiStatus.SUCCESS,
        message=f"Pre-game pipelines triggered for {target_date}. Job ID: {job.job_id}",
    )


@router.post("/post-game", response_model=PipelineResponse)
async def trigger_post_game(
    _: str = Security(verify_pipeline_token),
    force: bool = Query(False, description="Bypass all gates and dedup check. Use for backfills."),
    date: Optional[date] = Query(None, description="Override game date (YYYY-MM-DD). Implies force=true."),
) -> PipelineResponse:
    """
    Post-game pipeline trigger with per-pipeline self-gating.

    Called every 15 minutes by the cron-runner. Applies two batch-level gates
    (time window and data readiness) then per-pipeline dedup:
    1. Time window: only attempt within [estimated_last_game_end, estimated_last_game_end + window]
    2. Data readiness: all games on the NBA date are Final (live scoreboard check)
    3. Per-pipeline dedup: skip if the pipeline already ran successfully today
    4. Per-pipeline concurrency: skip if the pipeline is already running

    Per-pipeline dedup enables partial batch retries — if one pipeline fails, the
    next cron invocation will retry only the failed pipeline, not the whole batch.

    Pass ?force=true to skip all gates (useful for manual re-triggers or backfills).
    Pass ?date=YYYY-MM-DD to backfill a specific date (implies force=true).
    """
    import pytz
    from datetime import datetime, timedelta

    from core.settings import settings
    from db.models.nba.games import Game
    from db.models.pipeline_run import PipelineRun
    from pipelines.extractors.nba_api import NBAApiExtractor

    # A date override implies force — skip all time/readiness gating
    force = force or (date is not None)

    eastern = pytz.timezone("US/Eastern")
    now_et = datetime.now(eastern)

    # NBA date: before 6am ET means we're still on last night's game date
    if now_et.hour < 6:
        nba_date = (now_et - timedelta(days=1)).date()
    else:
        nba_date = now_et.date()

    if not force:
        # Check if there are any scheduled games on the NBA date
        latest_game_time = Game.get_latest_game_time_on_date(nba_date)
        if not latest_game_time:
            log.info("post_game_no_games", nba_date=str(nba_date))
            return PipelineResponse(
                status=ApiStatus.SUCCESS,
                message=f"No games scheduled for NBA date {nba_date}",
            )

        # Gate 1: Time window — only attempt within [estimated_end, estimated_end + window]
        latest_game_dt = datetime.combine(nba_date, latest_game_time)
        estimated_end_dt = latest_game_dt + timedelta(minutes=settings.estimated_game_duration_minutes)
        window_end_dt = estimated_end_dt + timedelta(minutes=settings.post_game_pipeline_window_minutes)
        now_et_naive = now_et.replace(tzinfo=None)

        if not (estimated_end_dt <= now_et_naive <= window_end_dt):
            log.info(
                "post_game_outside_window",
                nba_date=str(nba_date),
                estimated_end=str(estimated_end_dt),
                window_end=str(window_end_dt),
                current_time=str(now_et_naive),
            )
            return PipelineResponse(
                status=ApiStatus.SUCCESS,
                message="Outside post-game window",
            )

        # Convert estimated end to UTC for dedup cutoff (started_at is stored as UTC)
        estimated_end_utc = eastern.localize(estimated_end_dt).astimezone(pytz.utc).replace(tzinfo=None)

        # Gate 2: Data readiness — verify all games are actually Final via live scoreboard
        nba_extractor = NBAApiExtractor()
        try:
            all_final = nba_extractor.check_all_games_final(nba_date)
        except Exception as e:
            log.error("post_game_scoreboard_error", nba_date=str(nba_date), error=str(e))
            return PipelineResponse(
                status=ApiStatus.SUCCESS,
                message="Live scoreboard check failed, will retry",
            )

        if not all_final:
            log.info(
                "post_game_games_not_final",
                nba_date=str(nba_date),
            )
            return PipelineResponse(
                status=ApiStatus.SUCCESS,
                message="Games still in progress, will retry next interval",
            )

    # Per-pipeline dedup: determine which pipelines still need to run
    post_game_pipelines = get_pipelines_by_category(PipelineCategory.POST_GAME)
    pipelines_to_run = []
    time_gated_pipelines = []

    for cls in post_game_pipelines:
        name = cls.config.name

        if not force:
            if PipelineRun.is_running(name):
                log.info("post_game_pipeline_concurrency_skip", pipeline=name)
                continue

            # Time gate: skip until the pipeline's earliest_run_time_cst is reached.
            # Tracked separately so the response can distinguish "deferred" from "done".
            if cls.config.earliest_run_time_cst is not None:
                central = pytz.timezone("US/Central")
                now_cst = datetime.now(central)
                if now_cst.time() < cls.config.earliest_run_time_cst:
                    log.info(
                        "post_game_pipeline_time_gated",
                        pipeline=name,
                        earliest_cst=str(cls.config.earliest_run_time_cst),
                        current_cst=now_cst.strftime("%H:%M"),
                    )
                    time_gated_pipelines.append(name)
                    continue

            if cls.config.espn_gated:
                # For ESPN-gated pipelines the scoring-period check acts as the
                # dedup. We intentionally skip was_successful_on_date here: the
                # time fallback may have fired before ESPN's nightly batch ran,
                # writing stale records. Once ESPN's latestScoringPeriod advances,
                # _espn_scoring_period_advanced() flips to True and we re-run.
                # After a correct run (stored period == ESPN period) it returns
                # False, preventing further re-runs — no separate dedup needed.
                if not _espn_scoring_period_advanced():
                    log.info("post_game_espn_gate_not_ready", pipeline=name)
                    continue
            else:
                if PipelineRun.was_successful_on_date(name, nba_date, after=estimated_end_utc):
                    log.info("post_game_pipeline_dedup_skip", pipeline=name, nba_date=str(nba_date))
                    continue

        pipelines_to_run.append(name)

    if not pipelines_to_run:
        if time_gated_pipelines:
            log.info(
                "post_game_pipelines_time_gated",
                nba_date=str(nba_date),
                time_gated=time_gated_pipelines,
            )
            return PipelineResponse(
                status=ApiStatus.SUCCESS,
                message=f"Post-game pipelines deferred for {nba_date}: {time_gated_pipelines} waiting for earliest_run_time_cst, will retry",
            )
        log.info("post_game_all_complete", nba_date=str(nba_date))
        return PipelineResponse(
            status=ApiStatus.SUCCESS,
            message=f"All post-game pipelines already completed for {nba_date}",
        )

    target_date = date or nba_date
    job_manager = get_job_manager()
    job = await job_manager.create_job(len(pipelines_to_run))
    asyncio.create_task(
        _run_pipelines_background(
            job.job_id,
            date_override=target_date if date else None,
            pipeline_names=pipelines_to_run,
        )
    )

    log.info(
        "post_game_triggered",
        nba_date=str(target_date),
        job_id=job.job_id,
        pipeline_count=len(pipelines_to_run),
        pipelines=pipelines_to_run,
        forced=force,
    )

    return PipelineResponse(
        status=ApiStatus.SUCCESS,
        message=f"Post-game pipelines triggered for {target_date}. Job ID: {job.job_id}",
    )


@router.post("/lineup-alerts", response_model=PipelineResponse)
async def trigger_lineup_alerts(
    _: str = Security(verify_pipeline_token),
) -> PipelineResponse:
    """
    Trigger the lineup alerts pipeline.

    Checks all eligible users' lineups and sends notifications if issues
    are found. Self-gates based on game start times - if no games are
    within the notification window, returns immediately.

    Safe to call frequently (every 15 min); deduplication prevents
    repeat notifications.
    """
    from pipelines.lineup_alerts import LineupAlertsPipeline

    pipeline = LineupAlertsPipeline()
    result = await pipeline.run()
    return PipelineResponse(
        status=result.status,
        message=result.message,
        data=result,
    )


@router.post("/live-stats", response_model=LiveStatsResponse)
async def trigger_live_stats(
    _: str = Security(verify_pipeline_token),
) -> LiveStatsResponse:
    """
    Trigger the live game stats pipeline.

    Called every ~60 seconds by the cron-runner's live loop. Self-gates
    against the game schedule — returns immediately (no-op) if no games
    are scheduled today or if we're more than 15 minutes before tip-off.

    The all_games_complete field in the response signals the cron-runner
    loop to exit once all games for the day are final.

    Safe to call frequently — runs in milliseconds when outside game window.
    """
    import time
    from datetime import datetime, timedelta

    import pytz

    from db.models.nba.games import Game
    from db.models.nba.live_player_stats import LivePlayerStats
    from pipelines.extractors.nba_api import NBAApiExtractor
    from pipelines.live_game_stats import LiveGameStatsPipeline

    start_time = time.monotonic()
    eastern = pytz.timezone("US/Eastern")
    now_et = datetime.now(eastern)

    # ET-based NBA date (before 6am = still yesterday's game date)
    if now_et.hour < 6:
        game_date = (now_et - timedelta(days=1)).date()
    else:
        game_date = now_et.date()

    # Check if there are any games today (DB path, covers regular season + known playoff dates)
    games_today = Game.get_games_on_date(game_date)
    scoreboard_fallback = False

    if not games_today:
        # DB has no games for this date — may be a playoff round whose dates weren't
        # in the static schedule file (rounds 2+ are unscheduled until bracket finalizes).
        # Fall back to the live NBA scoreboard to check for actual games.
        fallback_extractor = NBAApiExtractor()
        try:
            scoreboard_games = fallback_extractor.get_scoreboard_games(game_date)
        except Exception as e:
            log.warning("live_stats_scoreboard_fallback_failed", error=str(e), game_date=str(game_date))
            scoreboard_games = []

        if not scoreboard_games:
            log.info("live_stats_no_games", game_date=str(game_date))
            return LiveStatsResponse(
                status=ApiStatus.SUCCESS,
                message=f"No games scheduled for {game_date}",
                data=LiveStatsData(
                    pipeline_name="live_game_stats",
                    status="skipped",
                    games_total=0,
                    all_games_complete=True,
                    done=True,
                    duration_seconds=round(time.monotonic() - start_time, 3),
                ),
            )

        # Scoreboard has games not in DB — use status to gate pre-tip-off.
        # game_status: 1=scheduled, 2=in-progress, 3=final
        games_count = len(scoreboard_games)
        if all(g["game_status"] == 1 for g in scoreboard_games):
            log.info("live_stats_pregame_scoreboard_fallback", game_date=str(game_date), games=games_count)
            return LiveStatsResponse(
                status=ApiStatus.SUCCESS,
                message=f"Games haven't started yet (scoreboard fallback).",
                data=LiveStatsData(
                    pipeline_name="live_game_stats",
                    status="skipped",
                    games_total=games_count,
                    all_games_complete=False,
                    done=False,
                    duration_seconds=round(time.monotonic() - start_time, 3),
                ),
            )

        scoreboard_fallback = True
        log.info("live_stats_scoreboard_fallback", game_date=str(game_date), games=games_count)
    else:
        games_count = len(games_today)

        # Pre-tip-off gate: don't run until 15 min before first game
        earliest_start = Game.get_earliest_game_time_on_date(game_date)
        if earliest_start:
            now_et_naive = now_et.replace(tzinfo=None)
            earliest_dt = datetime.combine(game_date, earliest_start)
            gate_dt = earliest_dt - timedelta(minutes=15)

            if now_et_naive < gate_dt:
                log.info(
                    "live_stats_pregame",
                    game_date=str(game_date),
                    earliest_start=str(earliest_start),
                    gate_time=str(gate_dt),
                    current_time=str(now_et_naive),
                )
                return LiveStatsResponse(
                    status=ApiStatus.SUCCESS,
                    message=f"Games haven't started yet. First tip-off at {earliest_start} ET.",
                    data=LiveStatsData(
                        pipeline_name="live_game_stats",
                        status="skipped",
                        games_total=games_count,
                        all_games_complete=False,
                        done=False,
                        duration_seconds=round(time.monotonic() - start_time, 3),
                    ),
                )

    # Run the pipeline
    pipeline = LiveGameStatsPipeline()
    result = await pipeline.run()

    # Check if all games are final so the cron-runner knows when to exit
    nba_extractor = NBAApiExtractor()
    try:
        all_complete = nba_extractor.check_all_games_final(game_date)
    except Exception as e:
        log.warning("live_stats_final_check_failed", error=str(e))
        all_complete = False

    # Time-based finalize: force any game_status=2 records with stale last_updated.
    # Runs on EVERY trigger as a safety net, not gated on check_all_games_final.
    time_finalized = LivePlayerStats.finalize_stale_by_time(game_date, stale_minutes=30)
    if time_finalized:
        log.info("live_stats_time_finalized", game_date=str(game_date), records=time_finalized)

    # Full finalize when scoreboard confirms all games are done
    if all_complete:
        finalized = LivePlayerStats.finalize_stale_games(game_date)
        if finalized:
            log.info("live_stats_finalized_stale", game_date=str(game_date), records=finalized)

    log.info(
        "live_stats_triggered",
        game_date=str(game_date),
        records_processed=result.records_processed or 0,
        all_games_complete=all_complete,
    )

    return LiveStatsResponse(
        status=result.status,
        message=result.message,
        data=LiveStatsData(
            pipeline_name="live_game_stats",
            status=result.status,
            records_processed=result.records_processed or 0,
            games_total=games_count,
            all_games_complete=all_complete,
            done=all_complete,
            duration_seconds=result.duration_seconds,
        ),
    )


@router.post("/all", response_model=JobCreatedResponse)
async def trigger_all_pipelines(
    _: str = Security(verify_pipeline_token),
    date: Optional[date] = Query(None, description="Override game date (YYYY-MM-DD) for all pipelines. Omit for automatic date."),
) -> JobCreatedResponse:
    """
    Trigger all non-SCHEDULED pipelines in the background (fire-and-forget).

    Returns immediately with a job ID. Use GET /jobs/{job_id} to check status.
    Intended for backfills and manual use — production runs use the category
    endpoints (/pre-game, /post-game, /live-stats) instead.

    Pass ?date=YYYY-MM-DD to backfill all pipelines for a specific date.
    """
    pipeline_names = [
        name for name, cls in PIPELINE_REGISTRY.items()
        if cls.config.category != PipelineCategory.SCHEDULED
    ]

    job_manager = get_job_manager()
    job = await job_manager.create_job(len(pipeline_names))
    asyncio.create_task(
        _run_pipelines_background(job.job_id, date_override=date, pipeline_names=pipeline_names)
    )

    log.info(
        "pipeline_job_started",
        job_id=job.job_id,
        pipeline_count=len(pipeline_names),
        date_override=str(date) if date else None,
    )

    return JobCreatedResponse(
        status=ApiStatus.SUCCESS,
        message=f"Pipeline job started. Use GET /jobs/{job.job_id} to check status.",
        data=PipelineJobInfo(
            job_id=job.job_id,
            status=job.status,
            created_at=job.created_at,
            pipelines_total=job.pipelines_total,
        ),
    )


@router.post("/all/sync", response_model=AllPipelinesResponse)
async def trigger_all_pipelines_sync(
    _: str = Security(verify_pipeline_token),
    date: Optional[date] = Query(None, description="Override game date (YYYY-MM-DD) for all pipelines. Omit for automatic date."),
) -> AllPipelinesResponse:
    """
    Trigger all non-SCHEDULED pipelines synchronously (blocks until complete).

    WARNING: This can take several minutes. Use POST /all for fire-and-forget.
    Only use this endpoint if you need the results immediately and can wait.

    Pass ?date=YYYY-MM-DD to backfill all pipelines for a specific date.
    """
    results = await run_all_pipelines(date_override=date)

    # Determine overall status
    all_success = all(r.status == ApiStatus.SUCCESS for r in results.values())
    overall_status = ApiStatus.SUCCESS if all_success else ApiStatus.ERROR
    message = (
        "All pipelines completed successfully"
        if all_success
        else "Some pipelines failed"
    )

    return AllPipelinesResponse(
        status=overall_status,
        message=message,
        data=results,
    )


@router.get("/jobs", response_model=JobListResponse)
async def list_jobs(
    _: str = Security(verify_pipeline_token),
    limit: int = Query(default=10, ge=1, le=50, description="Max jobs to return"),
) -> JobListResponse:
    """
    List recent pipeline jobs.

    Returns most recent jobs first.
    """
    job_manager = get_job_manager()
    jobs = await job_manager.list_jobs(limit=limit)

    return JobListResponse(
        status=ApiStatus.SUCCESS,
        message=f"Found {len(jobs)} jobs",
        data=[
            PipelineJobInfo(
                job_id=j.job_id,
                status=j.status,
                created_at=j.created_at,
                started_at=j.started_at,
                completed_at=j.completed_at,
                duration_seconds=j.duration_seconds,
                pipelines_total=j.pipelines_total,
                pipelines_completed=j.pipelines_completed,
                pipelines_failed=j.pipelines_failed,
                current_pipeline=j.current_pipeline,
            )
            for j in jobs
        ],
    )


@router.get("/jobs/{job_id}", response_model=JobStatusResponse)
async def get_job_status(
    job_id: str,
    _: str = Security(verify_pipeline_token),
) -> JobStatusResponse:
    """
    Get the status of a pipeline job.

    Returns current status, progress, and results for completed pipelines.
    """
    job_manager = get_job_manager()
    job = await job_manager.get_job(job_id)

    if job is None:
        raise HTTPException(
            status_code=404,
            detail=f"Job {job_id} not found. Jobs are kept in memory and may be lost on restart.",
        )

    # Convert internal results to API results
    results = {
        name: PipelineJobResult(
            pipeline_name=r.pipeline_name,
            status=r.status,
            message=r.message,
            started_at=r.started_at,
            completed_at=r.completed_at,
            duration_seconds=r.duration_seconds,
            records_processed=r.records_processed,
            error=r.error,
        )
        for name, r in job.results.items()
    }

    return JobStatusResponse(
        status=ApiStatus.SUCCESS,
        message=f"Job is {job.status.value}",
        data=PipelineJobDetail(
            job_id=job.job_id,
            status=job.status,
            created_at=job.created_at,
            started_at=job.started_at,
            completed_at=job.completed_at,
            duration_seconds=job.duration_seconds,
            pipelines_total=job.pipelines_total,
            pipelines_completed=job.pipelines_completed,
            pipelines_failed=job.pipelines_failed,
            current_pipeline=job.current_pipeline,
            results=results,
            error=job.error,
        ),
    )


def _check_unmet_dependencies(
    pipeline_cls,
    succeeded_in_batch: set[str],
    date_override: Optional[date] = None,
) -> list[str]:
    """
    Check if a pipeline's depends_on requirements are met.

    A dependency is met if it either succeeded earlier in this batch OR has a
    successful PipelineRun for today's NBA date.

    Returns:
        List of unmet dependency names (empty if all met).
    """
    deps = pipeline_cls.config.depends_on
    if not deps:
        return []

    from db.models.pipeline_run import PipelineRun as PRModel

    # Compute target date (NBA date convention)
    if date_override:
        target_date = date_override
    else:
        import pytz
        from datetime import datetime as dt, timedelta

        eastern = pytz.timezone("US/Eastern")
        now_et = dt.now(eastern)
        if now_et.hour < 6:
            target_date = (now_et - timedelta(days=1)).date()
        else:
            target_date = now_et.date()

    unmet = []
    for dep_name in deps:
        if dep_name in succeeded_in_batch:
            continue
        if not PRModel.was_successful_on_date(dep_name, target_date):
            unmet.append(dep_name)

    return unmet


async def _run_pipelines_background(
    job_id: str,
    date_override: Optional[date] = None,
    pipeline_names: Optional[list[str]] = None,
) -> None:
    """
    Run pipelines in the background and update job status.

    This function is spawned as a background task and runs independently.
    pipeline_names: subset of PIPELINE_REGISTRY to run; defaults to all non-SCHEDULED.
    """
    job_manager = get_job_manager()

    if pipeline_names is None:
        pipeline_names = [
            name for name, cls in PIPELINE_REGISTRY.items()
            if cls.config.category != PipelineCategory.SCHEDULED
        ]

    log.info(
        "background_job_starting",
        job_id=job_id,
        pipelines=pipeline_names,
        date_override=str(date_override) if date_override else None,
    )

    await job_manager.update_job_started(job_id)

    # Track which pipelines succeeded in this batch for dependency checks.
    succeeded_in_batch: set[str] = set()

    try:
        for i, name in enumerate(pipeline_names, 1):
            # Dependency enforcement: skip if depends_on pipelines haven't succeeded.
            pipeline_cls = PIPELINE_REGISTRY[name]
            unmet_deps = _check_unmet_dependencies(
                pipeline_cls, succeeded_in_batch, date_override
            )

            if unmet_deps:
                log.warning(
                    "pipeline_dependency_not_met",
                    job_id=job_id,
                    pipeline=name,
                    unmet_dependencies=unmet_deps,
                )
                job_result = JobResultInternal(
                    pipeline_name=name,
                    status=ApiStatus.SKIPPED,
                    message=f"Skipped: dependencies not met ({', '.join(unmet_deps)})",
                )
                await job_manager.add_pipeline_result(job_id, name, job_result)
                continue

            log.info(
                "background_pipeline_starting",
                job_id=job_id,
                pipeline=name,
                step=f"{i}/{len(pipeline_names)}",
            )

            await job_manager.update_current_pipeline(job_id, name)

            try:
                result = await run_pipeline(name, date_override=date_override)

                job_result = JobResultInternal(
                    pipeline_name=name,
                    status=result.status,
                    message=result.message,
                    started_at=result.started_at,
                    completed_at=result.completed_at,
                    duration_seconds=result.duration_seconds,
                    records_processed=result.records_processed,
                    error=result.error,
                )

                await job_manager.add_pipeline_result(job_id, name, job_result)

                if result.status == ApiStatus.SUCCESS:
                    succeeded_in_batch.add(name)

                log.info(
                    "background_pipeline_completed",
                    job_id=job_id,
                    pipeline=name,
                    status=result.status,
                )

            except Exception as e:
                log.error(
                    "background_pipeline_error",
                    job_id=job_id,
                    pipeline=name,
                    error=str(e),
                )

                job_result = JobResultInternal(
                    pipeline_name=name,
                    status="error",
                    message=f"Pipeline failed with exception: {e}",
                    error=str(e),
                )
                await job_manager.add_pipeline_result(job_id, name, job_result)

        # Check if all succeeded
        job = await job_manager.get_job(job_id)
        all_success = job.pipelines_failed == 0 if job else False

        await job_manager.complete_job(job_id, success=all_success)

        log.info(
            "background_job_completed",
            job_id=job_id,
            success=all_success,
            completed=job.pipelines_completed if job else 0,
            failed=job.pipelines_failed if job else 0,
        )

    except Exception as e:
        log.error("background_job_failed", job_id=job_id, error=str(e))
        await job_manager.complete_job(job_id, success=False, error=str(e))


# ── Deployment ────────────────────────────────────────────────────────────────

@router.post("/deploy")
async def trigger_deploy(
    source: Optional[str] = Query(None, description="'manual' when triggered from dashboard"),
    service: Optional[str] = Query(None, description="'backend' or 'data_platform'; omit to deploy both"),
    _: str = Security(verify_pipeline_token),
) -> dict:
    """
    Trigger nightly production deploys via GitHub repository_dispatch.

    Sends a `nightly-deploy` event to both the backend and data-platform GitHub repos,
    which fires their deploy workflows (railway up + smoke tests). Called by cron-runner
    daily at 2AM CST and available for manual dashboard triggers.

    When source=manual the endpoint self-records to cron_job_runs so the result
    appears in the scheduler timeline and deployments section immediately. Cron-runner
    triggered runs are recorded by the cron-runner reporter instead.

    Any dispatch failing answers **502** `{status: "error", data: outcomes}` (so the
    cron-runner records a failure and its streak alert can fire) and posts a
    `deploy_dispatch_failed` alert to the ops webhook.

    Requires:
        GITHUB_DEPLOY_TOKEN  — GitHub PAT with repo + workflow scopes
        BACKEND_GITHUB_REPO  — e.g. "username/backend"
        DATA_PLATFORM_GITHUB_REPO — e.g. "username/data-platform"
    """
    import json as _json
    from core.settings import settings

    token = settings.github_deploy_token.get_secret_value() if settings.github_deploy_token else None
    all_repos = {
        "backend": settings.backend_github_repo,
        "data_platform": settings.data_platform_github_repo,
    }
    repos = {service: all_repos[service]} if service and service in all_repos else all_repos

    missing: list[str] = []
    if not token:
        missing.append("GITHUB_DEPLOY_TOKEN")
    missing += [k for k, v in repos.items() if not v]
    if missing:
        raise HTTPException(
            status_code=503,
            detail=f"GitHub deploy config not set: {missing}",
        )

    log.info("github_dispatch_firing", repos=list(repos.values()), source=source)
    triggered_at = datetime.now(timezone.utc)

    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }

    async def dispatch(repo: str) -> httpx.Response:
        url = f"https://api.github.com/repos/{repo}/dispatches"
        return await client.post(url, json={"event_type": "nightly-deploy"}, headers=headers)

    async with httpx.AsyncClient(timeout=30) as client:
        results = await asyncio.gather(
            *[dispatch(repo) for repo in repos.values()],
            return_exceptions=True,
        )

    completed_at = datetime.now(timezone.utc)
    duration_ms = int((completed_at - triggered_at).total_seconds() * 1000)

    outcomes: dict = {}
    all_ok = True
    for name, result in zip(repos.keys(), results):
        if isinstance(result, Exception):
            outcomes[name] = {"ok": False, "error": str(result)}
            all_ok = False
        else:
            # GitHub returns 204 No Content on success
            ok = result.status_code in (204, 200)
            outcomes[name] = {"ok": ok, "status": result.status_code}
            if not ok:
                outcomes[name]["body"] = result.text[:200]
                all_ok = False

    deploy_status = "success" if all_ok else "error"
    failed_repos = {
        name: outcome.get("error") or f"HTTP {outcome.get('status')}: {outcome.get('body', '')}".strip()
        for name, outcome in outcomes.items()
        if not outcome.get("ok")
    }
    if all_ok:
        log.info("github_dispatch_fired", status=deploy_status, outcomes=outcomes)
    else:
        log.error("github_dispatch_failed", status=deploy_status, outcomes=outcomes, source=source)
        await get_alert_service().notify_async(AlertEvent(
            key="deploy_dispatch_failed",
            severity="critical",
            title="Nightly deploy dispatch failed",
            body="\n".join(f"{name} ({repos[name]}): {reason[:300]}" for name, reason in failed_repos.items()),
            fields={
                "source": source or "cron",
                "service": service or "both",
                "failed": ", ".join(failed_repos),
                "ok": ", ".join(name for name, outcome in outcomes.items() if outcome.get("ok")) or None,
            },
            dedupe=DEPLOY_ALERT_DEDUPE,
        ))

    # Self-record when triggered manually from the dashboard so the run
    # appears in the timeline and deployment cards without waiting for cron-runner.
    if source == "manual":
        snippet = _json.dumps(outcomes)[:300]
        error_msg = None if all_ok else _json.dumps(failed_repos)

        def _record() -> None:
            from db.models.nba.cron_job_run import CronJobRun
            CronJobRun.create(
                job_name="deploy",
                triggered_at=triggered_at,
                completed_at=completed_at,
                duration_ms=duration_ms,
                result="success" if all_ok else "failure",
                http_status=None,
                attempts=1,
                error_message=error_msg,
                response_snippet=snippet,
            )

        await run_in_db_thread(_record)

    if not all_ok:
        return JSONResponse(
            status_code=502,
            content={
                "status": deploy_status,
                "message": f"Deploy dispatch failed for: {', '.join(failed_repos)}",
                "data": outcomes,
                "error_code": DEPLOY_DISPATCH_FAILED_CODE,
            },
        )

    return {"status": deploy_status, "message": f"Deploy dispatched to GitHub ({deploy_status})", "data": outcomes}


# ── Playoffs ──────────────────────────────────────────────────────────────────

@router.post("/playoffs", response_model=PipelineResponse)
async def trigger_playoffs(
    _: str = Security(verify_pipeline_token),
) -> PipelineResponse:
    """
    Trigger the playoff bracket pipeline.

    Fetches current NBA playoff series standings from NBA Stats API (SeriesStandings)
    and upserts to nba.playoff_series. Called once nightly at ~1 AM ET by the
    'playoffs' cron job in cron-runner.
    """
    result = await run_pipeline("playoff_bracket")
    return PipelineResponse(
        status=result.status,
        message=result.message,
        data=result,
    )
