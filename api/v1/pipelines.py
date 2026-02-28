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
from datetime import date
from typing import Optional

from fastapi import APIRouter, Security, HTTPException, Query

from core.job_manager import (
    get_job_manager,
    PipelineJobResult as JobResultInternal,
)
from core.logging import get_logger
from core.pipeline_auth import verify_pipeline_token
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

router = APIRouter(prefix="/pipelines", tags=["pipelines"])
log = get_logger("pipeline_api")


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
) -> PipelineResponse:
    """
    Trigger the daily matchup scores pipeline.

    Fetches current matchup scores for all saved teams and records
    daily snapshots for visualization.
    Pass ?date=YYYY-MM-DD to backfill a specific date.
    """
    result = await run_pipeline("daily_matchup_scores", date_override=date)
    return PipelineResponse(
        status=result.status,
        message=result.message,
        data=result,
    )


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
) -> PipelineResponse:
    """
    Trigger the game start times pipeline.

    Fetches scheduled tip-off times for upcoming games and upserts
    to nba.games. Used by the live stats and post-game gates.
    Pass ?date=YYYY-MM-DD to backfill a specific date.
    """
    result = await run_pipeline("game_start_times", date_override=date)
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

            # Dedup gate: skip if already ran successfully today
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

    for cls in post_game_pipelines:
        name = cls.config.name

        if not force:
            if PipelineRun.was_successful_on_date(name, nba_date):
                log.info("post_game_pipeline_dedup_skip", pipeline=name, nba_date=str(nba_date))
                continue
            if PipelineRun.is_running(name):
                log.info("post_game_pipeline_concurrency_skip", pipeline=name)
                continue

        pipelines_to_run.append(name)

    if not pipelines_to_run:
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

    # Check if there are any games today
    games_today = Game.get_games_on_date(game_date)
    if not games_today:
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
                    games_total=len(games_today),
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
            games_total=len(games_today),
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

    try:
        for i, name in enumerate(pipeline_names, 1):
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
