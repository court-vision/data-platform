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
from typing import Literal, NamedTuple, Optional

import httpx
import pytz
from fastapi import APIRouter, Security, HTTPException, Query
from fastapi.responses import JSONResponse

from core.job_manager import (
    get_job_manager,
    PipelineJobResult as JobResultInternal,
)
from core.logging import get_logger
from core.nba_calendar import nba_date_et
from core.pipeline_auth import verify_pipeline_token
from db.base import run_in_db_thread
from db.models.nba.pipeline_batch import (
    ALL_SKIPPED,
    DISPATCHED,
    WINDOW_CLOSED,
    PipelineBatch,
)
from pipelines import (
    run_pipeline,
    run_all_pipelines,
    list_pipelines,
    PIPELINE_REGISTRY,
    get_pipelines_by_category,
)
from pipelines.config import PipelineCategory
from pipelines.gates import (
    GateDecision,
    espn_batch_gate,
    post_game_window,
    pre_game_window,
)
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
from services.alert_service import AlertEvent, get_alert_service

router = APIRouter(prefix="/pipelines", tags=["pipelines"])
log = get_logger("pipeline_api")

DEPLOY_DISPATCH_FAILED_CODE = "DEPLOY_DISPATCH_FAILED"
DEPLOY_ALERT_DEDUPE = timedelta(hours=12)
# Keyed per NBA date, so this is belt-and-braces over the durable
# `PipelineBatch.swept` latch — long enough that a restart mid-sweep cannot
# produce a second message for the same night.
POST_GAME_INCOMPLETE_DEDUPE = timedelta(hours=12)


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

    Direct trigger: the ESPN batch gate lives in the post-game endpoint, which
    is what production calls. A `?watch=true` loop mode used to duplicate that
    gate here for a cron-runner job that was never registered; it referenced an
    `ApiStatus` member that does not exist, so it would have raised on its first
    real request. Removed rather than fixed — one gate is the point.
    """
    result = await run_pipeline("daily_matchup_scores", date_override=date)
    return PipelineResponse(
        status=result.status,
        message=result.message,
        data=result,
    )


_ESPN_ENDPOINT = (
    "https://lm-api-reads.fantasy.espn.com/apis/v3/games/fba/seasons/{}/segments/0/leagues/{}"
)


def _first_espn_league():
    """(team, league_info) for the first ESPN team found, or (None, None).

    A single league is enough for both the gate and the watermark probe:
    `latestScoringPeriod` is ESPN's league-level day index, and every NBA league
    shares the same calendar.
    """
    import json
    from db.models.teams import Team
    from services import credential_service

    for team in Team.select().limit(10):
        try:
            league_info = credential_service.hydrate(team, json.loads(team.league_info))
            if league_info.get("provider", "espn") == "espn":
                return team, league_info
        except Exception:
            continue
    return None, None


def _fetch_espn_league(league_info, views):
    """One GET against a league. Returns the parsed payload, or None on any failure."""
    import requests
    from core.settings import settings

    endpoint = _ESPN_ENDPOINT.format(
        league_info.get("year", settings.espn_year), league_info.get("league_id")
    )
    resp = requests.get(
        endpoint,
        params={"view": views},
        cookies={
            "espn_s2": league_info.get("espn_s2", ""),
            "SWID": league_info.get("swid", ""),
        },
        timeout=10,
    )
    resp.raise_for_status()
    return resp.json()


def _espn_team_ids() -> list[int]:
    """Team ids of every saved ESPN team.

    The gate compares ESPN's day index against what we stored **for each of
    them**: one team's newest row is not evidence about another's, and reading
    it as if it were is why a partial run could never retry the teams it
    missed. `league_info` is read unhydrated — the provider tag is not a secret
    and decrypting five credential blobs to read it would be silly.
    """
    import json
    from db.models.teams import Team

    ids = []
    for team in Team.select():
        try:
            if json.loads(team.league_info).get("provider", "espn") == "espn":
                ids.append(team.team_id)
        except Exception:
            continue
    return ids


def _espn_baselines(team_ids: list[int], matchup_period) -> dict[int, Optional[int]]:
    """team_id -> newest stored watermark in `matchup_period` (None if no row yet).

    Scoped to ESPN's *current* matchup period, so the first day of a new period
    reads as "not seeded" rather than inheriting the last period's watermark —
    which is how a period's day-0 row goes missing (PENDING_PROD_CHECKS #3).
    Falls back to the team's highest watermark overall when ESPN did not report
    a current period; that still answers "has ESPN moved past us", just without
    the seeding signal.
    """
    from peewee import fn
    from db.models.stats.daily_matchup_score import DailyMatchupScore

    baselines: dict[int, Optional[int]] = {team_id: None for team_id in team_ids}
    if not team_ids:
        return baselines

    query = (
        DailyMatchupScore
        .select(
            DailyMatchupScore.team_id,
            fn.MAX(DailyMatchupScore.scoring_period_id).alias("watermark"),
        )
        .where(DailyMatchupScore.team_id.in_(team_ids))
        .group_by(DailyMatchupScore.team_id)
    )
    if matchup_period is not None:
        query = query.where(DailyMatchupScore.matchup_period == matchup_period)

    for row in query.dicts():
        baselines[row["team_id"]] = row["watermark"]
    return baselines


class EspnObservation(NamedTuple):
    """One look at ESPN's league status, shared by the probe and the gate."""

    latest_scoring_period: Optional[int]
    current_matchup_period: Optional[int]


def probe_espn_watermark() -> Optional[EspnObservation]:
    """Record one observation of ESPN's day watermark beside its materialized totals.

    Answers the one assumption the live-score watermark design rests on: that ESPN
    advances `status.latestScoringPeriod` at or *after* the moment matchup
    `totalPoints` absorbs the previous day. If it advances first, a snapshot taken
    in between records a watermark that overstates what its score covers, and the
    read path drops a day.

    The measurement is Δ = t(totalPoints changes) − t(latestScoringPeriod changes),
    taken across a night of these log lines. **Δ ≤ 0 means the assumption holds**
    and `live_window_from_watermark` is safe to enable. See
    docs/PENDING_PROD_CHECKS.md #4.

    `latestScoringPeriod` is undefined or 0 before opening night, so this produces
    nothing useful until 2026-10-20 — which is why it has to be deployed first.

    Instrumentation only: never raises, never affects whether a pipeline runs.
    Returns the observation so the gate can reuse this request instead of
    making a second one.
    """
    try:
        _, league_info = _first_espn_league()
        if not league_info:
            return None

        payload = _fetch_espn_league(league_info, ["mSettings", "mMatchup"])
        status = payload.get("status", {}) or {}
        latest_scoring_period = status.get("latestScoringPeriod")
        current_matchup_period = status.get("currentMatchupPeriod")

        # totalPoints for the current matchup period. Both sides are logged: they
        # move together in ESPN's batch, so either changing marks the same instant.
        home_total = away_total = None
        for matchup in payload.get("schedule") or []:
            if matchup.get("matchupPeriodId") == current_matchup_period:
                home_total = (matchup.get("home") or {}).get("totalPoints")
                away_total = (matchup.get("away") or {}).get("totalPoints")
                break

        log.info(
            "espn_watermark_probe",
            latest_scoring_period=latest_scoring_period,
            current_matchup_period=current_matchup_period,
            home_total_points=home_total,
            away_total_points=away_total,
        )
        return EspnObservation(latest_scoring_period, current_matchup_period)
    except Exception as exc:
        log.warning("espn_watermark_probe_failed", error=type(exc).__name__)
        return None


def _espn_scoring_period_advanced(
    nba_date: date,
    observed: Optional[EspnObservation] = None,
    succeeded_tonight: bool = False,
    attempts_tonight: int = 0,
) -> GateDecision:
    """Should an ESPN-gated pipeline run on this poll?

    The I/O half of `gates.espn_batch_gate`: reads ESPN's league status (reusing
    `probe_espn_watermark`'s response when this poll already made one) and the
    stored watermarks, then hands both to the pure decision.

    Args:
        nba_date: the game date this batch covers — the 02:30 CST fallback is
            anchored to the morning after it, not to a bare hour-of-day.
        observed: `latestScoringPeriod` / `currentMatchupPeriod` already fetched
            this poll; passing it avoids a second identical request.
        succeeded_tonight: the pipeline already completed since the window opened.
        attempts_tonight: runs started since the window opened (the retry budget).
    """
    from core.settings import settings

    now_cst = datetime.now(pytz.timezone("US/Central"))
    espn_team, espn_league_info = _first_espn_league()

    if not espn_team or not espn_league_info:
        return espn_batch_gate(
            now_cst=now_cst,
            nba_date=nba_date,
            league_present=False,
            espn_period=None,
            baselines={},
            succeeded_tonight=succeeded_tonight,
            attempts_tonight=attempts_tonight,
            max_attempts=settings.espn_gate_max_attempts,
        )

    if observed is None:
        try:
            payload = _fetch_espn_league(espn_league_info, "mSettings")
            status = payload.get("status", {}) or {}
            observed = EspnObservation(
                status.get("latestScoringPeriod"), status.get("currentMatchupPeriod")
            )
        except Exception as e:
            log.warning("espn_peek_failed", error=str(e))
            observed = EspnObservation(None, None)

    baselines = _espn_baselines(_espn_team_ids(), observed.current_matchup_period)

    return espn_batch_gate(
        now_cst=now_cst,
        nba_date=nba_date,
        league_present=True,
        espn_period=observed.latest_scoring_period,
        baselines=baselines,
        succeeded_tonight=succeeded_tonight,
        attempts_tonight=attempts_tonight,
        max_attempts=settings.espn_gate_max_attempts,
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
    from core.settings import settings
    from db.models.nba.games import Game
    from db.models.pipeline_run import PipelineRun

    force = force or (date is not None)

    eastern = pytz.timezone("US/Eastern")
    now_et = datetime.now(eastern)
    now_et_naive = now_et.replace(tzinfo=None)
    nba_date = nba_date_et(now_et)

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
    decisions: dict[str, dict] = {}

    for cls in pre_game_pipelines:
        name = cls.config.name

        if not force:
            # Window gate: skip if too early for this pipeline
            if first_game_time:
                window_minutes = cls.config.pre_game_window_minutes or settings.pre_game_window_minutes
                window = pre_game_window(
                    now_et_naive, first_game_time, nba_date, window_minutes
                )
                if not window.open:
                    log.info(
                        "pre_game_window_not_open",
                        pipeline=name,
                        window_minutes=window_minutes,
                        opens_at=str(window.opens_at),
                        current_time=str(now_et_naive),
                    )
                    decisions[name] = {"decision": "skip", "reason": "window_not_open"}
                    continue

            # Dedup gate: skip if already ran successfully today.
            # Pipelines with skip_batch_dedup manage their own internal dedup
            # and must not be blocked by an early "success with 0 records" run.
            if not cls.config.skip_batch_dedup:
                if PipelineRun.was_successful_on_date(name, nba_date):
                    log.info("pre_game_already_ran", pipeline=name, nba_date=str(nba_date))
                    decisions[name] = {"decision": "skip", "reason": "already_ran"}
                    continue

            # Concurrency gate: skip if already running
            if PipelineRun.is_running(name):
                log.info("pre_game_already_running", pipeline=name)
                decisions[name] = {"decision": "skip", "reason": "already_running"}
                continue

            decisions[name] = {"decision": "run", "reason": "due"}
        else:
            decisions[name] = {"decision": "run", "reason": "forced"}

        pipelines_to_run.append(name)

    if not pipelines_to_run:
        # Not recorded as a batch, unlike post-game's equivalent. The pre-game
        # window has no upper bound, so it stays open from ~150 min before tip
        # until the NBA date rolls at 06:00 ET — "not open yet" or "already ran"
        # is the answer on nearly all of the day's 52 polls. `nba.cron_job_runs`
        # already holds a row per trigger with the response body.
        return PipelineResponse(
            status=ApiStatus.SUCCESS,
            message=f"All pre-game pipelines skipped (window not open or already ran) for {nba_date}",
        )

    job_manager = get_job_manager()
    job = await job_manager.create_job(len(pipelines_to_run))
    batch = await _record_batch(
        "pre_game",
        target_date,
        DISPATCHED,
        "dispatched",
        decisions,
        job_id=job.job_id,
        forced=force,
    )
    asyncio.create_task(
        _run_pipelines_background(
            job.job_id,
            date_override=target_date if date else None,
            pipeline_names=pipelines_to_run,
            nba_date=target_date,
            batch_id=batch,
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

    Once the window has **closed**, one last poll sweeps the night: any pipeline
    with no successful run for the date is recorded and alerted
    (`post_game_incomplete`). That is the check that turns a silent skip into a
    message — a night where every poll decided "nothing to do" otherwise leaves
    no trace outside the logs.

    Pass ?force=true to skip all gates (useful for manual re-triggers or backfills).
    Pass ?date=YYYY-MM-DD to backfill a specific date (implies force=true).
    """
    from core.settings import settings
    from db.models.nba.games import Game
    from db.models.pipeline_run import PipelineRun
    from pipelines.extractors.nba_api import NBAApiExtractor

    # A date override implies force — skip all time/readiness gating
    force = force or (date is not None)
    observed: Optional[EspnObservation] = None

    eastern = pytz.timezone("US/Eastern")
    now_et = datetime.now(eastern)
    nba_date = nba_date_et(now_et)

    # Cutoff for "tonight": runs at or after the window opened. Assigned inside
    # the gated path below, where the window is known.
    window_open_utc: Optional[datetime] = None

    if not force:
        # Check if there are any scheduled games on the NBA date
        latest_game_time = Game.get_latest_game_time_on_date(nba_date)
        if not latest_game_time:
            log.info("post_game_no_games", nba_date=str(nba_date))
            return PipelineResponse(
                status=ApiStatus.SUCCESS,
                message=f"No games scheduled for NBA date {nba_date}",
            )

        # One watermark observation per poll, taken before any gate can return
        # early — the ESPN flip happens partway through this window and the
        # whole point is to bracket it across the full 20:00–07:59 CST span the
        # cron polls, not just the post-game window. Only on nights that have
        # games, so it costs nothing in the offseason. The gate below is handed
        # the result rather than making a second request.
        # See docs/PENDING_PROD_CHECKS.md #4.
        observed = probe_espn_watermark()

        now_et_naive = now_et.replace(tzinfo=None)
        window = post_game_window(
            now_et_naive,
            latest_game_time,
            nba_date,
            settings.estimated_game_duration_minutes,
            settings.post_game_pipeline_window_minutes,
        )
        window_open_utc = (
            eastern.localize(window.opens_at).astimezone(pytz.utc).replace(tzinfo=None)
        )

        if window.closed:
            return await _sweep_closed_post_game_window(nba_date, window_open_utc)

        if not window.open:
            log.info(
                "post_game_outside_window",
                nba_date=str(nba_date),
                estimated_end=str(window.opens_at),
                window_end=str(window.closes_at),
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
    time_gated_pipelines = []
    decisions: dict[str, dict] = {}

    for cls in post_game_pipelines:
        name = cls.config.name

        if not force:
            if PipelineRun.is_running(name):
                log.info("post_game_pipeline_concurrency_skip", pipeline=name)
                decisions[name] = {"decision": "skip", "reason": "already_running"}
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
                    decisions[name] = {"decision": "skip", "reason": "time_gated"}
                    continue

            already_ran = PipelineRun.was_successful_on_date(
                name, nba_date, after=window_open_utc
            )

            if cls.config.espn_gated:
                # The ESPN gate subsumes dedup for these pipelines: "every team's
                # stored watermark already covers ESPN's current period" is a
                # stronger statement than "a run succeeded", because a run that
                # succeeded before ESPN's batch landed wrote stale totals. It is
                # handed the success and attempt counts so it can stop retrying
                # rather than re-running every poll for the rest of the window.
                gate = await run_in_db_thread(
                    _espn_scoring_period_advanced,
                    nba_date,
                    observed,
                    already_ran,
                    PipelineRun.count_since(name, window_open_utc)
                    if window_open_utc
                    else 0,
                )
                if not gate.run:
                    log.info(
                        "post_game_espn_gate_not_ready",
                        pipeline=name,
                        reason=gate.reason,
                        **gate.detail,
                    )
                    decisions[name] = {"decision": "skip", "reason": gate.reason}
                    continue
                log.info(
                    "post_game_espn_gate_open",
                    pipeline=name,
                    reason=gate.reason,
                    **gate.detail,
                )
                decisions[name] = {"decision": "run", "reason": gate.reason}
            else:
                if already_ran:
                    log.info("post_game_pipeline_dedup_skip", pipeline=name, nba_date=str(nba_date))
                    decisions[name] = {"decision": "skip", "reason": "already_ran"}
                    continue
                decisions[name] = {"decision": "run", "reason": "due"}
        else:
            decisions[name] = {"decision": "run", "reason": "forced"}

        pipelines_to_run.append(name)

    if not pipelines_to_run:
        if time_gated_pipelines:
            log.info(
                "post_game_pipelines_time_gated",
                nba_date=str(nba_date),
                time_gated=time_gated_pipelines,
            )
            await _record_batch(
                "post_game", nba_date, ALL_SKIPPED, "time_gated", decisions, forced=force
            )
            return PipelineResponse(
                status=ApiStatus.SUCCESS,
                message=f"Post-game pipelines deferred for {nba_date}: {time_gated_pipelines} waiting for earliest_run_time_cst, will retry",
            )
        log.info("post_game_all_complete", nba_date=str(nba_date))
        await _record_batch(
            "post_game", nba_date, ALL_SKIPPED, "already_complete", decisions, forced=force
        )
        return PipelineResponse(
            status=ApiStatus.SUCCESS,
            message=f"All post-game pipelines already completed for {nba_date}",
        )

    target_date = date or nba_date
    job_manager = get_job_manager()
    job = await job_manager.create_job(len(pipelines_to_run))
    batch = await _record_batch(
        "post_game",
        target_date,
        DISPATCHED,
        "dispatched",
        decisions,
        job_id=job.job_id,
        forced=force,
    )
    asyncio.create_task(
        _run_pipelines_background(
            job.job_id,
            date_override=target_date if date else None,
            pipeline_names=pipelines_to_run,
            nba_date=target_date,
            batch_id=batch,
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


async def _sweep_closed_post_game_window(
    nba_date: date, window_open_utc: datetime
) -> PipelineResponse:
    """The night's last word: did every post-game pipeline actually run?

    Runs on the first poll after the window closes and never again for that
    date — `PipelineBatch.swept` is the latch, durable so it survives the
    redeploy that happens at 03:00 CST every night. Anything that never
    succeeded is written into the batch row and alerted once.

    This is the check that was missing when `daily_matchup_scores` went five
    days without running (PENDING_PROD_CHECKS #3): every individual poll
    correctly decided there was nothing to do, and nothing ever asked whether
    the night as a whole had come up empty.
    """
    from db.models.pipeline_run import PipelineRun

    def _survey() -> Optional[dict[str, dict]]:
        if PipelineBatch.swept("post_game", nba_date):
            return None
        return {
            cls.config.name: {
                "status": (
                    "success"
                    if PipelineRun.was_successful_on_date(
                        cls.config.name, nba_date, after=window_open_utc
                    )
                    else "never_ran"
                )
            }
            for cls in get_pipelines_by_category(PipelineCategory.POST_GAME)
        }

    survey = await run_in_db_thread(_survey)
    if survey is None:
        return PipelineResponse(
            status=ApiStatus.SUCCESS,
            message=f"Post-game window closed for {nba_date}; already swept",
        )

    missing = sorted(name for name, row in survey.items() if row["status"] != "success")
    if missing:
        log.error(
            "post_game_incomplete",
            nba_date=str(nba_date),
            missing=missing,
            ran=len(survey) - len(missing),
        )
    else:
        log.info("post_game_window_closed_complete", nba_date=str(nba_date))

    await run_in_db_thread(
        PipelineBatch.open,
        "post_game",
        nba_date,
        WINDOW_CLOSED,
        "incomplete" if missing else "complete",
        survey,
        None,
        False,
        bool(missing),
    )

    if missing:
        await get_alert_service().notify_async(AlertEvent(
            key=f"post_game_incomplete:{nba_date}",
            severity="critical",
            title=f"Post-game pipelines never ran for {nba_date}",
            body=(
                f"The post-game window has closed and {len(missing)} of {len(survey)} "
                f"pipelines have no successful run for this NBA date: {', '.join(missing)}. "
                "That data is not coming back on its own — a manual "
                "`?force=true&date=` trigger is the only way to fill it."
            ),
            fields={
                "nba_date": str(nba_date),
                "missing": ", ".join(missing),
                "ran": len(survey) - len(missing),
            },
            dedupe=POST_GAME_INCOMPLETE_DEDUPE,
        ))

    return PipelineResponse(
        status=ApiStatus.SUCCESS,
        message=(
            f"Post-game window closed for {nba_date}; never ran: {', '.join(missing)}"
            if missing
            else f"Post-game window closed for {nba_date}; all pipelines ran"
        ),
    )


async def _record_batch(
    category: str,
    nba_date: date,
    decision: str,
    reason: str,
    decisions: dict[str, dict],
    job_id: Optional[str] = None,
    forced: bool = False,
):
    """Write the batch's decision row and return its id (None if unwritable)."""
    batch = await run_in_db_thread(
        PipelineBatch.open,
        category,
        nba_date,
        decision,
        reason,
        decisions,
        job_id,
        forced,
    )
    return batch.id if batch is not None else None


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
    game_date = nba_date_et(now_et)

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
    # The pipeline is handed the date this endpoint gated on, rather than
    # deriving its own. It used to compute a *Central* 6 AM cutoff while every
    # reader of nba.live_player_stats — and this endpoint's own finalize calls
    # below — used an Eastern one, so for one hour a day the rows were written
    # under a date nothing would ask for.
    pipeline = LiveGameStatsPipeline()
    result = await pipeline.run(nba_date=game_date)

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

    target_date = date_override or nba_date_et()

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
    nba_date: Optional[date] = None,
    batch_id=None,
) -> None:
    """
    Run pipelines in the background and update job status.

    This function is spawned as a background task and runs independently.
    pipeline_names: subset of PIPELINE_REGISTRY to run; defaults to all non-SCHEDULED.
    nba_date: the batch's game date, shared by every pipeline in it.
    batch_id: `nba.pipeline_batches` row to fold the outcomes back into, so the
        durable record says what happened rather than only what was intended.
    """
    job_manager = get_job_manager()
    outcomes: dict[str, dict] = {}

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
                outcomes[name] = {
                    "status": "skipped",
                    "reason": f"unmet_dependencies:{','.join(unmet_deps)}",
                }
                continue

            log.info(
                "background_pipeline_starting",
                job_id=job_id,
                pipeline=name,
                step=f"{i}/{len(pipeline_names)}",
            )

            await job_manager.update_current_pipeline(job_id, name)

            try:
                result = await run_pipeline(
                    name, date_override=date_override, nba_date=nba_date
                )

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
                outcomes[name] = {
                    "status": result.status,
                    "records": result.records_processed,
                    "partial": result.partial,
                }

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
                outcomes[name] = {"status": "error", "error": str(e)[:200]}

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
    finally:
        # Outside the try so a job that died halfway still leaves a durable
        # record of how far it got.
        if batch_id is not None:
            await run_in_db_thread(PipelineBatch.close, batch_id, outcomes)


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
