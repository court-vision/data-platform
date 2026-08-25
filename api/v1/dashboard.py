"""
Pipeline Monitoring Dashboard

Serves the internal dashboard UI and provides the JSON status endpoint
that powers it. The HTML page is public; the status API requires the
standard pipeline bearer token.

Routes:
    GET  /v1/dashboard          — renders dashboard.html (no auth)
    GET  /v1/dashboard/status   — pipeline health + recent jobs (token auth)
"""

import asyncio
from datetime import datetime, timedelta, timezone
from typing import Optional

from fastapi import APIRouter, Request, Security
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from core.job_manager import get_job_manager
from core.logging import get_logger
from core.pipeline_auth import verify_pipeline_token
from db.models.pipeline_run import PipelineRun
from db.models.nba.cron_job_run import CronJobRun
from pipelines import PIPELINE_REGISTRY
from schemas.cron import CronJobRunEntry
from schemas.dashboard import (
    DashboardStatusData,
    DashboardStatusResponse,
    PipelineHealthEntry,
    QualityRunEntry,
    QualityCheckEntry,
)
from schemas.pipeline import PipelineJobInfo
from services.data_quality_service import DataQualityService

router = APIRouter(prefix="/dashboard", tags=["dashboard"])
log = get_logger("dashboard_api")


def _naive(dt: datetime | None) -> datetime | None:
    """Strip timezone info for safe naive-UTC comparisons."""
    if dt is None:
        return None
    return dt.replace(tzinfo=None) if dt.tzinfo else dt

# Lazy-initialized templates (set by main.py after app creation)
_templates: Optional[Jinja2Templates] = None


def set_templates(templates: Jinja2Templates) -> None:
    global _templates
    _templates = templates


# Pipeline name → individual trigger endpoint path
# Kept here so the dashboard JS can POST directly without hardcoding URLs.
PIPELINE_TRIGGER_ENDPOINTS: dict[str, str] = {
    "player_game_stats":    "/v1/internal/pipelines/daily-player-stats",
    "player_ownership":     "/v1/internal/pipelines/player-ownership",
    "player_season_stats":  "/v1/internal/pipelines/cumulative-player-stats",
    "daily_matchup_scores": "/v1/internal/pipelines/daily-matchup-scores",
    "player_rolling_stats": "/v1/internal/pipelines/player-rolling-stats",
    "team_stats":           "/v1/internal/pipelines/team-stats",
    "player_advanced_stats":"/v1/internal/pipelines/player-advanced-stats",
    "game_schedule":        "/v1/internal/pipelines/game-schedule",
    "game_start_times":     "/v1/internal/pipelines/game-start-times",
    "player_profiles":      "/v1/internal/pipelines/player-profiles",
    "live_game_stats":      "/v1/internal/pipelines/live-stats",
    "espn_injury_status":   "/v1/internal/pipelines/espn-injury-status",
    "breakout_detection":   "/v1/internal/pipelines/breakout-detection",
    "lineup_alerts":        "/v1/internal/pipelines/lineup-alerts",
    "playoff_bracket":      "/v1/internal/pipelines/playoffs",
}

# Pipeline name → cron-runner job name.
# Used to supplement stale PipelineRun records with fresher CronJobRun data.
# Category-triggered pipelines (pre-game / post-game) share a job name because
# the cron-runner fires a single endpoint that runs all pipelines in that group.
PIPELINE_CRON_JOB_MAP: dict[str, str] = {
    "live_game_stats":      "live-stats",
    "playoff_bracket":      "playoffs",
    "game_start_times":     "schedule-sync",
    # Post-game category (all share the same cron trigger)
    "player_game_stats":    "post-game",
    "player_ownership":     "post-game",
    "player_season_stats":  "post-game",
    "player_rolling_stats": "post-game",
    "player_advanced_stats":"post-game",
    "team_stats":           "post-game",
    "game_schedule":        "post-game",
    "daily_matchup_scores": "post-game",
    # Pre-game category
    "espn_injury_status":   "pre-game",
    "breakout_detection":   "pre-game",
    "lineup_alerts":        "pre-game",
}


@router.get("", response_class=HTMLResponse)
async def get_dashboard(request: Request) -> HTMLResponse:
    """Serve the pipeline monitoring dashboard."""
    if _templates is None:
        return HTMLResponse("<h1>Templates not configured</h1>", status_code=500)
    return _templates.TemplateResponse("dashboard.html", {"request": request})


@router.get("/status", response_model=DashboardStatusResponse)
async def get_dashboard_status(
    _: str = Security(verify_pipeline_token),
) -> DashboardStatusResponse:
    """
    Return pipeline health summary and recent job history.

    For each registered pipeline: last run time, status, duration,
    records processed, last success, current running state, and
    consecutive error streak.

    Also returns the last 10 background jobs from the in-memory job manager.
    """
    pipeline_entries, quality_payload, cron_runs = await asyncio.gather(
        asyncio.to_thread(_build_pipeline_health),
        asyncio.to_thread(_build_quality_status),
        asyncio.to_thread(_build_cron_runs),
    )

    job_manager = get_job_manager()
    raw_jobs = await job_manager.list_jobs(limit=10)
    recent_jobs = [
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
        for j in raw_jobs
    ]

    return DashboardStatusResponse(
        status="success",
        message=f"Dashboard status for {len(pipeline_entries)} pipelines",
        data=DashboardStatusData(
            pipelines=pipeline_entries,
            recent_jobs=recent_jobs,
            quality_latest=quality_payload["quality_latest"],
            recent_quality_runs=quality_payload["recent_quality_runs"],
            quality_failed_checks=quality_payload["quality_failed_checks"],
            cron_job_runs=cron_runs,
        ),
    )


def _build_cron_runs() -> list[CronJobRunEntry]:
    """
    Query cron job runs for the dashboard timeline and deployment cards.

    Uses a 6-hour time window for high-frequency jobs (live-stats, pre-game,
    post-game) so they don't crowd out other runs.  Deploy runs are fetched
    separately with a 48-hour window so the Deployments section always shows
    cards — the deploy job only fires once per day at 2 AM CST and would
    otherwise fall outside the 6h window for most of the day.

    Runs synchronously — caller must wrap in asyncio.to_thread.
    """
    try:
        now = datetime.now(timezone.utc)
        window_start = now - timedelta(hours=6)
        deploy_window_start = now - timedelta(hours=48)

        windowed_rows = list(
            CronJobRun.select()
            .where(CronJobRun.triggered_at >= window_start)
            .order_by(CronJobRun.triggered_at.desc())
        )

        # Always include recent deploy runs so the Deployments cards render
        # even when the last deploy happened more than 6 hours ago.
        deploy_rows = list(
            CronJobRun.select()
            .where(
                (CronJobRun.job_name == "deploy")
                & (CronJobRun.triggered_at >= deploy_window_start)
                & (CronJobRun.triggered_at < window_start)
            )
            .order_by(CronJobRun.triggered_at.desc())
            .limit(10)
        )

        rows = windowed_rows + deploy_rows
        return [
            CronJobRunEntry(
                id=str(r.id),
                job_name=r.job_name,
                triggered_at=r.triggered_at,
                completed_at=r.completed_at,
                duration_ms=r.duration_ms,
                duration_seconds=r.duration_seconds,
                result=r.result,
                http_status=r.http_status,
                attempts=r.attempts,
                error_message=r.error_message,
                response_snippet=r.response_snippet,
            )
            for r in rows
        ]
    except Exception as exc:
        log.warning("dashboard_cron_runs_failed", error=str(exc))
        return []


def _build_pipeline_health() -> list[PipelineHealthEntry]:
    """
    Query pipeline_run table for each registered pipeline, supplemented by
    CronJobRun when PipelineRun data is missing or stale.

    PipelineRun is the authoritative source (records actual execution results
    including record counts and error messages). CronJobRun is written by the
    cron-runner after each HTTP trigger and reflects recency accurately even
    when PipelineRun records stop being written (e.g. after a service restart
    that called reset_stale_runs()).

    When CronJobRun.triggered_at is more recent than PipelineRun.started_at,
    CronJobRun data is used for last_run_at and last_status. PipelineRun data
    is still used for error_streak, records_processed, and is_running.

    Runs synchronously — caller must wrap in asyncio.to_thread.
    """
    # Pre-load the latest CronJobRun per job name so we avoid N extra queries.
    cron_job_names = set(PIPELINE_CRON_JOB_MAP.values())
    cron_latest: dict[str, CronJobRun] = {}
    for job_name in cron_job_names:
        row = (
            CronJobRun.select()
            .where(CronJobRun.job_name == job_name)
            .order_by(CronJobRun.triggered_at.desc())
            .first()
        )
        if row:
            cron_latest[job_name] = row

    entries: list[PipelineHealthEntry] = []

    all_pipelines = PIPELINE_REGISTRY
    for name, cls in all_pipelines.items():
        config = cls.config
        # PipelineRun records are written using config.name (set in BasePipeline._run_sync),
        # which may differ from the registry key (e.g. "advanced_stats" vs "player_advanced_stats").
        db_name = config.name
        trigger_endpoint = PIPELINE_TRIGGER_ENDPOINTS.get(name, "")

        # Most-recent run (any status)
        latest_run = (
            PipelineRun.select()
            .where(PipelineRun.pipeline_name == db_name)
            .order_by(PipelineRun.started_at.desc())
            .first()
        )

        # Most-recent successful run
        latest_success = (
            PipelineRun.select()
            .where(
                (PipelineRun.pipeline_name == db_name)
                & (PipelineRun.status == "success")
            )
            .order_by(PipelineRun.completed_at.desc())
            .first()
        )

        # Consecutive failure streak (look at last 10 runs in order)
        recent_runs = list(
            PipelineRun.select(PipelineRun.status)
            .where(PipelineRun.pipeline_name == db_name)
            .order_by(PipelineRun.started_at.desc())
            .limit(10)
        )
        error_streak = 0
        for run in recent_runs:
            if run.status == "failed":
                error_streak += 1
            else:
                break

        is_running = PipelineRun.is_running(db_name)

        # Determine whether CronJobRun data is fresher than PipelineRun.
        # Both tables store naive UTC datetimes — compare directly.
        cron_job_name = PIPELINE_CRON_JOB_MAP.get(name)
        cron_run = cron_latest.get(cron_job_name) if cron_job_name else None

        pipeline_run_at: datetime | None = latest_run.started_at if latest_run else None
        cron_run_at: datetime | None = cron_run.triggered_at if cron_run else None

        use_cron = cron_run is not None and (
            _naive(cron_run_at) is not None
            and (
                _naive(pipeline_run_at) is None
                or _naive(cron_run_at) > _naive(pipeline_run_at)  # type: ignore[operator]
            )
        )

        if use_cron:
            # CronJobRun has fresher data than PipelineRun — use it for recency/status.
            # Map cron result ("success"/"failure") to PipelineRun status vocabulary.
            last_run_at = cron_run_at
            last_status = "success" if cron_run.result == "success" else "failed" if not is_running else None
            last_duration_seconds = cron_run.duration_ms / 1000.0 if cron_run.duration_ms else None
            # Record counts are only available from PipelineRun; show whatever we have.
            last_records_processed = latest_run.records_processed if latest_run else None
            # For last_success_at: prefer cron if it was a success, else fall back to PipelineRun.
            if cron_run.result == "success" and cron_run_at:
                last_success_at = cron_run.completed_at or cron_run_at
            else:
                last_success_at = latest_success.completed_at if latest_success else None
        else:
            last_run_at = pipeline_run_at
            last_status = latest_run.status if latest_run and not is_running else None
            last_duration_seconds = latest_run.duration_seconds if latest_run else None
            last_records_processed = latest_run.records_processed if latest_run else None
            last_success_at = latest_success.completed_at if latest_success else None

        entry = PipelineHealthEntry(
            name=name,
            display_name=config.display_name,
            category=config.category.value,
            trigger_endpoint=trigger_endpoint,
            last_run_at=last_run_at,
            last_status=last_status,
            last_duration_seconds=last_duration_seconds,
            last_records_processed=last_records_processed,
            last_success_at=last_success_at,
            is_running=is_running,
            error_streak=error_streak,
        )
        entries.append(entry)

    return entries


def _build_quality_status() -> dict:
    """
    Build quality run summary payload for the dashboard.

    Returns:
        Dict with keys:
            quality_latest: Optional[QualityRunEntry]
            recent_quality_runs: list[QualityRunEntry]
            quality_failed_checks: list[QualityCheckEntry]
    """
    service = DataQualityService()
    latest: QualityRunEntry | None = None
    failed_checks: list[QualityCheckEntry] = []
    recent: list[QualityRunEntry] = []

    try:
        recent_runs = service.list_runs(limit=5)
        recent = [QualityRunEntry(**r) for r in recent_runs]
        latest = recent[0] if recent else None

        if latest and latest.failed_checks > 0:
            detail = service.get_run(latest.run_id)
            raw_checks = detail.get("checks", []) if detail else []
            for check in raw_checks:
                if check.get("status") == "passed":
                    continue
                failed_checks.append(
                    QualityCheckEntry(
                        check_name=check.get("check_name", "unknown_check"),
                        status=check.get("status", "error"),
                        severity=check.get("severity", "critical"),
                        failures=check.get("failures", 0),
                        message=check.get("message"),
                        duration_ms=check.get("duration_ms"),
                    )
                )
    except Exception as exc:
        log.warning("dashboard_quality_status_failed", error=str(exc))

    return {
        "quality_latest": latest,
        "recent_quality_runs": recent,
        "quality_failed_checks": failed_checks[:10],
    }
