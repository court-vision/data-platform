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
from typing import Optional

from fastapi import APIRouter, Request, Security
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from core.job_manager import get_job_manager
from core.logging import get_logger
from core.pipeline_auth import verify_pipeline_token
from db.models.pipeline_run import PipelineRun
from pipelines import PIPELINE_REGISTRY, LIVE_PIPELINE_REGISTRY
from schemas.dashboard import DashboardStatusData, DashboardStatusResponse, PipelineHealthEntry
from schemas.pipeline import PipelineJobInfo

router = APIRouter(prefix="/dashboard", tags=["dashboard"])
log = get_logger("dashboard_api")

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
    pipeline_entries = await asyncio.to_thread(_build_pipeline_health)

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
        ),
    )


def _build_pipeline_health() -> list[PipelineHealthEntry]:
    """
    Query pipeline_run table for each registered pipeline.
    Runs synchronously — caller must wrap in asyncio.to_thread.
    """
    entries: list[PipelineHealthEntry] = []

    all_pipelines = {**PIPELINE_REGISTRY, **LIVE_PIPELINE_REGISTRY}
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

        entry = PipelineHealthEntry(
            name=name,
            display_name=config.display_name,
            trigger_endpoint=trigger_endpoint,
            last_run_at=latest_run.started_at if latest_run else None,
            last_status=latest_run.status if latest_run and not is_running else None,
            last_duration_seconds=latest_run.duration_seconds if latest_run else None,
            last_records_processed=latest_run.records_processed if latest_run else None,
            last_success_at=latest_success.completed_at if latest_success else None,
            is_running=is_running,
            error_streak=error_streak,
        )
        entries.append(entry)

    return entries
