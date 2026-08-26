"""
Cron Job Run API Routes

Endpoints for cron-runner execution reports. The cron-runner POSTs a report
after each job completes; the dashboard reads the history for the timeline.

Routes:
    POST /v1/internal/cron/job-runs   — ingest one run report (cron-runner)
    GET  /v1/internal/cron/job-runs   — list recent runs (dashboard)
"""

from fastapi import APIRouter, HTTPException, Query, Security

from core.logging import get_logger
from core.pipeline_auth import verify_pipeline_token
from db.base import run_in_db_thread
from db.models.nba.cron_job_run import CronJobRun
from schemas.cron import CronJobRunCreate, CronJobRunEntry

router = APIRouter(prefix="/cron", tags=["cron"])
log = get_logger("cron_api")


def _row_to_entry(row: CronJobRun) -> CronJobRunEntry:
    return CronJobRunEntry(
        id=str(row.id),
        job_name=row.job_name,
        triggered_at=row.triggered_at,
        completed_at=row.completed_at,
        duration_ms=row.duration_ms,
        duration_seconds=row.duration_seconds,
        result=row.result,
        http_status=row.http_status,
        attempts=row.attempts,
        error_message=row.error_message,
        response_snippet=row.response_snippet,
    )


@router.post("/job-runs", status_code=201)
async def ingest_cron_run(
    body: CronJobRunCreate,
    _: str = Security(verify_pipeline_token),
) -> dict:
    """
    Ingest a cron job execution report from the cron-runner.
    Called fire-and-forget by the cron-runner after each trigger attempt.
    """
    def _write() -> None:
        CronJobRun.create(
            job_name=body.job_name,
            triggered_at=body.triggered_at,
            completed_at=body.completed_at,
            duration_ms=body.duration_ms,
            result=body.result,
            http_status=body.http_status,
            attempts=body.attempts,
            error_message=body.error_message,
            response_snippet=body.response_snippet,
        )

    await run_in_db_thread(_write)
    log.info("cron_run_ingested", job=body.job_name, result=body.result)
    return {"status": "ok"}


@router.get("/job-runs", response_model=list[CronJobRunEntry])
async def list_cron_runs(
    _: str = Security(verify_pipeline_token),
    limit: int = Query(default=200, ge=1, le=500),
    job_name: str | None = Query(default=None),
) -> list[CronJobRunEntry]:
    """
    Return recent cron job runs for the dashboard timeline.
    Optionally filter to a single job by name.
    """
    def _query() -> list[CronJobRun]:
        if job_name:
            return CronJobRun.recent_for_job(job_name, limit=limit)
        return CronJobRun.recent(limit=limit)

    rows = await run_in_db_thread(_query)
    return [_row_to_entry(r) for r in rows]
