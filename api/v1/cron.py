"""
Cron Job Run API Routes

Endpoints for cron-runner execution reports. The cron-runner POSTs a report
after each job completes; the dashboard reads the history for the timeline.

Routes:
    POST /v1/internal/cron/job-runs   — ingest one run report (cron-runner)
    GET  /v1/internal/cron/job-runs   — list recent runs (dashboard)

Alerting (`cron_failure_streak`): after a failure report the job's consecutive
failures are counted from its last STREAK_LOOKBACK rows; the ops webhook gets
one critical alert exactly when the streak reaches the job's threshold
(`settings.alert_cron_streak_thresholds`; unknown jobs use the default) and a
"recovered" note when a success follows a streak of at least that length.
"""

from datetime import timedelta
from typing import Iterable, Optional

from fastapi import APIRouter, HTTPException, Query, Security

from core.logging import get_logger
from core.pipeline_auth import verify_pipeline_token
from db.base import run_in_db_thread
from db.models.nba.cron_job_run import CronJobRun
from schemas.cron import CronJobRunCreate, CronJobRunEntry
from services.alert_service import AlertEvent, cron_streak_threshold, get_alert_service

router = APIRouter(prefix="/cron", tags=["cron"])
log = get_logger("cron_api")

STREAK_LOOKBACK = 10
STREAK_ALERT_DEDUPE = timedelta(hours=6)


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


def consecutive_failures(results: Iterable[str]) -> int:
    """Length of the failure run at the head of a newest-first result list."""
    streak = 0
    for result in results:
        if result == "success":
            break
        streak += 1
    return streak


def streak_key(job_name: str) -> str:
    return f"cron_failure_streak:{job_name}"


def _alert_on_streak(body: CronJobRunCreate, recent_results: list[str]) -> None:
    """Runs on the worker thread right after the row is written (`recent_results` includes it)."""
    threshold = cron_streak_threshold(body.job_name)
    alerts = get_alert_service()

    if body.result == "success":
        # The new success is the newest row; the streak it ended is everything after it.
        prior_streak = consecutive_failures(recent_results[1:])
        if prior_streak >= threshold:
            log.info("cron_streak_recovered", job=body.job_name, streak=prior_streak, threshold=threshold)
            alerts.recovered(
                streak_key(body.job_name),
                title=f"Cron job recovered: {body.job_name}",
                body=f"`{body.job_name}` succeeded after {prior_streak} consecutive failure(s).",
                fields={"job": body.job_name, "failures_before": prior_streak, "http_status": body.http_status},
            )
        return

    streak = consecutive_failures(recent_results)
    if streak >= threshold:
        log.warning("cron_failure_streak", job=body.job_name, streak=streak, threshold=threshold,
                    http_status=body.http_status, error=body.error_message)
    if streak != threshold:
        return
    alerts.notify(AlertEvent(
        key=streak_key(body.job_name),
        severity="critical",
        title=f"Cron job failing: {body.job_name} ({streak} in a row)",
        body=(body.error_message or "no error message")[:500],
        fields={
            "job": body.job_name,
            "streak": streak,
            "threshold": threshold,
            "http_status": body.http_status,
            "attempts": body.attempts,
            "triggered_at": body.triggered_at.isoformat(),
            "response": (body.response_snippet or "")[:200] or None,
        },
        dedupe=STREAK_ALERT_DEDUPE,
    ))


@router.post("/job-runs", status_code=201)
async def ingest_cron_run(
    body: CronJobRunCreate,
    _: str = Security(verify_pipeline_token),
) -> dict:
    """
    Ingest a cron job execution report from the cron-runner.
    Called fire-and-forget by the cron-runner after each trigger attempt.
    """
    def _write() -> list[str]:
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
        return [row.result for row in CronJobRun.recent_for_job(body.job_name, limit=STREAK_LOOKBACK)]

    recent_results = await run_in_db_thread(_write)
    log.info("cron_run_ingested", job=body.job_name, result=body.result)

    try:
        await run_in_db_thread(_alert_on_streak, body, recent_results)
    except Exception as exc:  # alerting never fails the ingest
        log.warning("cron_streak_alert_failed", job=body.job_name, error=type(exc).__name__)

    return {"status": "ok"}


@router.get("/job-runs", response_model=list[CronJobRunEntry])
async def list_cron_runs(
    _: str = Security(verify_pipeline_token),
    limit: int = Query(default=200, ge=1, le=500),
    job_name: Optional[str] = Query(default=None),
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
