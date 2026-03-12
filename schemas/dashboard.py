"""
Dashboard Response Schemas

Pydantic models for the pipeline monitoring dashboard API.
"""

from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field

from schemas.pipeline import PipelineJobInfo
from schemas.cron import CronJobRunEntry


class PipelineHealthEntry(BaseModel):
    """Health status for a single registered pipeline."""

    name: str
    display_name: str
    category: str  # "pre_game" | "live" | "post_game" | "scheduled"
    trigger_endpoint: str  # relative path to POST to trigger, e.g. "/v1/internal/pipelines/daily-player-stats"
    last_run_at: Optional[datetime] = None
    last_status: Optional[str] = None  # "success" | "failed" | None
    last_duration_seconds: Optional[float] = None
    last_records_processed: Optional[int] = None
    last_success_at: Optional[datetime] = None
    is_running: bool = False
    error_streak: int = 0


class QualityRunEntry(BaseModel):
    """Summary of a data quality run."""

    run_id: str
    status: str
    started_at: str
    completed_at: Optional[str] = None
    duration_seconds: Optional[float] = None
    total_checks: int = 0
    passed_checks: int = 0
    failed_checks: int = 0
    triggered_by: Optional[str] = None
    error_message: Optional[str] = None


class QualityCheckEntry(BaseModel):
    """Single failed/errored quality check entry."""

    check_name: str
    status: str
    severity: str
    failures: int = 0
    message: Optional[str] = None
    duration_ms: Optional[int] = None


class DashboardStatusData(BaseModel):
    """Data payload for the dashboard status endpoint."""

    pipelines: list[PipelineHealthEntry]
    recent_jobs: list[PipelineJobInfo]
    quality_latest: Optional[QualityRunEntry] = None
    recent_quality_runs: list[QualityRunEntry] = Field(default_factory=list)
    quality_failed_checks: list[QualityCheckEntry] = Field(default_factory=list)
    cron_job_runs: list[CronJobRunEntry] = Field(default_factory=list)


class DashboardStatusResponse(BaseModel):
    """Response for GET /v1/dashboard/status."""

    status: str
    message: str
    data: DashboardStatusData
