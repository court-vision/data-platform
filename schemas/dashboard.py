"""
Dashboard Response Schemas

Pydantic models for the pipeline monitoring dashboard API.
"""

from datetime import datetime
from typing import Optional

from pydantic import BaseModel

from schemas.pipeline import PipelineJobInfo


class PipelineHealthEntry(BaseModel):
    """Health status for a single registered pipeline."""

    name: str
    display_name: str
    trigger_endpoint: str  # relative path to POST to trigger, e.g. "/v1/internal/pipelines/daily-player-stats"
    last_run_at: Optional[datetime] = None
    last_status: Optional[str] = None  # "success" | "failed" | None
    last_duration_seconds: Optional[float] = None
    last_records_processed: Optional[int] = None
    last_success_at: Optional[datetime] = None
    is_running: bool = False
    error_streak: int = 0


class DashboardStatusData(BaseModel):
    """Data payload for the dashboard status endpoint."""

    pipelines: list[PipelineHealthEntry]
    recent_jobs: list[PipelineJobInfo]


class DashboardStatusResponse(BaseModel):
    """Response for GET /v1/dashboard/status."""

    status: str
    message: str
    data: DashboardStatusData
