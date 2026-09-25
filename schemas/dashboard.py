"""
Dashboard Response Schemas

Pydantic models for the pipeline monitoring dashboard API.
"""

from datetime import date, datetime
from typing import Literal, Optional

from pydantic import Field

from schemas.common import ApiModel
from schemas.pipeline import PipelineJobInfo
from schemas.cron import CronJobRunEntry


class PipelineHealthEntry(ApiModel):
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
    # The trigger route takes ?date=YYYY-MM-DD (a backfill). Most do; the live,
    # lineup-alerts and playoffs routes run for "now" only.
    accepts_date: bool = False


class QualityRunEntry(ApiModel):
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


class QualityCheckEntry(ApiModel):
    """Single failed/errored quality check entry."""

    check_name: str
    status: str
    severity: str
    failures: int = 0
    message: Optional[str] = None
    duration_ms: Optional[int] = None


class DashboardStatusData(ApiModel):
    """Data payload for the dashboard status endpoint."""

    pipelines: list[PipelineHealthEntry]
    recent_jobs: list[PipelineJobInfo]
    quality_latest: Optional[QualityRunEntry] = None
    recent_quality_runs: list[QualityRunEntry] = Field(default_factory=list)
    quality_failed_checks: list[QualityCheckEntry] = Field(default_factory=list)
    cron_job_runs: list[CronJobRunEntry] = Field(default_factory=list)


class DashboardStatusResponse(ApiModel):
    """Response for GET /v1/dashboard/status."""

    status: str
    message: str
    data: DashboardStatusData


class ServiceInfo(ApiModel):
    """One deployed service, as its own /health reports it."""

    key: str  # "data_platform" | "backend"
    name: str
    configured: bool = True  # False: no URL to ask (local dev)
    ok: bool = False
    version: Optional[str] = None  # git SHA[:7], "dev" locally
    environment: Optional[str] = None
    uptime_s: Optional[int] = None
    error: Optional[str] = None


class ServicesData(ApiModel):
    services: list[ServiceInfo]
    fetched_at: datetime


class ServicesResponse(ApiModel):
    """Response for GET /v1/dashboard/services."""

    status: str
    message: str
    data: ServicesData


class TableWriter(ApiModel):
    """A registered pipeline that writes a table."""

    name: str          # registry key
    display_name: str


class TableFreshness(ApiModel):
    """What date one table runs through, when it was last written, and the verdict."""

    table: str                              # "nba.player_game_stats"
    pipelines: list[TableWriter]            # its writers, registry order
    category: str                           # the most time-critical writer's
    date_column: Optional[str] = None       # the business date: game_date, as_of_date, ...
    latest_date: Optional[date] = None
    write_column: Optional[str] = None      # updated_at, created_at, ...
    latest_written_at: Optional[datetime] = None
    rows_estimate: Optional[int] = None     # planner statistics, not a count
    # The game date a nightly table was expected to run through when judged.
    expected_date: Optional[date] = None
    state: Literal["fresh", "stale", "idle", "empty", "unjudged", "error"]
    error: Optional[str] = None


class FreshnessData(ApiModel):
    tables: list[TableFreshness]
    season: str
    phase: Literal["preseason", "regular", "offseason"]
    today: date                             # the ET calendar date
    # The last game date whose post-game deadline (6 AM ET next morning) has passed.
    settled_through: date
    last_game_date: Optional[date] = None   # last final game on or before settled_through
    next_game_date: Optional[date] = None
    fetched_at: datetime


class FreshnessResponse(ApiModel):
    """Response for GET /v1/dashboard/freshness."""

    status: str
    message: str
    data: FreshnessData
