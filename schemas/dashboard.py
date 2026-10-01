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
    # The regular-season game day each cadence is held to; None while nothing is due.
    post_game_due: Optional[date] = None    # last settled game day
    pre_game_due: Optional[date] = None     # last game day whose first tip-off has passed
    next_game_date: Optional[date] = None
    fetched_at: datetime


class FreshnessResponse(ApiModel):
    """Response for GET /v1/dashboard/freshness."""

    status: str
    message: str
    data: FreshnessData


class PipelineInfo(ApiModel):
    """What the registry says about one pipeline: its config, as the page shows it."""

    name: str                    # registry key, the URL segment
    display_name: str
    description: str
    category: str
    target_table: str
    trigger_endpoint: str
    accepts_date: bool
    cron_job: Optional[str] = None            # cron-runner job that fires it
    depends_on: list[str] = Field(default_factory=list)
    allow_concurrent: bool = False
    espn_gated: bool = False                  # post-game: waits for ESPN's scoring period to flip
    earliest_run_time_cst: Optional[str] = None   # post-game: "HH:MM" wall-clock gate
    # pre-game: minutes before first tip, the settings default resolved; None for any other category
    pre_game_window_minutes: Optional[int] = None
    is_running: bool = False


class PipelineRunEntry(ApiModel):
    """One row of nba.pipeline_runs."""

    id: str
    started_at: datetime
    completed_at: Optional[datetime] = None
    # running | stuck | success | failed. `stuck` is not a stored status: it is a
    # row left `running` longer than PipelineRun.is_running counts as live.
    status: str
    duration_seconds: Optional[float] = None
    records_processed: int = 0
    error_message: Optional[str] = None


class RunsSummary(ApiModel):
    """Over the runs returned (a window, newest first), not all time, but for last_success_at."""

    total: int
    succeeded: int
    failed: int
    running: int                                  # live runs only
    stuck: int                                    # left `running` past the cutoff: see PipelineRunEntry.status
    success_rate: Optional[float] = None          # succeeded / finished; None with nothing finished
    median_duration_seconds: Optional[float] = None   # of the successful runs
    max_duration_seconds: Optional[float] = None      # of the successful runs
    last_success_at: Optional[datetime] = None    # all time: a window of failures is not "never"
    oldest_started_at: Optional[datetime] = None  # how far back the window reaches


class PipelineRunsData(ApiModel):
    pipeline: PipelineInfo
    runs: list[PipelineRunEntry]                  # newest first
    summary: RunsSummary
    limit: int
    fetched_at: datetime


class PipelineRunsResponse(ApiModel):
    """Response for GET /v1/dashboard/pipelines/{name}/runs."""

    status: str
    message: str
    data: PipelineRunsData


class QualityCheckInfo(ApiModel):
    """A quality check as it is defined in code: what it asserts and what it guards."""

    name: str
    severity: str                # critical | warning
    group: str                   # structural | timing
    table: str                   # the table it reads, "schema.table"
    # Timing checks: the pipeline whose runs they watch. Structural checks: the
    # registered pipelines that write `table` (none for a framework table).
    pipelines: list[str]
    failure_message: str         # what a failure means
    sql: str                     # the assertion: it counts the offending rows


class QualityCheckRow(QualityCheckInfo):
    """A check with its result in each run of the window."""

    # Aligned with `QualityOverviewData.runs` (newest first): passed | failed |
    # error, or None where that run did not include the check.
    results: list[Optional[str]]


class QualityOverviewData(ApiModel):
    runs: list[QualityRunEntry]          # newest first
    checks: list[QualityCheckRow]        # catalogue order: structural, then timing
    limit: int
    fetched_at: datetime


class QualityOverviewResponse(ApiModel):
    """Response for GET /v1/dashboard/quality."""

    status: str
    message: str
    data: QualityOverviewData


class QualityCheckOutcome(ApiModel):
    """One check's result in one run, with its definition when it still exists."""

    check_name: str
    status: str                  # passed | failed | error
    severity: str
    failures: int = 0
    message: Optional[str] = None
    details: Optional[dict] = None
    duration_ms: Optional[int] = None
    definition: Optional[QualityCheckInfo] = None  # None: the check has since been removed


class QualityRunDetailData(ApiModel):
    run: QualityRunEntry
    checks: list[QualityCheckOutcome]    # what needs a look first: errors, failures, then passes
    older_run_id: Optional[str] = None   # the run before this one, by start time
    newer_run_id: Optional[str] = None
    fetched_at: datetime


class QualityRunDetailResponse(ApiModel):
    """Response for GET /v1/dashboard/quality/runs/{run_id}."""

    status: str
    message: str
    data: QualityRunDetailData
