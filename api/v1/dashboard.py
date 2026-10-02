"""
Pipeline Monitoring Dashboard

The JSON behind the React dashboard (dashboard/, served from / by
main_public). Everything here but the redirect takes the pipeline bearer token.

Routes:
    GET  /v1/dashboard           — redirect to /, where the app lives (no auth)
    GET  /v1/dashboard/status    — pipeline health, cron runs, quality, jobs (token auth)
    GET  /v1/dashboard/services  — running version of each deployed service (token auth)
    GET  /v1/dashboard/freshness — what date each pipeline's table runs through (token auth)
    GET  /v1/dashboard/pipelines/{name}/runs — one pipeline's config and run history (token auth)
    GET  /v1/dashboard/quality   — every check's definition and its result in recent runs (token auth)
    GET  /v1/dashboard/quality/runs/{run_id} — one run: every check's outcome (token auth)
"""

import asyncio
import statistics
import textwrap
import uuid
from datetime import datetime, timedelta, timezone
from typing import Iterable, Optional

import httpx
from fastapi import APIRouter, HTTPException, Query, Security
from fastapi.responses import RedirectResponse

from api.v1.pipelines import router as pipelines_router
from core.health import service_info
from core.job_manager import get_job_manager
from core.logging import get_logger
from core.pipeline_auth import verify_pipeline_token
from core.settings import settings
from db.base import run_in_db_thread
from db.models.pipeline_run import RUNNING_MAX_AGE_MINUTES, PipelineRun
from db.models.nba.cron_job_run import CronJobRun
from pipelines import PIPELINE_REGISTRY, PipelineCategory, PipelineConfig
from schemas.cron import CronJobRunEntry
from schemas.dashboard import (
    DashboardStatusData,
    DashboardStatusResponse,
    FreshnessResponse,
    PipelineInfo,
    PipelineRunEntry,
    PipelineRunsData,
    PipelineRunsResponse,
    RunsSummary,
    PipelineHealthEntry,
    QualityRunEntry,
    QualityCheckEntry,
    QualityCheckInfo,
    QualityCheckOutcome,
    QualityCheckRow,
    QualityOverviewData,
    QualityOverviewResponse,
    QualityRunDetailData,
    QualityRunDetailResponse,
    ServiceInfo,
    ServicesData,
    ServicesResponse,
)
from schemas.pipeline import PipelineJobInfo
from services.data_quality_service import DataQualityService, SQLQualityCheck
from services.freshness_service import build_freshness

router = APIRouter(prefix="/dashboard", tags=["dashboard"])
log = get_logger("dashboard_api")


def _naive(dt: datetime | None) -> datetime | None:
    """Strip timezone info for safe naive-UTC comparisons."""
    if dt is None:
        return None
    return dt.replace(tzinfo=None) if dt.tzinfo else dt

# Where main.py / main_public.py mount api.v1.pipelines.router. A pipeline's own
# trigger route is this plus its config.trigger_slug; the dashboard posts to it.
PIPELINE_ROUTE_PREFIX = "/v1/internal/pipelines"


def trigger_endpoint(config: PipelineConfig) -> str:
    """Path the dashboard POSTs to run one pipeline ("" when it has no route)."""
    if not config.trigger_slug:
        return ""
    return f"{PIPELINE_ROUTE_PREFIX}/{config.trigger_slug}"


def trigger_accepts_date(config: PipelineConfig) -> bool:
    """Whether the pipeline's trigger route takes ?date= — read off the route
    itself, so the dashboard's date box appears exactly where a backfill can go."""
    if not config.trigger_slug:
        return False
    path = f"/pipelines/{config.trigger_slug}"  # the router's own prefix
    for route in pipelines_router.routes:
        if getattr(route, "path", None) != path:
            continue
        if "POST" not in (getattr(route, "methods", None) or ()):
            continue
        return any(param.name == "date" for param in route.dependant.query_params)
    return False


@router.get("", include_in_schema=False)
async def get_dashboard() -> RedirectResponse:
    """Where the Jinja dashboard was until 2026-09. Bookmarks land on the app."""
    return RedirectResponse(url="/", status_code=307)


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
        run_in_db_thread(_build_pipeline_health),
        run_in_db_thread(_build_quality_status),
        run_in_db_thread(_build_cron_runs),
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


BACKEND_HEALTH_TIMEOUT_S = 3.0


@router.get("/services", response_model=ServicesResponse)
async def get_services(
    _: str = Security(verify_pipeline_token),
) -> ServicesResponse:
    """
    The running version of each deployed service, for the dashboard's service
    cards: this process from its own settings, the backend from its /health
    over Railway's private network. Replaces the Deployments section, which
    read a nightly `deploy` cron job that no longer exists.
    """
    # Reads no database, and gets no connection (NO_DB_V1_PATHS in
    # core/db_middleware.py), so the cards still answer in a Postgres outage.
    # A query added here has to come off that list first.
    own = service_info()
    services = [
        ServiceInfo(
            key="data_platform",
            name="Data Platform",
            ok=True,
            version=own["version"],
            environment=own["environment"],
            uptime_s=own["uptime_s"],
        ),
        await asyncio.to_thread(_probe_backend),
    ]
    return ServicesResponse(
        status="success",
        message=f"{len(services)} services",
        data=ServicesData(services=services, fetched_at=datetime.now(timezone.utc)),
    )


@router.get("/freshness", response_model=FreshnessResponse)
async def get_freshness(
    _: str = Security(verify_pipeline_token),
) -> FreshnessResponse:
    """
    What date each pipeline's table runs through and when it was last written,
    judged against the season calendar and the last settled game date. Its own
    route, so its per-table queries never slow the 30 s status poll.
    """
    data = await run_in_db_thread(build_freshness)
    stale = sum(1 for table in data.tables if table.state == "stale")
    return FreshnessResponse(
        status="success",
        message=f"{len(data.tables)} tables, {stale} stale",
        data=data,
    )


def _probe_backend() -> ServiceInfo:
    """GET /health on the backend. A 503 (degraded) still carries the body."""
    base = (settings.backend_internal_url or "").rstrip("/")
    if not base:
        return ServiceInfo(
            key="backend", name="Backend", configured=False,
            error="BACKEND_INTERNAL_URL is not set",
        )
    try:
        response = httpx.get(f"{base}/health", timeout=BACKEND_HEALTH_TIMEOUT_S)
        return _backend_card(response)
    except Exception as exc:
        return ServiceInfo(key="backend", name="Backend", error=type(exc).__name__)


def _backend_card(response: httpx.Response) -> ServiceInfo:
    """The backend's card from its /health answer. Raises on a body that is not
    one (not JSON, a mistyped field): _probe_backend makes that the card's error,
    so a strange answer costs the backend its card, not this endpoint a 500."""
    body = response.json()
    if not isinstance(body, dict):
        return ServiceInfo(
            key="backend", name="Backend",
            error=f"HTTP {response.status_code}: not a /health body",
        )

    failing = [
        name for name, check in (body.get("checks") or {}).items()
        if isinstance(check, dict) and not check.get("ok")
    ]
    ok = body.get("status") == "ok"
    return ServiceInfo(
        key="backend",
        name="Backend",
        ok=ok,
        version=body.get("version"),
        environment=body.get("environment"),
        uptime_s=body.get("uptime_s"),
        error=None if ok else f"degraded: {', '.join(failing) or f'HTTP {response.status_code}'}",
    )


RUNS_DEFAULT_LIMIT = 50
RUNS_MAX_LIMIT = 200


@router.get("/pipelines/{name}/runs", response_model=PipelineRunsResponse)
async def get_pipeline_runs(
    name: str,
    limit: int = Query(RUNS_DEFAULT_LIMIT, ge=1, le=RUNS_MAX_LIMIT, description="Newest runs to return"),
    _: str = Security(verify_pipeline_token),
) -> PipelineRunsResponse:
    """
    One pipeline's page: what the registry says about it (trigger, cron job,
    gates, dependencies) and its newest runs with a summary over that window.
    """
    if name not in PIPELINE_REGISTRY:
        raise HTTPException(status_code=404, detail=f"Unknown pipeline '{name}'")
    data = await run_in_db_thread(_build_runs, name, limit)
    return PipelineRunsResponse(
        status="success",
        message=f"{len(data.runs)} runs of {name}",
        data=data,
    )


def pipeline_info(name: str, *, is_running: bool = False) -> PipelineInfo:
    """
    The registry entry as the page shows it: only what something acts on.
    config.timeout_seconds is left out because nothing enforces it, and the
    pre-game window is the one the gate uses, default included.
    """
    config = PIPELINE_REGISTRY[name].config
    pre_game = config.category == PipelineCategory.PRE_GAME
    return PipelineInfo(
        name=name,
        display_name=config.display_name,
        description=config.description,
        category=config.category.value,
        target_table=config.target_table,
        trigger_endpoint=trigger_endpoint(config),
        accepts_date=trigger_accepts_date(config),
        cron_job=config.cron_job_name,
        depends_on=list(config.depends_on),
        allow_concurrent=config.allow_concurrent,
        espn_gated=config.espn_gated,
        earliest_run_time_cst=(
            config.earliest_run_time_cst.strftime("%H:%M") if config.earliest_run_time_cst else None
        ),
        # As trigger_pre_game resolves it.
        pre_game_window_minutes=(
            (config.pre_game_window_minutes or settings.pre_game_window_minutes) if pre_game else None
        ),
        is_running=is_running,
    )


def _utc(dt: datetime) -> datetime:
    """Aware, reading a naive value as UTC. The migrated table's timestamps are
    timestamptz and come back aware; one created from the model (tests) is naive."""
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def run_status(status: str, started_at: datetime, stale_before: datetime) -> str:
    """
    A row's status as the page shows it. `stuck` is a row still marked running
    after PipelineRun.is_running stopped counting it as live: the run hung or
    was cut off, and nothing rewrites the row until the service restarts.
    """
    if status == "running" and _utc(started_at) < _utc(stale_before):
        return "stuck"
    return status


def summarize_runs(runs: list[PipelineRunEntry], *, last_success_at: Optional[datetime] = None) -> RunsSummary:
    """
    Counts and durations over the window. Pure: tested without a database.

    Durations are those of the runs that worked. A failed run's is how long it
    took to fail, and one swept at startup (PipelineRun.reset_stale_runs) ends
    at the next boot, hours after the work stopped.

    `last_success_at` is the caller's, from all time: a window of nothing but
    failures does not mean the pipeline has never succeeded.
    """
    finished = [r for r in runs if r.status in ("success", "failed")]
    succeeded = [r for r in finished if r.status == "success"]
    durations = [r.duration_seconds for r in succeeded if r.duration_seconds is not None]
    return RunsSummary(
        total=len(runs),
        succeeded=len(succeeded),
        failed=len(finished) - len(succeeded),
        running=sum(1 for r in runs if r.status == "running"),
        stuck=sum(1 for r in runs if r.status == "stuck"),
        success_rate=(len(succeeded) / len(finished)) if finished else None,
        median_duration_seconds=statistics.median(durations) if durations else None,
        max_duration_seconds=max(durations) if durations else None,
        last_success_at=last_success_at,
        oldest_started_at=min((r.started_at for r in runs), default=None),
    )


def _build_runs(name: str, limit: int) -> PipelineRunsData:
    """Runs synchronously — caller must wrap in run_in_db_thread."""
    # Rows are keyed by config.name, which can differ from the registry key.
    db_name = PIPELINE_REGISTRY[name].config.name
    rows = (
        PipelineRun.select()
        .where(PipelineRun.pipeline_name == db_name)
        .order_by(PipelineRun.started_at.desc())
        .limit(limit)
    )
    # The cutoff PipelineRun.is_running applies, so a row is `running` here
    # exactly when it would hold the pipeline's is_running true.
    stale_before = datetime.now(timezone.utc) - timedelta(minutes=RUNNING_MAX_AGE_MINUTES)
    runs = [
        PipelineRunEntry(
            id=str(r.id),
            started_at=r.started_at,
            completed_at=r.completed_at,
            status=run_status(r.status, r.started_at, stale_before),
            duration_seconds=r.duration_seconds,
            records_processed=r.records_processed or 0,
            error_message=r.error_message,
        )
        for r in rows
    ]
    # All time, as the Overview row reads it: not only the rows above.
    latest_success = PipelineRun.get_latest_successful(db_name)
    last_success_at = (latest_success.completed_at or latest_success.started_at) if latest_success else None
    return PipelineRunsData(
        pipeline=pipeline_info(name, is_running=PipelineRun.is_running(db_name)),
        runs=runs,
        summary=summarize_runs(runs, last_success_at=last_success_at),
        limit=limit,
        fetched_at=datetime.now(timezone.utc),
    )


QUALITY_DEFAULT_LIMIT = 20
QUALITY_MAX_LIMIT = 100


@router.get("/quality", response_model=QualityOverviewResponse)
async def get_quality(
    limit: int = Query(QUALITY_DEFAULT_LIMIT, ge=1, le=QUALITY_MAX_LIMIT, description="Newest runs to return"),
    _: str = Security(verify_pipeline_token),
) -> QualityOverviewResponse:
    """
    The quality page: every check as it is defined (what it asserts, the table
    and pipelines it guards, its SQL) with its result in each of the newest
    runs, so a failure can be read as "new tonight" or "failing for a week".
    """
    data = await run_in_db_thread(_build_quality_overview, limit)
    return QualityOverviewResponse(
        status="success",
        message=f"{len(data.checks)} checks over {len(data.runs)} runs",
        data=data,
    )


@router.get("/quality/runs/{run_id}", response_model=QualityRunDetailResponse)
async def get_quality_run(
    run_id: str,
    _: str = Security(verify_pipeline_token),
) -> QualityRunDetailResponse:
    """One run: every check's outcome, not only the failures, each with its definition."""
    try:
        # Postgres is asked for the id as Python read it, not as it was typed:
        # uuid.UUID also takes "0x…", a sign, "_" and padding, none of which cast.
        canonical = str(uuid.UUID(run_id))
    except ValueError:
        # Not an id at all. Asked of Postgres it would be a cast error, a 500.
        raise HTTPException(status_code=404, detail=f"Quality run '{run_id}' not found")
    data = await run_in_db_thread(_build_quality_run, canonical)
    if data is None:
        raise HTTPException(status_code=404, detail=f"Quality run '{run_id}' not found")
    return QualityRunDetailResponse(
        status="success",
        message=f"{len(data.checks)} checks, {data.run.failed_checks} failed",
        data=data,
    )


def table_writers() -> dict[str, list[str]]:
    """Table -> the registered pipelines that write it, in registry order."""
    writers: dict[str, list[str]] = {}
    for name, cls in PIPELINE_REGISTRY.items():
        writers.setdefault(cls.config.target_table, []).append(name)
    return writers


def quality_check_info(
    check: SQLQualityCheck, writers: Optional[dict[str, list[str]]] = None
) -> QualityCheckInfo:
    """A check's definition as the page shows it. A timing check watches one
    pipeline; a structural check guards a table, so its pipelines are that
    table's writers."""
    writers = table_writers() if writers is None else writers
    pipelines = [check.pipeline] if check.pipeline else writers.get(check.table, [])
    return QualityCheckInfo(
        name=check.name,
        severity=check.severity,
        group=check.group,
        table=check.table,
        pipelines=list(pipelines),
        failure_message=check.failure_message,
        sql=textwrap.dedent(check.sql).strip(),
    )


def quality_matrix(
    checks: Iterable[SQLQualityCheck],
    runs: list[QualityRunEntry],
    results: dict[str, dict[str, str]],
) -> list[QualityCheckRow]:
    """One row per defined check, its results lined up with `runs`. Pure."""
    writers = table_writers()
    return [
        QualityCheckRow(
            **quality_check_info(check, writers).model_dump(),
            results=[results.get(check.name, {}).get(run.run_id) for run in runs],
        )
        for check in checks
    ]


_OUTCOME_RANK = {"error": 0, "failed": 1}
_SEVERITY_RANK = {"critical": 0, "warning": 1}


def order_outcomes(outcomes: list[QualityCheckOutcome]) -> list[QualityCheckOutcome]:
    """What needs a look first: checks that could not run, then failures
    (critical before warning), then passes; the run's own order within each."""
    return sorted(
        outcomes,
        key=lambda outcome: (
            _OUTCOME_RANK.get(outcome.status, 2),
            _SEVERITY_RANK.get(outcome.severity, 2) if outcome.status != "passed" else 0,
        ),
    )


def _build_quality_overview(limit: int) -> QualityOverviewData:
    """Runs synchronously — caller must wrap in run_in_db_thread."""
    service = DataQualityService()
    runs = [QualityRunEntry(**run) for run in service.list_runs(limit)]
    results = service.results_for_runs([run.run_id for run in runs])
    return QualityOverviewData(
        runs=runs,
        checks=quality_matrix(service.checks(), runs, results),
        limit=limit,
        fetched_at=datetime.now(timezone.utc),
    )


def _build_quality_run(run_id: str) -> Optional[QualityRunDetailData]:
    """Runs synchronously — caller must wrap in run_in_db_thread."""
    service = DataQualityService()
    detail = service.get_run(run_id)
    if detail is None:
        return None
    definitions = {check.name: check for check in service.checks()}
    writers = table_writers()
    outcomes = [
        QualityCheckOutcome(
            **check,
            definition=(
                quality_check_info(definitions[check["check_name"]], writers)
                if check["check_name"] in definitions else None
            ),
        )
        for check in detail["checks"]
    ]
    older, newer = service.neighbours(run_id)
    return QualityRunDetailData(
        run=QualityRunEntry(**{key: value for key, value in detail.items() if key != "checks"}),
        checks=order_outcomes(outcomes),
        older_run_id=older,
        newer_run_id=newer,
        fetched_at=datetime.now(timezone.utc),
    )


def _build_cron_runs() -> list[CronJobRunEntry]:
    """
    Cron job runs for the dashboard's scheduler timeline: the last 6 hours,
    newest first. (A 48-hour side query for the nightly `deploy` job used to
    live here; that job went with the 2026-09-06 move to deploy-on-merge.)

    Runs synchronously — caller must wrap in asyncio.to_thread.
    """
    try:
        window_start = datetime.now(timezone.utc) - timedelta(hours=6)
        rows = list(
            CronJobRun.select()
            .where(CronJobRun.triggered_at >= window_start)
            .order_by(CronJobRun.triggered_at.desc())
        )
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
    # Category-triggered pipelines share a job: cron-runner fires one endpoint
    # that runs every pipeline in the group.
    cron_job_names = {
        cls.config.cron_job_name for cls in PIPELINE_REGISTRY.values()
    } - {None}
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
        cron_job_name = config.cron_job_name
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
            # A finished cron run is always "success" or "failed" (never None); is_running is reported separately.
            last_status = "success" if cron_run.result == "success" else "failed"
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
            trigger_endpoint=trigger_endpoint(config),
            last_run_at=last_run_at,
            last_status=last_status,
            last_duration_seconds=last_duration_seconds,
            last_records_processed=last_records_processed,
            last_success_at=last_success_at,
            is_running=is_running,
            error_streak=error_streak,
            accepts_date=trigger_accepts_date(config),
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
