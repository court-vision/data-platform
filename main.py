"""
Court Vision Data Platform

Standalone ETL service that runs all data pipelines for the Court Vision
fantasy basketball platform. Triggered by cron-runner instances and writes
to the shared PostgreSQL database via Railway's private network.

No user-facing routes — this service is only called by the cron-runner.

Usage:
    uvicorn main:app --host 0.0.0.0 --port 8001
"""

# Apply NBA API patch early, before any nba_api imports elsewhere
import utils.patches  # noqa: F401 - imported for side effect (patches nba_api)

from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from fastapi.templating import Jinja2Templates

from core.middleware import setup_middleware
from core.db_middleware import DatabaseMiddleware
from core.correlation_middleware import RequestContextMiddleware
from core.health import health_response
from core.logging import setup_logging, get_logger
from core.settings import settings
from core.telemetry import init_sentry
from db.base import init_db, close_db
from services.schedule_service import assert_calendar_available
from api.v1 import pipelines, live, dashboard, quality, cron

# Sentry must be initialised before the app exists so its ASGI integration wraps it.
# No SENTRY_DSN (dev, tests) -> nothing happens.
init_sentry(settings, process="private")


@asynccontextmanager
async def lifespan(app: FastAPI):
    setup_logging(
        log_level=settings.log_level,
        json_format=settings.log_format == "json",
        service_name=settings.service_name,
        version=settings.version,
    )
    log = get_logger()
    log.info("application_starting", service=settings.service_name, process="private",
             version=settings.version, environment=settings.environment)

    init_db()
    log.info("database_initialized")

    # The season's fantasy calendar ships with the image (static/schedule{yy}-{yy}.json
    # + matchupsPerDay{yy}-{yy}.json). Warn rather than fail: most pipelines don't
    # need it, and the ones that do raise a clear FileNotFoundError when they run.
    try:
        assert_calendar_available()
    except FileNotFoundError as e:
        log.warning("calendar_unavailable", season=settings.nba_season, error=str(e))

    from db.models.pipeline_run import PipelineRun
    reset_count = PipelineRun.reset_stale_runs()
    if reset_count:
        log.warning("stale_runs_reset", count=reset_count)

    yield

    close_db()
    log.info("application_stopped")


app = FastAPI(
    title="Court Vision Data Platform",
    description="ETL pipeline service for Court Vision fantasy basketball analytics",
    version="1.0.0",
    lifespan=lifespan,
    docs_url="/docs",
    redoc_url="/redoc",
)

# Middlewares (order matters — first added = outermost)
app.add_middleware(RequestContextMiddleware)  # correlation id + one http_request log line
app.add_middleware(DatabaseMiddleware)        # per-request connection on the loop thread
setup_middleware(app)                         # exception handlers + CORS

# Templates
_templates = Jinja2Templates(directory=Path(__file__).parent / "templates")
dashboard.set_templates(_templates)

# Routes
app.include_router(pipelines.router, prefix="/v1/internal")
app.include_router(quality.router, prefix="/v1/internal")
app.include_router(cron.router, prefix="/v1/internal")
app.include_router(live.router, prefix="/v1/live")
app.include_router(dashboard.router, prefix="/v1")


@app.get("/")
async def root():
    return {"message": "Court Vision Data Platform"}


# Liveness only; never touches the database
@app.get("/ping")
async def ping():
    return {"message": "Pong!"}


# Readiness: database + calendar. 503 "degraded" when the database is unreachable.
@app.get("/health")
async def health():
    return await health_response()
