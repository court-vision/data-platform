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
from core.correlation_middleware import CorrelationMiddleware
from core.logging import setup_logging, get_logger
from core.settings import settings
from db.base import init_db, close_db
from api.v1 import pipelines, live, dashboard


@asynccontextmanager
async def lifespan(app: FastAPI):
    setup_logging(
        log_level=settings.log_level,
        json_format=settings.log_format == "json",
        service_name=settings.service_name,
    )
    log = get_logger()
    log.info("application_starting", service=settings.service_name)

    init_db()
    log.info("database_initialized")

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
app.add_middleware(CorrelationMiddleware)
app.add_middleware(DatabaseMiddleware)
setup_middleware(app)

# Templates
_templates = Jinja2Templates(directory=Path(__file__).parent / "templates")
dashboard.set_templates(_templates)

# Routes
app.include_router(pipelines.router, prefix="/v1/internal")
app.include_router(live.router, prefix="/v1/live")
app.include_router(dashboard.router, prefix="/v1")


@app.get("/")
async def root():
    return {"message": "Court Vision Data Platform"}


@app.get("/ping")
async def ping():
    return {"message": "Pong!"}
