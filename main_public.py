"""
Court Vision Data Platform — Public Interface

Serves only the pipeline monitoring dashboard on a public-facing port
(0.0.0.0:$PORT). Pipeline trigger endpoints, live routes, and all
internal APIs remain on the private port (::8001) only.

Started alongside main.py by entrypoint.sh.
"""

# Apply NBA API patch early, before any nba_api imports elsewhere
import utils.patches  # noqa: F401 - imported for side effect (patches nba_api)

from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from fastapi.templating import Jinja2Templates

from core.correlation_middleware import CorrelationMiddleware
from core.db_middleware import DatabaseMiddleware
from core.logging import setup_logging, get_logger
from core.middleware import setup_middleware
from core.settings import settings
from db.base import close_db, init_db
from api.v1 import dashboard


@asynccontextmanager
async def lifespan(app: FastAPI):
    setup_logging(
        log_level=settings.log_level,
        json_format=settings.log_format == "json",
        service_name=settings.service_name,
    )
    log = get_logger()
    log.info("public_interface_starting", service=settings.service_name)

    init_db()
    log.info("public_database_initialized")

    yield

    close_db()
    log.info("public_interface_stopped")


app = FastAPI(
    title="Court Vision Data Platform",
    description="Pipeline monitoring dashboard",
    version="1.0.0",
    lifespan=lifespan,
    # No API docs on the public interface
    docs_url=None,
    redoc_url=None,
)

# Middlewares (order matters — first added = outermost)
app.add_middleware(CorrelationMiddleware)
app.add_middleware(DatabaseMiddleware)
setup_middleware(app)

# Templates
_templates = Jinja2Templates(directory=Path(__file__).parent / "templates")
dashboard.set_templates(_templates)

# Only the dashboard — no pipeline triggers, no live routes
app.include_router(dashboard.router, prefix="/v1")


@app.get("/")
async def root():
    return {"message": "Court Vision Data Platform"}


@app.get("/ping")
async def ping():
    return {"message": "Pong!"}
