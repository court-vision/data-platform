"""
Court Vision Data Platform — Public Interface

Serves the pipeline monitoring dashboard on a public-facing port
(0.0.0.0:$PORT): the React app at / (core/spa.py) and, beside it, the routes
the dashboard calls, all token-authed: its status API, the pipeline triggers
and the quality checks. Live routes, cron reporting and the API docs stay on
the private port (::8001) only.

Started alongside main.py by entrypoint.sh. `GET /health` here also probes
the private process, so Railway / Better Stack (which only see this port)
notice when the pipeline server is down.
"""

# Apply NBA API patch early, before nba_api imports elsewhere
import utils.patches  # noqa: F401 - imported for side effect (patches nba_api)

import asyncio
import time
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI

from core.correlation_middleware import RequestContextMiddleware
from core.db_middleware import DatabaseMiddleware
from core.health import health_response
from core.logging import setup_logging, get_logger
from core.middleware import setup_middleware
from core.settings import settings
from core.spa import DIST, mount_dashboard
from core.telemetry import init_sentry
from db.base import close_db, init_db
from api.v1 import dashboard, pipelines, quality

# Sentry must be initialised before the app exists so its ASGI integration wraps it.
# No SENTRY_DSN (dev, tests) -> nothing happens.
init_sentry(settings, process="public")

PRIVATE_HEALTH_TIMEOUT_S = 2.0


@asynccontextmanager
async def lifespan(app: FastAPI):
    setup_logging(
        log_level=settings.log_level,
        json_format=settings.log_format == "json",
        service_name=settings.service_name,
        version=settings.version,
    )
    log = get_logger()
    log.info("public_interface_starting", service=settings.service_name, process="public",
             version=settings.version, environment=settings.environment)

    init_db()
    log.info("public_database_initialized")

    if DASHBOARD_MOUNTED:
        log.info("dashboard_mounted", path=str(DIST))
    else:
        log.warning("dashboard_dist_missing", path=str(DIST))

    yield

    close_db()
    log.info("public_interface_stopped")


app = FastAPI(
    title="Court Vision Data Platform",
    description="Pipeline monitoring dashboard",
    version="1.0.0",
    lifespan=lifespan,
    # No API docs on the public interface. openapi_url too: /openapi.json alone
    # lists every trigger route. app.openapi() still builds the schema offline
    # (scripts/export_openapi.py).
    docs_url=None,
    redoc_url=None,
    openapi_url=None,
)

# Middlewares (order matters — first added = outermost)
app.add_middleware(RequestContextMiddleware)  # correlation id + one http_request log line
app.add_middleware(DatabaseMiddleware)        # per-request connection on the loop thread
setup_middleware(app)                         # exception handlers + CORS

# Dashboard + pipeline triggers (triggers are token-authed via verify_pipeline_token)
app.include_router(dashboard.router, prefix="/v1")
app.include_router(pipelines.router, prefix="/v1/internal")
app.include_router(quality.router, prefix="/v1/internal")


async def private_server_check() -> dict:
    """`GET /health` on the private process (main.py) from this one."""
    url = settings.private_health_url
    started = time.perf_counter()
    try:
        response = await asyncio.to_thread(httpx.get, url, timeout=PRIVATE_HEALTH_TIMEOUT_S)
    except Exception as exc:
        return {"ok": False, "error": type(exc).__name__}
    latency_ms = round((time.perf_counter() - started) * 1000, 1)
    ok = response.status_code == 200
    result = {"ok": ok, "latency_ms": latency_ms, "status_code": response.status_code}
    if not ok:
        result["error"] = f"HTTP {response.status_code}"
    return result


# Liveness only; never touches the database
@app.get("/ping")
async def ping():
    return {"message": "Pong!"}


# Readiness for the whole service: this process's database + the private pipeline server.
@app.get("/health")
async def health():
    return await health_response({"private_server": await private_server_check()})


# The React dashboard at / — last, because its fallback route matches every
# path. Without a build (tests, a Python-only checkout) / keeps its JSON reply,
# out of the schema so the export does not depend on whether dist/ exists.
DASHBOARD_MOUNTED = mount_dashboard(app)
if not DASHBOARD_MOUNTED:

    @app.get("/", include_in_schema=False)
    async def root():
        return {"message": "Court Vision Data Platform"}
