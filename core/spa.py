"""
Serve the React dashboard from the public app.

`dashboard/` is a Vite app; its build output, `dashboard/dist`, is static
files. The Docker image always contains it (a build stage produces it). A
Python-only checkout does not, and then nothing is mounted: the test suite and
a backend-only dev loop must never need bun.

What is served:
    /assets/*       Vite's content-hashed bundles and fonts — cached forever
    /<file>         a real file at the dist root (favicon.svg)
    anything else   index.html, so a hard refresh on a client-side route
                    (/pipelines/player_game_stats) lands back in the app

What is never answered with index.html: /v1/*, the probes, and a missing
asset. An unknown API path stays a JSON 404, and a stale bundle URL fails as a
404 rather than as HTML with a JavaScript MIME error.
"""

from pathlib import Path

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import FileResponse
from starlette.staticfiles import StaticFiles

DIST = Path(__file__).resolve().parent.parent / "dashboard" / "dist"

IMMUTABLE = "public, max-age=31536000, immutable"
REVALIDATE = "no-cache"

NOT_APP_PREFIXES = ("v1/", "assets/")
NOT_APP_PATHS = frozenset({"v1", "assets", "health", "ping", "openapi.json", "docs", "redoc"})

# Registered for every method so an unknown POST stays a 404. With GET alone,
# Starlette would see a path match and answer 405, which reads as "that trigger
# route exists" to whoever mistyped it.
ALL_METHODS = ["GET", "HEAD", "POST", "PUT", "PATCH", "DELETE"]


class ImmutableStaticFiles(StaticFiles):
    """Vite names assets by content hash, so a URL's bytes never change."""

    async def get_response(self, path: str, scope):
        try:
            response = await super().get_response(path, scope)
        except ValueError:
            # A NUL byte in the path (os.path.realpath refuses it): a scanner's
            # probe, not a server error.
            raise HTTPException(status_code=404)
        if response.status_code == 200:
            response.headers["Cache-Control"] = IMMUTABLE
        return response


def mount_dashboard(app: FastAPI, dist: Path = DIST) -> bool:
    """Mount the built dashboard on `app`. Call last: the fallback route matches
    every path, so anything registered after it is unreachable.

    Returns False, mounting nothing, when there is no build. Says nothing
    either way: this runs at import, before logging is configured, and a line
    on stdout would land in scripts/export_openapi.py's JSON. The caller's
    lifespan reports the outcome.
    """
    root = dist.resolve()
    index = root / "index.html"
    if not index.is_file():
        return False

    app.mount("/assets", ImmutableStaticFiles(directory=root / "assets"), name="dashboard-assets")

    @app.api_route("/{path:path}", methods=ALL_METHODS, include_in_schema=False)
    async def dashboard_app(path: str, request: Request) -> FileResponse:
        # Matching every path also switches off Starlette's trailing-slash
        # redirect, so /health/ must be refused here too: answered with the
        # page, it would read 200 to an uptime check while /health says 503.
        if (
            request.method not in ("GET", "HEAD")
            or path.rstrip("/") in NOT_APP_PATHS
            or path.startswith(NOT_APP_PREFIXES)
        ):
            raise HTTPException(status_code=404, detail="Not Found")

        try:
            candidate = (root / path).resolve()
            is_file = bool(path) and candidate.is_relative_to(root) and candidate.is_file()
        except (OSError, ValueError):
            # A NUL byte or an over-long segment: not a file here, and not a 500.
            is_file = False
        if is_file and candidate != index:
            return FileResponse(candidate)
        return FileResponse(index, headers={"Cache-Control": REVALIDATE})

    return True
