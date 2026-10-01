"""
`DatabaseMiddleware` opens a connection for /v1 routes and nothing else
(the rule itself is tested in tests/unit/test_db_middleware.py).

The price of a prefix rule is that a DB-reading route added outside /v1 would
silently get no connection. This makes that a failure instead.
"""

import pytest
from fastapi.routing import iter_route_contexts

import main
import main_public
from core.db_middleware import needs_db

# Routes outside /v1, none of which reads the database. `/health` probes it on
# its own thread (core.health); the rest are liveness, docs and the dashboard.
NO_DB_ROUTES = {
    "/",
    "/ping",
    "/health",
    "/docs",
    "/docs/oauth2-redirect",
    "/redoc",
    "/openapi.json",
    "/assets",
    "/{path:path}",
}


@pytest.mark.api
@pytest.mark.parametrize("app_module", [main, main_public], ids=["private", "public"])
def test_no_route_outside_v1_is_unaccounted_for(app_module):
    # iter_route_contexts: app.routes holds included routers as tree nodes
    paths = {route.path for route in iter_route_contexts(app_module.app.routes)}
    assert any(needs_db(path) for path in paths), "found no /v1 routes: the route walk is broken"
    outside = {path for path in paths if not needs_db(path)}
    assert outside <= NO_DB_ROUTES, (
        f"{sorted(outside - NO_DB_ROUTES)} on {app_module.__name__} is outside /v1, so "
        "DatabaseMiddleware gives it no connection. Move it under /v1, or add it to "
        "NO_DB_ROUTES if it really reads nothing."
    )
