"""
`DatabaseMiddleware` opens a connection for /v1 routes and nothing else
(the rule itself is tested in tests/unit/test_db_middleware.py).

The price of a prefix rule is that a DB-reading route added outside /v1 would
silently get no connection. This makes that a failure instead.
"""

import pytest

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
    outside = {route.path for route in app_module.app.routes if not needs_db(route.path)}
    assert outside <= NO_DB_ROUTES, (
        f"{sorted(outside - NO_DB_ROUTES)} on {app_module.__name__} is outside /v1, so "
        "DatabaseMiddleware gives it no connection. Move it under /v1, or add it to "
        "NO_DB_ROUTES if it really reads nothing."
    )
