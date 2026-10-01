"""
`DatabaseMiddleware` opens a connection for /v1 routes and nothing else
(the rule itself is tested in tests/unit/test_db_middleware.py).

The price of a prefix rule is that a DB-reading route added outside /v1 would
silently get no connection. This makes that a failure instead. The two /v1
routes the middleware exempts by name (`NO_DB_V1_PATHS`) are accounted for
here like the rest.
"""

import os

import pytest
from fastapi.routing import iter_route_contexts
from fastapi.testclient import TestClient
from peewee import OperationalError

import main
import main_public
from api.v1 import dashboard
from core import db_middleware
from core.db_middleware import NO_DB_V1_PATHS, needs_db
from schemas.dashboard import ServiceInfo

# Routes that get no connection, none of which reads the database. `/health`
# probes it on its own thread (core.health); the rest are liveness, docs and the
# dashboard, down to its two /v1 routes that only read settings and the backend.
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
    "/v1/dashboard",
    "/v1/dashboard/services",
}


@pytest.mark.api
@pytest.mark.parametrize("app_module", [main, main_public], ids=["private", "public"])
def test_no_route_outside_v1_is_unaccounted_for(app_module):
    # iter_route_contexts: app.routes holds included routers as tree nodes
    paths = {route.path for route in iter_route_contexts(app_module.app.routes)}
    assert any(needs_db(path) for path in paths), "found no /v1 routes: the route walk is broken"
    outside = {path for path in paths if not needs_db(path)}
    assert outside <= NO_DB_ROUTES, (
        f"{sorted(outside - NO_DB_ROUTES)} on {app_module.__name__} is outside /v1 (or "
        "exempted by name), so DatabaseMiddleware gives it no connection. Move it under "
        "/v1, or add it to NO_DB_ROUTES if it really reads nothing."
    )


@pytest.mark.api
@pytest.mark.parametrize("app_module", [main, main_public], ids=["private", "public"])
def test_every_exempted_v1_path_is_a_route(app_module):
    paths = {route.path for route in iter_route_contexts(app_module.app.routes)}
    assert NO_DB_V1_PATHS <= paths, (
        f"{sorted(NO_DB_V1_PATHS - paths)} is exempted in core.db_middleware but is "
        f"no route on {app_module.__name__}: the exemption outlived it."
    )


class DeadDB:
    """`db` with Postgres gone: every connect is refused."""

    def is_closed(self) -> bool:
        return True

    def connect(self, reuse_if_open: bool = False) -> bool:
        raise OperationalError("connection refused")


# The service cards are how the dashboard says *what* is down. The backend shares
# this database, so its "degraded: database" has to get through in the outage.
@pytest.mark.api
@pytest.mark.parametrize("app_module", [main, main_public], ids=["private", "public"])
def test_the_service_cards_and_the_redirect_answer_while_the_database_is_down(app_module, monkeypatch):
    monkeypatch.setattr(db_middleware, "db", DeadDB())
    monkeypatch.setattr(
        dashboard, "_probe_backend",
        lambda: ServiceInfo(key="backend", name="Backend", version="abc1234",
                            error="degraded: database"),
    )
    client = TestClient(app_module.app, raise_server_exceptions=False)
    auth = {"Authorization": f"Bearer {os.environ.get('PIPELINE_API_TOKEN', 'test-token')}"}

    # the outage is real for a route that reads: the 503 envelope, as before
    down = client.get("/v1/dashboard/status", headers=auth)
    assert down.status_code == 503
    assert down.json()["error_code"] == "DATABASE_UNAVAILABLE"

    res = client.get("/v1/dashboard/services", headers=auth)
    assert res.status_code == 200
    services = {s["key"]: s for s in res.json()["data"]["services"]}
    assert services["data_platform"]["ok"] is True and services["data_platform"]["version"]
    assert services["backend"]["error"] == "degraded: database"

    # still token-authed: the exemption is from the connection, not from auth
    assert client.get("/v1/dashboard/services").status_code in (401, 403)

    bookmark = client.get("/v1/dashboard", follow_redirects=False)
    assert bookmark.status_code == 307
    assert bookmark.headers["location"] == "/"
