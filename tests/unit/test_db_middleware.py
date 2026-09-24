"""
`DatabaseMiddleware` opens and releases the connection on the thread that
runs the handler (peewee state is thread-local), releases it even when the
handler raises, and turns a failed connect into the 503 envelope.
"""

import threading

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from peewee import OperationalError

from core import db_middleware
from core.db_middleware import DatabaseMiddleware, needs_db
from core.middleware import setup_middleware


class StubDB:
    def __init__(self, fail_connect: bool = False):
        self.events: list[tuple[str, int]] = []
        self._closed = True
        self.fail_connect = fail_connect

    def is_closed(self) -> bool:
        return self._closed

    def connect(self, reuse_if_open: bool = False) -> bool:
        if self.fail_connect:
            raise OperationalError("connection refused")
        self._closed = False
        self.events.append(("connect", threading.get_ident()))
        return True

    def close(self) -> bool:
        self._closed = True
        self.events.append(("close", threading.get_ident()))
        return True

    def manual_close(self) -> bool:
        self._closed = True
        self.events.append(("manual_close", threading.get_ident()))
        return True


def _make_app(stub: StubDB, monkeypatch) -> tuple[FastAPI, dict]:
    monkeypatch.setattr(db_middleware, "db", stub)
    app = FastAPI()
    app.add_middleware(DatabaseMiddleware)
    setup_middleware(app)
    seen: dict = {}

    @app.get("/v1/work")
    async def work():
        seen["thread"] = threading.get_ident()
        seen["open_during_handler"] = not stub.is_closed()
        return {"ok": True}

    @app.get("/v1/boom")
    async def boom():
        seen["thread"] = threading.get_ident()
        raise RuntimeError("handler failed")

    @app.get("/ping")
    async def ping():
        return {"message": "Pong!"}

    return app, seen


@pytest.mark.unit
def test_connects_and_releases_on_the_handler_thread(monkeypatch):
    stub = StubDB()
    app, seen = _make_app(stub, monkeypatch)

    res = TestClient(app).get("/v1/work")

    assert res.status_code == 200
    assert seen["open_during_handler"] is True
    assert [kind for kind, _ in stub.events] == ["connect", "close"]
    assert {thread for _, thread in stub.events} == {seen["thread"]}
    assert stub.is_closed()


@pytest.mark.unit
def test_releases_when_the_handler_raises(monkeypatch):
    stub = StubDB()
    app, seen = _make_app(stub, monkeypatch)

    res = TestClient(app, raise_server_exceptions=False).get("/v1/boom")

    assert res.status_code == 500
    assert res.json()["error_code"] == "INTERNAL_ERROR"
    assert [kind for kind, _ in stub.events] == ["connect", "close"]
    assert stub.events[-1][1] == seen["thread"]


@pytest.mark.unit
def test_connect_failure_is_a_503_envelope(monkeypatch):
    stub = StubDB(fail_connect=True)
    app, seen = _make_app(stub, monkeypatch)

    res = TestClient(app, raise_server_exceptions=False).get("/v1/work")

    assert res.status_code == 503
    body = res.json()
    assert body["status"] == "server_error"
    assert body["error_code"] == "DATABASE_UNAVAILABLE"
    assert "refused" not in res.text
    assert "thread" not in seen  # the handler never ran
    assert stub.events == []


@pytest.mark.unit
def test_paths_outside_v1_skip_the_database(monkeypatch):
    stub = StubDB()
    app, _ = _make_app(stub, monkeypatch)

    assert TestClient(app).get("/ping").status_code == 200
    assert stub.events == []


@pytest.mark.unit
@pytest.mark.parametrize("path", [
    "/v1/dashboard/status",
    "/v1/dashboard",
    "/v1/internal/pipelines/post-game",
    "/v1/internal/cron/job-runs",
])
def test_api_paths_get_a_connection(path):
    assert needs_db(path)


# The rule is a prefix, not a list of exceptions, so the React dashboard's files
# and client-side routes (any path at all) load without the database. They have
# to: a database outage is when the dashboard gets opened.
@pytest.mark.unit
@pytest.mark.parametrize("path", [
    "/",
    "/ping",
    "/health",
    "/favicon.svg",
    "/assets/index-abc123.js",
    "/pipelines/player_game_stats",
    "/v1",          # not a route; the prefix is "/v1/"
    "/v10/whatever",
])
def test_everything_else_does_not(path):
    assert not needs_db(path)


@pytest.mark.unit
def test_physically_close_variant_uses_manual_close(monkeypatch):
    stub = StubDB()
    app, _ = _make_app(stub, monkeypatch)
    monkeypatch.setattr(db_middleware, "PHYSICALLY_CLOSE", True)

    TestClient(app).get("/v1/work")

    assert [kind for kind, _ in stub.events] == ["connect", "manual_close"]
