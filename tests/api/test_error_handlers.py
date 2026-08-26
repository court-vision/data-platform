"""
Every error renders as the standard envelope with `data.correlation_id` and
the `X-Correlation-ID` / `X-Error-Code` headers; a 500 never leaks exception
text; a bad pipeline token is a 401 envelope on the real app.
"""

import pytest
from fastapi import APIRouter, FastAPI, HTTPException
from fastapi.testclient import TestClient
from peewee import OperationalError

from core import db_middleware
from core.correlation_middleware import RequestContextMiddleware
from core.errors import BadRequestError, NotFoundError
from core.middleware import setup_middleware


def _make_app() -> FastAPI:
    app = FastAPI()
    app.add_middleware(RequestContextMiddleware)
    setup_middleware(app)
    router = APIRouter(prefix="/boom")

    @router.get("/not-found")
    async def not_found():
        raise NotFoundError("RUN_NOT_FOUND", "Quality run not found")

    @router.get("/bad-request")
    async def bad_request():
        raise BadRequestError("BAD_DATE", "date must be YYYY-MM-DD")

    @router.get("/http")
    async def http():
        raise HTTPException(status_code=401, detail="Invalid pipeline authentication token")

    @router.get("/db")
    async def db():
        raise OperationalError("server closed the connection unexpectedly")

    @router.get("/crash")
    async def crash():
        raise RuntimeError("secret-internal-detail")

    @router.get("/validate")
    async def validate(limit: int):
        return {"limit": limit}

    app.include_router(router)
    return app


@pytest.fixture
def client():
    return TestClient(_make_app(), raise_server_exceptions=False)


@pytest.mark.api
@pytest.mark.parametrize("path,status,api_status,code", [
    ("/boom/not-found", 404, "not_found", "RUN_NOT_FOUND"),
    ("/boom/bad-request", 400, "bad_request", "BAD_DATE"),
    ("/boom/http", 401, "authentication_error", "HTTP_401"),
    ("/boom/db", 503, "server_error", "DATABASE_UNAVAILABLE"),
    ("/boom/crash", 500, "server_error", "INTERNAL_ERROR"),
])
def test_errors_render_the_envelope(client, path, status, api_status, code):
    res = client.get(path, headers={"X-Correlation-ID": "t-1"})

    assert res.status_code == status
    body = res.json()
    assert set(body) >= {"status", "message", "data", "error_code"}
    assert body["status"] == api_status
    assert body["error_code"] == code
    assert body["data"]["correlation_id"] == "t-1"
    assert res.headers["X-Correlation-ID"] == "t-1"
    assert res.headers["X-Error-Code"] == code


@pytest.mark.api
def test_500_and_503_bodies_carry_no_exception_text(client):
    assert "secret-internal-detail" not in client.get("/boom/crash").text
    assert "server closed" not in client.get("/boom/db").text


@pytest.mark.api
def test_validation_error_lists_errors(client):
    res = client.get("/boom/validate?limit=x")
    assert res.status_code == 422
    body = res.json()
    assert body["error_code"] == "VALIDATION_ERROR"
    assert body["data"]["errors"][0]["loc"] == ["query", "limit"]


@pytest.mark.api
def test_unknown_route_is_a_404_envelope(client):
    body = client.get("/nope").json()
    assert body["status"] == "not_found"
    assert body["error_code"] == "HTTP_404"


class _NoopDB:
    def is_closed(self):
        return True

    def connect(self, reuse_if_open=False):
        return True

    def close(self):
        return True

    def manual_close(self):
        return True


@pytest.mark.api
def test_bad_pipeline_token_is_a_401_envelope_on_the_real_app(monkeypatch):
    import main

    monkeypatch.setattr(db_middleware, "db", _NoopDB())
    client = TestClient(main.app, raise_server_exceptions=False)

    res = client.post(
        "/v1/internal/pipelines/daily-player-stats",
        headers={"Authorization": "Bearer wrong-token", "X-Correlation-ID": "cron-1"},
    )

    assert res.status_code == 401
    body = res.json()
    assert body["status"] == "authentication_error"
    assert body["error_code"] == "HTTP_401"
    assert body["message"] == "Invalid pipeline authentication token"
    assert body["data"]["correlation_id"] == "cron-1"
    assert res.headers["X-Correlation-ID"] == "cron-1"
