"""
`GET /health` on both processes: the private server reports its database and
calendar; the public server additionally probes the private server, so a
dead pipeline process degrades the only port Railway / Better Stack can see.
"""

import asyncio

import pytest
from fastapi.testclient import TestClient

import main
import main_public
from core import health as health_module
from core.settings import settings

DB_OK = {"ok": True, "latency_ms": 1.0}
DB_DOWN = {"ok": False, "error": "OperationalError"}


def _fake_database_check(result):
    async def check(timeout=2.0):
        return result
    return check


def _fake_private_check(result):
    async def check():
        return result
    return check


@pytest.fixture
def db_ok(monkeypatch):
    monkeypatch.setattr(health_module, "database_check", _fake_database_check(DB_OK))


@pytest.fixture
def db_down(monkeypatch):
    monkeypatch.setattr(health_module, "database_check", _fake_database_check(DB_DOWN))
    monkeypatch.setattr(health_module, "_last_degraded_log_at", 0.0)


@pytest.mark.api
def test_private_health_ok_shape(db_ok):
    res = TestClient(main.app, raise_server_exceptions=False).get("/health", headers={"X-Correlation-ID": "t-h"})

    assert res.status_code == 200
    body = res.json()
    assert body["status"] == "ok"
    assert body["service"] == "court-vision-data-platform"
    assert body["version"]
    assert body["environment"] == "development"
    assert isinstance(body["uptime_s"], int)
    assert body["checks"]["database"] == DB_OK
    assert body["checks"]["calendar"]["season"] == "2025-26"
    assert "private_server" not in body["checks"]
    assert res.headers["X-Correlation-ID"] == "t-h"
    assert res.headers["Cache-Control"] == "no-store"


@pytest.mark.api
def test_private_health_degraded_when_database_down(db_down):
    res = TestClient(main.app, raise_server_exceptions=False).get("/health")
    assert res.status_code == 503
    body = res.json()
    assert body["status"] == "degraded"
    assert body["checks"]["database"] == DB_DOWN


@pytest.mark.api
def test_public_health_includes_the_private_server(db_ok, monkeypatch):
    monkeypatch.setattr(main_public, "private_server_check",
                        _fake_private_check({"ok": True, "latency_ms": 3.0, "status_code": 200}))

    res = TestClient(main_public.app, raise_server_exceptions=False).get("/health")

    assert res.status_code == 200
    body = res.json()
    assert body["status"] == "ok"
    assert body["checks"]["database"] == DB_OK
    assert body["checks"]["private_server"]["ok"] is True
    assert body["checks"]["private_server"]["status_code"] == 200


@pytest.mark.api
def test_public_health_degrades_when_the_private_server_is_down(db_ok, monkeypatch):
    monkeypatch.setattr(health_module, "_last_degraded_log_at", 0.0)
    monkeypatch.setattr(main_public, "private_server_check",
                        _fake_private_check({"ok": False, "error": "ConnectError"}))

    res = TestClient(main_public.app, raise_server_exceptions=False).get("/health")

    assert res.status_code == 503
    body = res.json()
    assert body["status"] == "degraded"
    assert body["checks"]["database"]["ok"] is True
    assert body["checks"]["private_server"] == {"ok": False, "error": "ConnectError"}


@pytest.mark.api
def test_private_server_check_reports_unreachable(monkeypatch):
    # TCP port 9 (discard) refuses connections on a dev box; nothing listens there
    monkeypatch.setattr(settings, "private_health_url", "http://127.0.0.1:9/health")
    result = asyncio.run(main_public.private_server_check())
    assert result["ok"] is False
    assert result["error"]


@pytest.mark.api
def test_private_health_url_targets_the_ipv6_loopback():
    """uvicorn binds `::` IPv6-only, so 127.0.0.1 would never reach the private process."""
    assert settings.private_health_url == "http://[::1]:8001/health"


@pytest.mark.api
@pytest.mark.parametrize("app", [main.app, main_public.app])
def test_ping_stays_static(app):
    res = TestClient(app).get("/ping")
    assert res.status_code == 200
    assert res.json() == {"message": "Pong!"}


# ---- health_degraded alert ----------------------------------------------------


@pytest.mark.api
def test_degraded_health_alerts_once_per_window(db_down, alerts):
    from datetime import timedelta

    client = TestClient(main.app, raise_server_exceptions=False)
    assert client.get("/health").status_code == 503
    assert client.get("/health").status_code == 503  # pollers keep asking; one alert

    assert alerts.keys() == ["health_degraded"]
    event = alerts.events[0]
    assert event.severity == "critical"
    assert event.dedupe == timedelta(minutes=30)
    assert event.title == "Health degraded: court-vision-data-platform"
    assert "database: OperationalError" in event.body
    assert event.fields["failing"] == "database"
    assert event.fields["environment"] == "development"


@pytest.mark.api
def test_healthy_response_never_alerts(db_ok, alerts):
    assert TestClient(main.app, raise_server_exceptions=False).get("/health").status_code == 200
    assert alerts.events == []
