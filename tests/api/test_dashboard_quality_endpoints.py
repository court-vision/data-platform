"""
GET /v1/dashboard/quality and /v1/dashboard/quality/runs/{run_id}: token-gated,
and the payloads the quality pages read. The queries are stubbed here; they run
against real rows in tests/integration/test_quality_dashboard_queries.py.
"""

import os
import re
import uuid
from datetime import datetime, timezone

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from api.v1 import dashboard
from core.middleware import setup_middleware
from schemas.dashboard import (
    QualityCheckOutcome,
    QualityOverviewData,
    QualityRunDetailData,
    QualityRunEntry,
)
from services.data_quality_service import CORE_SQL_CHECKS

_AUTH = {"Authorization": f"Bearer {os.environ.get('PIPELINE_API_TOKEN', 'test-token')}"}
RUN_ID = str(uuid.UUID(int=7))
FETCHED = datetime(2026, 3, 5, 13, 0, tzinfo=timezone.utc)


def _make_app() -> FastAPI:
    app = FastAPI()
    setup_middleware(app)  # the JSON error envelope, for the 404s
    app.include_router(dashboard.router, prefix="/v1")
    return app


def _entry(run_id: str = RUN_ID, failed: int = 1) -> QualityRunEntry:
    return QualityRunEntry(
        run_id=run_id, status="failed" if failed else "success",
        started_at="2026-03-05T08:00:00", completed_at="2026-03-05T08:00:48",
        duration_seconds=48.0, total_checks=23, passed_checks=23 - failed,
        failed_checks=failed, triggered_by="schedule",
    )


@pytest.mark.api
@pytest.mark.parametrize("path", ["/v1/dashboard/quality", f"/v1/dashboard/quality/runs/{RUN_ID}"])
def test_quality_routes_require_bearer_token(path) -> None:
    assert TestClient(_make_app()).get(path).status_code in (401, 403)


@pytest.mark.api
def test_overview_limit_is_bounded() -> None:
    client = TestClient(_make_app())
    assert client.get("/v1/dashboard/quality?limit=0", headers=_AUTH).status_code == 422
    assert client.get("/v1/dashboard/quality?limit=101", headers=_AUTH).status_code == 422


@pytest.mark.api
def test_overview_payload(monkeypatch) -> None:
    seen = {}

    def fake_build(limit):
        seen["limit"] = limit
        runs = [_entry(), _entry(str(uuid.UUID(int=6)), failed=0)]
        results = {CORE_SQL_CHECKS[0].name: {runs[0].run_id: "failed", runs[1].run_id: "passed"}}
        return QualityOverviewData(
            runs=runs, checks=dashboard.quality_matrix(CORE_SQL_CHECKS, runs, results),
            limit=limit, fetched_at=FETCHED,
        )

    monkeypatch.setattr(dashboard, "_build_quality_overview", fake_build)
    res = TestClient(_make_app()).get("/v1/dashboard/quality?limit=50", headers=_AUTH)

    assert res.status_code == 200 and seen["limit"] == 50
    body = res.json()
    assert body["message"] == f"{len(CORE_SQL_CHECKS)} checks over 2 runs"
    data = body["data"]
    assert [run["run_id"] for run in data["runs"]] == [RUN_ID, str(uuid.UUID(int=6))]
    first, second = data["checks"][0], data["checks"][1]
    assert first["results"] == ["failed", "passed"] and second["results"] == [None, None]
    # The definition is on the wire: what it asserts, what it guards, the SQL.
    assert first["table"] == "nba.player_game_stats" and first["pipelines"] == ["player_game_stats"]
    assert first["sql"].startswith("SELECT") and first["failure_message"]
    assert {check["group"] for check in data["checks"]} == {"structural", "consistency", "timing"}
    # A consistency check names what it is compared against, and both sides' writers.
    paced = next(c for c in data["checks"] if c["name"] == "player_season_stats_games_keep_pace_with_game_log")
    assert (paced["table"], paced["against"]) == ("nba.player_season_stats", ["nba.player_game_stats"])
    assert paced["pipelines"] == ["player_season_stats", "player_game_stats"]
    assert first["against"] == []


@pytest.mark.api
@pytest.mark.parametrize("run_id", ["nope", "123", "'; DROP TABLE nba.data_quality_runs; --"])
def test_a_run_id_that_is_not_an_id_is_a_404_without_asking_the_database(monkeypatch, run_id) -> None:
    def never(_run_id):
        raise AssertionError("the database was asked about a malformed id")

    monkeypatch.setattr(dashboard, "_build_quality_run", never)
    res = TestClient(_make_app()).get(f"/v1/dashboard/quality/runs/{run_id}", headers=_AUTH)
    assert res.status_code == 404
    assert res.headers["content-type"].startswith("application/json")


@pytest.mark.api
@pytest.mark.parametrize("run_id", [
    "0x111111111111111111111111111111",    # a hex literal
    "+1111111111111111111111111111111",    # a sign
    "1_111111111111111111111111111111",    # a digit separator
    "%0A1111111111111111111111111111111",  # whitespace in front
    "1111111111111111111111111111111%20",  # whitespace behind
])
def test_an_id_only_python_reads_as_a_uuid_is_asked_for_in_its_canonical_form(monkeypatch, run_id) -> None:
    # uuid.UUID reads these 32 characters as a number; Postgres would refuse to cast them.
    asked = []
    monkeypatch.setattr(dashboard, "_build_quality_run", lambda run_id: asked.append(run_id))
    res = TestClient(_make_app()).get(f"/v1/dashboard/quality/runs/{run_id}", headers=_AUTH)
    assert res.status_code == 404
    assert len(asked) == 1 and re.fullmatch(r"[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}", asked[0])


@pytest.mark.api
def test_an_unknown_run_is_a_json_404(monkeypatch) -> None:
    monkeypatch.setattr(dashboard, "_build_quality_run", lambda run_id: None)
    res = TestClient(_make_app()).get(f"/v1/dashboard/quality/runs/{RUN_ID}", headers=_AUTH)
    assert res.status_code == 404 and RUN_ID in res.json()["message"]


@pytest.mark.api
def test_run_detail_payload(monkeypatch) -> None:
    check = CORE_SQL_CHECKS[0]

    def fake_build(run_id):
        outcomes = [
            QualityCheckOutcome(check_name="retired_check", status="passed", severity="warning"),
            QualityCheckOutcome(
                check_name=check.name, status="failed", severity=check.severity, failures=37,
                message=check.failure_message, details={"failures": 37}, duration_ms=12,
                definition=dashboard.quality_check_info(check),
            ),
        ]
        return QualityRunDetailData(
            run=_entry(run_id), checks=dashboard.order_outcomes(outcomes),
            older_run_id=str(uuid.UUID(int=6)), newer_run_id=None, fetched_at=FETCHED,
        )

    monkeypatch.setattr(dashboard, "_build_quality_run", fake_build)
    res = TestClient(_make_app()).get(f"/v1/dashboard/quality/runs/{RUN_ID}", headers=_AUTH)

    assert res.status_code == 200
    body = res.json()
    assert body["message"] == "2 checks, 1 failed"
    data = body["data"]
    assert data["run"]["run_id"] == RUN_ID
    assert (data["older_run_id"], data["newer_run_id"]) == (str(uuid.UUID(int=6)), None)
    failed, retired = data["checks"]
    assert failed["check_name"] == check.name and failed["failures"] == 37
    assert failed["details"] == {"failures": 37}
    assert failed["definition"]["sql"].startswith("SELECT")
    # A check removed from the code since still shows its result, without a definition.
    assert retired["check_name"] == "retired_check" and retired["definition"] is None
