"""
The quality pages' queries against real rows: each check's result per run (the
matrix), stepping between runs, and both page payloads end to end.
"""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timedelta

import pytest

from api.v1 import dashboard
from db.base import db
from db.models.data_quality_check import DataQualityCheck
from db.models.data_quality_run import DataQualityRun
from services.data_quality_service import DataQualityService

RANGES = "player_game_stats_stat_ranges_valid"      # structural, critical
OWNERSHIP = "player_ownership_ran_within_24h"        # timing, warning


@pytest.fixture(scope="session")
def quality_tables(integration_db):
    db.create_tables([DataQualityRun, DataQualityCheck], safe=True)
    yield


@pytest.fixture(autouse=True)
def clean_quality_tables(quality_tables):
    DataQualityCheck.delete().execute()
    DataQualityRun.delete().execute()
    yield


def _run(minutes_ago: int, checks: list[tuple[str, str, str, int]]) -> str:
    """A finished run `minutes_ago`, with (name, status, severity, failures) checks."""
    started = datetime.utcnow() - timedelta(minutes=minutes_ago)
    failed = sum(1 for _, status, _, _ in checks if status != "passed")
    run = DataQualityRun.create(
        status="failed" if failed else "success", triggered_by="schedule",
        started_at=started, completed_at=started + timedelta(seconds=5),
        total_checks=len(checks), passed_checks=len(checks) - failed, failed_checks=failed,
    )
    for name, status, severity, failures in checks:
        DataQualityCheck.create(
            run=run, check_name=name, status=status, severity=severity, failures=failures,
            message=None if status == "passed" else "something is wrong",
            details_json=None if status == "passed" else json.dumps({"failures": failures}),
            duration_ms=3,
        )
    return str(run.id)


@pytest.fixture
def three_runs() -> tuple[str, str, str]:
    oldest = _run(180, [(RANGES, "failed", "critical", 4)])
    middle = _run(120, [(RANGES, "passed", "critical", 0), (OWNERSHIP, "failed", "warning", 1)])
    newest = _run(60, [
        (RANGES, "passed", "critical", 0),
        (OWNERSHIP, "failed", "warning", 1),
        ("a_check_since_removed", "error", "critical", 1),
    ])
    return oldest, middle, newest


@pytest.mark.integration
class TestResultsForRuns:
    def test_each_check_maps_to_its_status_in_each_run(self, three_runs):
        oldest, middle, newest = three_runs
        results = DataQualityService().results_for_runs([newest, middle, oldest])
        assert results[RANGES] == {oldest: "failed", middle: "passed", newest: "passed"}
        assert results[OWNERSHIP] == {middle: "failed", newest: "failed"}

    def test_only_the_runs_asked_for(self, three_runs):
        oldest, _, newest = three_runs
        assert DataQualityService().results_for_runs([newest])[RANGES] == {newest: "passed"}

    def test_no_runs_asks_nothing(self):
        assert DataQualityService().results_for_runs([]) == {}


@pytest.mark.integration
class TestNeighbours:
    def test_the_run_before_and_the_run_after(self, three_runs):
        oldest, middle, newest = three_runs
        service = DataQualityService()
        assert service.neighbours(middle) == (oldest, newest)
        assert service.neighbours(oldest) == (None, middle)
        assert service.neighbours(newest) == (middle, None)

    def test_an_unknown_run_has_none(self):
        assert DataQualityService().neighbours(str(uuid.uuid4())) == (None, None)


@pytest.mark.integration
class TestOverview:
    def test_every_defined_check_with_its_results_lined_up_with_the_runs(self, three_runs):
        oldest, middle, newest = three_runs
        data = dashboard._build_quality_overview(limit=10)

        assert [run.run_id for run in data.runs] == [newest, middle, oldest]
        rows = {row.name: row for row in data.checks}
        assert rows[RANGES].results == ["passed", "passed", "failed"]
        assert rows[OWNERSHIP].results == ["failed", "failed", None]  # not in the oldest run
        # A check that exists in code but no run has included is all gaps.
        assert rows["player_game_stats_non_negative_minutes"].results == [None, None, None]
        # The matrix is the catalogue: a removed check has no row here.
        assert "a_check_since_removed" not in rows
        assert rows[OWNERSHIP].pipelines == ["player_ownership"]

    def test_the_limit_is_the_number_of_runs(self, three_runs):
        _, middle, newest = three_runs
        data = dashboard._build_quality_overview(limit=2)
        assert [run.run_id for run in data.runs] == [newest, middle]
        assert all(len(row.results) == 2 for row in data.checks)

    def test_no_runs_yet_still_lists_the_checks(self):
        data = dashboard._build_quality_overview(limit=20)
        assert data.runs == [] and len(data.checks) == len(DataQualityService().checks())


@pytest.mark.integration
class TestRunDetail:
    def test_every_outcome_failures_first_each_with_its_definition(self, three_runs):
        _, middle, newest = three_runs
        data = dashboard._build_quality_run(newest)

        assert data.run.run_id == newest and (data.run.total_checks, data.run.failed_checks) == (3, 2)
        assert [o.check_name for o in data.checks] == ["a_check_since_removed", OWNERSHIP, RANGES]
        removed, ownership, ranges = data.checks
        assert removed.status == "error" and removed.definition is None
        assert ownership.details == {"failures": 1}
        assert ownership.definition.group == "timing" and ownership.definition.pipelines == ["player_ownership"]
        assert ranges.status == "passed" and ranges.definition.table == "nba.player_game_stats"
        assert (data.older_run_id, data.newer_run_id) == (middle, None)

    def test_an_unknown_run_is_none(self):
        assert dashboard._build_quality_run(str(uuid.uuid4())) is None
