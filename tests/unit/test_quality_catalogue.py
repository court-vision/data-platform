"""
The quality checks as a catalogue: what the dashboard says each one asserts and
guards. Every structural check names a real table, every consistency check
names the tables it compares, every timing check names a registered pipeline,
and the page's matrix and ordering are pure functions.
"""

import pytest

from api.v1 import dashboard
from pipelines import PIPELINE_REGISTRY
from schemas.dashboard import QualityCheckOutcome, QualityRunEntry
from services import freshness_service
from datetime import date
from decimal import Decimal

from services.consistency_checks import (
    CONSISTENCY_CHECKS,
    LAST_DUE_NIGHT_SQL,
    _fill,
    build_consistency_checks,
)
from services.data_quality_service import (
    CORE_SQL_CHECKS,
    STRUCTURAL_CHECKS,
    TIMING_CHECKS,
    _MANUAL_PIPELINES,
    DataQualityService,
    _json_safe,
)

MODELS = freshness_service._all_models()
BY_NAME = {check.name: check for check in CORE_SQL_CHECKS}


@pytest.mark.unit
class TestCatalogue:
    def test_names_are_unique(self):
        assert len(BY_NAME) == len(CORE_SQL_CHECKS)

    def test_the_service_lists_them_structural_then_consistency_then_timing(self):
        assert DataQualityService().checks() == (
            STRUCTURAL_CHECKS + CONSISTENCY_CHECKS + TIMING_CHECKS
        )

    @pytest.mark.parametrize("check", CORE_SQL_CHECKS, ids=lambda c: c.name)
    def test_a_check_guards_a_table_some_model_declares(self, check):
        assert check.table in MODELS, (
            f"{check.name} says it guards {check.table!r}, which is no model's table"
        )

    @pytest.mark.parametrize("check", STRUCTURAL_CHECKS, ids=lambda c: c.name)
    def test_a_structural_check_reads_the_table_it_names(self, check):
        assert (check.group, check.pipeline) == ("structural", None)
        assert check.table in check.sql, f"{check.name} names {check.table} but its SQL never reads it"

    @pytest.mark.parametrize("check", TIMING_CHECKS, ids=lambda c: c.name)
    def test_a_timing_check_watches_one_registered_pipeline(self, check):
        assert check.group == "timing" and check.table == "nba.pipeline_runs"
        assert check.pipeline in PIPELINE_REGISTRY
        assert check.name == f"{check.pipeline}_ran_within_24h"

    def test_every_scheduled_pipeline_has_a_timing_check(self):
        watched = {check.pipeline for check in TIMING_CHECKS}
        assert watched == set(PIPELINE_REGISTRY) - _MANUAL_PIPELINES


@pytest.mark.unit
class TestConsistencyCatalogue:
    @pytest.mark.parametrize("check", CONSISTENCY_CHECKS, ids=lambda c: c.name)
    def test_a_consistency_check_reads_every_table_it_names(self, check):
        assert (check.group, check.pipeline) == ("consistency", None)
        for table in (check.table, *check.against):
            assert table in MODELS, f"{check.name} names {table!r}, which is no model's table"
            assert table in check.sql, f"{check.name} names {table} but its SQL never reads it"

    @pytest.mark.parametrize("check", CONSISTENCY_CHECKS, ids=lambda c: c.name)
    def test_it_starts_as_a_warning(self, check):
        # A critical check pages. None of these has run against a live season
        # yet: promote one deliberately, and change this test when you do.
        assert check.severity == "warning"

    @pytest.mark.parametrize("check", CONSISTENCY_CHECKS, ids=lambda c: c.name)
    def test_the_count_and_the_sample_are_the_same_query(self, check):
        assert check.sql.startswith("SELECT COUNT(*) FROM (\n")
        assert check.sample_sql.startswith("SELECT * FROM (\n")
        body = check.sql.removeprefix("SELECT COUNT(*) FROM (")
        assert check.sample_sql.removeprefix("SELECT * FROM (").startswith(body)
        assert check.sample_sql.endswith("LIMIT 5")
        assert "{" not in check.sql, "an unfilled fragment"

    @pytest.mark.parametrize("check", CONSISTENCY_CHECKS, ids=lambda c: c.name)
    def test_the_live_window_ends_on_the_last_night_that_is_due(self, check):
        assert LAST_DUE_NIGHT_SQL in check.sql
        assert "night - 6 AS since, night AS through" in check.sql

    def test_a_pinned_window_replays_past_nights(self):
        live = {check.name for check in CONSISTENCY_CHECKS}
        pinned = build_consistency_checks(through=date(2026, 4, 12), days=3)
        assert {check.name for check in pinned} == live
        for check in pinned:
            assert "DATE '2026-04-12'" in check.sql and "NOW()" not in check.sql
            assert "night - 2 AS since" in check.sql

    @pytest.mark.parametrize("check", CORE_SQL_CHECKS, ids=lambda c: c.name)
    def test_the_sql_has_no_percent_sign(self, check):
        # psycopg2 gets the SQL with an empty parameter list and would read a
        # bare % as a placeholder: write LEFT(...) and MOD(...) instead.
        assert "%" not in check.sql and "%" not in (check.sample_sql or "")

    def test_only_checks_with_a_sample_query_keep_offending_rows(self):
        assert all(check.sample_sql is None for check in STRUCTURAL_CHECKS + TIMING_CHECKS)

    def test_a_fragments_later_lines_sit_under_its_first(self):
        filled = _fill(
            """
            SELECT 1
            WHERE {cond}
            """,
            {"cond": "a = 1\nOR b = 2"},
        )
        assert filled == "SELECT 1\nWHERE a = 1\n      OR b = 2"


@pytest.mark.unit
@pytest.mark.parametrize("value, expected", [
    (date(2026, 4, 12), "2026-04-12"),
    (Decimal("28.00"), 28),
    (Decimal("19.07"), 19.07),
    (None, None),
    (3, 3),
    ("PHX", "PHX"),
])
def test_a_sampled_cell_is_json_safe(value, expected):
    assert _json_safe(value) == expected


@pytest.mark.unit
class TestCheckInfo:
    def test_a_consistency_checks_pipelines_write_either_side_of_the_comparison(self):
        info = dashboard.quality_check_info(
            BY_NAME["player_season_stats_games_keep_pace_with_game_log"]
        )
        assert (info.group, info.table) == ("consistency", "nba.player_season_stats")
        assert info.against == ["nba.player_game_stats"]
        assert info.pipelines == ["player_season_stats", "player_game_stats"]

    def test_a_check_within_one_table_compares_against_nothing(self):
        assert dashboard.quality_check_info(BY_NAME["games_final_after_game_night"]).against == []
        assert dashboard.quality_check_info(BY_NAME["player_game_stats_stat_ranges_valid"]).against == []

    def test_a_structural_checks_pipelines_are_its_tables_writers(self):
        info = dashboard.quality_check_info(BY_NAME["player_game_stats_stat_ranges_valid"])
        assert (info.table, info.pipelines) == ("nba.player_game_stats", ["player_game_stats"])
        assert (info.severity, info.group) == ("critical", "structural")

    def test_a_framework_table_has_no_writer_pipeline(self):
        assert dashboard.quality_check_info(BY_NAME["pipeline_runs_no_stale_running"]).pipelines == []

    def test_a_timing_checks_pipeline_is_the_one_it_watches(self):
        info = dashboard.quality_check_info(BY_NAME["player_ownership_ran_within_24h"])
        assert info.pipelines == ["player_ownership"]
        assert "expected on off-days" in info.failure_message

    @pytest.mark.parametrize("check", CORE_SQL_CHECKS, ids=lambda c: c.name)
    def test_the_sql_is_shown_without_its_source_indentation(self, check):
        sql = dashboard.quality_check_info(check).sql
        assert sql == sql.strip() and sql.upper().startswith("SELECT")
        assert not any(line.startswith("            ") for line in sql.splitlines()[:1])


def _entry(run_id: str) -> QualityRunEntry:
    return QualityRunEntry(run_id=run_id, status="success", started_at="2026-03-05T08:00:00")


@pytest.mark.unit
class TestMatrix:
    CHECKS = (BY_NAME["player_game_stats_stat_ranges_valid"], BY_NAME["player_ownership_ran_within_24h"])

    def test_results_line_up_with_the_runs_newest_first(self):
        runs = [_entry("new"), _entry("mid"), _entry("old")]
        results = {
            "player_game_stats_stat_ranges_valid": {"new": "passed", "mid": "passed", "old": "failed"},
            "player_ownership_ran_within_24h": {"mid": "error"},
        }
        ranges, ownership = dashboard.quality_matrix(self.CHECKS, runs, results)
        assert ranges.results == ["passed", "passed", "failed"]
        # A run that did not include the check is a gap, not a pass.
        assert ownership.results == [None, "error", None]

    def test_a_check_no_run_has_included_is_all_gaps(self):
        rows = dashboard.quality_matrix(self.CHECKS, [_entry("a"), _entry("b")], {})
        assert [row.results for row in rows] == [[None, None], [None, None]]

    def test_no_runs_still_lists_every_check(self):
        rows = dashboard.quality_matrix(CORE_SQL_CHECKS, [], {})
        assert [row.name for row in rows] == [check.name for check in CORE_SQL_CHECKS]
        assert all(row.results == [] for row in rows)


def _outcome(name: str, status: str, severity: str) -> QualityCheckOutcome:
    return QualityCheckOutcome(check_name=name, status=status, severity=severity)


@pytest.mark.unit
def test_outcomes_are_ordered_by_what_needs_a_look_first():
    ordered = dashboard.order_outcomes([
        _outcome("pass_a", "passed", "critical"),
        _outcome("warn_fail", "failed", "warning"),
        _outcome("pass_b", "passed", "warning"),
        _outcome("crit_fail", "failed", "critical"),
        _outcome("broken", "error", "warning"),
    ])
    # Could-not-run first, then failures critical before warning, then passes in run order.
    assert [o.check_name for o in ordered] == ["broken", "crit_fail", "warn_fail", "pass_a", "pass_b"]
