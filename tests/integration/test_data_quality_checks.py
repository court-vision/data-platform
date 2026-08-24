"""
Data Quality Check Integration Tests

Exercises the 7 SQL-based quality checks from DataQualityService against
controlled data states in the CI Postgres container. Verifies that each
check correctly detects violations and passes on clean data.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta

import pytest

from db.base import db
from db.models.data_quality_check import DataQualityCheck
from db.models.data_quality_run import DataQualityRun
from db.models.nba.games import Game
from db.models.nba.player_game_stats import PlayerGameStats
from db.models.nba.player_rolling_stats import PlayerRollingStats
from db.models.nba.player_season_stats import PlayerSeasonStats
from db.models.nba.players import Player
from db.models.pipeline_run import PipelineRun
from pipelines import PIPELINE_REGISTRY
from services.data_quality_service import DataQualityService, _MANUAL_PIPELINES


# ---------------------------------------------------------------------------
# Additional models for quality check tables
# ---------------------------------------------------------------------------

QUALITY_MODELS = [DataQualityRun, DataQualityCheck]


@pytest.fixture(scope="session")
def quality_tables(integration_db):
    """Ensure data quality tables exist alongside integration tables."""
    db.create_tables(QUALITY_MODELS, safe=True)
    yield


@pytest.fixture(autouse=True)
def clean_quality_tables(quality_tables):
    """Clean quality run/check tables between tests."""
    DataQualityCheck.delete().execute()
    DataQualityRun.delete().execute()
    yield


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

NOW = "NOW()"

# 9 structural checks + one timing check per non-manual registered pipeline
EXPECTED_CHECK_COUNT = 9 + sum(1 for name in PIPELINE_REGISTRY if name not in _MANUAL_PIPELINES)


def _seed_clean_data(game_date: date = date(2026, 2, 14)) -> None:
    """Seed a minimal valid dataset that passes all quality checks."""
    Game.create(
        game_id="0022500001",
        game_date=game_date,
        season="2025-26",
        home_team_id="GSW",
        away_team_id="LAL",
        status="final",
        home_score=120,
        away_score=115,
    )
    Player.create(id=201939, name="Stephen Curry", name_normalized="stephen curry")
    PlayerGameStats.create(
        player_id=201939,
        team_id="GSW",
        game_date=game_date,
        fpts=53,
        pts=30, reb=5, ast=6, stl=2, blk=0, tov=3,
        min=34,
        fgm=10, fga=20, fg3m=5, fg3a=12, ftm=5, fta=6,
    )
    PlayerSeasonStats.create(
        player_id=201939,
        team_id="GSW",
        as_of_date=game_date,
        season="2025-26",
        gp=1,
        fpts=53,
        pts=30, reb=5, ast=6, stl=2, blk=0, tov=3,
        min=34,
        fgm=10, fga=20, fg3m=5, fg3a=12, ftm=5, fta=6,
        rank=5,
    )
    # Include fg_pct, fg3_pct, ft_pct — all NOT NULL
    PlayerRollingStats.create(
        player_id=201939,
        team_id="GSW",
        as_of_date=game_date,
        window_days=7,
        gp=3,
        fpts=45.0,
        pts=25.0, reb=5.0, ast=6.0, stl=2.0, blk=0.5, tov=2.5,
        min=34.0,
        fgm=10.0, fga=20.0, fg_pct=0.5000,
        fg3m=4.0, fg3a=10.0, fg3_pct=0.4000,
        ftm=4.0, fta=5.0, ft_pct=0.8000,
    )
    # Seed a successful run for every timed pipeline so timing checks pass.
    # Derived from the registry the same way the service builds its checks, so a
    # newly registered pipeline can't silently break the happy path.
    for pipeline_name in PIPELINE_REGISTRY:
        if pipeline_name in _MANUAL_PIPELINES:
            continue
        PipelineRun.create(
            pipeline_name=pipeline_name,
            started_at=datetime.utcnow(),
            completed_at=datetime.utcnow(),
            status="success",
            records_processed=1,
        )


def _run_single_check(service: DataQualityService, check_name: str) -> dict:
    """Run a single quality check and return its result dict."""
    run = service.run_checks(check_names=[check_name], triggered_by="test")
    detail = service.get_run(str(run.id))
    assert detail is not None
    assert len(detail["checks"]) == 1
    return detail["checks"][0]


# ---------------------------------------------------------------------------
# Happy path: all checks pass on clean data
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestQualityChecksHappyPath:
    """All 7 checks pass when data is valid."""

    def test_all_checks_pass_on_clean_data(self, integration_db, quality_tables):
        _seed_clean_data()
        service = DataQualityService()
        run = service.run_checks(triggered_by="test")

        assert run.status == "success"
        assert run.failed_checks == 0
        assert run.passed_checks == run.total_checks


# ---------------------------------------------------------------------------
# Individual check failure detection
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestNullCoreFieldsCheck:
    """player_game_stats_required_fields_not_null

    player_id has a NOT NULL DB constraint, so we test via team_id which is
    nullable by design (null=True for trades/unknown). The check catches any
    null in player_id OR game_date OR team_id.
    """

    def test_detects_null_team_id(self, integration_db, quality_tables):
        """A row with NULL team_id should trigger the null fields check."""
        Player.create(id=201939, name="Stephen Curry", name_normalized="stephen curry")
        # team_id is nullable (null=True in model), so this insert succeeds
        PlayerGameStats.create(
            player_id=201939,
            team_id=None,  # explicitly null — nullable FK
            game_date=date(2026, 2, 14),
            fpts=30,
            pts=20, reb=5, ast=5, stl=1, blk=0, tov=2,
            min=30,
            fgm=8, fga=16, fg3m=3, fg3a=8, ftm=3, fta=4,
        )

        service = DataQualityService()
        check = _run_single_check(service, "player_game_stats_required_fields_not_null")
        assert check["status"] == "failed"
        assert check["failures"] >= 1


@pytest.mark.integration
class TestNonNegativeMinutesCheck:
    """player_game_stats_non_negative_minutes"""

    def test_detects_negative_minutes(self, integration_db, quality_tables):
        Player.create(id=201939, name="Stephen Curry", name_normalized="stephen curry")
        # Use raw SQL to insert negative minutes — ORM would accept it since
        # IntegerField has no min-value constraint, but the quality check catches it.
        db.execute_sql(
            """
            INSERT INTO nba.player_game_stats
              (player_id, team_id, game_date, fpts, pts, reb, ast, stl, blk, tov, min,
               fgm, fga, fg3m, fg3a, ftm, fta, created_at, updated_at)
            VALUES (201939, 'GSW', '2026-02-14', 10, 10, 5, 3, 1, 0, 2, -5,
                    4, 10, 2, 5, 2, 3, NOW(), NOW())
            """
        )

        service = DataQualityService()
        check = _run_single_check(service, "player_game_stats_non_negative_minutes")
        assert check["status"] == "failed"
        assert check["failures"] >= 1


@pytest.mark.integration
class TestStalePipelineRunsCheck:
    """pipeline_runs_no_stale_running"""

    def test_detects_stale_running_pipeline(self, integration_db, quality_tables):
        """A pipeline stuck in 'running' for >6 hours should be flagged."""
        PipelineRun.create(
            pipeline_name="test_stale",
            started_at=datetime.utcnow() - timedelta(hours=7),
            status="running",
        )

        service = DataQualityService()
        check = _run_single_check(service, "pipeline_runs_no_stale_running")
        assert check["status"] == "failed"
        assert check["failures"] >= 1

    def test_recent_running_is_not_stale(self, integration_db, quality_tables):
        """A pipeline running for <6 hours should not be flagged."""
        PipelineRun.create(
            pipeline_name="test_recent",
            started_at=datetime.utcnow() - timedelta(hours=1),
            status="running",
        )

        service = DataQualityService()
        check = _run_single_check(service, "pipeline_runs_no_stale_running")
        assert check["status"] == "passed"


@pytest.mark.integration
class TestFreshnessCheck:
    """pipeline_runs_recent_success_freshness"""

    def test_detects_no_recent_success(self, integration_db, quality_tables):
        """No successful run in 36 hours should trigger failure."""
        PipelineRun.create(
            pipeline_name="old_run",
            started_at=datetime.utcnow() - timedelta(hours=48),
            completed_at=datetime.utcnow() - timedelta(hours=48),
            status="success",
            records_processed=10,
        )

        service = DataQualityService()
        check = _run_single_check(service, "pipeline_runs_recent_success_freshness")
        assert check["status"] == "failed"

    def test_recent_success_passes(self, integration_db, quality_tables):
        """A successful run within 36 hours should pass."""
        PipelineRun.create(
            pipeline_name="recent_run",
            started_at=datetime.utcnow() - timedelta(hours=1),
            completed_at=datetime.utcnow(),
            status="success",
            records_processed=10,
        )

        service = DataQualityService()
        check = _run_single_check(service, "pipeline_runs_recent_success_freshness")
        assert check["status"] == "passed"


@pytest.mark.integration
class TestTeamGameMatchCheck:
    """player_game_stats_team_has_matching_game"""

    def test_detects_orphan_game_stats(self, integration_db, quality_tables):
        """Game stats for a team/date with no matching game record."""
        Player.create(id=201939, name="Stephen Curry", name_normalized="stephen curry")
        # Stats on a date with no game record seeded for GSW
        PlayerGameStats.create(
            player_id=201939,
            team_id="GSW",
            game_date=date(2026, 3, 1),  # no Game row for this date
            fpts=30,
            pts=20, reb=5, ast=5, stl=1, blk=0, tov=2,
            min=30,
            fgm=8, fga=16, fg3m=3, fg3a=8, ftm=3, fta=4,
        )

        service = DataQualityService()
        check = _run_single_check(service, "player_game_stats_team_has_matching_game")
        assert check["status"] == "failed"
        assert check["failures"] >= 1


@pytest.mark.integration
class TestOrphanPlayersCheck:
    """player_season_stats_no_orphan_players

    The FK constraint (player_season_stats_player_id_fkey) normally prevents
    orphan rows from being inserted. This quality check is defense-in-depth for
    cases where orphans can be introduced outside the ORM: pg_restore, bulk COPY
    loads, or migration scripts that temporarily disable triggers.

    To test it we bypass FK enforcement with session_replication_role=replica,
    which is the same mechanism pg_restore uses.
    """

    def test_detects_orphan_season_stats(self, integration_db, quality_tables):
        """Season stats referencing a player_id not in the players table."""
        # Bypass FK enforcement for this insert — simulates a bulk load or restore
        db.execute_sql("SET session_replication_role = replica")
        try:
            db.execute_sql(
                """
                INSERT INTO nba.player_season_stats
                  (player_id, team_id, as_of_date, season, gp, fpts,
                   pts, reb, ast, stl, blk, tov, min,
                   fgm, fga, fg3m, fg3a, ftm, fta, rank,
                   created_at, updated_at)
                VALUES (999999, 'GSW', '2026-02-14', '2025-26', 10, 500,
                        200, 50, 60, 20, 5, 30, 340,
                        80, 160, 30, 80, 40, 50, 99,
                        NOW(), NOW())
                """
            )
        finally:
            db.execute_sql("SET session_replication_role = DEFAULT")

        service = DataQualityService()
        check = _run_single_check(service, "player_season_stats_no_orphan_players")
        assert check["status"] == "failed"
        assert check["failures"] >= 1


@pytest.mark.integration
class TestRollingStatsWindowCheck:
    """player_rolling_stats_window_allowed_values"""

    def test_detects_invalid_window_days(self, integration_db, quality_tables):
        """window_days=99 should be flagged as invalid."""
        Player.create(id=201939, name="Stephen Curry", name_normalized="stephen curry")
        db.execute_sql(
            """
            INSERT INTO nba.player_rolling_stats
              (player_id, team_id, as_of_date, window_days, gp, fpts,
               pts, reb, ast, stl, blk, tov, min,
               fgm, fga, fg_pct, fg3m, fg3a, fg3_pct, ftm, fta, ft_pct,
               created_at, updated_at)
            VALUES (201939, 'GSW', '2026-02-14', 99, 3, 45.0,
                    25.0, 5.0, 6.0, 2.0, 0.5, 2.5, 34.0,
                    10.0, 20.0, 0.5000, 4.0, 10.0, 0.4000, 4.0, 5.0, 0.8000,
                    NOW(), NOW())
            """
        )

        service = DataQualityService()
        check = _run_single_check(service, "player_rolling_stats_window_allowed_values")
        assert check["status"] == "failed"
        assert check["failures"] >= 1


@pytest.mark.integration
class TestStatRangesCheck:
    """player_game_stats_stat_ranges_valid"""

    def test_detects_made_greater_than_attempted(self, integration_db, quality_tables):
        """fgm > fga should be flagged as invalid."""
        Player.create(id=201939, name="Stephen Curry", name_normalized="stephen curry")
        db.execute_sql(
            """
            INSERT INTO nba.player_game_stats
              (player_id, team_id, game_date, fpts, pts, reb, ast, stl, blk, tov, min,
               fgm, fga, fg3m, fg3a, ftm, fta, created_at, updated_at)
            VALUES (201939, 'GSW', '2026-02-14', 30, 20, 5, 3, 1, 0, 2, 30,
                    15, 10, 3, 8, 2, 3, NOW(), NOW())
            """
        )

        service = DataQualityService()
        check = _run_single_check(service, "player_game_stats_stat_ranges_valid")
        assert check["status"] == "failed"
        assert check["failures"] >= 1

    def test_detects_negative_stat(self, integration_db, quality_tables):
        """A negative pts value should be flagged."""
        Player.create(id=201939, name="Stephen Curry", name_normalized="stephen curry")
        db.execute_sql(
            """
            INSERT INTO nba.player_game_stats
              (player_id, team_id, game_date, fpts, pts, reb, ast, stl, blk, tov, min,
               fgm, fga, fg3m, fg3a, ftm, fta, created_at, updated_at)
            VALUES (201939, 'GSW', '2026-02-14', -5, -5, 5, 3, 1, 0, 2, 30,
                    5, 10, 2, 8, 2, 3, NOW(), NOW())
            """
        )

        service = DataQualityService()
        check = _run_single_check(service, "player_game_stats_stat_ranges_valid")
        assert check["status"] == "failed"
        assert check["failures"] >= 1


@pytest.mark.integration
class TestSeasonTotalsCheck:
    """player_season_stats_totals_match_game_log"""

    def test_detects_large_discrepancy(self, integration_db, quality_tables):
        """Season pts deviating >50 from summed game log should be flagged."""
        Player.create(id=201939, name="Stephen Curry", name_normalized="stephen curry")
        # Seed 5 game stats totalling 150 pts
        for i in range(5):
            PlayerGameStats.create(
                player_id=201939,
                team_id="GSW",
                game_date=date(2026, 2, i + 1),
                fpts=30, pts=30, reb=5, ast=6, stl=2, blk=0, tov=3,
                min=34,
                fgm=10, fga=20, fg3m=5, fg3a=12, ftm=5, fta=6,
            )
        # Season stats claim 500 pts — 350 off from game log
        PlayerSeasonStats.create(
            player_id=201939,
            team_id="GSW",
            as_of_date=date(2026, 2, 14),
            season="2025-26",
            gp=5,
            fpts=2500,
            pts=500, reb=25, ast=30, stl=10, blk=5, tov=15,
            min=170,
            fgm=50, fga=100, fg3m=25, fg3a=60, ftm=25, fta=30,
            rank=1,
        )

        service = DataQualityService()
        check = _run_single_check(service, "player_season_stats_totals_match_game_log")
        assert check["status"] == "failed"
        assert check["failures"] >= 1

    def test_passes_when_totals_match(self, integration_db, quality_tables):
        """Season pts matching game log sum should pass."""
        Player.create(id=201939, name="Stephen Curry", name_normalized="stephen curry")
        for i in range(5):
            PlayerGameStats.create(
                player_id=201939,
                team_id="GSW",
                game_date=date(2026, 2, i + 1),
                fpts=30, pts=30, reb=5, ast=6, stl=2, blk=0, tov=3,
                min=34,
                fgm=10, fga=20, fg3m=5, fg3a=12, ftm=5, fta=6,
            )
        PlayerSeasonStats.create(
            player_id=201939,
            team_id="GSW",
            as_of_date=date(2026, 2, 14),
            season="2025-26",
            gp=5,
            fpts=150,
            pts=150, reb=25, ast=30, stl=10, blk=0, tov=15,
            min=170,
            fgm=50, fga=100, fg3m=25, fg3a=60, ftm=25, fta=30,
            rank=1,
        )

        service = DataQualityService()
        check = _run_single_check(service, "player_season_stats_totals_match_game_log")
        assert check["status"] == "passed"


@pytest.mark.integration
class TestPipelineTimingChecks:
    """Per-pipeline execution recency checks."""

    def test_timing_check_fails_when_pipeline_never_ran(self, integration_db, quality_tables):
        """A pipeline with no recent successful run should trigger the timing check."""
        service = DataQualityService()
        check = _run_single_check(service, "player_game_stats_ran_within_24h")
        assert check["status"] == "failed"

    def test_timing_check_passes_when_pipeline_ran_recently(self, integration_db, quality_tables):
        """A pipeline that ran within 24 hours should pass."""
        PipelineRun.create(
            pipeline_name="player_game_stats",
            started_at=datetime.utcnow(),
            completed_at=datetime.utcnow(),
            status="success",
            records_processed=10,
        )

        service = DataQualityService()
        check = _run_single_check(service, "player_game_stats_ran_within_24h")
        assert check["status"] == "passed"

    def test_timing_check_fails_when_only_old_run_exists(self, integration_db, quality_tables):
        """A successful run older than 24 hours should not satisfy the timing check."""
        PipelineRun.create(
            pipeline_name="player_game_stats",
            started_at=datetime.utcnow() - timedelta(hours=25),
            completed_at=datetime.utcnow() - timedelta(hours=25),
            status="success",
            records_processed=10,
        )

        service = DataQualityService()
        check = _run_single_check(service, "player_game_stats_ran_within_24h")
        assert check["status"] == "failed"


# ---------------------------------------------------------------------------
# Service-level tests
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestDataQualityServiceIntegration:
    """Test the DataQualityService orchestration."""

    def test_run_stores_results_in_db(self, integration_db, quality_tables):
        """Running checks creates DataQualityRun and DataQualityCheck records."""
        _seed_clean_data()
        service = DataQualityService()
        run = service.run_checks(triggered_by="test")

        assert DataQualityRun.select().count() == 1
        stored_run = DataQualityRun.select().first()
        assert str(stored_run.id) == str(run.id)
        assert stored_run.triggered_by == "test"

        checks = list(DataQualityCheck.select().where(DataQualityCheck.run_id == run.id))
        assert len(checks) == EXPECTED_CHECK_COUNT

    def test_selective_check_execution(self, integration_db, quality_tables):
        """Running a subset of checks only executes those checks."""
        _seed_clean_data()
        service = DataQualityService()
        run = service.run_checks(
            check_names=["player_game_stats_non_negative_minutes"],
            triggered_by="test",
        )

        checks = list(DataQualityCheck.select().where(DataQualityCheck.run_id == run.id))
        assert len(checks) == 1
        assert checks[0].check_name == "player_game_stats_non_negative_minutes"

    def test_list_runs_returns_history(self, integration_db, quality_tables):
        """list_runs() returns past run summaries in reverse chronological order."""
        _seed_clean_data()
        service = DataQualityService()
        service.run_checks(triggered_by="test-1")
        service.run_checks(triggered_by="test-2")

        runs = service.list_runs(limit=10)
        assert len(runs) == 2
        assert runs[0]["triggered_by"] == "test-2"
        assert runs[1]["triggered_by"] == "test-1"

    def test_get_run_returns_checks(self, integration_db, quality_tables):
        """get_run() includes per-check details."""
        _seed_clean_data()
        service = DataQualityService()
        run = service.run_checks(triggered_by="test")

        detail = service.get_run(str(run.id))
        assert detail is not None
        assert "checks" in detail
        assert len(detail["checks"]) == EXPECTED_CHECK_COUNT
        for check in detail["checks"]:
            assert "check_name" in check
            assert "status" in check
            assert "severity" in check
