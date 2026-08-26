"""
Data Quality Service

Runs SQL-based data quality checks and stores run/check history.

Check groups:
  STRUCTURAL_CHECKS  — schema integrity, referential consistency, field validity
  TIMING_CHECKS      — per-pipeline execution recency (generated from registry)

Alerting (`quality_critical`): once a run is recorded, every critical check that
failed (or could not execute) posts one critical alert to the ops webhook,
deduped per check for 24 h. Warning-severity checks never alert here (the
nightly quality-check workflow reports them).
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass
from datetime import timedelta
from typing import Any

from db.base import db
from db.models.data_quality_check import DataQualityCheck
from db.models.data_quality_run import DataQualityRun
from services.alert_service import AlertEvent, get_alert_service

QUALITY_ALERT_DEDUPE = timedelta(hours=24)


@dataclass(frozen=True)
class SQLQualityCheck:
    name: str
    severity: str
    sql: str
    failure_message: str


# ---------------------------------------------------------------------------
# Structural checks — schema integrity, referential consistency, field validity
# ---------------------------------------------------------------------------

STRUCTURAL_CHECKS: tuple[SQLQualityCheck, ...] = (
    SQLQualityCheck(
        name="player_game_stats_required_fields_not_null",
        severity="critical",
        sql="""
            SELECT COUNT(*)
            FROM nba.player_game_stats
            WHERE player_id IS NULL OR game_date IS NULL OR team_id IS NULL
        """,
        failure_message="player_game_stats contains null values in required fields",
    ),
    SQLQualityCheck(
        name="player_game_stats_non_negative_minutes",
        severity="critical",
        sql="""
            SELECT COUNT(*)
            FROM nba.player_game_stats
            WHERE min < 0
        """,
        failure_message="player_game_stats contains negative minutes",
    ),
    SQLQualityCheck(
        name="pipeline_runs_no_stale_running",
        severity="warning",
        sql="""
            SELECT COUNT(*)
            FROM nba.pipeline_runs
            WHERE status = 'running'
              AND started_at < (NOW() - INTERVAL '6 hours')
        """,
        failure_message="stale running pipeline_run rows detected",
    ),
    SQLQualityCheck(
        name="pipeline_runs_recent_success_freshness",
        severity="warning",
        sql="""
            SELECT
              CASE
                WHEN MAX(started_at) IS NULL THEN 1
                WHEN MAX(started_at) < (NOW() - INTERVAL '36 hours') THEN 1
                ELSE 0
              END
            FROM nba.pipeline_runs
            WHERE status = 'success'
        """,
        failure_message="no successful pipeline runs in the last 36 hours",
    ),
    SQLQualityCheck(
        name="player_game_stats_team_has_matching_game",
        severity="critical",
        sql="""
            SELECT COUNT(*)
            FROM nba.player_game_stats pgs
            LEFT JOIN nba.games g
              ON g.game_date = pgs.game_date
             AND (g.home_team_id = pgs.team_id OR g.away_team_id = pgs.team_id)
            WHERE g.game_id IS NULL
        """,
        failure_message="player_game_stats rows without matching game/team schedule entry",
    ),
    SQLQualityCheck(
        name="player_season_stats_no_orphan_players",
        severity="critical",
        sql="""
            SELECT COUNT(*)
            FROM nba.player_season_stats pss
            LEFT JOIN nba.players p ON p.id = pss.player_id
            WHERE p.id IS NULL
        """,
        failure_message="player_season_stats has orphan player references",
    ),
    SQLQualityCheck(
        name="player_rolling_stats_window_allowed_values",
        severity="critical",
        sql="""
            SELECT COUNT(*)
            FROM nba.player_rolling_stats
            WHERE window_days NOT IN (7, 14, 30)
        """,
        failure_message="player_rolling_stats contains unsupported window_days values",
    ),
    # Business correctness — stat values within realistic bounds
    SQLQualityCheck(
        name="player_game_stats_stat_ranges_valid",
        severity="critical",
        sql="""
            SELECT COUNT(*)
            FROM nba.player_game_stats
            WHERE pts < 0 OR reb < 0 OR ast < 0
               OR stl < 0 OR blk < 0 OR tov < 0
               OR fgm > fga OR fg3m > fg3a OR ftm > fta
               OR min > 60
        """,
        failure_message="player_game_stats contains out-of-range values (negative stats, made > attempted, or minutes > 60)",
    ),
    # Business correctness — season totals should roughly match summed game log.
    # Tolerance of 50 pts handles mid-season corrections and data source lag.
    # Only applied to players with 5+ games to filter sparse/mid-season records.
    SQLQualityCheck(
        name="player_season_stats_totals_match_game_log",
        severity="warning",
        sql="""
            SELECT COUNT(*) FROM (
                WITH latest_season AS (
                    SELECT DISTINCT ON (player_id)
                        player_id,
                        pts,
                        gp,
                        season
                    FROM nba.player_season_stats
                    ORDER BY player_id, as_of_date DESC
                )
                SELECT
                    pss.player_id,
                    pss.pts      AS season_pts,
                    SUM(pgs.pts) AS game_log_pts
                FROM latest_season pss
                JOIN nba.player_game_stats pgs
                  ON pgs.player_id = pss.player_id
                 AND pgs.game_date >= (SPLIT_PART(pss.season, '-', 1) || '-10-01')::date
                GROUP BY pss.player_id, pss.pts, pss.gp
                HAVING pss.gp >= 5
                   AND ABS(pss.pts - SUM(pgs.pts)) > 50
            ) mismatches
        """,
        failure_message="player_season_stats totals deviate >50 pts from summed game log for players with 5+ games",
    ),
)


# ---------------------------------------------------------------------------
# Timing checks — per-pipeline execution recency
# Generated from the pipeline registry so they stay in sync automatically.
#
# Pipelines excluded from timing checks:
#   player_profiles   — manual ad-hoc via dashboard
#   game_start_times  — manual ad-hoc via dashboard
#
# All other pipelines are expected to have run within 24 hours.
# WARNING severity — timing checks will fire on off-days (no games) which is
# expected behaviour, not a data error. Investigate in context of game schedule.
# ---------------------------------------------------------------------------

_MANUAL_PIPELINES: frozenset[str] = frozenset({"player_profiles", "game_start_times"})


def _build_timing_checks() -> tuple[SQLQualityCheck, ...]:
    """Generate one timing check per pipeline, excluding manual ad-hoc ones."""
    # Import here to avoid circular imports at module load time.
    from pipelines import PIPELINE_REGISTRY

    checks = []
    for name, cls in PIPELINE_REGISTRY.items():
        if name in _MANUAL_PIPELINES:
            continue

        display_name = cls.config.display_name
        category = cls.config.category.value

        checks.append(SQLQualityCheck(
            name=f"{name}_ran_within_24h",
            severity="warning",
            sql=f"""
                SELECT CASE
                    WHEN EXISTS (
                        SELECT 1 FROM nba.pipeline_runs
                        WHERE pipeline_name = '{name}'
                          AND status = 'success'
                          AND started_at >= NOW() - INTERVAL '24 hours'
                    ) THEN 0 ELSE 1 END
            """,
            failure_message=(
                f"{display_name} ({category}) has not run successfully in the last 24 hours"
                " — expected on off-days or when no games were scheduled"
            ),
        ))

    return tuple(checks)


TIMING_CHECKS: tuple[SQLQualityCheck, ...] = _build_timing_checks()

# ---------------------------------------------------------------------------
# Combined check set — used by default when no check_names filter is applied
# ---------------------------------------------------------------------------

CORE_SQL_CHECKS: tuple[SQLQualityCheck, ...] = STRUCTURAL_CHECKS + TIMING_CHECKS


class DataQualityService:
    """Runs and retrieves SQL-backed quality checks."""

    def __init__(self) -> None:
        self._checks = {c.name: c for c in CORE_SQL_CHECKS}

    def list_available_checks(self) -> list[str]:
        return sorted(self._checks.keys())

    def run_checks(
        self,
        check_names: list[str] | None = None,
        triggered_by: str = "manual",
    ) -> DataQualityRun:
        if check_names:
            selected_checks = [self._checks[name] for name in check_names if name in self._checks]
        else:
            selected_checks = list(CORE_SQL_CHECKS)

        run = DataQualityRun.start_run(triggered_by=triggered_by)
        passed = 0
        failed = 0
        critical_failures: list[dict[str, Any]] = []

        try:
            for check in selected_checks:
                start = time.perf_counter()
                check_status = "passed"
                failures = 0
                message = None
                details: dict[str, Any] | None = None

                try:
                    cursor = db.execute_sql(check.sql)
                    row = cursor.fetchone()
                    failures = self._coerce_failures(row[0] if row else 0)
                    if failures > 0:
                        check_status = "failed"
                        message = check.failure_message
                        details = {"failures": failures}
                except Exception as exc:
                    check_status = "error"
                    failures = 1
                    message = f"check execution error: {exc}"
                    details = {"error": str(exc)}

                duration_ms = int((time.perf_counter() - start) * 1000)

                DataQualityCheck.create(
                    run_id=run.id,
                    check_name=check.name,
                    status=check_status,
                    severity=check.severity,
                    failures=failures,
                    message=message,
                    details_json=json.dumps(details) if details else None,
                    duration_ms=duration_ms,
                )

                if check_status == "passed":
                    passed += 1
                else:
                    failed += 1
                    if check.severity == "critical":
                        critical_failures.append({
                            "name": check.name,
                            "status": check_status,
                            "failures": failures,
                            "message": message,
                        })

            run.mark_completed(
                total_checks=len(selected_checks),
                passed_checks=passed,
                failed_checks=failed,
            )
        except Exception as exc:
            run.mark_completed(
                total_checks=len(selected_checks),
                passed_checks=passed,
                failed_checks=failed + 1,
                error_message=str(exc),
            )

        self._alert_critical(run, critical_failures)
        return run

    @staticmethod
    def _alert_critical(run: DataQualityRun, critical_failures: list[dict[str, Any]]) -> None:
        """One `quality_critical` alert per failed/errored critical check (never raises)."""
        alerts = get_alert_service()
        for failure in critical_failures:
            alerts.notify(AlertEvent(
                key=f"quality_critical:{failure['name']}",
                severity="critical",
                title=f"Data quality: {failure['name']}",
                body=failure["message"] or "critical check failed",
                fields={
                    "check": failure["name"],
                    "status": failure["status"],
                    "failures": failure["failures"],
                    "triggered_by": run.triggered_by,
                    "run_id": str(run.id),
                },
                dedupe=QUALITY_ALERT_DEDUPE,
            ))

    def list_runs(self, limit: int = 20) -> list[dict[str, Any]]:
        rows = (
            DataQualityRun.select()
            .order_by(DataQualityRun.started_at.desc())
            .limit(limit)
        )
        return [self._serialize_run(r) for r in rows]

    def get_run(self, run_id: str) -> dict[str, Any] | None:
        run = (
            DataQualityRun.select()
            .where(DataQualityRun.id == run_id)
            .first()
        )
        if run is None:
            return None

        checks = (
            DataQualityCheck.select()
            .where(DataQualityCheck.run_id == run.id)
            .order_by(DataQualityCheck.id.asc())
        )

        payload = self._serialize_run(run)
        payload["checks"] = [self._serialize_check(c) for c in checks]
        return payload

    @staticmethod
    def _serialize_run(run: DataQualityRun) -> dict[str, Any]:
        return {
            "run_id": str(run.id),
            "status": run.status,
            "started_at": run.started_at.isoformat(),
            "completed_at": run.completed_at.isoformat() if run.completed_at else None,
            "duration_seconds": run.duration_seconds,
            "total_checks": run.total_checks,
            "passed_checks": run.passed_checks,
            "failed_checks": run.failed_checks,
            "triggered_by": run.triggered_by,
            "error_message": run.error_message,
        }

    @staticmethod
    def _serialize_check(check: DataQualityCheck) -> dict[str, Any]:
        details = None
        if check.details_json:
            try:
                details = json.loads(check.details_json)
            except json.JSONDecodeError:
                details = {"raw": check.details_json}
        return {
            "check_name": check.check_name,
            "status": check.status,
            "severity": check.severity,
            "failures": check.failures,
            "message": check.message,
            "details": details,
            "duration_ms": check.duration_ms,
        }

    @staticmethod
    def _coerce_failures(value: Any) -> int:
        """Normalize SQL scalar output into an integer failure count."""
        if value is None:
            return 0
        if isinstance(value, bool):
            return 1 if value else 0
        try:
            return int(value)
        except (TypeError, ValueError):
            return 1
