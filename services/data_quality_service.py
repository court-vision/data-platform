"""
Data Quality Service

Runs SQL-based data quality checks and stores run/check history.
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass
from typing import Any

from db.base import db
from db.models.data_quality_check import DataQualityCheck
from db.models.data_quality_run import DataQualityRun


@dataclass(frozen=True)
class SQLQualityCheck:
    name: str
    severity: str
    sql: str
    failure_message: str


CORE_SQL_CHECKS: tuple[SQLQualityCheck, ...] = (
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
)


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

        return run

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
