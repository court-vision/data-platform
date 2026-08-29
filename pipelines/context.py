"""
Pipeline Context

Manages pipeline execution context including run tracking, logging, and timing.
"""

from __future__ import annotations

import traceback
import uuid
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from typing import Optional, Any

import pytz
import sentry_sdk

from core.logging import get_correlation_id, get_logger
from core.nba_calendar import nba_date_et
from db.models.pipeline_run import PipelineRun
from schemas.pipeline import PipelineResult
from schemas.common import ApiStatus
from services.alert_service import AlertEvent, get_alert_service

# A successful run alerts (`pipeline_partial`, warning) when nothing succeeded
# or more than this share of attempted records failed.
PARTIAL_ALERT_FAILED_SHARE = 0.20
PARTIAL_ALERT_DEDUPE = timedelta(hours=24)


@dataclass
class PipelineContext:
    """
    Manages pipeline execution context including:
    - Correlation ID for log tracing
    - PipelineRun database record
    - Timing information
    - Records processed / failed / skipped counters (partial-success reporting)
    - Free-form run options from the trigger (e.g. {"source": "cdn"})

    Usage:
        ctx = PipelineContext("daily_player_stats")
        ctx.start_tracking()
        try:
            # Do work
            ctx.increment_records(10)
            ctx.increment_failed(1, "team_processing_error")   # kept going, one item lost
            return ctx.mark_success()                          # -> result.partial is True
        except Exception as e:
            return ctx.mark_failed(e)

    Counters live only on the context / `PipelineResult` / the run's message and
    logs — `nba.pipeline_runs` keeps its schema (owned by backend/migrations).
    """

    pipeline_name: str
    run_id: uuid.UUID = field(default_factory=uuid.uuid4)
    started_at: datetime = field(
        default_factory=lambda: datetime.now(pytz.timezone("US/Central"))
    )
    records_processed: int = 0
    records_failed: int = 0
    records_skipped: int = 0
    failure_reasons: dict[str, int] = field(default_factory=dict)
    skip_reasons: dict[str, int] = field(default_factory=dict)
    date_override: Optional[date] = None
    # The NBA game date of the batch that triggered this run, computed once by
    # the trigger endpoint and shared by every pipeline in the batch. Distinct
    # from date_override, which means "this is a backfill" and is what
    # pipelines key their data-readiness checks off.
    nba_date: Optional[date] = None
    # Per-run options passed through from the trigger endpoint; pipelines that
    # don't declare any simply ignore it (default {}).
    options: dict = field(default_factory=dict)

    _db_run: Optional[PipelineRun] = field(default=None, repr=False)
    _log: Any = field(default=None, repr=False)

    def __post_init__(self):
        """Initialize the bound logger.

        The triggering request's correlation id is bound explicitly: pipelines
        run in worker threads (`asyncio.to_thread` copies contextvars, so it is
        reachable) and a bound field survives any later context changes.
        """
        bindings = {"pipeline": self.pipeline_name, "run_id": str(self.run_id)}
        correlation_id = get_correlation_id()
        if correlation_id:
            bindings["correlation_id"] = correlation_id
        self._log = get_logger("pipeline").bind(**bindings)

    @property
    def log(self):
        """Get the bound logger for this context."""
        return self._log

    def game_date(self) -> date:
        """The NBA game date this run is for.

        In precedence order: an explicit backfill date, then the date the
        triggering batch computed, then the 6 AM **Eastern** rule applied to
        this run's start. Every pipeline goes through here so that one batch
        cannot straddle the day boundary and write half its rows under one date
        and half under the next — and so that the date written matches the one
        the API reads back. See `core/nba_calendar`.
        """
        if self.date_override:
            return self.date_override
        if self.nba_date:
            return self.nba_date
        return nba_date_et(self.started_at)

    def start_tracking(self) -> None:
        """
        Create PipelineRun record in database.

        This creates the audit trail record and updates the run_id
        to match the database record.
        """
        self._db_run = PipelineRun.start_run(self.pipeline_name)
        self.run_id = self._db_run.id
        self._log = self._log.bind(run_id=str(self.run_id))
        self._log.info("pipeline_started")

    def increment_records(self, count: int = 1) -> None:
        """Increment the records processed counter."""
        self.records_processed += count

    def increment_failed(self, count: int = 1, reason: Optional[str] = None) -> None:
        """Count items the pipeline gave up on but kept running past (makes the run partial)."""
        self.records_failed += count
        if reason:
            self.failure_reasons[reason] = self.failure_reasons.get(reason, 0) + count

    def increment_skipped(self, count: int = 1, reason: Optional[str] = None) -> None:
        """Count items intentionally not processed (not started, filtered out); informational only."""
        self.records_skipped += count
        if reason:
            self.skip_reasons[reason] = self.skip_reasons.get(reason, 0) + count

    @property
    def partial(self) -> bool:
        return self.records_failed > 0

    @property
    def failed_share(self) -> float:
        """Failed items over attempted items (processed + failed); 0 when nothing was attempted."""
        attempted = self.records_processed + self.records_failed
        return self.records_failed / attempted if attempted else 0.0

    @staticmethod
    def _format_reasons(reasons: dict[str, int]) -> str:
        return ", ".join(f"{reason}={count}" for reason, count in sorted(reasons.items())) or "unspecified"

    def mark_success(self, message: Optional[str] = None) -> PipelineResult:
        """
        Mark pipeline as successful and return result.

        A run with `records_failed > 0` is still a success but `partial`; the
        message names the failure reasons and a `pipeline_partial` alert goes
        out when nothing succeeded or the failed share exceeds 20 %.

        Args:
            message: Optional custom success message

        Returns:
            PipelineResult with success status
        """
        completed_at = datetime.now(pytz.timezone("US/Central"))
        duration = (completed_at - self.started_at).total_seconds()
        partial = self.partial

        if self._db_run:
            self._db_run.mark_success(records_processed=self.records_processed)

        message = message or f"{self.pipeline_name} completed successfully"
        if partial:
            message = (
                f"{message} — {self.records_failed} of "
                f"{self.records_processed + self.records_failed} records failed "
                f"({self._format_reasons(self.failure_reasons)})"
            )

        self._log.info(
            "pipeline_completed",
            records_processed=self.records_processed,
            records_failed=self.records_failed,
            records_skipped=self.records_skipped,
            partial=partial,
            duration_seconds=duration,
        )
        if partial:
            self._log.warning(
                "pipeline_partial",
                records_processed=self.records_processed,
                records_failed=self.records_failed,
                failed_share=round(self.failed_share, 3),
                failure_reasons=self.failure_reasons,
            )
            self._alert_partial()

        return PipelineResult(
            status=ApiStatus.SUCCESS,
            message=message,
            started_at=self.started_at.isoformat(),
            completed_at=completed_at.isoformat(),
            duration_seconds=duration,
            records_processed=self.records_processed,
            records_failed=self.records_failed,
            records_skipped=self.records_skipped,
            partial=partial,
        )

    def _alert_partial(self) -> None:
        """`pipeline_partial` (warning) when nothing succeeded or > 20 % of attempts failed."""
        nothing_succeeded = self.records_processed == 0
        if not nothing_succeeded and self.failed_share <= PARTIAL_ALERT_FAILED_SHARE:
            return
        reasons = self._format_reasons(self.failure_reasons)
        get_alert_service().notify(AlertEvent(
            key=f"pipeline_partial:{self.pipeline_name}",
            severity="warning",
            title=f"Pipeline partial: {self.pipeline_name}",
            body=(
                f"{self.records_failed} of {self.records_processed + self.records_failed} records failed "
                f"({'nothing succeeded' if nothing_succeeded else f'{self.failed_share:.0%} of attempts'}): {reasons}"
            ),
            fields={
                "pipeline": self.pipeline_name,
                "run_id": str(self.run_id),
                "processed": self.records_processed,
                "failed": self.records_failed,
                "skipped": self.records_skipped,
                "reasons": reasons,
            },
            dedupe=PARTIAL_ALERT_DEDUPE,
        ))

    def mark_failed(self, error: Exception) -> PipelineResult:
        """
        Mark pipeline as failed and return error result.

        Args:
            error: The exception that caused the failure

        Returns:
            PipelineResult with error status
        """
        completed_at = datetime.now(pytz.timezone("US/Central"))
        duration = (completed_at - self.started_at).total_seconds()
        error_msg = f"{type(error).__name__}: {str(error)}"
        tb = traceback.format_exc()

        if self._db_run:
            self._db_run.mark_failed(error_msg)

        self._log.error(
            "pipeline_failed",
            error=error_msg,
            traceback=tb,
        )
        self._report_failure(error)

        return PipelineResult(
            status=ApiStatus.ERROR,
            message=f"{self.pipeline_name} failed",
            started_at=self.started_at.isoformat(),
            completed_at=completed_at.isoformat(),
            duration_seconds=duration,
            error=f"{error_msg}\n{tb}",
        )

    def _report_failure(self, error: Exception) -> None:
        """Send the failure to Sentry tagged with the pipeline and run (no-op without a DSN)."""
        if not sentry_sdk.is_initialized():
            return
        try:
            with sentry_sdk.new_scope() as scope:
                scope.set_tag("pipeline", self.pipeline_name)
                scope.set_tag("run_id", str(self.run_id))
                correlation_id = get_correlation_id()
                if correlation_id:
                    scope.set_tag("correlation_id", correlation_id)
                sentry_sdk.capture_exception(error)
        except Exception as exc:  # reporting must never mask the pipeline result
            self._log.warning("sentry_capture_failed", error=type(exc).__name__)
