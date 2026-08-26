"""
Pipeline Context

Manages pipeline execution context including run tracking, logging, and timing.
"""

from __future__ import annotations

import traceback
import uuid
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Optional, Any

import pytz
import sentry_sdk

from core.logging import get_correlation_id, get_logger
from db.models.pipeline_run import PipelineRun
from schemas.pipeline import PipelineResult
from schemas.common import ApiStatus


@dataclass
class PipelineContext:
    """
    Manages pipeline execution context including:
    - Correlation ID for log tracing
    - PipelineRun database record
    - Timing information
    - Records processed counter
    - Free-form run options from the trigger (e.g. {"source": "cdn"})

    Usage:
        ctx = PipelineContext("daily_player_stats")
        ctx.start_tracking()
        try:
            # Do work
            ctx.increment_records(10)
            return ctx.mark_success()
        except Exception as e:
            return ctx.mark_failed(e)
    """

    pipeline_name: str
    run_id: uuid.UUID = field(default_factory=uuid.uuid4)
    started_at: datetime = field(
        default_factory=lambda: datetime.now(pytz.timezone("US/Central"))
    )
    records_processed: int = 0
    date_override: Optional[date] = None
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

    def mark_success(self, message: Optional[str] = None) -> PipelineResult:
        """
        Mark pipeline as successful and return result.

        Args:
            message: Optional custom success message

        Returns:
            PipelineResult with success status
        """
        completed_at = datetime.now(pytz.timezone("US/Central"))
        duration = (completed_at - self.started_at).total_seconds()

        if self._db_run:
            self._db_run.mark_success(records_processed=self.records_processed)

        self._log.info(
            "pipeline_completed",
            records_processed=self.records_processed,
            duration_seconds=duration,
        )

        return PipelineResult(
            status=ApiStatus.SUCCESS,
            message=message or f"{self.pipeline_name} completed successfully",
            started_at=self.started_at.isoformat(),
            completed_at=completed_at.isoformat(),
            duration_seconds=duration,
            records_processed=self.records_processed,
        )

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
