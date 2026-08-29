"""
Base Pipeline

Abstract base class for all data pipelines.
"""

import asyncio
from abc import ABC, abstractmethod
from contextlib import nullcontext
from datetime import date, datetime, timedelta
from typing import ClassVar, Optional

import pytz

from core.locks import LockNotAcquired, pipeline_lock
from core.logging import get_logger
from db.base import db
from pipelines.config import PipelineConfig
from pipelines.context import PipelineContext
from schemas.common import ApiStatus
from schemas.pipeline import PipelineResult
from services.alert_service import AlertEvent, get_alert_service

# One `pipeline_failed` alert per pipeline per window (live-stats failing all
# night is one message).
PIPELINE_FAILED_DEDUPE = timedelta(hours=6)


class BasePipeline(ABC):
    """
    Abstract base class for all data pipelines.

    Provides:
    - Automatic run tracking via PipelineContext
    - Structured logging with correlation IDs
    - Standardized error handling
    - Template method pattern for run lifecycle
    - Thread-based execution to avoid blocking the async event loop

    Subclasses must implement:
    - config: PipelineConfig class attribute
    - execute(): The actual pipeline logic (synchronous)

    Example:
        class DailyPlayerStatsPipeline(BasePipeline):
            config = PipelineConfig(
                name="daily_player_stats",
                display_name="Daily Player Stats",
                description="Fetches yesterday's game stats from NBA API",
                target_table="nba.player_game_stats",
            )

            def execute(self, ctx: PipelineContext) -> None:
                # Pipeline implementation
                data = self.espn_extractor.get_player_data()
                ctx.increment_records(len(data))
    """

    # Class-level configuration - must be overridden by subclasses
    config: ClassVar[PipelineConfig]

    def __init__(self):
        """Initialize pipeline and validate configuration."""
        self._validate_config()

    def _validate_config(self) -> None:
        """Validate that config is properly defined."""
        if not hasattr(self.__class__, "config") or self.__class__.config is None:
            raise ValueError(
                f"{self.__class__.__name__} must define a 'config' class attribute"
            )

    @abstractmethod
    def execute(self, ctx: PipelineContext) -> None:
        """
        Execute the pipeline logic.

        This is the main method subclasses implement. It runs in a separate
        thread to avoid blocking the async event loop. All synchronous I/O
        (HTTP requests, database calls) is safe to call directly here.

        Args:
            ctx: Pipeline context with logging, tracking, and timing

        Raises:
            Any exception will be caught and converted to a failed result
        """
        pass

    def _run_sync(
        self,
        date_override: Optional[date] = None,
        options: Optional[dict] = None,
        nba_date: Optional[date] = None,
    ) -> PipelineResult:
        """
        Run the full pipeline lifecycle synchronously.

        Called via asyncio.to_thread() from run() so that all blocking I/O
        (Peewee DB calls, HTTP requests) executes in a thread pool worker
        instead of on the async event loop.

        Manages its own DB connection since it runs in a separate thread
        from the request handler (Peewee uses thread-local connections).

        The whole run is held under an execution lock (`core/locks.py`) unless
        the pipeline opts out with `allow_concurrent`. Losing the lock is not a
        failure: another run of this pipeline is already doing the work, so the
        result is `skipped` and no run record is written.
        """
        if db.is_closed():
            db.connect()

        lock = (
            nullcontext()
            if self.config.allow_concurrent
            else pipeline_lock(self.config.name)
        )
        try:
            try:
                with lock:
                    return self._run_locked(date_override, options, nba_date)
            except LockNotAcquired as e:
                return self._skipped_result(str(e))
        finally:
            if not db.is_closed():
                db.close()

    def _run_locked(
        self,
        date_override: Optional[date],
        options: Optional[dict],
        nba_date: Optional[date],
    ) -> PipelineResult:
        """The lifecycle proper, with the execution lock already held."""
        ctx = PipelineContext(
            self.config.name,
            date_override=date_override,
            nba_date=nba_date,
            options=dict(options or {}),
        )
        ctx.start_tracking()

        try:
            self.before_execute(ctx)
            self.execute(ctx)
            self.after_execute(ctx)
            return ctx.mark_success()
        except Exception as e:
            result = ctx.mark_failed(e)
            self._notify_failure(ctx, e)
            return result

    def _skipped_result(self, message: str) -> PipelineResult:
        """A run that never started because another one holds the lock."""
        now = datetime.now(pytz.timezone("US/Central"))
        get_logger("pipeline").warning(
            "pipeline_lock_contended", pipeline=self.config.name
        )
        return PipelineResult(
            status=ApiStatus.SKIPPED,
            message=f"{self.config.name} skipped — {message}",
            started_at=now.isoformat(),
            completed_at=now.isoformat(),
            duration_seconds=0.0,
            records_processed=0,
        )

    def _notify_failure(self, ctx: PipelineContext, error: Exception) -> None:
        """`pipeline_failed` (critical) to the ops webhook; never raises."""
        get_alert_service().notify(AlertEvent(
            key=f"pipeline_failed:{self.config.name}",
            severity="critical",
            title=f"Pipeline failed: {self.config.display_name}",
            body=f"{type(error).__name__}: {str(error)[:500]}",
            fields={
                "pipeline": self.config.name,
                "category": self.config.category.value,
                "run_id": str(ctx.run_id),
                "date_override": str(ctx.date_override) if ctx.date_override else None,
                "records_processed": ctx.records_processed,
                "target_table": self.config.target_table,
            },
            dedupe=PIPELINE_FAILED_DEDUPE,
        ))

    async def run(
        self,
        date_override: Optional[date] = None,
        options: Optional[dict] = None,
        nba_date: Optional[date] = None,
    ) -> PipelineResult:
        """
        Run the pipeline with full lifecycle management.

        This is the public entry point. The entire pipeline execution
        (including DB and HTTP I/O) runs in a thread pool worker via
        asyncio.to_thread() to avoid blocking the event loop.

        Args:
            date_override: If provided, pipelines use this date instead of
                           computing from the current time. Useful for backfills.
            options: Free-form per-run options exposed as ctx.options
                     (e.g. {"source": "static"}). Defaults to {}.
            nba_date: The triggering batch's NBA game date, shared by every
                      pipeline in that batch. Unlike date_override it does not
                      mark the run as a backfill.

        Returns:
            PipelineResult with status, timing, and records processed
        """
        return await asyncio.to_thread(
            self._run_sync, date_override, options, nba_date
        )

    def before_execute(self, ctx: PipelineContext) -> None:
        """
        Hook called before execute().

        Override for validation or setup tasks.
        """
        pass

    def after_execute(self, ctx: PipelineContext) -> None:
        """
        Hook called after successful execute().

        Override for cleanup tasks.
        """
        pass

    @classmethod
    def get_name(cls) -> str:
        """Get the pipeline name from config."""
        return cls.config.name

    @classmethod
    def get_info(cls) -> dict:
        """Get pipeline information for listing."""
        return {
            "name": cls.config.name,
            "display_name": cls.config.display_name,
            "description": cls.config.description,
            "target_table": cls.config.target_table,
        }

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__}(name={self.config.name})>"
