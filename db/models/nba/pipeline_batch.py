"""
Pipeline Batch Model

A durable record of what a batch trigger *decided*, alongside `PipelineRun`'s
record of what ran. The gap between those two is the failure this table exists
to make visible: a batch that decides "nothing to do" on every poll for a whole
night leaves no trace in `nba.pipeline_runs` at all.

See `backend/migrations/0009__pipeline_batches.sql` for the schema and the
reasoning about which polls are worth a row.

Every write here is best-effort. A batch record is observability; losing one
must never stop a pipeline from running — including when the migration has not
been applied yet, which is the state between the two services' deploys.
"""

import uuid
from datetime import date as date_type, datetime, timedelta, timezone
from typing import Optional

from peewee import (
    BooleanField,
    CharField,
    DateField,
    DateTimeField,
    UUIDField,
)
from playhouse.postgres_ext import BinaryJSONField

from core.logging import get_logger
from db.base import BaseModel

log = get_logger("pipeline_batch")

# decision values
DISPATCHED = "dispatched"
ALL_SKIPPED = "all_skipped"
WINDOW_CLOSED = "window_closed"


class PipelineBatch(BaseModel):
    """One batch invocation's decision, and how it turned out.

    Attributes:
        category:     "pre_game" | "post_game"
        nba_date:     the batch's NBA game date (6 AM ET rule)
        decision:     "dispatched" | "all_skipped" | "window_closed"
        reason:       stable slug, mostly from pipelines/gates.py
        forced:       gates were bypassed (?force=true or ?date=)
        job_id:       the in-memory JobManager id, when one was created
        pipelines:    name -> {decision, reason, status, records}
        alerted:      this row fired the completeness alert for its date
    """

    id = UUIDField(primary_key=True, default=uuid.uuid4)
    category = CharField(max_length=20, index=True)
    nba_date = DateField(index=True)
    triggered_at = DateTimeField()
    completed_at = DateTimeField(null=True)
    decision = CharField(max_length=20)
    reason = CharField(max_length=64)
    forced = BooleanField(default=False)
    job_id = CharField(max_length=64, null=True)
    pipelines = BinaryJSONField(default=dict)
    alerted = BooleanField(default=False)

    class Meta:
        table_name = "pipeline_batches"
        schema = "nba"

    def __repr__(self) -> str:
        return (
            f"<PipelineBatch(category={self.category}, nba_date={self.nba_date}, "
            f"decision={self.decision}, reason={self.reason})>"
        )

    # -- writes ------------------------------------------------------------

    @classmethod
    def open(
        cls,
        category: str,
        nba_date: date_type,
        decision: str,
        reason: str,
        pipelines: Optional[dict] = None,
        job_id: Optional[str] = None,
        forced: bool = False,
        alerted: bool = False,
    ) -> Optional["PipelineBatch"]:
        """Record a batch decision. Returns None (having logged) on any failure.

        Never raises: an unwritable audit row is not a reason to skip the
        night's data.
        """
        try:
            return cls.create(
                id=uuid.uuid4(),
                category=category,
                nba_date=nba_date,
                triggered_at=datetime.now(timezone.utc),
                completed_at=None if decision == DISPATCHED else datetime.now(timezone.utc),
                decision=decision,
                reason=reason,
                forced=forced,
                job_id=job_id,
                pipelines=pipelines or {},
                alerted=alerted,
            )
        except Exception as exc:
            log.warning(
                "pipeline_batch_write_failed",
                category=category,
                nba_date=str(nba_date),
                decision=decision,
                error=f"{type(exc).__name__}: {exc}",
            )
            return None

    @classmethod
    def close(cls, batch_id, outcomes: dict) -> None:
        """Fold each pipeline's outcome into the batch row. Never raises."""
        if batch_id is None:
            return
        try:
            batch = cls.get_or_none(cls.id == batch_id)
            if batch is None:
                return
            merged = dict(batch.pipelines or {})
            for name, outcome in outcomes.items():
                merged[name] = {**merged.get(name, {}), **outcome}
            batch.pipelines = merged
            batch.completed_at = datetime.now(timezone.utc)
            batch.save()
        except Exception as exc:
            log.warning(
                "pipeline_batch_close_failed",
                batch_id=str(batch_id),
                error=f"{type(exc).__name__}: {exc}",
            )

    # -- reads -------------------------------------------------------------

    @classmethod
    def swept(cls, category: str, nba_date: date_type) -> bool:
        """Has the end-of-window completeness check already run for this date?

        Durable rather than in-memory, because the post-game endpoint keeps
        being polled for hours after the window closes and the check must run
        exactly once — including across a redeploy mid-night.
        """
        try:
            return (
                cls.select()
                .where(
                    (cls.category == category)
                    & (cls.nba_date == nba_date)
                    & (cls.decision == WINDOW_CLOSED)
                )
                .exists()
            )
        except Exception as exc:
            # Unreadable audit table: claim the sweep already happened rather
            # than alerting on incomplete information.
            log.warning(
                "pipeline_batch_swept_check_failed",
                category=category,
                nba_date=str(nba_date),
                error=f"{type(exc).__name__}: {exc}",
            )
            return True

    @classmethod
    def recent(cls, limit: int = 50, days: int = 7) -> list["PipelineBatch"]:
        """Recent batches, newest first — for the ops dashboard."""
        cutoff = datetime.now(timezone.utc) - timedelta(days=days)
        return list(
            cls.select()
            .where(cls.triggered_at >= cutoff)
            .order_by(cls.triggered_at.desc())
            .limit(limit)
        )
