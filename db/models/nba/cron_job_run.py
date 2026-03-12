"""
Cron Job Run Model

Tracks execution history for cron-runner jobs (pre-game, live-stats, post-game).
Written by the cron-runner service after each trigger attempt completes.
"""

import uuid
from datetime import datetime

from peewee import (
    UUIDField,
    CharField,
    DateTimeField,
    IntegerField,
    TextField,
)

from db.base import BaseModel


class CronJobRun(BaseModel):
    """
    Records one execution of a cron-runner job.

    Attributes:
        id:               Unique run identifier
        job_name:         "pre-game" | "live-stats" | "post-game"
        triggered_at:     When the cron fired (UTC)
        completed_at:     When the trigger call returned (UTC)
        duration_ms:      Wall-clock duration in milliseconds
        result:           "success" | "failure"
        http_status:      HTTP status code returned by data-platform (null if no response)
        attempts:         Total retry attempts made by the cron-runner
        error_message:    Error string on failure (null on success)
        response_snippet: First 300 chars of the response body (for context)
    """

    id = UUIDField(primary_key=True, default=uuid.uuid4)
    job_name = CharField(max_length=50, index=True)
    triggered_at = DateTimeField(index=True)
    completed_at = DateTimeField()
    duration_ms = IntegerField(default=0)
    result = CharField(max_length=20, index=True)  # "success" | "failure"
    http_status = IntegerField(null=True)
    attempts = IntegerField(default=1)
    error_message = TextField(null=True)
    response_snippet = TextField(null=True)

    class Meta:
        table_name = "cron_job_runs"
        schema = "nba"

    def __repr__(self) -> str:
        return (
            f"<CronJobRun("
            f"job={self.job_name}, "
            f"result={self.result}, "
            f"triggered_at={self.triggered_at})>"
        )

    @property
    def duration_seconds(self) -> float:
        return self.duration_ms / 1000.0

    @classmethod
    def recent(cls, limit: int = 200) -> list["CronJobRun"]:
        """Return the most recent runs ordered newest-first."""
        return list(
            cls.select()
            .order_by(cls.triggered_at.desc())
            .limit(limit)
        )

    @classmethod
    def recent_for_job(cls, job_name: str, limit: int = 50) -> list["CronJobRun"]:
        """Return recent runs for a specific job, newest-first."""
        return list(
            cls.select()
            .where(cls.job_name == job_name)
            .order_by(cls.triggered_at.desc())
            .limit(limit)
        )
