"""
Data Quality Run Model

Tracks execution metadata for data quality check runs.
"""

from __future__ import annotations

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


class DataQualityRun(BaseModel):
    """Execution metadata for a data quality run."""

    id = UUIDField(primary_key=True, default=uuid.uuid4)
    status = CharField(max_length=20, index=True)  # running, success, failed
    triggered_by = CharField(max_length=20, default="manual")
    started_at = DateTimeField(default=datetime.utcnow, index=True)
    completed_at = DateTimeField(null=True)
    total_checks = IntegerField(default=0)
    passed_checks = IntegerField(default=0)
    failed_checks = IntegerField(default=0)
    error_message = TextField(null=True)
    created_at = DateTimeField(default=datetime.utcnow)
    updated_at = DateTimeField(default=datetime.utcnow)

    class Meta:
        table_name = "data_quality_runs"
        schema = "nba"

    def save(self, *args, **kwargs):
        self.updated_at = datetime.utcnow()
        return super().save(*args, **kwargs)

    @property
    def duration_seconds(self) -> float | None:
        if self.completed_at and self.started_at:
            return (self.completed_at - self.started_at).total_seconds()
        return None

    @classmethod
    def start_run(cls, triggered_by: str = "manual") -> "DataQualityRun":
        return cls.create(
            status="running",
            triggered_by=triggered_by,
            started_at=datetime.utcnow(),
        )

    def mark_completed(
        self,
        total_checks: int,
        passed_checks: int,
        failed_checks: int,
        error_message: str | None = None,
    ) -> None:
        self.total_checks = total_checks
        self.passed_checks = passed_checks
        self.failed_checks = failed_checks
        self.error_message = error_message
        self.completed_at = datetime.utcnow()
        self.status = "success" if failed_checks == 0 and not error_message else "failed"
        self.save()

