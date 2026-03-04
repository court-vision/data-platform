"""
Data Quality Check Model

Stores per-check results for each DataQualityRun.
"""

from __future__ import annotations

from datetime import datetime

from peewee import (
    AutoField,
    ForeignKeyField,
    CharField,
    DateTimeField,
    IntegerField,
    TextField,
)

from db.base import BaseModel
from db.models.data_quality_run import DataQualityRun


class DataQualityCheck(BaseModel):
    """Single check result for a quality run."""

    id = AutoField(primary_key=True)
    run = ForeignKeyField(
        DataQualityRun,
        backref="checks",
        on_delete="CASCADE",
        column_name="run_id",
    )
    check_name = CharField(max_length=100, index=True)
    status = CharField(max_length=20, index=True)  # passed, failed, error
    severity = CharField(max_length=20, default="critical")  # critical, warning
    failures = IntegerField(default=0)
    message = TextField(null=True)
    details_json = TextField(null=True)
    duration_ms = IntegerField(null=True)
    created_at = DateTimeField(default=datetime.utcnow)

    class Meta:
        table_name = "data_quality_checks"
        schema = "nba"
        indexes = (
            (("run", "check_name"), False),
        )

