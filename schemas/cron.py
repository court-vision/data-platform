"""
Cron Job Run Schemas

Pydantic models for cron-runner execution reports.
"""

from datetime import datetime
from typing import Optional

from pydantic import BaseModel


class CronJobRunCreate(BaseModel):
    """Payload sent by the cron-runner after each job execution."""

    job_name: str
    triggered_at: datetime
    completed_at: datetime
    duration_ms: int
    result: str          # "success" | "failure"
    http_status: Optional[int] = None
    attempts: int = 1
    error_message: Optional[str] = None
    response_snippet: Optional[str] = None


class CronJobRunEntry(BaseModel):
    """A single cron job run record returned by the API."""

    id: str
    job_name: str
    triggered_at: datetime
    completed_at: datetime
    duration_ms: int
    duration_seconds: float
    result: str
    http_status: Optional[int] = None
    attempts: int
    error_message: Optional[str] = None
    response_snippet: Optional[str] = None
