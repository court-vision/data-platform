"""
Data Quality Schemas

Pydantic models for quality run APIs.
"""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict

from schemas.common import ApiStatus


class DataQualityCheckResult(BaseModel):
    check_name: str
    status: str
    severity: str
    failures: int = 0
    message: str | None = None
    details: dict | None = None
    duration_ms: int | None = None


class DataQualityRunInfo(BaseModel):
    run_id: str
    status: str
    started_at: str
    completed_at: str | None = None
    duration_seconds: float | None = None
    total_checks: int = 0
    passed_checks: int = 0
    failed_checks: int = 0
    triggered_by: str | None = None
    error_message: str | None = None


class DataQualityRunDetail(DataQualityRunInfo):
    checks: list[DataQualityCheckResult] = []


class DataQualityRunResponse(BaseModel):
    status: ApiStatus
    message: str
    data: DataQualityRunDetail

    model_config = ConfigDict(use_enum_values=True)


class DataQualityRunListResponse(BaseModel):
    status: ApiStatus
    message: str
    data: list[DataQualityRunInfo]

    model_config = ConfigDict(use_enum_values=True)

