"""
Data Quality API Routes

Endpoints for running and inspecting SQL-based data quality checks.
"""

from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query, Security

from core.pipeline_auth import verify_pipeline_token
from db.base import run_in_db_thread
from schemas.common import ApiStatus
from schemas.quality import (
    DataQualityRunResponse,
    DataQualityRunListResponse,
    DataQualityRunDetail,
    DataQualityRunInfo,
    DataQualityCheckResult,
)
from services.data_quality_service import DataQualityService


router = APIRouter(prefix="/quality", tags=["quality"])
_service = DataQualityService()


def _to_run_detail(payload: dict) -> DataQualityRunDetail:
    checks = [DataQualityCheckResult(**c) for c in payload.get("checks", [])]
    base = {k: v for k, v in payload.items() if k != "checks"}
    return DataQualityRunDetail(**base, checks=checks)


@router.post("/run", response_model=DataQualityRunResponse)
async def run_quality_checks(
    _: str = Security(verify_pipeline_token),
    checks: str | None = Query(
        default=None,
        description="Optional comma-separated check names",
    ),
    triggered_by: str = Query(
        default="manual",
        description="Who triggered this run (manual|schedule|ci)",
    ),
) -> DataQualityRunResponse:
    check_names = None
    if checks:
        check_names = [c.strip() for c in checks.split(",") if c.strip()]

    run = await run_in_db_thread(_service.run_checks, check_names, triggered_by)
    detail = await run_in_db_thread(_service.get_run, str(run.id))
    if detail is None:
        raise HTTPException(status_code=500, detail="Failed to load quality run details")

    return DataQualityRunResponse(
        status=ApiStatus.SUCCESS,
        message="Data quality run completed",
        data=_to_run_detail(detail),
    )


@router.get("/runs", response_model=DataQualityRunListResponse)
async def list_quality_runs(
    _: str = Security(verify_pipeline_token),
    limit: int = Query(default=20, ge=1, le=100),
) -> DataQualityRunListResponse:
    runs = await run_in_db_thread(_service.list_runs, limit)
    return DataQualityRunListResponse(
        status=ApiStatus.SUCCESS,
        message=f"Found {len(runs)} quality runs",
        data=[DataQualityRunInfo(**r) for r in runs],
    )


@router.get("/runs/{run_id}", response_model=DataQualityRunResponse)
async def get_quality_run(
    run_id: str,
    _: str = Security(verify_pipeline_token),
) -> DataQualityRunResponse:
    detail = await run_in_db_thread(_service.get_run, run_id)
    if detail is None:
        raise HTTPException(status_code=404, detail=f"Quality run {run_id} not found")

    return DataQualityRunResponse(
        status=ApiStatus.SUCCESS,
        message="Quality run details",
        data=_to_run_detail(detail),
    )

