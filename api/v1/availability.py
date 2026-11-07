"""
Availability

The API behind the dashboard's /availability page: the availability
pipeline's dark-week scorecard. For each scratch, when our verdict called him
out against when ESPN fantasy's own status flipped, and every false out.
Replayed from nba.availability_reports (services/availability_scorecard.py).
Takes the pipeline bearer token.

Routes:
    GET /v1/dashboard/availability  — the last `days` game dates, today's included
"""

from __future__ import annotations

from fastapi import APIRouter, Query, Security

from core.pipeline_auth import verify_pipeline_token
from db.base import run_in_db_thread
from schemas.availability import AvailabilityResponse
from services.availability_scorecard import MAX_DAYS, build_scorecard

router = APIRouter(
    prefix="/dashboard/availability",
    tags=["availability"],
    dependencies=[Security(verify_pipeline_token)],
)


def _count(n: int, one: str, many: str = "") -> str:
    return f"{n} {one if n == 1 else many or one + 's'}"


@router.get("", response_model=AvailabilityResponse)
async def get_availability(
    days: int = Query(7, ge=1, le=MAX_DAYS, description="Game dates to score, today's included"),
) -> AvailabilityResponse:
    """
    Each scratch on the covered dates: whether our verdict or ESPN fantasy said
    out first, and by how many minutes; every player out at his tip who was
    active in the box score; and the summary the dark-week gate is read from.
    """
    data = await run_in_db_thread(build_scorecard, days)
    covered = sum(1 for day in data.dates if day.covered)
    if not data.available:
        message = f"No availability reports in the last {_count(days, 'day')}"
    else:
        s = data.summary
        message = (
            f"{_count(s.races, 'scratch', 'scratches')} raced and {_count(s.false_outs, 'false out')} "
            f"over {_count(covered, 'covered date')}"
        )
    return AvailabilityResponse(status="success", message=message, data=data)
