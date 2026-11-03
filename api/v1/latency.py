"""
Backend Latency

The API behind the dashboard's /latency page: every backend route's response
times, and where each request's time went (ESPN, Postgres, our own code, ...).
Read from ops.request_timings, which the backend writes one row per request.
Everything takes the pipeline bearer token.

Routes:
    GET /v1/dashboard/latency        — every route over a window, slowest overall first
    GET /v1/dashboard/latency/route  — one route: its times over the window and its slowest requests
"""

from __future__ import annotations

from fastapi import APIRouter, Query, Security

from core.pipeline_auth import verify_pipeline_token
from db.base import run_in_db_thread
from schemas.latency import LatencyResponse, LatencyWindow, RouteLatencyResponse
from services.latency_service import build_overview, build_route

router = APIRouter(
    prefix="/dashboard/latency",
    tags=["latency"],
    dependencies=[Security(verify_pipeline_token)],
)

_WINDOW = Query("24h", description="How far back: 1h, 24h, 7d or 30d")
_SERVICE = Query("api", description="The service that wrote the rows")


def _unavailable_message() -> str:
    return "No request timings yet: the backend has not created ops.request_timings"


@router.get("", response_model=LatencyResponse)
async def get_latency(window: LatencyWindow = _WINDOW, service: str = _SERVICE) -> LatencyResponse:
    """
    Every route that answered in the window: its request count, 5xx count,
    p50/p95/p99, and the mean request split into where the time went. Routes
    are ordered by the time they cost the service (requests × mean).
    """
    data = await run_in_db_thread(build_overview, window, service)
    if not data.available:
        message = _unavailable_message()
    else:
        message = f"{data.totals.requests} requests over {len(data.routes)} routes in {window}"
    return LatencyResponse(status="success", message=message, data=data)


@router.get("/route", response_model=RouteLatencyResponse)
async def get_route_latency(
    route: str = Query(..., min_length=1, max_length=512, description="The route's path template, as the table shows it"),
    method: str = Query("GET", min_length=1, max_length=16),
    window: LatencyWindow = _WINDOW,
    service: str = _SERVICE,
) -> RouteLatencyResponse:
    """
    One route over the window: its summary, its p50/p95 and mean split per
    slice of the window (empty slices included), and its slowest requests,
    each with its own split and, when Railway has it, its trace id.
    """
    data = await run_in_db_thread(build_route, method.upper(), route, window, service)
    if not data.available:
        message = _unavailable_message()
    elif data.route is None:
        message = f"No {method.upper()} {route} requests in {window}"
    else:
        message = f"{data.route.requests} requests in {window}"
    return RouteLatencyResponse(status="success", message=message, data=data)
