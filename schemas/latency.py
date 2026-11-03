"""
Latency page schemas: each backend route's response times and where they go.

The rows come from ops.request_timings, which the backend writes, one per
request (backend/services/request_timings.py). `parts` splits a request's
duration into buckets that add up to it ("espn", "postgres", "wait" for a
queue, "app" for the backend's own code, ...); here they are averaged per
request, so a route's parts add up to its mean.
"""

from __future__ import annotations

from datetime import datetime
from typing import Literal, Optional

from pydantic import Field

from schemas.common import ApiModel

LatencyWindow = Literal["1h", "24h", "7d", "30d"]


class LatencyStats(ApiModel):
    """Response times over a set of requests. Percentiles are None with no requests."""

    requests: int = 0
    errors: int = Field(default=0, description="Requests answered with a 5xx")
    p50_ms: Optional[float] = None
    p95_ms: Optional[float] = None
    p99_ms: Optional[float] = None
    mean_ms: Optional[float] = None
    max_ms: Optional[float] = None


class RouteLatency(LatencyStats):
    """One route (method + path template) over the window."""

    method: str
    route: str
    parts: dict[str, float] = Field(default_factory=dict, description="Mean ms per request, by bucket; sums to mean_ms")
    calls: dict[str, float] = Field(default_factory=dict, description="Mean calls per request, by bucket")


class LatencyOverview(ApiModel):
    service: str
    window: LatencyWindow
    since: datetime
    until: datetime
    available: bool = Field(description="False until the backend's migration has created ops.request_timings")
    totals: LatencyStats
    routes: list[RouteLatency] = Field(description="Most total time first (requests × mean)")


class LatencyBin(ApiModel):
    """One slice of the window. Empty slices are included, with no percentiles."""

    at: datetime = Field(description="Start of the slice")
    requests: int = 0
    errors: int = 0
    p50_ms: Optional[float] = None
    p95_ms: Optional[float] = None
    parts: dict[str, float] = Field(default_factory=dict, description="Mean ms per request, by bucket")


class SlowRequest(ApiModel):
    started_at: datetime
    duration_ms: float
    status_code: int
    parts: dict[str, float]
    calls: dict[str, int]
    trace_id: Optional[str] = Field(default=None, description="Set when the request's trace went to Railway")
    version: Optional[str] = None


class RouteLatencyDetail(ApiModel):
    service: str
    window: LatencyWindow
    since: datetime
    until: datetime
    available: bool
    bin_seconds: int
    route: Optional[RouteLatency] = Field(description="None when the route had no requests in the window")
    series: list[LatencyBin]
    slowest: list[SlowRequest] = Field(description="The slowest requests in the window, slowest first")


class LatencyResponse(ApiModel):
    """Response for GET /v1/dashboard/latency."""

    status: str
    message: str
    data: LatencyOverview


class RouteLatencyResponse(ApiModel):
    """Response for GET /v1/dashboard/latency/route."""

    status: str
    message: str
    data: RouteLatencyDetail
