"""
Availability page schemas: the availability pipeline's dark-week scorecard.

For each scratch (a player the box score lists inactive), when our verdict
called him out against when ESPN fantasy's own status flipped, and every false
out (out at the tip, active in the box score). Built by replaying the resolver
over nba.availability_reports (services/availability_scorecard.py, which says
what each outcome means).
"""

from __future__ import annotations

from datetime import date, datetime
from typing import Literal, Optional

from pydantic import Field

from schemas.common import ApiModel

AvailabilityOutcome = Literal[
    "us_first",
    "same_poll",
    "espn_first",
    "us_only",
    "espn_only",
    "carried",
    "unannounced",
    "false_out",
    "not_in_box",
    "pending",
]
BoxResult = Literal["inactive", "active"]


class AvailabilityCall(ApiModel):
    """One player on one game date, as the scorecard judges him."""

    game_date: date
    player_id: int
    name: str
    team: Optional[str] = None
    game_id: Optional[str] = None
    tip: datetime
    outcome: AvailabilityOutcome
    ours_at: Optional[datetime] = Field(
        default=None,
        description="When our verdict, ESPN fantasy left out, went out and stayed out to the tip",
    )
    ours_by: Optional[str] = Field(default=None, description="The source that decided that out")
    espn_at: Optional[datetime] = Field(
        default=None,
        description="When our ESPN fantasy poll saw his status go out and stay out to the tip",
    )
    lead_minutes: Optional[float] = Field(
        default=None, description="espn_at minus ours_at; positive when we were first",
    )
    verdict_at_tip: Optional[str] = Field(
        default=None,
        description="The full verdict at the tip, ESPN fantasy's floor included, replayed from the report "
        "log with the current resolver (not read from nba.player_availability)",
    )
    verdict_by: Optional[str] = None
    espn_at_tip: Optional[str] = Field(default=None, description="ESPN fantasy's status at the tip")
    box: Optional[BoxResult] = Field(default=None, description="The box score's last word on him")
    reason: Optional[str] = None
    detail: Optional[str] = None


class AvailabilityDay(ApiModel):
    game_date: date
    games: int
    first_tip: Optional[datetime] = None
    first_poll: Optional[datetime] = Field(default=None, description="The pipeline's first report for the date")
    covered: bool = Field(
        description="Both ESPN sources were first read before the first tip; only covered dates are scored",
    )
    calls: int = 0


class AvailabilitySummary(ApiModel):
    races: int = Field(default=0, description="Scratches at least one side called out by the tip, carried ones aside")
    us_first: int = 0
    same_poll: int = 0
    espn_first: int = 0
    us_only: int = 0
    espn_only: int = 0
    carried: int = 0
    unannounced: int = 0
    not_in_box: int = 0
    pending: int = 0
    first_share: Optional[float] = Field(
        default=None, description="(us_first + us_only) / races; None with no races",
    )
    median_lead_minutes: Optional[float] = Field(
        default=None, description="Over the races both sides called; positive when we were first",
    )
    first_by: dict[str, int] = Field(
        default_factory=dict, description="The source behind each us_first and us_only",
    )
    false_outs: int = Field(default=0, description="Out at the tip, active in the box score")
    false_outs_floor: int = Field(default=0, description="False outs that only ESPN fantasy's floor made out")
    espn_false_outs: int = Field(default=0, description="Active players ESPN fantasy itself had out at the tip")
    gate_met: bool = Field(
        default=False,
        description="races > 0, first_share > 0.5, and no false out beyond the floor's",
    )


class AvailabilityScorecard(ApiModel):
    days: int
    since: date
    until: date
    generated_at: datetime
    available: bool = Field(description="False until the pipeline has written a report in the window")
    summary: AvailabilitySummary
    dates: list[AvailabilityDay] = Field(description="Newest first")
    calls: list[AvailabilityCall] = Field(description="Newest date first, then by tip")


class AvailabilityResponse(ApiModel):
    """Response for GET /v1/dashboard/availability."""

    status: str
    message: str
    data: AvailabilityScorecard
