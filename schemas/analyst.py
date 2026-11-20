"""
The analyst in shadow (the Live rankings plan, step 4): what a read answers,
and the morning digest the projections editor shows.

`Proposal` is the backend's answer to one read
(`POST /v1/internal/jobs/analyst/read`, its `AnalystProposal`), parsed again
here because this side decides what to do with it (`services.analyst_notes`).
The rest is the digest: every read of a morning with the note it left, what it
rests on and what the note moved on the second, unseen projection and boards.
"""

from __future__ import annotations

from datetime import date, datetime
from typing import Literal, Optional

from pydantic import BaseModel, ConfigDict, Field

from schemas.common import ApiModel
from schemas.projections import AdjustmentEntry


class Proposal(BaseModel):
    """One read's answer. Inputs only: a date, minutes, a share of the offense."""

    # The backend's schema may grow a field before this side reads it
    model_config = ConfigDict(extra="ignore")

    decision: Literal["write", "keep", "retire", "none"]
    reason: str = ""
    kind: Literal["injury_current", "injury_return", "role", "trade", "other"] = "other"
    return_date: Optional[date] = None
    return_earliest: Optional[date] = None
    return_latest: Optional[date] = None
    out_for_season: bool = False
    minutes: Optional[float] = None
    usage: Optional[float] = None
    until_date: Optional[date] = None
    until_player_id: Optional[int] = None
    confidence: Literal["confirmed", "likely", "possible"] = "possible"
    review_date: Optional[date] = None
    cites: list[str] = Field(default_factory=list)
    against_owner_note: Optional[str] = None


# ---- the morning digest ----------------------------------------------------------------------


class DigestReason(ApiModel):
    rule: str = Field(description="note_due | status_changed | new_item | teammate_change | minutes_gap | rank_gap")
    text: str


class DigestSide(ApiModel):
    """One player on one of the two projections, and where its boards rank him."""

    games: Optional[float] = Field(default=None, description="Games he is expected to play from here")
    minutes: Optional[float] = Field(default=None, description="A game, when he plays")
    points: Optional[float] = Field(default=None, description="Points a game, when he plays")
    points_rank: Optional[int] = Field(default=None, description="On the standard points board")
    categories_rank: Optional[int] = Field(default=None, description="On the standard nine-category board")


class DigestRead(ApiModel):
    """One read: why he was picked, what it came to, and what it would have moved."""

    id: int
    player_id: int
    name: str
    team: Optional[str] = None
    as_of: date
    read_at: datetime
    reasons: list[DigestReason] = Field(default_factory=list)
    outcome: str = Field(description="note | unchanged | none | retired | refused | declined | failed")
    summary: Optional[str] = None
    disagrees: Optional[str] = Field(
        default=None, description="What it says against the note written by hand, which is the one applied"
    )
    note: Optional[AdjustmentEntry] = Field(default=None, description="The note it left, as stored")
    note_live: bool = Field(default=False, description="Whether that note is still the analyst's live one")
    owner_note: bool = Field(default=False, description="Whether a note written by hand stands for him")
    live: Optional[DigestSide] = Field(default=None, description="The projection and boards everybody sees")
    shadow: Optional[DigestSide] = Field(default=None, description="The same with the analyst's notes applied")
    cost_usd: float = 0.0


class DigestSpend(ApiModel):
    """The analyst's month, from the backend's ledger. None of it when the backend cannot be asked."""

    available: bool
    enabled: bool = False
    month_spent_usd: float = 0.0
    month_cap_usd: float = 0.0
    model: Optional[str] = None
    prompt_version: Optional[str] = None


class DigestMove(ApiModel):
    """A player whose place on the points board the analyst's notes moved, his own note or a teammate's."""

    player_id: int
    name: str
    team: Optional[str] = None
    live: DigestSide
    shadow: DigestSide


class AnalystDigest(ApiModel):
    """A morning of the analyst in shadow."""

    season: str
    as_of: Optional[date] = Field(default=None, description="The morning shown; None before its first run")
    days: list[date] = Field(default_factory=list, description="The mornings with reads, newest first")
    in_shadow: bool = Field(default=True, description="Its notes reach only the second projection and boards")
    reads: list[DigestRead] = Field(default_factory=list)
    counts: dict[str, int] = Field(default_factory=dict, description="Reads by outcome")
    cost_usd: float = Field(default=0.0, description="What the morning's reads cost")
    notes_live: int = Field(default=0, description="Notes of the analyst's standing today")
    shadow_as_of: Optional[date] = Field(default=None, description="The morning of the newest shadow boards")
    moved: list[DigestMove] = Field(
        default_factory=list, description="The widest moves between the two points boards that morning"
    )
    spend: DigestSpend


class AnalystDigestResponse(ApiModel):
    status: str
    message: str
    data: AnalystDigest
