"""
Projections editor schemas: the lines a Court Vision projection is built from,
side by side, and the curated adjustment that sits on top of them.
"""

from __future__ import annotations

from datetime import date, datetime
from typing import Literal, Optional

from pydantic import Field, field_validator, model_validator

from schemas.common import ApiModel, BaseRequest
from schemas.pipeline import PipelineResult

AdjustmentKind = Literal["year2", "trade", "role", "injury_return", "injury_current", "age", "other"]

# The stats a `rates` multiplier may scale: the projected line itself.
RATE_KEYS: tuple[str, ...] = (
    "pts", "reb", "ast", "stl", "blk", "tov", "fgm", "fga", "fg3m", "fg3a", "ftm", "fta",
)


class ProjectionLine(ApiModel):
    """One per-game stat line and the games it is expected over."""

    games: Optional[float] = Field(default=None, description="Expected games; None where the source has none")
    min: Optional[float] = None
    pts: float = 0.0
    reb: float = 0.0
    ast: float = 0.0
    stl: float = 0.0
    blk: float = 0.0
    tov: float = 0.0
    fgm: float = 0.0
    fga: float = 0.0
    fg3m: float = 0.0
    fg3a: float = 0.0
    ftm: float = 0.0
    fta: float = 0.0


class AdjustmentEntry(ApiModel):
    """One version of one player's adjustment (nba.projection_adjustments)."""

    id: int
    kind: str
    minutes: Optional[float] = None
    games: Optional[int] = None
    return_date: Optional[date] = None
    usage: Optional[float] = None
    rates: Optional[dict[str, float]] = None
    note: str
    source_url: Optional[str] = None
    author: str
    created_at: datetime
    # live | superseded | retired
    state: str = "live"


class StandardRanks(ApiModel):
    """A place in the standard league: 12 teams, ESPN's default points weights or 9-cat."""

    points: Optional[int] = None
    categories: Optional[int] = None


class ProjectionRow(ApiModel):
    """One projected player: the four lines, his ranks, and his live adjustment."""

    player_id: int
    name: str
    team: Optional[str] = None
    position: Optional[str] = None
    age: Optional[float] = Field(default=None, description="Age in the season being projected")
    seasons: list[int] = Field(
        default_factory=list, description="Start years of the seasons the statistical line was built from"
    )
    espn_weight: Optional[float] = Field(
        default=None, description="ESPN's share of the blend: 0 with no ESPN line, 1 with no NBA history"
    )
    statistical: Optional[ProjectionLine] = Field(
        default=None, description="History alone: three seasons, aged and regressed. None for a rookie."
    )
    espn: Optional[ProjectionLine] = Field(default=None, description="ESPN's projection, where ESPN has one")
    blended: ProjectionLine = Field(description="The two combined, before any adjustment")
    final: ProjectionLine = Field(description="With the live adjustment applied: what is published")
    ranks: StandardRanks = Field(description="Court Vision's rank for the final line; nulls when the backend could not be asked")
    espn_ranks: StandardRanks = Field(description="ESPN's published draft ranks: points board and category board")
    adjustment: Optional[AdjustmentEntry] = None


class StandardLeague(ApiModel):
    """What the ranks were measured in."""

    league_size: int
    rounds: int
    playoff_weight: float
    playoff_weeks: list[int] = Field(default_factory=list)


class ProjectionsData(ApiModel):
    season: str
    coefficients_version: str
    espn_weight: float = Field(description="The blend's default weight on ESPN's line")
    espn_as_of: Optional[date] = Field(default=None, description="The ESPN snapshot the lines were read from")
    published_as_of: Optional[date] = Field(
        default=None, description="The latest Court Vision snapshot in nba.player_projections"
    )
    unpublished: int = Field(
        default=0,
        description=(
            "Players whose line here differs from the published snapshot (or is missing from it): "
            "0 means the board is reading exactly what this page shows"
        ),
    )
    ranks_available: bool = Field(description="Whether the backend valued the pool")
    ranks_reason: Optional[str] = Field(default=None, description="Why not, when it did not")
    league: Optional[StandardLeague] = None
    kinds: list[str] = Field(default_factory=list, description="The adjustment kinds the table accepts")
    players: list[ProjectionRow]
    fetched_at: datetime


class ProjectionsResponse(ApiModel):
    """Response for GET /v1/dashboard/projections."""

    status: str
    message: str
    data: ProjectionsData


# ---- editing -----------------------------------------------------------------------------


class AdjustmentChange(BaseRequest):
    """The numbers an adjustment sets. Minutes and games are targets, not deltas."""

    minutes: Optional[float] = Field(default=None, ge=0, le=48, description="Target minutes per game")
    games: Optional[int] = Field(default=None, ge=0, le=82, description="Target games played")
    return_date: Optional[date] = Field(
        default=None, description="First game back: games are capped at the ones his team plays from then"
    )
    usage: Optional[float] = Field(
        default=None, gt=0, le=2,
        description="Multiplier on scoring and playmaking together (pts, makes and attempts, ast, tov)",
    )
    rates: Optional[dict[str, float]] = Field(
        default=None, description='Per-stat multipliers on the final line, e.g. {"blk": 1.1}'
    )

    @field_validator("rates")
    @classmethod
    def _known_rates(cls, rates: Optional[dict[str, float]]) -> Optional[dict[str, float]]:
        if not rates:
            return None
        unknown = sorted(set(rates) - set(RATE_KEYS))
        if unknown:
            raise ValueError(f"unknown stat(s) {', '.join(unknown)}; allowed: {', '.join(RATE_KEYS)}")
        for key, multiplier in rates.items():
            if not 0 < multiplier <= 3:
                raise ValueError(f"{key}: a multiplier must be above 0 and at most 3")
        return rates

    @property
    def changes_something(self) -> bool:
        return any(
            value is not None
            for value in (self.minutes, self.games, self.return_date, self.usage, self.rates)
        )


class PreviewRequest(BaseRequest):
    player_id: int
    adjustment: Optional[AdjustmentChange] = Field(
        default=None, description="The adjustment to try. Omit (or null) to preview having none."
    )


class PreviewSide(ApiModel):
    final: ProjectionLine
    ranks: StandardRanks


class ProjectionPreview(ApiModel):
    """One player's published line and ranks, and what they would be under the edit."""

    player_id: int
    before: PreviewSide
    after: PreviewSide
    ranks_available: bool
    ranks_reason: Optional[str] = None


class PreviewResponse(ApiModel):
    status: str
    message: str
    data: ProjectionPreview


class AdjustmentSave(AdjustmentChange):
    """A new version of a player's adjustment. Saving supersedes the live one."""

    kind: AdjustmentKind
    note: str = Field(min_length=1, max_length=2000, description="Why — the judgment being recorded")
    source_url: Optional[str] = Field(default=None, max_length=2000, description="The report it came from")
    author: str = Field(default="dashboard", min_length=1, max_length=64)

    @field_validator("note", "author")
    @classmethod
    def _not_blank(cls, value: str) -> str:
        value = value.strip()
        if not value:
            raise ValueError("must not be blank")
        return value

    @field_validator("source_url")
    @classmethod
    def _blank_url_is_none(cls, value: Optional[str]) -> Optional[str]:
        value = (value or "").strip()
        return value or None

    @model_validator(mode="after")
    def _changes_something(self) -> "AdjustmentSave":
        if not self.changes_something:
            raise ValueError("an adjustment has to change something: minutes, games, return_date, usage or rates")
        return self


class AdjustmentSaved(ApiModel):
    """What a save or a retire did: the table write, and the republish that followed."""

    player_id: int
    adjustment: Optional[AdjustmentEntry] = Field(
        default=None, description="The new live adjustment; None after a retire"
    )
    published: bool = Field(description="Whether cv-projection republished the board's snapshot")
    pipeline: Optional[PipelineResult] = None


class AdjustmentSavedResponse(ApiModel):
    status: str
    message: str
    data: AdjustmentSaved


class AdjustmentHistory(ApiModel):
    player_id: int
    season: str
    versions: list[AdjustmentEntry] = Field(description="Every version, newest first")


class AdjustmentHistoryResponse(ApiModel):
    status: str
    message: str
    data: AdjustmentHistory
