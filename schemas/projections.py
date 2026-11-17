"""
Projections editor schemas: the lines a Court Vision projection is built from,
side by side, the curated adjustment that sits on top of them, and, in season,
the dated note the live projection reads (the Live rankings plan, step 3).
"""

from __future__ import annotations

from datetime import date, datetime
from typing import Literal, Optional

from pydantic import ConfigDict, Field, field_validator, model_validator

from schemas.common import ApiModel, BaseRequest
from schemas.pipeline import PipelineResult

AdjustmentKind = Literal["year2", "trade", "role", "injury_return", "injury_current", "age", "other"]
Confidence = Literal["confirmed", "likely", "possible"]

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


class EvidenceItem(ApiModel):
    """A stored item a note rests on, kept as it read when the note was written."""

    type: str = Field(default="news", description="news: an nba.news_items row")
    id: Optional[int] = None
    source: Optional[str] = Field(default=None, description="espn_injuries, rotowire_rss, ...")
    kind: Optional[str] = Field(default=None, description="injury, availability, lineup, transaction, performance, rumor")
    headline: Optional[str] = None
    url: Optional[str] = None
    published_at: Optional[datetime] = None


class NoteStanding(ApiModel):
    """How a live season note stands today (services.live_notes.standing)."""

    state: str = Field(description="in_force | review_due | ended")
    games: bool = Field(description="Its return date, or written-off season, still applies")
    role: bool = Field(description="Its minutes and usage still apply")
    reasons: list[str] = Field(default_factory=list, description="Why a part, or all of it, no longer does")


class AdjustmentEntry(ApiModel):
    """One version of one player's adjustment or season note (nba.projection_adjustments)."""

    id: int
    scope: str = Field(default="preseason", description="preseason: an input to cv-projection. season: a dated note")
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
    # A season note's own fields
    return_earliest: Optional[date] = None
    return_latest: Optional[date] = None
    until_date: Optional[date] = Field(default=None, description="The minutes and usage hold through this day")
    depends_on_player_id: Optional[int] = Field(default=None, description="... or until this teammate plays again")
    depends_on_name: Optional[str] = None
    evidence: list[EvidenceItem] = Field(default_factory=list)
    confidence: Optional[str] = None
    review_date: Optional[date] = Field(default=None, description="Past this day the note is no longer applied")
    standing: Optional[NoteStanding] = Field(default=None, description="For the live season note: how it stands today")


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
    live: Optional[ProjectionLine] = Field(
        default=None,
        description=(
            "The live projection as stored this morning: his line in a game he plays, "
            "and the games he is expected to play from here. None before the season's first snapshot"
        ),
    )
    note: Optional[AdjustmentEntry] = Field(default=None, description="His live season note, with how it stands today")


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
    live_as_of: Optional[date] = Field(
        default=None, description="The live projection's latest snapshot; None before the season's first"
    )
    confidences: list[str] = Field(default_factory=list, description="The confidence levels a season note accepts")
    review_days: int = Field(default=14, description="How long a season note runs before it must be read again, by default")
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


# ---- notes in season -----------------------------------------------------------------------


class NoteChange(BaseRequest):
    """What a season note states. Minutes and usage are targets, in force while
    the role part holds; a return date says he is out until then."""

    # A note is not a preseason adjustment: a games target or a per-stat
    # multiplier sent here is refused, not dropped without a word
    model_config = ConfigDict(extra="forbid")

    return_date: Optional[date] = Field(default=None, description="The likely first game back: out until then")
    return_earliest: Optional[date] = Field(default=None, description="The early end of the range around it")
    return_latest: Optional[date] = Field(default=None, description="The late end")
    out_for_season: bool = Field(default=False, description="The season is written off")
    minutes: Optional[float] = Field(default=None, ge=0, le=48, description="Minutes a game while the note holds")
    usage: Optional[float] = Field(
        default=None, gt=0, le=2, description="His share of the offense against his usual one while it holds"
    )
    until_date: Optional[date] = Field(default=None, description="The minutes and usage hold through this day")
    depends_on_player_id: Optional[int] = Field(
        default=None, ge=1, description="... or until this teammate plays again, whichever comes first"
    )

    @property
    def has_role_part(self) -> bool:
        return self.minutes is not None or self.usage is not None

    @property
    def states_something(self) -> bool:
        return self.return_date is not None or self.out_for_season or self.has_role_part

    @model_validator(mode="after")
    def _coherent(self) -> "NoteChange":
        if not self.states_something:
            raise ValueError("a note has to state something: a return date, a season written off, minutes or usage")
        if self.out_for_season and self.return_date is not None:
            raise ValueError("a season written off has no return date")
        if (self.return_earliest or self.return_latest) and self.return_date is None:
            raise ValueError("a range is around a return date: give the likely date too")
        if self.return_earliest and self.return_earliest > self.return_date:
            raise ValueError("the early end of the range is after the return date")
        if self.return_latest and self.return_latest < self.return_date:
            raise ValueError("the late end of the range is before the return date")
        if (self.until_date or self.depends_on_player_id) and not self.has_role_part:
            raise ValueError("an end date or a teammate ends the minutes and usage: set one of those")
        return self


class NoteSave(NoteChange):
    """A new version of a player's season note. Saving supersedes his live one."""

    kind: AdjustmentKind
    note: str = Field(min_length=1, max_length=2000, description="Why: the fact being recorded")
    source_url: Optional[str] = Field(default=None, max_length=2000, description="The report it came from")
    author: str = Field(default="dashboard", min_length=1, max_length=64)
    confidence: Optional[Confidence] = None
    review_date: Optional[date] = Field(
        default=None, description="Read again by this day; past it the note is not applied. Default: two weeks on"
    )
    evidence_news_ids: list[int] = Field(
        default_factory=list, max_length=20, description="The stored news items it rests on (nba.news_items ids)"
    )

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


class NotePreviewRequest(BaseRequest):
    player_id: int
    note: Optional[NoteChange] = Field(
        default=None, description="The note to try. Omit (or null) to preview having none."
    )
    review_date: Optional[date] = Field(default=None, description="As on a save: the default when omitted")


class LiveStretch(ApiModel):
    """A run of his team's remaining games he is expected to play alike."""

    first: date
    last: date
    games: int = Field(description="His team's games in the stretch")
    chance: float = Field(description="That he plays each")
    minutes: float
    usage: float = Field(description="His share of the offense against his usual one")


class LiveSide(ApiModel):
    """One player's live projection: the stored line, and the stretches it averages."""

    line: ProjectionLine = Field(description="A game he plays, and the games he is expected to play from here")
    stretches: list[LiveStretch] = Field(default_factory=list)


class NotePreview(ApiModel):
    """One player's live projection this morning with his note as it is, and as the note tried would leave it."""

    player_id: int
    as_of: date
    before: LiveSide
    after: LiveSide
    standing: Optional[NoteStanding] = Field(
        default=None, description="How the note tried would stand today; None when previewing having none"
    )


class NotePreviewResponse(ApiModel):
    status: str
    message: str
    data: NotePreview


class NoteSaved(ApiModel):
    """What a save or a retire of a season note did: the table write, and the rebuild that followed."""

    player_id: int
    note: Optional[AdjustmentEntry] = Field(default=None, description="The new live note; None after a retire")
    rebuilt: bool = Field(description="Whether the day's live projection was rebuilt with it")
    pipeline: Optional[PipelineResult] = None


class NoteSavedResponse(ApiModel):
    status: str
    message: str
    data: NoteSaved


class PlayerNews(ApiModel):
    player_id: int
    items: list[EvidenceItem] = Field(description="His recent stored news, newest first: what a note can cite")


class PlayerNewsResponse(ApiModel):
    status: str
    message: str
    data: PlayerNews
