"""
The projections editor's read model, and the two things it does to the
curated layer.

For every projected player the page shows four lines side by side — the
statistical line, ESPN's, the blend, and the final line with his adjustment
applied — next to where the final line ranks in standard points and standard
9-cat, and where ESPN ranks him. Editing an adjustment previews the final line
and the rank move before anything is saved; saving writes a new version and
republishes the projection.

Three kinds of work, kept apart so each runs where it should:

- `load_state` reads the database, once (the caller runs it on a DB thread);
- `breakdowns`, `build_view` and `build_preview` are pure;
- the ranks come from the backend (`services.valuation_client`), which is
  where the valuation lives. A page without ranks is still a page: the four
  lines are computed here.

The lines sent for valuation are rounded exactly as cv-projection stores them,
so the rank shown for a saved state is the rank a draft room shows.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from typing import Mapping, Optional

from db.models.nba import DraftMarket, Player, PlayerProjection, ProjectionAdjustment
from db.models.nba.projection_adjustments import ADJUSTMENT_KINDS
from schemas.projections import (
    AdjustmentChange,
    AdjustmentEntry,
    PreviewSide,
    ProjectionLine,
    ProjectionPreview,
    ProjectionRow,
    ProjectionsData,
    StandardLeague,
    StandardRanks,
)
from services.projection_inputs import ProjectionInputs, load_inputs
from services.projection_model import (
    LINE_KEYS,
    Adjustment,
    Breakdown,
    EspnLine,
    Projection,
    project_breakdown,
    team_games_after,
)
from services.valuation_client import Valuation

# How far a stored stat may sit from the computed one and still be "the same
# line": the table keeps two decimals.
PUBLISHED_TOLERANCE = 0.006


@dataclass
class EditorState:
    """Everything the editor reads from the database, fully materialized."""

    inputs: ProjectionInputs
    players: dict[int, tuple[str, Optional[str]]] = field(default_factory=dict)     # id -> (name, position)
    # id -> ESPN's (points-board rank, category-board rank), latest market snapshot
    market: dict[int, tuple[Optional[int], Optional[int]]] = field(default_factory=dict)
    published_as_of: Optional[date] = None
    # id -> (the stored cv line, projected games) of the latest published snapshot
    published: dict[int, tuple[dict[str, float], Optional[int]]] = field(default_factory=dict)


def load_state(season: str) -> EditorState:
    """One trip to the database for the editor. Run it on a DB thread."""
    inputs = load_inputs(season)

    wanted = set(inputs.roster)
    players: dict[int, tuple[str, Optional[str]]] = {}
    if wanted:
        for rec in Player.select(Player.id, Player.name, Player.position).where(Player.id.in_(list(wanted))):
            players[rec.id] = (rec.name, rec.position)

    market = {
        rec.player_id: (
            int(rec.overall_rank) if rec.overall_rank is not None else None,
            int(rec.roto_rank) if rec.roto_rank is not None else None,
        )
        for rec in DraftMarket.latest_for_season(season)
    }

    published_rows = list(PlayerProjection.latest_for_season(season, source="cv"))
    published = {
        rec.player_id: (
            {k: float(getattr(rec, k) or 0) for k in PlayerProjection.STAT_KEYS},
            int(rec.projected_gp) if rec.projected_gp is not None else None,
        )
        for rec in published_rows
    }
    return EditorState(
        inputs=inputs,
        players=players,
        market=market,
        published_as_of=max((rec.as_of_date for rec in published_rows), default=None),
        published=published,
    )


# ---- the model, with an edit tried on ------------------------------------------------------


def resolve_change(
    inputs: ProjectionInputs, player_id: int, change: Optional[AdjustmentChange]
) -> Optional[Adjustment]:
    """A requested change as the numbers the model applies, or None for "no
    adjustment". A return date becomes the games his team has left after it,
    exactly as the pipeline resolves a stored one."""
    if change is None or not change.changes_something:
        return None
    games_available = None
    if change.return_date is not None:
        games_available = team_games_after(change.return_date, inputs.per_day, inputs.current.get(player_id))
    return Adjustment(
        id=None,
        minutes=change.minutes,
        games=float(change.games) if change.games is not None else None,
        games_available=float(games_available) if games_available is not None else None,
        usage=change.usage,
        rates=dict(change.rates or {}),
    )


def breakdowns(
    inputs: ProjectionInputs, override: Optional[tuple[int, Optional[Adjustment]]] = None
) -> list[Breakdown]:
    """Every projected player's lines. `override` tries one player's adjustment
    (or, with None, his having none) in place of the live one."""
    adjustments = inputs.adjustments
    if override is not None:
        player_id, adjustment = override
        if adjustment is None:
            adjustments.pop(player_id, None)
        else:
            adjustments[player_id] = adjustment
    return project_breakdown(
        inputs.target, inputs.history, inputs.espn, adjustments, inputs.roster, inputs.coeffs
    )


def valuation_players(lines: list[Breakdown], current: Mapping[int, str]) -> list[dict]:
    """The pool as the backend is asked to rank it: each final line rounded the
    way cv-projection stores it, so a saved state ranks as the board ranks it."""
    return [
        {
            "player_id": b.player_id,
            "line": b.final.line(),
            "games": int(round(b.final.games)),
            "team": current.get(b.player_id),
            "dd_rate": round(b.final.dd_rate, 4),
            "td_rate": round(b.final.td_rate, 4),
        }
        for b in lines
    ]


# ---- the page ----------------------------------------------------------------------------


def _line(p: Projection) -> ProjectionLine:
    return ProjectionLine(games=round(p.games, 1), **p.line())


def _espn_line(e: Optional[EspnLine]) -> Optional[ProjectionLine]:
    if e is None or not e.minutes:
        return None
    return ProjectionLine(
        games=e.games, min=round(e.minutes, 2),
        **{k: round(float(e.per_game.get(k, 0.0)), 2) for k in LINE_KEYS},
    )


def adjustment_entry(record: ProjectionAdjustment) -> AdjustmentEntry:
    state = "retired" if record.retired_at is not None else (
        "superseded" if record.superseded_by_id is not None else "live"
    )
    return AdjustmentEntry(
        id=record.id,
        kind=record.kind,
        minutes=float(record.minutes) if record.minutes is not None else None,
        games=int(record.games) if record.games is not None else None,
        return_date=record.return_date,
        usage=float(record.usage) if record.usage is not None else None,
        rates={k: float(v) for k, v in record.rates.items()} if record.rates else None,
        note=record.note,
        source_url=record.source_url,
        author=record.author,
        created_at=record.created_at,
        state=state,
    )


def _ranks(valuation: Valuation, player_id: int) -> StandardRanks:
    rank = valuation.ranks.get(player_id) if valuation.available else None
    if rank is None:
        return StandardRanks()
    return StandardRanks(points=rank.points_rank, categories=rank.category_rank)


def unpublished_count(state: EditorState, lines: list[Breakdown]) -> int:
    """Players whose computed line is not the one in the published snapshot:
    edited since, never published, or published and no longer projected."""
    differing = 0
    computed = set()
    for b in lines:
        computed.add(b.player_id)
        stored = state.published.get(b.player_id)
        if stored is None:
            differing += 1
            continue
        stored_line, stored_games = stored
        line = b.final.line()
        same = stored_games == int(round(b.final.games)) and all(
            abs(stored_line.get(k, 0.0) - v) <= PUBLISHED_TOLERANCE for k, v in line.items()
        )
        differing += 0 if same else 1
    return differing + len(set(state.published) - computed)


def build_view(
    state: EditorState, lines: list[Breakdown], valuation: Valuation, now: Optional[datetime] = None
) -> ProjectionsData:
    """The whole page: one row per projected player, best standard-points rank
    first (name order when the backend could not rank them)."""
    inputs = state.inputs
    rows: list[ProjectionRow] = []
    for b in lines:
        name, position = state.players.get(b.player_id, (str(b.player_id), None))
        espn_points, espn_categories = state.market.get(b.player_id, (None, None))
        record = inputs.records.get(b.player_id)
        components = b.final.components
        rows.append(ProjectionRow(
            player_id=b.player_id,
            name=name,
            team=inputs.current.get(b.player_id),
            position=position,
            age=round(components["age"], 1) if components.get("age") is not None else None,
            seasons=list(components.get("seasons") or []),
            espn_weight=components.get("espn_weight"),
            statistical=_line(b.statistical) if b.statistical is not None else None,
            espn=_espn_line(b.espn),
            blended=_line(b.blended),
            final=_line(b.final),
            ranks=_ranks(valuation, b.player_id),
            espn_ranks=StandardRanks(points=espn_points, categories=espn_categories),
            adjustment=adjustment_entry(record) if record is not None else None,
        ))
    rows.sort(key=lambda r: (r.ranks.points is None, r.ranks.points or 0, r.name))

    return ProjectionsData(
        season=inputs.season,
        coefficients_version=inputs.coeffs.version,
        espn_weight=inputs.coeffs.espn_weight,
        espn_as_of=inputs.espn_as_of,
        published_as_of=state.published_as_of,
        unpublished=unpublished_count(state, lines),
        ranks_available=valuation.available,
        ranks_reason=valuation.reason,
        league=(
            StandardLeague(
                league_size=valuation.league_size, rounds=valuation.rounds,
                playoff_weight=valuation.playoff_weight, playoff_weeks=list(valuation.playoff_weeks),
            )
            if valuation.available and valuation.league_size is not None
            and valuation.rounds is not None and valuation.playoff_weight is not None
            else None
        ),
        kinds=list(ADJUSTMENT_KINDS),
        players=rows,
        fetched_at=now or datetime.now(timezone.utc),
    )


def build_preview(
    player_id: int,
    before: list[Breakdown], before_valuation: Valuation,
    after: list[Breakdown], after_valuation: Valuation,
) -> Optional[ProjectionPreview]:
    """One player's line and ranks as they stand and as the edit would leave
    them. None when he is not a projected player."""
    was = next((b for b in before if b.player_id == player_id), None)
    would = next((b for b in after if b.player_id == player_id), None)
    if was is None or would is None:
        return None
    available = before_valuation.available and after_valuation.available
    return ProjectionPreview(
        player_id=player_id,
        before=PreviewSide(final=_line(was.final), ranks=_ranks(before_valuation, player_id)),
        after=PreviewSide(final=_line(would.final), ranks=_ranks(after_valuation, player_id)),
        ranks_available=available,
        ranks_reason=None if available else (before_valuation.reason or after_valuation.reason),
    )


# ---- writes ------------------------------------------------------------------------------


def adjustment_history(player_id: int, season: str) -> list[AdjustmentEntry]:
    """Every version of a player's adjustment for the season, newest first."""
    records = (
        ProjectionAdjustment.select()
        .where((ProjectionAdjustment.player == player_id) & (ProjectionAdjustment.season == season))
        .order_by(ProjectionAdjustment.id.desc())
    )
    return [adjustment_entry(record) for record in records]


def player_exists(player_id: int) -> bool:
    return Player.select(Player.id).where(Player.id == player_id).exists()
