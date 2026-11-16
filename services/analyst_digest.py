"""
The analyst's morning, as the projections editor shows it (the Live rankings
plan, step 4).

In shadow nothing the analyst writes reaches a board anyone sees, so the digest
is where its work is judged: every read of a morning with why the screen picked
the player, what the read came to, the note it left and what that note rests
on, anything it says against a note written by hand, and what the note moved:
the player on the live projection and boards beside the same player on the
second ones, which have the analyst's notes applied.

Reads and boards are kept for good. The second projection is kept a week, so
an older morning shows its ranks without the lines under them.
"""

from __future__ import annotations

from datetime import date
from typing import Mapping, Optional

from peewee import fn

from db.models.nba import AnalystRead, DailyRanking, Player, PlayerProjection, ProjectionAdjustment
from db.models.nba.daily_rankings import BOARD_CATEGORIES, BOARD_POINTS, SOURCE_CV
from db.models.nba.projection_adjustments import SCOPE_ANALYST, SCOPE_SEASON
from schemas.analyst import AnalystDigest, DigestMove, DigestRead, DigestReason, DigestSide, DigestSpend
from services import live_projection_inputs as live_inputs
from services.analyst_client import Month
from services.live_notes import standing
from services.projection_notes import note_entry, player_names

LIVE_SOURCE = "cv_live"
SHADOW_SOURCE = "cv_live_shadow"
SHADOW_BOARDS = "cv_shadow"
DAYS_LISTED = 30
MOVES_LISTED = 12


def _usd(micros: int) -> float:
    return round(int(micros or 0) / 1_000_000, 4)


def spend_of(month: Month) -> DigestSpend:
    return DigestSpend(
        available=month.available, enabled=month.enabled,
        month_spent_usd=round(month.spent_micros / 1_000_000, 2), month_cap_usd=round(month.cap_micros / 1_000_000, 2),
        model=month.model, prompt_version=month.prompt_version,
    )


def read_days(season: str, limit: int = DAYS_LISTED) -> list[date]:
    """The mornings with reads, newest first."""
    R = AnalystRead
    return [
        day for (day,) in R.select(R.as_of_date).where(R.season == season).distinct()
        .order_by(R.as_of_date.desc()).limit(limit).tuples()
    ]


def _projections(season: str, day: date, source: str) -> dict[int, PlayerProjection]:
    P = PlayerProjection
    return {
        row.player_id: row
        for row in P.select(P.player, P.min, P.pts, P.projected_gp, P.raw)
        .where((P.season == season) & (P.source == source) & (P.as_of_date == day))
    }


def _side(pid: int, lines: Mapping[int, PlayerProjection], points: Mapping[int, int],
          categories: Mapping[int, int]) -> Optional[DigestSide]:
    row = lines.get(pid)
    if row is None and pid not in points and pid not in categories:
        return None
    games = None
    if row is not None:
        raw = row.raw if isinstance(row.raw, dict) else {}
        try:
            games = round(float(raw.get("games")), 1)
        except (TypeError, ValueError):
            games = float(row.projected_gp or 0)
    return DigestSide(
        games=games,
        minutes=round(float(row.min), 1) if row is not None and row.min is not None else None,
        points=round(float(row.pts), 1) if row is not None and row.pts is not None else None,
        points_rank=points.get(pid), categories_rank=categories.get(pid),
    )


def build(season: str, day: Optional[date], month: Month, today: date) -> AnalystDigest:
    """The digest of `day`'s reads (the newest morning with any when None)."""
    days = read_days(season)
    as_of = day or (days[0] if days else None)
    notes_live = ProjectionAdjustment.select().where(ProjectionAdjustment._live(season, SCOPE_ANALYST)).count()
    shadow_as_of = (
        DailyRanking.select(fn.MAX(DailyRanking.as_of_date))
        .where((DailyRanking.source == SHADOW_BOARDS) & (DailyRanking.season == season)).scalar()
    )
    out = AnalystDigest(season=season, as_of=as_of, days=days, notes_live=notes_live, shadow_as_of=shadow_as_of,
                        spend=spend_of(month))
    if as_of is None:
        return out

    reads = AnalystRead.of_day(season, as_of)
    live_lines = _projections(season, as_of, LIVE_SOURCE)
    shadow_lines = _projections(season, as_of, SHADOW_SOURCE)
    ranks = {
        (source, board): DailyRanking.ranks_on(as_of, source, board)
        for source in (SOURCE_CV, SHADOW_BOARDS) for board in (BOARD_POINTS, BOARD_CATEGORIES)
    }

    def sides(pid: int) -> tuple[Optional[DigestSide], Optional[DigestSide]]:
        return (
            _side(pid, live_lines, ranks[(SOURCE_CV, BOARD_POINTS)], ranks[(SOURCE_CV, BOARD_CATEGORIES)]),
            _side(pid, shadow_lines, ranks[(SHADOW_BOARDS, BOARD_POINTS)], ranks[(SHADOW_BOARDS, BOARD_CATEGORIES)]),
        )

    def team_of(pid: int) -> Optional[str]:
        row = live_lines.get(pid) or shadow_lines.get(pid)
        raw = row.raw if row is not None and isinstance(row.raw, dict) else {}
        return raw.get("team")

    # The widest moves between the two points boards, whoever was read
    live_points, shadow_points = ranks[(SOURCE_CV, BOARD_POINTS)], ranks[(SHADOW_BOARDS, BOARD_POINTS)]
    moves = sorted(
        ((abs(shadow_points[pid] - rank), pid) for pid, rank in live_points.items()
         if pid in shadow_points and shadow_points[pid] != rank),
        key=lambda pair: (-pair[0], live_points[pair[1]]),
    )[:MOVES_LISTED]

    note_ids = {r.note_id for r in reads if r.note_id is not None}
    notes = {n.id: n for n in ProjectionAdjustment.select().where(ProjectionAdjustment.id.in_(note_ids))} if note_ids else {}
    live_notes = [n for n in notes.values() if n.superseded_by_id is None and n.retired_at is None]
    involved = {n.player_id for n in live_notes} | {n.depends_on_player_id for n in live_notes if n.depends_on_player_id}
    tips = live_inputs.tips_of(involved, season, today)
    read_players = {r.player_id for r in reads}
    names = player_names(read_players | {pid for _gap, pid in moves}
                         | {n.depends_on_player_id for n in notes.values() if n.depends_on_player_id})
    owners = {
        r.player_id for r in ProjectionAdjustment.select(ProjectionAdjustment.player)
        .where(ProjectionAdjustment._live(season, SCOPE_SEASON) & ProjectionAdjustment.player.in_(read_players))
    } if read_players else set()

    for read in reads:
        pid = read.player_id
        note = notes.get(read.note_id) if read.note_id is not None else None
        is_live = note is not None and note in live_notes
        live, shadow = sides(pid)
        out.reads.append(DigestRead(
            id=read.id, player_id=pid, name=names.get(pid, f"player {pid}"), team=team_of(pid),
            as_of=read.as_of_date, read_at=read.created_at,
            reasons=[DigestReason(rule=str(r.get("rule", "")), text=str(r.get("text", "")))
                     for r in (read.reasons or []) if isinstance(r, dict)],
            outcome=read.outcome, summary=read.summary, disagrees=read.disagrees,
            note=note_entry(note, names=names,
                            standing=standing(live_inputs.note_of(note), today, tips) if is_live else None)
            if note is not None else None,
            note_live=is_live, owner_note=pid in owners, live=live, shadow=shadow,
            cost_usd=_usd(read.cost_micros),
        ))
        out.counts[read.outcome] = out.counts.get(read.outcome, 0) + 1
    out.cost_usd = round(sum(int(r.cost_micros or 0) for r in reads) / 1_000_000, 4)

    for _gap, pid in moves:
        live, shadow = sides(pid)
        if live is not None and shadow is not None:
            out.moved.append(DigestMove(player_id=pid, name=names.get(pid, f"player {pid}"), team=team_of(pid),
                                        live=live, shadow=shadow))
    return out
