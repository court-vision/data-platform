"""
The projections editor in season: the note a player carries, the live
projection it feeds, and what the page does with both (the Live rankings plan,
step 3).

In season the board is priced on the live projection (source `cv_live`,
pipelines/live_projection.py), and what a person adds to it is a season note
(services.live_notes): a return date, or minutes and a share of the offense
with an end. This module is the editor's side of that:

- `load_notes` and `load_live` read what the page shows beside the preseason
  lines: each player's live note with how it stands today, and his line in
  this morning's snapshot;
- `note_fields` turns a save into the row's columns, with the evidence kept as
  it read when the note was written;
- `preview` runs the live model twice, with the note as it is and as the edit
  would leave it. Nothing is written. It prices the line and the games, not a
  rank: a rank is a place among everybody under the board's own rules, and the
  backend works that out on the next read of the board;
- `recent_news` lists the stored items a note can cite.

A save rebuilds the day's live snapshot (the route forces `live_projection`),
so the Draft lab reads the note at once. The day's stored boards
(nba.daily_rankings) are the morning's and stay as they were.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from typing import Mapping, Optional

from core.nba_calendar import EASTERN
from db.models.nba import NewsItem, NewsItemPlayer, Player, PlayerProjection, ProjectionAdjustment
from db.models.nba.projection_adjustments import CONFIDENCES, SCOPE_SEASON
from schemas.projections import (
    AdjustmentEntry,
    EvidenceItem,
    LiveSide,
    LiveStretch,
    NoteChange,
    NotePreview,
    NoteSave,
    NoteStanding,
    ProjectionLine,
)
from services import live_projection_inputs as live_inputs
from services.live_notes import Note, Standing, standing
from services.live_projection_model import LINE_KEYS, LiveProjection, build_morning, project

LIVE_SOURCE = "cv_live"
# How long a note runs before it must be read again, when the save does not say
DEFAULT_REVIEW_DAYS = 14
# The stored news a note can cite: this far back, this many items
NEWS_DAYS = 21
NEWS_LIMIT = 25


def today_et(now: Optional[datetime] = None) -> date:
    """The Eastern calendar date: the morning the live projection is built for."""
    return (now.astimezone(EASTERN) if now is not None else datetime.now(EASTERN)).date()


# ---- what the page reads -----------------------------------------------------------------


def _standing(found: Standing) -> NoteStanding:
    return NoteStanding(state=found.state, games=found.games, role=found.role, reasons=list(found.reasons))


def note_entry(
    record: ProjectionAdjustment,
    *,
    standing: Optional[Standing] = None,
    names: Optional[Mapping[int, str]] = None,
) -> AdjustmentEntry:
    """One stored row, of either scope, as the page shows it."""
    state = "retired" if record.retired_at is not None else (
        "superseded" if record.superseded_by_id is not None else "live"
    )
    teammate = record.depends_on_player_id
    return AdjustmentEntry(
        id=record.id,
        scope=record.scope,
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
        return_earliest=record.return_earliest,
        return_latest=record.return_latest,
        until_date=record.until_date,
        depends_on_player_id=teammate,
        depends_on_name=(names or {}).get(teammate) if teammate is not None else None,
        evidence=[EvidenceItem(**item) for item in (record.evidence or []) if isinstance(item, dict)],
        confidence=record.confidence,
        review_date=record.review_date,
        standing=_standing(standing) if standing is not None else None,
    )


def player_names(player_ids) -> dict[int, str]:
    wanted = sorted({pid for pid in player_ids if pid is not None})
    if not wanted:
        return {}
    return dict(Player.select(Player.id, Player.name).where(Player.id.in_(wanted)).tuples())


def load_notes(season: str, today: date) -> dict[int, AdjustmentEntry]:
    """Every player's live season note, with how it stands on `today`."""
    records = ProjectionAdjustment.active_for(season, SCOPE_SEASON)
    if not records:
        return {}
    involved = {r.player_id for r in records} | {r.depends_on_player_id for r in records if r.depends_on_player_id}
    tips = live_inputs.tips_of(involved, season, today)
    names = player_names(r.depends_on_player_id for r in records)
    return {
        r.player_id: note_entry(r, standing=standing(live_inputs.note_of(r), today, tips), names=names)
        for r in records
    }


def stored_line(record: PlayerProjection) -> ProjectionLine:
    """A stored live row as a line: a game he plays, over the games he is expected to play."""
    raw = record.raw if isinstance(record.raw, dict) else {}
    try:
        games = float(raw.get("games"))
    except (TypeError, ValueError):
        games = float(record.projected_gp or 0)
    return ProjectionLine(
        games=round(games, 1),
        min=float(record.min) if record.min is not None else None,
        **{k: float(getattr(record, k) or 0) for k in LINE_KEYS},
    )


def load_live(season: str) -> tuple[Optional[date], dict[int, ProjectionLine]]:
    """The newest live snapshot: its morning, and each player's line in it."""
    rows = PlayerProjection.latest_for_season(season, source=LIVE_SOURCE)
    if not rows:
        return None, {}
    return max(r.as_of_date for r in rows), {r.player_id: stored_line(r) for r in rows}


def note_history_entries(records: list[ProjectionAdjustment], season: str, today: date) -> list[AdjustmentEntry]:
    """Every version of a player's rows as entries; the live season note carries its standing."""
    names = player_names(r.depends_on_player_id for r in records)
    live = [r for r in records if r.scope == SCOPE_SEASON and r.superseded_by_id is None and r.retired_at is None]
    tips = live_inputs.tips_of(
        {r.player_id for r in live} | {r.depends_on_player_id for r in live if r.depends_on_player_id}, season, today
    )
    return [
        note_entry(
            r, names=names,
            standing=standing(live_inputs.note_of(r), today, tips) if r in live else None,
        )
        for r in records
    ]


# ---- what a save writes --------------------------------------------------------------------


def evidence_for(news_ids: list[int]) -> tuple[list[dict], list[int]]:
    """The cited news items as the note keeps them, and the ids that are not stored."""
    wanted = list(dict.fromkeys(news_ids))
    if not wanted:
        return [], []
    found = {item.id: item for item in NewsItem.select().where(NewsItem.id.in_(wanted))}
    kept = [
        {
            "type": "news", "id": item.id, "source": item.source, "kind": item.kind,
            "headline": item.headline, "url": item.url,
            "published_at": item.published_at.isoformat() if item.published_at else None,
        }
        for item in (found[i] for i in wanted if i in found)
    ]
    return kept, [i for i in wanted if i not in found]


def note_fields(body: NoteSave, today: date, evidence: list[dict]) -> dict:
    """A save as the columns of its row."""
    return {
        "scope": SCOPE_SEASON,
        "kind": body.kind,
        "minutes": body.minutes,
        "usage": body.usage,
        "games": 0 if body.out_for_season else None,
        "return_date": body.return_date,
        "return_earliest": body.return_earliest,
        "return_latest": body.return_latest,
        "until_date": body.until_date,
        "depends_on_player": body.depends_on_player_id,
        "evidence": evidence or None,
        "confidence": body.confidence,
        "review_date": body.review_date or today + timedelta(days=DEFAULT_REVIEW_DAYS),
        "note": body.note,
        "source_url": body.source_url,
        "author": body.author,
    }


def recent_news(player_id: int, now: Optional[datetime] = None) -> list[EvidenceItem]:
    """The player's stored news of the last three weeks, newest first."""
    since = (now or datetime.now(timezone.utc)) - timedelta(days=NEWS_DAYS)
    when = NewsItem.published_at
    items = (
        NewsItem.select()
        .join(NewsItemPlayer)
        .where((NewsItemPlayer.player == player_id) & ((when >= since) | (when.is_null() & (NewsItem.fetched_at >= since))))
        .order_by(when.desc(nulls="last"), NewsItem.id.desc())
        .limit(NEWS_LIMIT)
    )
    return [
        EvidenceItem(type="news", id=item.id, source=item.source, kind=item.kind, headline=item.headline,
                     url=item.url, published_at=item.published_at)
        for item in items
    ]


# ---- a note tried on -------------------------------------------------------------------------


def tried_note(player_id: int, change: NoteChange, written: datetime, review_date: date) -> Note:
    """A requested note as the model would read it once saved at `written` (Eastern)."""
    return Note(
        id=0, player_id=player_id, kind="other", written=written, review_date=review_date,
        minutes=change.minutes, usage=change.usage, games=0 if change.out_for_season else None,
        return_date=change.return_date, until_date=change.until_date, depends_on=change.depends_on_player_id,
    )


def live_side(projection: LiveProjection) -> LiveSide:
    line = projection.line()
    return LiveSide(
        line=ProjectionLine(games=round(projection.games, 1), **line),
        stretches=[
            LiveStretch(first=date.fromisoformat(first), last=date.fromisoformat(last), games=games,
                        chance=chance, minutes=minutes, usage=usage)
            for first, last, games, chance, minutes, usage in projection.timeline()
        ],
    )


def run_live(inputs: live_inputs.LiveInputs) -> dict[int, LiveProjection]:
    """The morning's live projection from `inputs`, as the pipeline builds it."""
    morning = build_morning(inputs.lines, inputs.priors, inputs.ahead, inputs.as_of,
                            teams=inputs.teams, ages=inputs.ages)
    return project(morning, live_inputs.load_coefficients(), inputs.known, inputs.role_notes)


def preview(
    inputs: live_inputs.LiveInputs,
    player_id: int,
    change: Optional[NoteChange],
    *,
    written: datetime,
    review_date: Optional[date] = None,
) -> Optional[NotePreview]:
    """One player's live projection as it stands and as `change` would leave
    it (None: with no note at all). None when the model does not project him:
    no game this season and no preseason projection."""
    before = run_live(inputs).get(player_id)
    if before is None:
        return None
    note = None
    if change is not None:
        note = tried_note(player_id, change, written,
                          review_date or inputs.as_of + timedelta(days=DEFAULT_REVIEW_DAYS))
    tried = inputs.with_note(player_id, note)
    after = run_live(tried).get(player_id)
    if after is None:
        return None
    found = tried.standings.get(player_id)
    return NotePreview(
        player_id=player_id, as_of=inputs.as_of, before=live_side(before), after=live_side(after),
        standing=_standing(found) if found is not None else None,
    )


def confidences() -> list[str]:
    return list(CONFIDENCES)
