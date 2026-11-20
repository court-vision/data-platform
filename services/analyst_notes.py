"""
What becomes of a read: the analyst's proposal checked, and its note written
(the Live rankings plan, step 4).

The backend asks the model and hands back a proposal. Nothing in it is trusted
until it has passed here, because this is the side that owns the tables:

- **No source, no note.** Every `ref` it cites must be an item the read was
  shown, and at least one must be citable: a stored injury, availability,
  lineup or transaction item. A rumor backs nothing.
- **Bounds.** Minutes at most what the read's `bounds` allow (what he plays
  already, plus what the players who are out left behind), usage inside its
  band, a return date on or after the morning and inside the season, a
  teammate to wait on who is on his team and out.
- **Every note expires.** A review date after the morning and no later than
  `bounds.review_by`.
- **Inputs only.** The proposal has no field for a rank or a value, and a
  note that changes nothing is refused.

A proposal that fails is `refused`: the read is logged with why, and no note
is written. One that passes is written to nba.projection_adjustments in the
`analyst` scope under the author `analyst`, superseding the analyst's own live
note for the player and never a note written by hand.

`judge` is pure. `settle` does the writes.
"""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from datetime import date, datetime, timedelta
from typing import Mapping, Optional, Sequence

from pydantic import ValidationError

from db.models.nba import ProjectionAdjustment
from db.models.nba.analyst_reads import (
    OUTCOME_NONE,
    OUTCOME_NOTE,
    OUTCOME_REFUSED,
    OUTCOME_RETIRED,
    OUTCOME_UNCHANGED,
)
from db.models.nba.projection_adjustments import SCOPE_ANALYST
from schemas.analyst import Proposal
from services.analyst_inputs import Item
from services.live_notes import Standing, standing
from services.live_projection_inputs import note_of

AUTHOR = "analyst"
DEFAULT_REVIEW_DAYS = 14
NOTE_CHARS = 2000

WRITE = "write"
KEEP = "keep"
RETIRE = "retire"
NOTHING = "none"
REFUSED = "refused"

# The columns that make two notes say the same thing
SAME_FIELDS = (
    "kind", "minutes", "usage", "games", "return_date", "return_earliest", "return_latest",
    "until_date", "depends_on_player", "confidence",
)


@dataclass(frozen=True)
class Judgment:
    """What a proposal comes to once checked."""

    action: str                                 # write | keep | retire | none | refused
    summary: str
    fields: dict = field(default_factory=dict)  # the row to write, for write and keep
    problems: tuple[str, ...] = ()
    disagrees: Optional[str] = None             # what it says against the note written by hand


def parse(raw: object) -> Proposal:
    """The backend's `proposal` as a Proposal. Raises ValueError when it is not one."""
    try:
        return Proposal.model_validate(raw)
    except ValidationError as exc:
        raise ValueError(str(exc)) from exc


def _day(text: Optional[str]) -> Optional[date]:
    return date.fromisoformat(text) if text else None


def _number(value: float) -> str:
    return f"{value:.2f}".rstrip("0").rstrip(".")


def _cites(proposal: Proposal, items: Sequence[Item]) -> tuple[list[Item], list[str]]:
    """The cited items a note may rest on, and the refs the read was never shown."""
    shown = {item.ref: item for item in items}
    cited = list(dict.fromkeys(ref.strip() for ref in proposal.cites if ref and ref.strip()))
    return [shown[ref] for ref in cited if ref in shown and shown[ref].citable], [r for r in cited if r not in shown]


def _source_url(cited: Sequence[Item], fallback: Optional[str] = None) -> Optional[str]:
    return next((item.evidence.get("url") for item in cited if item.evidence.get("url")), fallback)


def _review(proposal: Proposal, as_of: date, review_by: date, problems: list[str]) -> date:
    review = proposal.review_date or min(as_of + timedelta(days=DEFAULT_REVIEW_DAYS), review_by)
    if review <= as_of:
        problems.append(f"its review date, {review.isoformat()}, is not after this morning")
    elif review > review_by:
        problems.append(f"its review date, {review.isoformat()}, is past the latest it may be, {review_by.isoformat()}")
    return review


def _refused(problems: Sequence[str], disagrees: Optional[str] = None) -> Judgment:
    return Judgment(REFUSED, "Refused: " + "; ".join(problems), problems=tuple(problems), disagrees=disagrees)


def judge_write(proposal: Proposal, body: Mapping, items: Sequence[Item], as_of: date,
                disagrees: Optional[str]) -> Judgment:
    limits = body.get("bounds") or {}
    review_by = _day(limits.get("review_by")) or as_of + timedelta(days=DEFAULT_REVIEW_DAYS)
    season_end = _day(limits.get("season_end"))
    problems: list[str] = []

    games_part = proposal.out_for_season or proposal.return_date is not None
    role_part = proposal.minutes is not None or proposal.usage is not None
    if not games_part and not role_part:
        problems.append("it changes nothing: no return date, no minutes and no share of the offense")

    back, earliest, latest = proposal.return_date, proposal.return_earliest, proposal.return_latest
    if proposal.out_for_season and (back or earliest or latest):
        problems.append("a season written off has no return date")
    elif (earliest or latest) and back is None:
        problems.append("a range needs the likely return date too")
    elif back is not None:
        if back < as_of:
            problems.append(f"its return date, {back.isoformat()}, is before this morning")
        if season_end is not None and back > season_end:
            problems.append(f"its return date, {back.isoformat()}, is past the regular season: "
                            "that is a season written off")
        if (earliest and earliest > back) or (latest and latest < back):
            problems.append("its range does not hold its return date")

    minutes_max = limits.get("minutes_max")
    if proposal.minutes is not None:
        if proposal.minutes <= 0:
            problems.append("minutes must be more than none")
        elif minutes_max is not None and proposal.minutes > float(minutes_max):
            problems.append(f"{_number(proposal.minutes)} minutes is over its bound of {_number(float(minutes_max))}")
    if proposal.usage is not None:
        low, high = float(limits.get("usage_min", 0.0)), float(limits.get("usage_max", 2.0))
        if not (low <= proposal.usage <= high) or proposal.usage <= 0:
            problems.append(f"a share of the offense of {_number(proposal.usage)} is outside "
                            f"{_number(low)} to {_number(high)}")

    until_date = proposal.until_date if role_part else None
    until_player = proposal.until_player_id if role_part else None
    if until_date is not None and until_date < as_of:
        problems.append(f"its end, {until_date.isoformat()}, is before this morning")
    if until_player is not None:
        mate = next((t for t in body.get("teammates", ()) if t.get("player_id") == until_player), None)
        if mate is None:
            problems.append("the teammate it waits on is not in his team's rotation")
        elif not mate.get("out"):
            problems.append(f"the teammate it waits on, {mate.get('name')}, is not out")

    review = _review(proposal, as_of, review_by, problems)

    cited, unknown = _cites(proposal, items)
    if unknown:
        problems.append("it cites items the read was not shown: " + ", ".join(unknown))
    elif not cited:
        problems.append("it cites no stored injury, availability, lineup or transaction item")

    if problems:
        return _refused(problems, disagrees)
    reason = proposal.reason.strip() or "The analyst gave no reason."
    return Judgment(WRITE, reason, disagrees=disagrees, fields={
        "scope": SCOPE_ANALYST,
        "kind": proposal.kind,
        "minutes": round(proposal.minutes, 1) if proposal.minutes is not None else None,
        "usage": round(proposal.usage, 3) if proposal.usage is not None else None,
        "games": 0 if proposal.out_for_season else None,
        "return_date": back,
        "return_earliest": max(earliest, as_of) if earliest else None,
        "return_latest": latest,
        "until_date": until_date,
        "depends_on_player": until_player,
        "evidence": [item.evidence for item in cited],
        "confidence": proposal.confidence,
        "review_date": review,
        "note": reason[:NOTE_CHARS],
        "source_url": _source_url(cited),
        "author": AUTHOR,
    })


def judge_keep(proposal: Proposal, body: Mapping, items: Sequence[Item], as_of: date,
               own: Optional[ProjectionAdjustment], last_played: Mapping[int, datetime],
               disagrees: Optional[str]) -> Judgment:
    """Its own note, read again: the parts of it that still hold, with a new review date.

    A part that has ended is not brought back by keeping the note: a return
    date spent by his playing stays spent, and a role that ran out stays out.
    """
    if own is None:
        return _refused(["there is no note of its own to keep"], disagrees)
    limits = body.get("bounds") or {}
    review_by = _day(limits.get("review_by")) or as_of + timedelta(days=DEFAULT_REVIEW_DAYS)
    problems: list[str] = []

    # How it stands this morning were its review date not the thing in question
    note = note_of(own)
    holds: Standing = standing(replace(note, review_date=max(note.review_date, as_of)), as_of, last_played)
    if not holds.games and not holds.role:
        problems.append(f"the note it would keep has ended ({'; '.join(holds.reasons) or 'nothing of it applies'})")

    review = _review(proposal, as_of, review_by, problems)
    cited, unknown = _cites(proposal, items)
    if unknown:
        problems.append("it cites items the read was not shown: " + ", ".join(unknown))
    if problems:
        return _refused(problems, disagrees)

    kept = [e for e in (own.evidence or []) if isinstance(e, dict)]
    have = {(e.get("type"), e.get("id")) for e in kept}
    evidence = kept + [i.evidence for i in cited if (i.evidence.get("type"), i.evidence.get("id")) not in have]
    reason = proposal.reason.strip() or own.note
    return Judgment(KEEP, reason, disagrees=disagrees, fields={
        "scope": SCOPE_ANALYST,
        "kind": own.kind,
        "minutes": own.minutes if holds.role else None,
        "usage": own.usage if holds.role else None,
        "games": own.games if holds.games else None,
        "return_date": own.return_date if holds.games else None,
        "return_earliest": own.return_earliest if holds.games else None,
        "return_latest": own.return_latest if holds.games else None,
        "until_date": own.until_date if holds.role else None,
        "depends_on_player": own.depends_on_player_id if holds.role else None,
        "evidence": evidence or None,
        "confidence": own.confidence,
        "review_date": review,
        "note": own.note,
        "source_url": own.source_url or _source_url(cited),
        "author": AUTHOR,
    })


def judge(
    raw: object,
    body: Mapping,
    items: Sequence[Item],
    as_of: date,
    own: Optional[ProjectionAdjustment] = None,
    last_played: Optional[Mapping[int, datetime]] = None,
) -> Judgment:
    """A read's proposal, checked against what the read was shown.

    `body` is the packet the read was sent, `items` the items in it, `own`
    the analyst's live note for the player and `last_played` each player's
    last tip (to say which parts of that note still hold).
    """
    try:
        proposal = parse(raw)
    except ValueError as exc:
        return _refused([f"the proposal could not be read ({str(exc)[:200]})"])
    disagrees = None
    if body.get("owner_note") and proposal.against_owner_note and proposal.against_owner_note.strip():
        disagrees = proposal.against_owner_note.strip()[:NOTE_CHARS]
    reason = proposal.reason.strip()

    if proposal.decision == WRITE:
        return judge_write(proposal, body, items, as_of, disagrees)
    if proposal.decision == KEEP:
        return judge_keep(proposal, body, items, as_of, own, last_played or {}, disagrees)
    if proposal.decision == RETIRE:
        if own is None:
            return Judgment(NOTHING, reason or "Nothing to retire.", disagrees=disagrees)
        return Judgment(RETIRE, reason or "The note no longer holds.", disagrees=disagrees)
    return Judgment(NOTHING, reason or "Nothing a box score cannot say.", disagrees=disagrees)


def same_note(own: ProjectionAdjustment, fields: Mapping) -> bool:
    """Whether `fields` say what the note says now, its review date aside."""
    def norm(value):
        return float(value) if isinstance(value, (int, float)) or hasattr(value, "quantize") else value

    for name in SAME_FIELDS:
        have = own.depends_on_player_id if name == "depends_on_player" else getattr(own, name)
        if norm(have) != norm(fields.get(name)):
            return False
    return True


def settle(judgment: Judgment, player_id: int, season: str,
           own: Optional[ProjectionAdjustment]) -> tuple[str, Optional[int]]:
    """Write what a judgment calls for. Returns the read's outcome and the id of
    the note the player is left with (None when he has none of the analyst's)."""
    if judgment.action == REFUSED:
        return OUTCOME_REFUSED, own.id if own is not None else None
    if judgment.action == RETIRE:
        ProjectionAdjustment.retire(player_id, season, SCOPE_ANALYST)
        return OUTCOME_RETIRED, None
    if judgment.action == NOTHING:
        return OUTCOME_NONE, own.id if own is not None else None
    # A note written again as it stands is the note it had: with the same review
    # date nothing is stored, with a later one the note is carried on to it
    unchanged = judgment.action == KEEP or (own is not None and same_note(own, judgment.fields))
    if unchanged and own is not None and own.review_date == judgment.fields.get("review_date"):
        return OUTCOME_UNCHANGED, own.id
    record = ProjectionAdjustment.record(player_id, season, **judgment.fields)
    return (OUTCOME_UNCHANGED if unchanged else OUTCOME_NOTE), record.id
