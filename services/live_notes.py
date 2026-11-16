"""
Notes in season: what the live projection is told that a box score cannot say
(the Live rankings plan, step 3).

A note is a live `scope='season'` row of nba.projection_adjustments: one per
player, written in the projections editor (and, from step 4, by the analyst).
It states inputs, never a rank, and it has two parts, each with its own end:

- **games**: a return date (he is out until then) or a season written off.
  It holds until he plays: a game of his that tipped after the note was
  written spends it. A return date that has passed while he is still out is
  no longer believed, as with the availability feed's.
- **role**: minutes, his share of the offense, or both, as targets. It holds
  through `until_date`, or until the teammate it depends on has played since
  the note was written; with neither, for as long as the note is kept.

All of it holds only until the note's review date. A note nobody has read
again by then is not applied: stale judgment calls are how a hand-kept ranking
rots, so every note expires.

Nothing is written when a note ends. `standing` works out, for a morning, what
of a live note is still in force, from the note and from who has played since;
the table stays append-only.

Pure: `services.live_projection_inputs` reads the rows, this says what they
mean, and `services.live_projection_model` prices them.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import Mapping, Optional, Sequence

IN_FORCE = "in_force"
REVIEW_DUE = "review_due"
ENDED = "ended"


@dataclass(frozen=True)
class Note:
    """One live season note, as the model reads it."""

    id: int
    player_id: int
    kind: str
    written: datetime                           # Eastern, naive: when it was saved
    review_date: date
    minutes: Optional[float] = None             # a game, while the role part holds
    usage: Optional[float] = None               # his share of the offense against his usual one
    games: Optional[int] = None                 # 0: the season is written off
    return_date: Optional[date] = None
    until_date: Optional[date] = None
    depends_on: Optional[int] = None            # the teammate whose return ends the role part

    @property
    def out_for_season(self) -> bool:
        return self.games is not None and int(self.games) == 0

    @property
    def has_games_part(self) -> bool:
        return self.return_date is not None or self.out_for_season

    @property
    def has_role_part(self) -> bool:
        return self.minutes is not None or self.usage is not None


@dataclass(frozen=True)
class Standing:
    """How a live note stands on a morning."""

    state: str                                  # in_force | review_due | ended
    games: bool                                 # its return date or written-off season applies
    role: bool                                  # its minutes and usage apply
    reasons: tuple[str, ...] = ()               # why a part, or all of it, no longer does

    @property
    def in_force(self) -> bool:
        return self.state == IN_FORCE


def standing(note: Note, as_of: date, last_played: Mapping[int, datetime]) -> Standing:
    """What of `note` still applies on the morning of `as_of`.

    `last_played` is each player's most recent game before the morning, as the
    Eastern time it tipped.
    """
    if as_of > note.review_date:
        return Standing(REVIEW_DUE, False, False, (f"due to be read again since {note.review_date.isoformat()}",))

    reasons: list[str] = []
    games = note.has_games_part
    back = last_played.get(note.player_id)
    if games and back is not None and back > note.written:
        games = False
        reasons.append(f"he played on {back.date().isoformat()}")
    elif games and not note.out_for_season and note.return_date < as_of:
        games = False
        reasons.append(f"its return date, {note.return_date.isoformat()}, passed with him still out")

    role = note.has_role_part
    if role and note.until_date is not None and as_of > note.until_date:
        role = False
        reasons.append(f"it ran through {note.until_date.isoformat()}")
    teammate = last_played.get(note.depends_on) if note.depends_on is not None else None
    if role and teammate is not None and teammate > note.written:
        role = False
        reasons.append(f"the teammate it depended on played on {teammate.date().isoformat()}")

    return Standing(IN_FORCE if (games or role) else ENDED, games, role, tuple(reasons))


@dataclass(frozen=True)
class RoleNote:
    """The role part of a note in force, as `live_projection_model.project` prices it."""

    note_id: int
    kind: str
    minutes: Optional[float] = None
    usage: Optional[float] = None
    until_date: Optional[date] = None
    depends_on: Optional[int] = None


def role_note(note: Note) -> RoleNote:
    return RoleNote(note_id=note.id, kind=note.kind, minutes=note.minutes, usage=note.usage,
                    until_date=note.until_date, depends_on=note.depends_on)


def holds(note: RoleNote, days: Sequence[date], teammate_chances: Optional[Sequence[float]] = None) -> list[float]:
    """How far the role part holds in each of his team's remaining games: 1.0
    while it does, 0.0 once it has ended.

    Past `until_date` it has ended. "Until the teammate plays again" is not a
    date, so it is the chance he has not played yet: `teammate_chances` is the
    chance the teammate plays each of the same games, and a note depending on
    him holds in a game by the chance he has missed every game up to and
    including it. With a return date known for the teammate that is all of
    the note until that day and almost none after.
    """
    out: list[float] = []
    still_out = 1.0
    for i, day in enumerate(days):
        if teammate_chances is not None and i < len(teammate_chances):
            still_out *= 1.0 - min(max(teammate_chances[i], 0.0), 1.0)
        dated = 1.0 if (note.until_date is None or day <= note.until_date) else 0.0
        out.append(dated * (still_out if teammate_chances is not None else 1.0))
    return out
