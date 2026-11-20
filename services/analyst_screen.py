"""
The analyst's morning screen: which players get a read (the Live rankings
plan, step 4).

The analyst reads the stored news about one player at a time, and a read costs
money, so the list is short and is built by fixed rules, never by the model:

    note_due         a note of its own is due to be read again, or has ended
    status_changed   the availability feed's verdict on him changed
    new_item         a new injury, availability, lineup or transaction item
    teammate_change  a prominent teammate went out, or came back
    minutes_gap      his minutes over his last five games are far from the projection's
    rank_gap         one of the day's widest gaps between his rank here and ESPN's

"Changed" and "new" are since his last read that came to an answer, or the
last two days for a player never read. A rule alone is not enough: a note
must cite a stored item, so a player with no citable item on file is skipped
unless he already carries a note of the analyst's (which may need retiring).
And the screen is for players who matter to a fantasy roster: a regular, or
somebody the board ranks.

The list comes back in the order it should be read: by the strongest rule that
picked him, then by how much he matters. The pipeline reads from the top until
the morning's cap, and skips a player whose facts are the ones his last read
already saw (`services.analyst_inputs.fingerprint`).

Pure: `services.analyst_inputs` gathers what each player's `Subject` says.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Optional

NOTE_DUE = "note_due"
STATUS_CHANGED = "status_changed"
NEW_ITEM = "new_item"
TEAMMATE_CHANGE = "teammate_change"
MINUTES_GAP = "minutes_gap"
RANK_GAP = "rank_gap"
# In order of priority: the first rule that picked a player decides his place in the list
RULES = (NOTE_DUE, STATUS_CHANGED, NEW_ITEM, TEAMMATE_CHANGE, MINUTES_GAP, RANK_GAP)

# Who the screen is for: he plays this much when his team is whole, or the board ranks him this high
MIN_MINUTES = 14.0
RANKED_WITHIN = 200
# A teammate whose absence moves a rotation
PROMINENT_MINUTES = 24.0
# Minutes a game over his last games, against the projection's
MINUTES_GAP_GAMES = 5
MINUTES_GAP_MIN = 6.0
# The day's widest gaps against ESPN's rank: how many, how wide, and how high either rank must be
RANK_GAP_TOP = 8
RANK_GAP_MIN = 30
RANK_GAP_WITHIN = 150


@dataclass(frozen=True)
class StatusChange:
    """The availability feed's verdict on him, before and now. None: no report."""

    was: Optional[str]
    now: Optional[str]


@dataclass(frozen=True)
class TeammateChange:
    name: str
    minutes: float                      # what he plays when the team is whole
    out: bool                           # True: he went out. False: he is back


@dataclass(frozen=True)
class Subject:
    """What the screen knows about one player this morning."""

    player_id: int
    name: str
    minutes_usual: float = 0.0
    out_now: bool = False                               # the projection has him out
    citable_items: int = 0                              # on file, in the days a read is shown
    new_items: tuple[str, ...] = ()                     # the kinds of the citable ones that are new
    status_change: Optional[StatusChange] = None
    teammate_changes: tuple[TeammateChange, ...] = ()
    recent_minutes: Optional[float] = None              # a game, over his last games played
    recent_games: int = 0
    projected_minutes: Optional[float] = None           # a game, in the games he is expected to play
    cv_rank: Optional[int] = None                       # on the points board
    espn_rank: Optional[int] = None
    has_note: bool = False                              # a live note of the analyst's
    note_due: Optional[str] = None                      # why that note should be read again


@dataclass(frozen=True)
class Reason:
    rule: str
    text: str

    def as_dict(self) -> dict:
        return {"rule": self.rule, "text": self.text}


@dataclass(frozen=True)
class Candidate:
    player_id: int
    reasons: tuple[Reason, ...]

    @property
    def rules(self) -> tuple[str, ...]:
        return tuple(r.rule for r in self.reasons)


def _minutes(value: float) -> str:
    return f"{value:.1f}".rstrip("0").rstrip(".")


def matters(subject: Subject) -> bool:
    return subject.minutes_usual >= MIN_MINUTES or (
        subject.cv_rank is not None and subject.cv_rank <= RANKED_WITHIN
    )


def status_reason(change: StatusChange) -> str:
    if change.was is None:
        return f"The availability feed now has him {change.now}; it had no report on him before"
    if change.now is None:
        return f"The availability feed had him {change.was} and no longer reports on him"
    return f"The availability feed moved him from {change.was} to {change.now}"


def minutes_gap(subject: Subject) -> Optional[float]:
    """How far his recent minutes are from the projection's, when that is far."""
    if subject.out_now or subject.recent_games < MINUTES_GAP_GAMES:
        return None
    if subject.recent_minutes is None or subject.projected_minutes is None:
        return None
    gap = subject.recent_minutes - subject.projected_minutes
    return gap if abs(gap) >= MINUTES_GAP_MIN else None


def rank_gap(subject: Subject) -> Optional[int]:
    """ESPN's rank less ours, when both rank him and at least one ranks him high."""
    if subject.cv_rank is None or subject.espn_rank is None:
        return None
    if min(subject.cv_rank, subject.espn_rank) > RANK_GAP_WITHIN:
        return None
    gap = subject.espn_rank - subject.cv_rank
    return gap if abs(gap) >= RANK_GAP_MIN else None


def widest_gaps(subjects: Iterable[Subject], top: int = RANK_GAP_TOP) -> set[int]:
    """The players behind the day's widest gaps against ESPN's rank."""
    gaps = [(abs(gap), s.player_id) for s in subjects if (gap := rank_gap(s)) is not None]
    gaps.sort(key=lambda pair: (-pair[0], pair[1]))
    return {pid for _gap, pid in gaps[:top]}


def reasons_for(subject: Subject, widest: set[int]) -> tuple[Reason, ...]:
    """Every rule that picks him, in the rules' order."""
    out: list[Reason] = []
    if subject.has_note and subject.note_due:
        out.append(Reason(NOTE_DUE, subject.note_due))
    if subject.status_change is not None:
        out.append(Reason(STATUS_CHANGED, status_reason(subject.status_change)))
    if subject.new_items:
        kinds = ", ".join(dict.fromkeys(subject.new_items))
        count = len(subject.new_items)
        out.append(Reason(NEW_ITEM, f"{count} new item{'s' if count != 1 else ''} since his last read: {kinds}"))
    for change in subject.teammate_changes:
        what = "is now out" if change.out else "is back"
        out.append(Reason(TEAMMATE_CHANGE,
                          f"{change.name}, who plays {_minutes(change.minutes)} minutes a game, {what}"))
    gap = minutes_gap(subject)
    if gap is not None:
        out.append(Reason(
            MINUTES_GAP,
            f"He played {_minutes(subject.recent_minutes)} minutes a game over his last {subject.recent_games}; "
            f"the projection has him at {_minutes(subject.projected_minutes)}",
        ))
    if subject.player_id in widest:
        out.append(Reason(RANK_GAP, f"Ranked {subject.cv_rank} here and {subject.espn_rank} by ESPN"))
    return tuple(out)


def screen(subjects: Iterable[Subject]) -> list[Candidate]:
    """The morning's list, in the order it should be read."""
    pool = list(subjects)
    widest = widest_gaps(s for s in pool if matters(s))
    picked: list[tuple[tuple, Candidate]] = []
    for subject in pool:
        reasons = reasons_for(subject, widest)
        if not reasons:
            continue
        due = reasons[0].rule == NOTE_DUE
        if not due and not matters(subject):
            continue
        # A note needs a stored item to cite. Without one there is nothing to
        # write, but a note he already carries can still be kept or retired.
        if not subject.citable_items and not subject.has_note:
            continue
        rank = subject.cv_rank if subject.cv_rank is not None else 10_000
        order = (RULES.index(reasons[0].rule), rank, -subject.minutes_usual, subject.player_id)
        picked.append((order, Candidate(subject.player_id, reasons)))
    picked.sort(key=lambda pair: pair[0])
    return [candidate for _order, candidate in picked]
