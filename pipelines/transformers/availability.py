"""
The availability resolver: what each source said about a player, in, one
verdict out. Pure: no I/O, no ORM, no clock (docs/NEWS_AND_INJURY_PIPELINES_PLAN.md § 2).

Sources rank, highest first; a higher-ranked source always wins, and within one
source the newest report (its own `source_time`, else when we saw it) wins:

    nba_boxscore    INACTIVE -> out, ACTIVE -> available; `final`, the inactive
                    list the teams hand the scorer
    nba_report      the official report's status for that game, as is
    espn_injuries   Out -> out (questionable when ESPN's return date is on or
                    before the game); Day-To-Day -> questionable, upgraded to out
                    when the blurb says he won't play *and* names the game day
    espn_fantasy    already normalized by the extractor (OUT / SUSPENSION /
                    INJURY_RESERVE -> out, DOUBTFUL -> doubtful, ...)
    news_claim      a claim extracted from free text, at its own confidence

Floor (decision 3): until the box score has spoken, the verdict is never better
than out while ESPN fantasy has the player out, because ESPN's own lineup rules
treat him as out regardless. A final box-score verdict overrides the floor.

A player with a game and no report at all has no verdict row (he is available);
`resolve` returns None for him. `next_row` decides what the stored verdict row
becomes and whether its verdict *changed*, which is the only thing the
late-scratch reactor ever hears about.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field, replace
from datetime import date, datetime
from typing import Iterable, Mapping, Optional, Sequence
from zoneinfo import ZoneInfo

ET = ZoneInfo("America/New_York")

SOURCE_RANK: tuple[str, ...] = ("nba_boxscore", "nba_report", "espn_injuries", "espn_fantasy", "news_claim")
CONFIDENCE: Mapping[str, int] = {
    "nba_boxscore": 100,
    "nba_report": 90,
    "espn_injuries": 80,
    "espn_fantasy": 70,
}
VERDICTS: frozenset[str] = frozenset({"out", "doubtful", "questionable", "probable", "available", "unknown"})
# Verdict changes the reactor acts on (decision 8); the rest only update the row.
REACTION_VERDICTS: frozenset[str] = frozenset({"out", "doubtful", "available"})

# Future-tense "he won't play" phrasing, as Rotowire writes it. A bare "not play"
# would also catch recaps ("did not play in Wednesday's loss"), so it is not here.
_WONT_PLAY = re.compile(
    r"\b(won'?t play|will not play|(?:is not|isn'?t|not) expected to play|ruled out|is out"
    r"|sit out|sits out|will sit|will miss|will be rested|to be rested)\b",
    re.IGNORECASE,
)
# A weekday names this game only in a blurb written in the week before it.
_WEEKDAY_WINDOW_DAYS = 6
_SAME_DAY_WORDS: tuple[str, ...] = ("tonight", "today", "this evening")


@dataclass(frozen=True)
class Report:
    """One stored observation (a row of nba.availability_reports)."""

    source: str
    status: str                              # out | doubtful | questionable | probable | available | inactive | active
    observed_at: datetime
    source_time: Optional[datetime] = None
    game_id: Optional[str] = None
    reason: Optional[str] = None
    detail: Optional[str] = None             # the source's wording (ESPN's shortComment for espn_injuries)
    expected_return: Optional[date] = None
    confidence: Optional[int] = None

    @property
    def when(self) -> datetime:
        return self.source_time or self.observed_at


@dataclass(frozen=True)
class Verdict:
    verdict: str
    decided_by: str
    final: bool = False
    game_id: Optional[str] = None
    reason: Optional[str] = None
    detail: Optional[str] = None
    expected_return: Optional[date] = None
    confidence: Optional[int] = None


@dataclass(frozen=True)
class GameContext:
    """What the resolver needs to know about the player's game on `game_date`.

    `day_words` are the words that name this game in a blurb: the weekday
    ("Tuesday") and the opponent ("Lakers", "Los Angeles"), both only in a blurb
    written in the six days before the game. "Tonight" and "today" name it only when
    the blurb was written on the game's ET date.
    """

    game_date: date
    has_game: bool = True
    game_id: Optional[str] = None
    day_words: Sequence[str] = field(default_factory=tuple)


# ------------------------------- helpers ------------------------------- #


def _newest(reports: Iterable[Report], source: str) -> Optional[Report]:
    mine = [r for r in reports if r.source == source]
    return max(mine, key=lambda r: (r.when, r.observed_at)) if mine else None


def names_game_day(text: str, written_at: Optional[datetime], game: GameContext) -> bool:
    """Whether `text` points at this game rather than some later one."""
    lowered = text.lower()
    written_on = written_at.astimezone(ET).date() if written_at is not None else None
    weekday = game.game_date.strftime("%A").lower()
    in_week = written_on is None or 0 <= (game.game_date - written_on).days <= _WEEKDAY_WINDOW_DAYS
    if in_week and re.search(rf"\b{weekday}\b", lowered):
        return True
    if in_week and any(w and re.search(rf"\b{re.escape(w.lower())}\b", lowered) for w in game.day_words):
        return True
    if written_on == game.game_date:
        return any(re.search(rf"\b{w}\b", lowered) for w in _SAME_DAY_WORDS)
    return False


def espn_injuries_status(report: Report, game: GameContext) -> str:
    """ESPN's injury list says Out or Day-To-Day; turn that into a status for this game."""
    status = report.status
    if status == "out":
        if report.expected_return is not None and report.expected_return <= game.game_date:
            return "questionable"      # ESPN expects him back by then
        return "out"
    if status == "questionable" and report.detail and _WONT_PLAY.search(report.detail):
        if names_game_day(report.detail, report.source_time, game):
            return "out"
    return status


def _verdict_from(report: Report, status: str, game: GameContext, *, final: bool = False) -> Verdict:
    return Verdict(
        verdict=status,
        decided_by=report.source,
        final=final,
        game_id=report.game_id or game.game_id,
        reason=report.reason,
        detail=report.detail,
        expected_return=report.expected_return,
        confidence=report.confidence if report.confidence is not None else CONFIDENCE.get(report.source),
    )


# ------------------------------- public API ------------------------------- #


def resolve(reports: Sequence[Report], game: GameContext) -> Optional[Verdict]:
    """The verdict for one (player, game_date), or None when there is nothing to say."""
    box = _newest(reports, "nba_boxscore")
    if box is not None and box.status in ("inactive", "active"):
        return _verdict_from(box, "out" if box.status == "inactive" else "available", game, final=True)

    verdict: Optional[Verdict] = None
    for source in SOURCE_RANK[1:]:
        report = _newest(reports, source)
        if report is None:
            continue
        status = espn_injuries_status(report, game) if source == "espn_injuries" else report.status
        if status in VERDICTS:
            verdict = _verdict_from(report, status, game)
            break

    fantasy = _newest(reports, "espn_fantasy")
    if fantasy is not None and fantasy.status == "out" and (verdict is None or verdict.verdict != "out"):
        verdict = _verdict_from(fantasy, "out", game)

    if verdict is None:
        return None
    if not game.has_game:
        return replace(verdict, game_id=None)
    return verdict


def should_record(latest: Optional[Report], new: Report) -> bool:
    """Append `new` to the report log only when it says something that source's
    latest row for the player and date did not."""
    if latest is None:
        return True
    return (latest.status, latest.reason) != (new.status, new.reason)


@dataclass(frozen=True)
class StoredVerdict:
    """The current nba.player_availability row, as far as `next_row` cares."""

    verdict: str
    previous_verdict: Optional[str]
    changed_at: datetime


def next_row(
    current: Optional[StoredVerdict],
    new: Verdict,
    now: datetime,
    *,
    verdict_at_previous_game: Optional[str] = None,
) -> tuple[dict, bool]:
    """The verdict row's new values, and whether `verdict` changed.

    `previous_verdict` is the verdict before the last change on the same date,
    else the player's verdict at his previous game; `changed_at` moves only when
    the verdict value moves. A change in reason or detail alone is an update,
    never a change.
    """
    row = {
        "game_id": new.game_id,
        "verdict": new.verdict,
        "final": new.final,
        "decided_by": new.decided_by,
        "reason": new.reason,
        "detail": new.detail,
        "expected_return": new.expected_return,
        "confidence": new.confidence,
        "updated_at": now,
    }
    if current is None:
        return {**row, "previous_verdict": verdict_at_previous_game, "changed_at": now}, True
    if current.verdict != new.verdict:
        return {**row, "previous_verdict": current.verdict, "changed_at": now}, True
    return {**row, "previous_verdict": current.previous_verdict, "changed_at": current.changed_at}, False


def is_reaction_event(changed: bool, verdict: str) -> bool:
    """A verdict change the late-scratch reactor should hear about (the caller also
    checks that the game has not tipped)."""
    return changed and verdict in REACTION_VERDICTS
