"""
The availability resolver: what each source said about a player, in, one
verdict out. Pure: no I/O, no ORM, no clock (docs/NEWS_AND_INJURY_PIPELINES_PLAN.md § 2).

Sources rank, highest first; a higher-ranked source always wins. Within one
source the newest report wins: for the three snapshot sources (the box score
and both ESPN feeds, read whole on every poll) the one we saw last, for the
others the one written last (its own `source_time`, else when we saw it):

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

The rest is the vocabulary the pipeline maps each feed into: ESPN fantasy's
`injuryStatus` (`fantasy_status`), the live box score's player list
(`boxscore_players`), and the words that name an opponent in a blurb
(`team_day_words`).
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
# Read whole on every poll, so the last one seen is the source's current word.
# The others are documents: a later-written one wins even if read first.
SNAPSHOT_SOURCES: frozenset[str] = frozenset({"nba_boxscore", "espn_injuries", "espn_fantasy"})

# ESPN fantasy `injuryStatus` -> report status (plan § 2, step 4).
FANTASY_STATUSES: Mapping[str, str] = {
    "OUT": "out",
    "SUSPENSION": "out",
    "INJURY_RESERVE": "out",
    "DOUBTFUL": "doubtful",
    "QUESTIONABLE": "questionable",
    "DAY_TO_DAY": "questionable",
    "PROBABLE": "probable",
    "ACTIVE": "available",
}

# notPlayingReason prefixes for a player on the active list who is not playing:
# did not dress, not with the team. INACTIVE_* comes with status INACTIVE.
_BOX_OUT_REASONS: tuple[str, ...] = ("DND", "NWT")

# The words a blurb uses for each team: city (or its short form) and nickname.
TEAM_WORDS: Mapping[str, tuple[str, ...]] = {
    "ATL": ("Atlanta", "Hawks"),
    "BOS": ("Boston", "Celtics"),
    "BKN": ("Brooklyn", "Nets"),
    "CHA": ("Charlotte", "Hornets"),
    "CHI": ("Chicago", "Bulls"),
    "CLE": ("Cleveland", "Cavaliers", "Cavs"),
    "DAL": ("Dallas", "Mavericks", "Mavs"),
    "DEN": ("Denver", "Nuggets"),
    "DET": ("Detroit", "Pistons"),
    "GSW": ("Golden State", "Warriors"),
    "HOU": ("Houston", "Rockets"),
    "IND": ("Indiana", "Pacers"),
    "LAC": ("Los Angeles", "Clippers"),
    "LAL": ("Los Angeles", "Lakers"),
    "MEM": ("Memphis", "Grizzlies"),
    "MIA": ("Miami", "Heat"),
    "MIL": ("Milwaukee", "Bucks"),
    "MIN": ("Minnesota", "Timberwolves", "Wolves"),
    "NOP": ("New Orleans", "Pelicans"),
    "NYK": ("New York", "Knicks"),
    "OKC": ("Oklahoma City", "Thunder"),
    "ORL": ("Orlando", "Magic"),
    "PHI": ("Philadelphia", "76ers", "Sixers"),
    "PHX": ("Phoenix", "Suns"),
    "POR": ("Portland", "Trail Blazers", "Blazers"),
    "SAC": ("Sacramento", "Kings"),
    "SAS": ("San Antonio", "Spurs"),
    "TOR": ("Toronto", "Raptors"),
    "UTA": ("Utah", "Jazz"),
    "WAS": ("Washington", "Wizards"),
}
_WEEKDAYS: tuple[str, ...] = ("monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday")

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
    if not mine:
        return None
    if source in SNAPSHOT_SOURCES:
        # A player ESPN drops and re-lists under the same note is listed again,
        # though the note's own date is older than the drop.
        return max(mine, key=lambda r: r.observed_at)
    return max(mine, key=lambda r: (r.when, r.observed_at))


def names_game_day(text: str, written_at: Optional[datetime], game: GameContext) -> bool:
    """Whether `text` points at this game rather than some later one.

    The game's weekday names it; so does the opponent, unless the blurb names
    another weekday instead (the second game of a two-game series).
    """
    lowered = text.lower()
    written_on = written_at.astimezone(ET).date() if written_at is not None else None
    weekday = game.game_date.strftime("%A").lower()
    in_week = written_on is None or 0 <= (game.game_date - written_on).days <= _WEEKDAY_WINDOW_DAYS
    named_days = {d for d in _WEEKDAYS if re.search(rf"\b{d}\b", lowered)}
    if in_week and weekday in named_days:
        return True
    if in_week and not named_days and any(
        w and re.search(rf"\b{re.escape(w.lower())}\b", lowered) for w in game.day_words
    ):
        return True
    if written_on == game.game_date:
        return any(re.search(rf"\b{w}\b", lowered) for w in _SAME_DAY_WORDS)
    return False


def team_day_words(tricode: Optional[str]) -> tuple[str, ...]:
    """The words that name `tricode` in a blurb, for GameContext.day_words."""
    return TEAM_WORDS.get(tricode or "", ())


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
    latest row for the player and date did not: its status, reason or wording.

    The wording counts because the ruling can live there: ESPN keeps a player
    Day-To-Day with the same body part while his note changes from "is
    questionable for Tuesday" to "won't play Tuesday", and only the stored note
    lets `espn_injuries_status` see that. A source's wording changes when it
    writes a new note, so a quiet poll still writes nothing.
    """
    if latest is None:
        return True
    return (latest.status, latest.reason, latest.detail) != (new.status, new.reason, new.detail)


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
    previous_game_changed_at: Optional[datetime] = None,
) -> tuple[dict, bool]:
    """The verdict row's new values, and whether `verdict` changed.

    `previous_verdict` is the verdict before the last change on the same date,
    else the player's verdict at his previous game; `changed_at` moves only when
    the verdict value moves. A change in reason or detail alone is an update,
    never a change.

    A date's first row changes the verdict only when it differs from the one at
    his previous game, which is `available` when that is unknown (no row there:
    nothing listed him). A player ESPN has had out all week is not news at the
    6 AM rollover, so his new row keeps the time his verdict last moved
    (`previous_game_changed_at`) and the reactor never hears of it.
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
        changed = new.verdict != (verdict_at_previous_game or "available")
        changed_at = now if changed or previous_game_changed_at is None else previous_game_changed_at
        return {**row, "previous_verdict": verdict_at_previous_game, "changed_at": changed_at}, changed
    if current.verdict != new.verdict:
        return {**row, "previous_verdict": current.verdict, "changed_at": now}, True
    return {**row, "previous_verdict": current.previous_verdict, "changed_at": current.changed_at}, False


def is_reaction_event(changed: bool, verdict: str) -> bool:
    """A verdict change the late-scratch reactor should hear about (the caller also
    checks that the game has not tipped)."""
    return changed and verdict in REACTION_VERDICTS


# ------------------------------- feed vocabulary ------------------------------- #


def fantasy_status(injured: bool, injury_status: Optional[str]) -> Optional[str]:
    """ESPN fantasy's word for a player as a report status, or None for a word we
    do not know. `injuryStatus` decides; without one, the `injured` flag does."""
    if injury_status:
        return FANTASY_STATUSES.get(str(injury_status).strip().upper())
    return "out" if injured else "available"


@dataclass(frozen=True)
class BoxPlayer:
    """One player on a live box score's list, as an availability report says it."""

    player_id: int                 # the NBA id (personId)
    team: Optional[str]            # NBA tricode
    status: str                    # inactive | active
    reason: Optional[str]          # notPlayingReason: INACTIVE_INJURY, DND_ILLNESS, DNP_COACH, ...
    detail: Optional[str]          # notPlayingDescription: "Left Knee; Soreness"


def boxscore_tipped(game: Mapping) -> bool:
    """Whether the box score's game is under way or over (gameStatus 2 or 3)."""
    try:
        return int(game.get("gameStatus") or 1) >= 2
    except (TypeError, ValueError):
        return False


def boxscore_players(game: Mapping) -> list[BoxPlayer]:
    """Every listed player of a cdn.nba.com live box score (`game`), home then away.

    INACTIVE is out, and so is a player on the active list who did not dress or is
    not with the team (DND_*, NWT_*). The rest are active; a coach's DNP was
    available. Whether a pre-tip ACTIVE means anything yet is the caller's call.
    """
    players: list[BoxPlayer] = []
    for side in ("homeTeam", "awayTeam"):
        team = game.get(side) or {}
        tricode = team.get("teamTricode") or None
        for entry in team.get("players") or []:
            try:
                player_id = int(entry.get("personId"))
            except (TypeError, ValueError):
                continue
            reason = str(entry.get("notPlayingReason") or "").strip() or None
            out = (
                str(entry.get("status") or "").strip().upper() == "INACTIVE"
                or (reason or "").upper().startswith(_BOX_OUT_REASONS)
            )
            players.append(BoxPlayer(
                player_id=player_id,
                team=tricode,
                status="inactive" if out else "active",
                reason=reason,
                detail=str(entry.get("notPlayingDescription") or "").strip() or None,
            ))
    return players
