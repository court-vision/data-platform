"""
The Availability page: the availability pipeline's dark-week scorecard.

The verdicts run dark until a gate is judged (docs/NEWS_AND_INJURY_PIPELINES_PLAN.md,
build step 2): for most scratches our verdict says out before ESPN fantasy's own
status flips, and no player who played was out at his tip. Both come from
replaying the resolver over the report log, one (player, date) at a time, in the
order we saw the reports:

    ours        what `resolve` says from every source but ESPN fantasy, after
                each poll before the tip. Fantasy is left out because its floor
                makes the stored verdict out the moment ESPN is, so the stored
                verdict could never be later than ESPN.
    ESPN        ESPN fantasy's own status, after each of its polls.
    at the tip  the full verdict, floor and all: what the reactor would have had.
    the result  the box score's last word on him: inactive (did not play) or active.

A side's time is when its out began and stayed out to the tip; an out it took
back does not count. ESPN's time is when our fantasy poll saw the flip (every 15
minutes from 11 AM ET on a game day, hourly before), so a lead shorter than the
poll may be the poll, not ESPN.

For a player the box score lists inactive:

    us_first, same_poll, espn_first   both called him out by the tip (same_poll:
                                      within one 2-minute tick of each other)
    us_only, espn_only                only one side did
    carried                           both had him out at their first read of
                                      the date: a long absence, not a race
    unannounced                       a source named him, neither called him out

And otherwise:

    false_out                         active in the box score, out at the tip
    not_in_box                        called out, tipped, never on the box score
    pending                           called out, not tipped yet

A player the box score lists inactive whom no other source ever named (a
two-way player, mostly) is left out, as is one neither side called who played.
A date is scored only when the pipeline's first poll came before its first
tip: the pipeline first polled at 3:30 AM ET on Nov 3, inside Nov 2's game date.

Blocking DB: call `build_scorecard` through `run_in_db_thread`.
"""

from __future__ import annotations

import statistics
from collections import Counter, defaultdict
from datetime import date, datetime, timedelta, timezone
from typing import Mapping, Optional, Sequence

from core.nba_calendar import nba_date_et
from db.base import db
from db.models.nba import Game, Player, PlayerProfile
from pipelines.availability import (
    TICK_MINUTES,
    Slate,
    SlateGame,
    _REPORT_COLUMNS,
    _stored_report,
)
from pipelines.transformers.availability import ET, GameContext, Report, Verdict, resolve
from schemas.availability import (
    AvailabilityCall,
    AvailabilityDay,
    AvailabilityScorecard,
    AvailabilitySummary,
)

MAX_DAYS = 30
FLOOR = "espn_fantasy"
BOX = "nba_boxscore"
# Two calls this close came from the same tick (or adjacent ones): neither won.
SAME_POLL = timedelta(minutes=TICK_MINUTES)
RACES = ("us_first", "same_poll", "espn_first", "us_only", "espn_only")
BOTH_CALLED = ("us_first", "same_poll", "espn_first")
ORDER = {outcome: i for i, outcome in enumerate(
    ("false_out", *RACES, "unannounced", "pending", "not_in_box", "carried")
)}

REPORTS_SQL = f"""
    SELECT game_date, {_REPORT_COLUMNS}
    FROM nba.availability_reports
    WHERE game_date BETWEEN %s AND %s
"""


# ------------------------------- the replay (pure) ------------------------------- #


def our_timeline(reports: Sequence[Report], game: GameContext) -> list[tuple[datetime, Optional[Verdict]]]:
    """Our verdict, ESPN fantasy left out, after each moment we saw something new."""
    mine = [r for r in reports if r.source != FLOOR]
    moments = sorted({r.observed_at for r in mine})
    return [(at, resolve([r for r in mine if r.observed_at <= at], game)) for at in moments]


def out_since(timeline: Sequence[tuple[datetime, Optional[str]]]) -> Optional[datetime]:
    """When the timeline's last stretch of `out` began, or None when it does not end out."""
    since: Optional[datetime] = None
    for at, status in timeline:
        if status == "out":
            since = since or at
        else:
            since = None
    return since


def _at_opening(at: datetime, source: Optional[str], opening: Mapping[str, datetime]) -> bool:
    first = opening.get(source or "")
    return first is not None and at - first < SAME_POLL


def _race(ours_at: Optional[datetime], espn_at: Optional[datetime]) -> str:
    if ours_at is None:
        return "espn_only"
    if espn_at is None:
        return "us_only"
    if abs(espn_at - ours_at) < SAME_POLL:
        return "same_poll"
    return "us_first" if ours_at < espn_at else "espn_first"


def score_call(
    reports: Sequence[Report],
    game: GameContext,
    tip: datetime,
    *,
    opening: Mapping[str, datetime],
    now: datetime,
    player_id: int,
    name: str,
    team: Optional[str] = None,
) -> Optional[AvailabilityCall]:
    """One (player, date): his outcome, or None when he is not part of the scorecard.

    `opening` is the date's first poll per source (its earliest report), which
    tells a long absence from a call made today.
    """
    before = [r for r in reports if r.observed_at < tip]
    box_rows = [r for r in reports if r.source == BOX and r.status in ("inactive", "active")]
    box = max(box_rows, key=lambda r: r.observed_at).status if box_rows else None

    ours = our_timeline(before, game)
    ours_at = out_since([(at, v.verdict if v else None) for at, v in ours])
    ours_verdict = next((v for at, v in ours if at == ours_at), None) if ours_at else None

    fantasy = sorted((r for r in before if r.source == FLOOR), key=lambda r: r.observed_at)
    espn_at = out_since([(r.observed_at, r.status) for r in fantasy])
    espn_at_tip = fantasy[-1].status if fantasy else None

    full = resolve(before, game)
    out_at_tip = full is not None and full.verdict == "out"
    called = ours_at is not None or espn_at is not None
    named = any(r.source != BOX for r in reports)

    if box == "active":
        if not out_at_tip:
            return None
        outcome = "false_out"
    elif box == "inactive":
        if not named:
            return None
        if not called:
            outcome = "unannounced"
        elif (
            ours_at is not None and espn_at is not None
            and _at_opening(ours_at, ours_verdict.decided_by if ours_verdict else None, opening)
            and _at_opening(espn_at, FLOOR, opening)
        ):
            outcome = "carried"
        else:
            outcome = _race(ours_at, espn_at)
    else:
        if not called:
            return None
        outcome = "pending" if now < tip else "not_in_box"

    shown = full if outcome == "false_out" or ours_verdict is None else ours_verdict
    lead = (espn_at - ours_at).total_seconds() / 60 if ours_at and espn_at else None
    return AvailabilityCall(
        game_date=game.game_date,
        player_id=player_id,
        name=name,
        team=team,
        game_id=game.game_id,
        tip=tip.astimezone(timezone.utc),
        outcome=outcome,
        ours_at=ours_at,
        ours_by=ours_verdict.decided_by if ours_verdict else None,
        espn_at=espn_at,
        lead_minutes=round(lead, 1) if lead is not None else None,
        verdict_at_tip=full.verdict if full else None,
        verdict_by=full.decided_by if full else None,
        espn_at_tip=espn_at_tip,
        box=box,
        reason=shown.reason if shown else None,
        detail=shown.detail if shown else None,
    )


def summarize(calls: Sequence[AvailabilityCall]) -> AvailabilitySummary:
    counts = Counter(call.outcome for call in calls)
    races = sum(counts[o] for o in RACES)
    first = counts["us_first"] + counts["us_only"]
    share = first / races if races else None
    leads = [c.lead_minutes for c in calls if c.outcome in BOTH_CALLED and c.lead_minutes is not None]
    false_outs = [c for c in calls if c.outcome == "false_out"]
    floor = sum(1 for c in false_outs if c.verdict_by == FLOOR)
    return AvailabilitySummary(
        races=races,
        **{o: counts[o] for o in (*RACES, "carried", "unannounced", "not_in_box", "pending")},
        first_share=round(share, 3) if share is not None else None,
        median_lead_minutes=round(statistics.median(leads), 1) if leads else None,
        first_by=dict(Counter(c.ours_by or "unknown" for c in calls if c.outcome in ("us_first", "us_only"))),
        false_outs=len(false_outs),
        false_outs_floor=floor,
        espn_false_outs=sum(1 for c in false_outs if c.espn_at_tip == "out"),
        gate_met=bool(races and share is not None and share > 0.5 and len(false_outs) == floor),
    )


# ------------------------------- the reads ------------------------------- #


def _slates(since: date, until: date) -> dict[date, Slate]:
    games: dict[date, list[SlateGame]] = defaultdict(list)
    rows = (
        Game.select(Game.game_id, Game.game_date, Game.home_team, Game.away_team, Game.start_time_et)
        .where(Game.game_date.between(since, until))
        .order_by(Game.game_id)
    )
    for g in rows:
        games[g.game_date].append(SlateGame(
            game_id=g.game_id,
            home=g.home_team_id,
            away=g.away_team_id,
            tip=datetime.combine(g.game_date, g.start_time_et, tzinfo=ET) if g.start_time_et else None,
        ))
    return {day: Slate(day, tuple(day_games)) for day, day_games in games.items()}


def _players(player_ids: set[int]) -> tuple[dict[int, str], dict[int, str]]:
    if not player_ids:
        return {}, {}
    ids = sorted(player_ids)
    names = {p.id: p.name for p in Player.select(Player.id, Player.name).where(Player.id.in_(ids))}
    teams = {
        row.player_id: row.team_id
        for row in PlayerProfile.select(PlayerProfile.player, PlayerProfile.team)
        .where(PlayerProfile.player.in_(ids) & PlayerProfile.team.is_null(False))
    }
    return names, teams


def build_scorecard(days: int = 7, now: Optional[datetime] = None) -> AvailabilityScorecard:
    """The last `days` game dates, today's included."""
    days = max(1, min(days, MAX_DAYS))
    now = now or datetime.now(timezone.utc)
    until = nba_date_et(now)
    since = until - timedelta(days=days - 1)

    reports: dict[tuple[date, int], list[Report]] = defaultdict(list)
    opening: dict[date, dict[str, datetime]] = defaultdict(dict)
    for row in db.execute_sql(REPORTS_SQL, (since, until)).fetchall():
        game_date, (player_id, report) = row[0], _stored_report(row[1:])
        reports[(game_date, player_id)].append(report)
        first = opening[game_date].get(report.source)
        if first is None or report.observed_at < first:
            opening[game_date][report.source] = report.observed_at

    slates = _slates(since, until)
    names, teams = _players({pid for _, pid in reports})

    calls: list[AvailabilityCall] = []
    dated: list[AvailabilityDay] = []
    for offset in range(days):
        day = until - timedelta(days=offset)
        slate = slates.get(day, Slate(day))
        tips = [g.tip for g in slate.games if g.tip is not None]
        first_tip = min(tips) if tips else None
        first_poll = min(opening[day].values()) if opening.get(day) else None
        covered = first_poll is not None and first_tip is not None and first_poll < first_tip
        mine: list[AvailabilityCall] = []
        if covered:
            for (game_date, pid), rows in reports.items():
                if game_date != day:
                    continue
                box_game = next((r.game_id for r in rows if r.source == BOX and r.game_id), None)
                game = slate.context(teams.get(pid), game_id=box_game)
                scheduled = slate.by_id(game.game_id)
                if not game.has_game or scheduled is None or scheduled.tip is None:
                    continue
                call = score_call(
                    rows, game, scheduled.tip, opening=opening[day], now=now,
                    player_id=pid, name=names.get(pid, str(pid)), team=teams.get(pid),
                )
                if call is not None:
                    mine.append(call)
        if slate.games or first_poll is not None:
            dated.append(AvailabilityDay(
                game_date=day,
                games=len(slate.games),
                first_tip=first_tip.astimezone(timezone.utc) if first_tip else None,
                first_poll=first_poll,
                covered=covered,
                calls=len(mine),
            ))
        calls += sorted(mine, key=lambda c: (c.tip, ORDER[c.outcome], c.name))

    return AvailabilityScorecard(
        days=days,
        since=since,
        until=until,
        generated_at=now,
        available=bool(reports),
        summary=summarize(calls),
        dates=dated,
        calls=calls,
    )
