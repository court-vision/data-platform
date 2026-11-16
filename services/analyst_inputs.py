"""
What the analyst is shown: the morning's facts about each player, gathered
once (the Live rankings plan, step 4).

Three things are built here, from the morning the live projection was built on
and from the stored news:

- each player's `Subject` for the screen (`services.analyst_screen`): what
  changed for him since his last read;
- the **packet** one read is sent (`POST /v1/internal/jobs/analyst/read` on
  the backend): what the projection assumes about him, his last games, the
  feed's verdict, his team's rotation, the stored items about him with a `ref`
  each, the notes he carries and the bounds a note is held to;
- the packet's **fingerprint**: a hash of the facts a read turns on (his
  newest items, the feed's verdict, whether the projection has him out, the
  notes he carries, which teammates are out). A player whose fingerprint is
  the one his last completed read saw is not read again, so a rule that keeps
  firing (a rank gap lasts for weeks) costs one read, not one a morning.

An item is citable when a note may rest on it: a news item of kind injury,
availability, lineup or transaction, or a report from the availability log.
Never a rumor or a performance recap.

`gather` is the one trip to the database; everything else is pure.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from typing import Mapping, Optional, Sequence

import pytz
from peewee import fn

from db.models.nba import (
    AnalystRead,
    AvailabilityReport,
    DailyRanking,
    Game,
    NewsItem,
    NewsItemPlayer,
    Player,
    PlayerAvailability,
    ProjectionAdjustment,
)
from db.models.nba.analyst_reads import SETTLED
from db.models.nba.daily_rankings import BOARD_POINTS, SOURCE_CV, SOURCE_ESPN
from db.models.nba.projection_adjustments import SCOPE_ANALYST, SCOPE_SEASON
from services.analyst_screen import (
    PROMINENT_MINUTES,
    Candidate,
    StatusChange,
    Subject,
    TeammateChange,
)
from services.live_notes import IN_FORCE, REVIEW_DUE, Standing, standing
from services.live_projection_inputs import LiveInputs, note_of
from services.live_projection_model import Availability, BoxLine, LiveProjection, Member, Morning

EASTERN = pytz.timezone("US/Eastern")

# How far back a read is shown a player's items, and how many
LOOKBACK_DAYS = 14
MAX_NEWS = 12
MAX_REPORTS = 6
TEXT_CHARS = 700
# With no earlier read, what counts as new
NEW_DAYS = 2
# The feed's verdict that counts as today's, and how far back the one before it is looked for
VERDICT_FRESH_DAYS = 2
VERDICT_BEFORE_DAYS = 10
RECENT_GAMES = 8
RECENT_MINUTES_GAMES = 5
MAX_TEAMMATES = 12
MAX_RUNS = 8

# The bounds a note is held to (services.analyst_notes checks them)
REVIEW_BY_DAYS = 21
MINUTES_CEILING = 38.0
ROLE_SLACK = 4.0
USAGE_MIN = 0.6
USAGE_MAX = 1.4

CITABLE_KINDS = frozenset({"injury", "availability", "lineup", "transaction"})
# The box score's own reports are facts the projection already reads, not news
REPORT_SOURCES = ("nba_report", "espn_injuries", "espn_fantasy", "news_claim")
OUT_VERDICTS = frozenset({"out", "doubtful"})


def aware(value: Optional[datetime]) -> Optional[datetime]:
    """A stored timestamp as UTC, whichever way the column gave it back."""
    if value is None or value.tzinfo is not None:
        return value
    return value.replace(tzinfo=timezone.utc)


def eastern_label(value: Optional[datetime]) -> Optional[str]:
    return aware(value).astimezone(EASTERN).strftime("%Y-%m-%d %H:%M") if value is not None else None


def _clip(text: str, limit: int = TEXT_CHARS) -> str:
    text = " ".join((text or "").split())
    return text if len(text) <= limit else text[: limit - 1].rstrip() + "…"


@dataclass(frozen=True)
class Item:
    """A stored item about a player, as a read is shown it."""

    ref: str                            # news:<id> or report:<id>
    kind: str
    citable: bool
    source: str
    at: Optional[datetime]              # when the source published it
    seen: Optional[datetime]            # when it was stored here
    text: str
    evidence: dict = field(default_factory=dict, compare=False)     # as a note keeps it

    def as_packet(self) -> dict:
        return {"ref": self.ref, "kind": self.kind, "citable": self.citable, "source": self.source,
                "at": eastern_label(self.at or self.seen), "text": self.text}


@dataclass(frozen=True)
class Feed:
    """The availability feed on one player: his newest verdict and the one before it."""

    verdict: Optional[str] = None
    game_date: Optional[date] = None
    expected_return: Optional[date] = None
    reason: Optional[str] = None
    changed_at: Optional[datetime] = None
    before: Optional[str] = None


@dataclass
class Gathered:
    """Everything the screen and the packets read beyond the projection itself."""

    season: str
    as_of: date
    now: datetime
    names: dict[int, str] = field(default_factory=dict)
    items: dict[int, list[Item]] = field(default_factory=dict)              # newest first
    feed: dict[int, Feed] = field(default_factory=dict)
    analyst_notes: dict[int, ProjectionAdjustment] = field(default_factory=dict)
    owner_notes: dict[int, ProjectionAdjustment] = field(default_factory=dict)
    last_reads: dict[int, datetime] = field(default_factory=dict)           # his newest read that came to an answer
    fingerprints: dict[int, str] = field(default_factory=dict)              # ... and what it saw
    cv_ranks: dict[int, int] = field(default_factory=dict)
    espn_ranks: dict[int, int] = field(default_factory=dict)
    season_end: Optional[date] = None

    def since(self, player_id: int) -> datetime:
        """From when a thing counts as new for him."""
        return self.last_reads.get(player_id) or self.now - timedelta(days=NEW_DAYS)


# ---- the one trip to the database -----------------------------------------------------------


def news_evidence(item_id: int, source: str, kind: str, headline: str, url: Optional[str],
                  published_at: Optional[datetime]) -> dict:
    """A news item as a note keeps it (the shape `services.projection_notes.evidence_for` writes)."""
    return {"type": "news", "id": item_id, "source": source, "kind": kind, "headline": headline, "url": url,
            "published_at": published_at.isoformat() if published_at else None}


def load_items(now: datetime) -> dict[int, list[Item]]:
    """Every player's stored items of the last `LOOKBACK_DAYS`, newest first."""
    since = now - timedelta(days=LOOKBACK_DAYS)
    out: dict[int, list[Item]] = {}

    N = NewsItem
    when = fn.COALESCE(N.published_at, N.fetched_at)
    news = (
        N.select(NewsItemPlayer.player, N.id, N.source, N.kind, N.headline, N.body, N.url, N.published_at,
                 N.fetched_at, N.content_hash)
        .join(NewsItemPlayer)
        .where(when >= since)
        .order_by(when.desc(), N.id.desc())
        .tuples()
    )
    told: dict[int, set[str]] = {}              # the same story from two sources is shown once
    counts: dict[int, int] = {}
    for pid, item_id, source, kind, headline, body, url, published, fetched, content_hash in news:
        if counts.get(pid, 0) >= MAX_NEWS:
            continue
        if content_hash:
            if content_hash in told.setdefault(pid, set()):
                continue
            told[pid].add(content_hash)
        counts[pid] = counts.get(pid, 0) + 1
        text = headline if not body else f"{headline.rstrip('. ')}. {body}"
        out.setdefault(pid, []).append(Item(
            ref=f"news:{item_id}", kind=kind, citable=kind in CITABLE_KINDS, source=source,
            at=aware(published), seen=aware(fetched), text=_clip(text),
            evidence=news_evidence(item_id, source, kind, headline, url, aware(published)),
        ))

    R = AvailabilityReport
    reports = (
        R.select(R.player, R.id, R.source, R.status, R.reason, R.detail, R.expected_return, R.game_date,
                 R.source_time, R.observed_at, R.news_item)
        .where((R.observed_at >= since) & R.source.in_(REPORT_SOURCES))
        .order_by(R.observed_at.desc(), R.id.desc())
        .tuples()
    )
    counts = {}
    for pid, report_id, source, status, reason, detail, back, game_date, source_time, observed, news_id in reports:
        if counts.get(pid, 0) >= MAX_REPORTS:
            continue
        if news_id is not None and any(i.ref == f"news:{news_id}" for i in out.get(pid, ())):
            continue                            # a claim read off a news item he is already shown
        counts[pid] = counts.get(pid, 0) + 1
        parts = [f"{status.capitalize()} for {game_date.isoformat()}"]
        if reason:
            parts.append(reason)
        if detail:
            parts.append(detail)
        if back is not None:
            parts.append(f"Expected back {back.isoformat()}")
        headline = ". ".join(p.rstrip(". ") for p in parts)
        published = aware(source_time) or aware(observed)
        out.setdefault(pid, []).append(Item(
            ref=f"report:{report_id}", kind="availability", citable=True, source=source,
            at=published, seen=aware(observed), text=_clip(headline),
            evidence={"type": "report", "id": report_id, "source": source, "kind": "availability",
                      "headline": _clip(headline, 200), "url": None,
                      "published_at": published.isoformat() if published else None},
        ))

    for items in out.values():
        items.sort(key=lambda i: (i.at or i.seen or since), reverse=True)
    return out


def status_class(verdict: Optional[str]) -> str:
    """Out, a question, or in: a move between these is a change of status."""
    if verdict in OUT_VERDICTS:
        return "out"
    return "questionable" if verdict == "questionable" else "in"


def load_feed(as_of: date) -> dict[int, Feed]:
    """Each player's newest verdict of the last two days, and the verdict he had before it."""
    A = PlayerAvailability
    rows = (
        A.select(A.player, A.game_date, A.verdict, A.expected_return, A.reason, A.previous_verdict, A.changed_at)
        .where((A.game_date <= as_of) & (A.game_date >= as_of - timedelta(days=VERDICT_BEFORE_DAYS)))
        .order_by(A.player, A.game_date.desc())
        .tuples()
    )
    by_player: dict[int, list[tuple]] = {}
    for row in rows:
        by_player.setdefault(row[0], []).append(row[1:])
    out: dict[int, Feed] = {}
    for pid, history in by_player.items():
        game_date, verdict, back, reason, previous, changed_at = history[0]
        if game_date < as_of - timedelta(days=VERDICT_FRESH_DAYS):
            continue
        if previous is not None and status_class(previous) != status_class(verdict):
            before = previous                   # it moved during the day of that game
        else:
            before = history[1][1] if len(history) > 1 else None
        out[pid] = Feed(verdict=verdict, game_date=game_date, expected_return=back, reason=reason,
                        changed_at=aware(changed_at), before=before)
    return out


def load_last_reads(season: str) -> tuple[dict[int, datetime], dict[int, str]]:
    """Each player's newest read that came to an answer: when, and the fingerprint it saw."""
    R = AnalystRead
    when: dict[int, datetime] = {}
    prints: dict[int, str] = {}
    rows = (
        R.select(R.player, R.created_at, R.fingerprint)
        .where((R.season == season) & R.outcome.in_(SETTLED))
        .order_by(R.created_at.desc(), R.id.desc())
        .tuples()
    )
    for pid, created_at, fingerprint_ in rows:
        if pid not in when:
            when[pid] = aware(created_at)
            prints[pid] = fingerprint_
    return when, prints


def load_ranks(season: str, as_of: date) -> tuple[dict[int, int], dict[int, int]]:
    """The points board's ranks: ours this morning, and ESPN's newest."""
    cv = DailyRanking.ranks_on(as_of, SOURCE_CV, BOARD_POINTS)
    espn_day = (
        DailyRanking.select(fn.MAX(DailyRanking.as_of_date))
        .where((DailyRanking.source == SOURCE_ESPN) & (DailyRanking.season == season)
               & (DailyRanking.as_of_date <= as_of))
        .scalar()
    )
    espn = DailyRanking.ranks_on(espn_day, SOURCE_ESPN, BOARD_POINTS) if espn_day is not None else {}
    return cv, espn


def gather(inputs: LiveInputs, now: Optional[datetime] = None) -> Gathered:
    """Everything a morning of reads needs beyond the projection's own inputs."""
    # Imported here: the pipelines package imports this module on its way up
    from pipelines.readiness import REGULAR_SEASON_PREFIX

    now = aware(now) or datetime.now(timezone.utc)
    season, as_of = inputs.season, inputs.as_of
    last_reads, fingerprints = load_last_reads(season)
    cv_ranks, espn_ranks = load_ranks(season, as_of)
    season_end = (
        Game.select(fn.MAX(Game.game_date))
        .where((Game.season == season) & Game.game_id.startswith(REGULAR_SEASON_PREFIX))
        .scalar()
    )
    return Gathered(
        season=season, as_of=as_of, now=now,
        names=dict(Player.select(Player.id, Player.name).tuples()),
        items=load_items(now),
        feed=load_feed(as_of),
        analyst_notes={r.player_id: r for r in ProjectionAdjustment.active_for(season, SCOPE_ANALYST)},
        owner_notes={r.player_id: r for r in ProjectionAdjustment.active_for(season, SCOPE_SEASON)},
        last_reads=last_reads, fingerprints=fingerprints,
        cv_ranks=cv_ranks, espn_ranks=espn_ranks, season_end=season_end,
    )


# ---- what the projection says about a player ------------------------------------------------


def model_out(known: Optional[Availability]) -> bool:
    """Whether the projection has him out this morning on what it was told (the feed, a note)."""
    return bool(known and (known.out_next or known.out_now or known.out_for_season))


def member_out(member: Member, known: Mapping[int, Availability]) -> bool:
    return member.streak > 0 or model_out(known.get(member.player_id))


def recent_minutes(minutes: Sequence[float], games: int = RECENT_MINUTES_GAMES) -> tuple[Optional[float], int]:
    """His minutes a game over his last games played, and how many that was."""
    last = list(minutes[-games:])
    return (round(sum(last) / len(last), 1), len(last)) if last else (None, 0)


def note_standing(record: ProjectionAdjustment, as_of: date, last_played: Mapping[int, datetime]) -> Standing:
    return standing(note_of(record), as_of, last_played)


def note_due(record: ProjectionAdjustment, found: Standing, as_of: date) -> Optional[str]:
    """Why the analyst's own note should be read again this morning, or None."""
    if found.state == REVIEW_DUE:
        return f"His note passed its review date, {record.review_date.isoformat()}, and is no longer applied"
    if found.reasons:
        ended = "has ended" if found.state != IN_FORCE else "has ended in part"
        return f"His note {ended}: {'; '.join(found.reasons)}"
    if record.review_date <= as_of:
        return "His note is due to be read again today"
    return None


def own_state(record: Optional[ProjectionAdjustment], as_of: date,
              last_played: Mapping[int, datetime]) -> Optional[list]:
    """The analyst's own note as the fingerprint reads it: how it stands, not which row it is."""
    if record is None:
        return None
    found = note_standing(record, as_of, last_played)
    return [found.state, found.games, found.role, note_due(record, found, as_of) is not None]


def subjects(
    gathered: Gathered,
    inputs: LiveInputs,
    morning: Morning,
    live: Mapping[int, LiveProjection],
) -> list[Subject]:
    """Every projected player as the screen sees him this morning."""
    recent_cut = gathered.now - timedelta(days=NEW_DAYS)

    def feed_change(pid: int, since: datetime) -> Optional[StatusChange]:
        feed = gathered.feed.get(pid)
        if feed is None or feed.changed_at is None or feed.changed_at < since:
            return None
        if status_class(feed.before) == status_class(feed.verdict):
            return None
        return StatusChange(was=feed.before, now=feed.verdict)

    # Each team's prominent players whose status moved in the last two days
    moved: dict[str, list[tuple[int, TeammateChange]]] = {}
    for team, members in morning.rosters.items():
        for member in members:
            if member.established < PROMINENT_MINUTES:
                continue
            change = feed_change(member.player_id, recent_cut)
            if change is None or "out" not in (status_class(change.was), status_class(change.now)):
                continue
            moved.setdefault(team, []).append((member.player_id, TeammateChange(
                name=gathered.names.get(member.player_id, f"player {member.player_id}"),
                minutes=round(member.established, 1), out=status_class(change.now) == "out",
            )))

    out: list[Subject] = []
    for pid, state in morning.players.items():
        projection = live.get(pid)
        if projection is None:
            continue
        items = gathered.items.get(pid, [])
        since = gathered.since(pid)
        note = gathered.analyst_notes.get(pid)
        due = None
        if note is not None:
            due = note_due(note, note_standing(note, gathered.as_of, inputs.last_played), gathered.as_of)
        minutes, games = recent_minutes(state.minutes)
        out.append(Subject(
            player_id=pid,
            name=gathered.names.get(pid, f"player {pid}"),
            minutes_usual=round(state.established, 1),
            out_now=model_out(inputs.known.get(pid)) or state.streak > 0,
            citable_items=sum(1 for i in items if i.citable),
            new_items=tuple(i.kind for i in items if i.citable and i.seen is not None and i.seen >= since),
            status_change=feed_change(pid, since),
            teammate_changes=tuple(change for other, change in moved.get(state.team or "", ()) if other != pid),
            recent_minutes=minutes, recent_games=games,
            projected_minutes=round(projection.minutes, 1),
            cv_rank=gathered.cv_ranks.get(pid), espn_rank=gathered.espn_ranks.get(pid),
            has_note=note is not None, note_due=due,
        ))
    return out


# ---- the packet one read is sent -------------------------------------------------------------


def note_packet(record: ProjectionAdjustment, found: Standing, names: Mapping[int, str]) -> dict:
    """A live note as a read is shown it."""
    teammate = record.depends_on_player_id
    cites = [f"{e.get('type', 'news')}:{e.get('id')}" for e in (record.evidence or [])
             if isinstance(e, dict) and e.get("id") is not None]
    out = {
        "written": aware(record.created_at).astimezone(EASTERN).date().isoformat(),
        "kind": record.kind,
        "return_date": record.return_date.isoformat() if record.return_date else None,
        "return_earliest": record.return_earliest.isoformat() if record.return_earliest else None,
        "return_latest": record.return_latest.isoformat() if record.return_latest else None,
        "out_for_season": record.games is not None and int(record.games) == 0,
        "minutes": float(record.minutes) if record.minutes is not None else None,
        "usage": float(record.usage) if record.usage is not None else None,
        "until_date": record.until_date.isoformat() if record.until_date else None,
        "until_player_id": teammate,
        "until_player": names.get(teammate) if teammate is not None else None,
        "confidence": record.confidence,
        "review_date": record.review_date.isoformat() if record.review_date else None,
        "reason": record.note,
        "cites": cites,
        "standing": {"state": found.state, "games_part_applies": found.games, "role_part_applies": found.role,
                     "why": list(found.reasons)},
    }
    return {k: v for k, v in out.items() if v is not None}


def bounds(as_of: date, usual: float, recent: Optional[float], vacated: float, season_end: Optional[date]) -> dict:
    """The limits a note is held to. A backup cannot be given more minutes than
    the players who are out left behind, on top of what he plays already."""
    ceiling = min(MINUTES_CEILING, max(usual, recent or 0.0) + vacated + ROLE_SLACK)
    out = {
        "minutes_max": round(max(ceiling, 0.0), 1),
        "usage_min": USAGE_MIN,
        "usage_max": USAGE_MAX,
        "review_by": (as_of + timedelta(days=REVIEW_BY_DAYS)).isoformat(),
    }
    if season_end is not None:
        out["season_end"] = season_end.isoformat()
    return out


def recent_games(lines: Sequence[BoxLine], player_id: int, limit: int = RECENT_GAMES) -> list[dict]:
    mine = sorted((l for l in lines if l.player_id == player_id), key=lambda l: l.game_date, reverse=True)
    return [
        {"date": l.game_date.isoformat(), "team": l.team, "min": round(l.minutes, 1),
         "pts": round(l.stats.get("pts", 0.0)), "reb": round(l.stats.get("reb", 0.0)),
         "ast": round(l.stats.get("ast", 0.0))}
        for l in mine[:limit]
    ]


def packet(
    candidate: Candidate,
    gathered: Gathered,
    inputs: LiveInputs,
    morning: Morning,
    live: Mapping[int, LiveProjection],
    player_lines: Optional[Sequence[BoxLine]] = None,
) -> dict:
    """The body of one read (the backend's `AnalystReadReq`)."""
    pid = candidate.player_id
    state = morning.players[pid]
    projection = live[pid]
    known = inputs.known.get(pid)
    feed = gathered.feed.get(pid)
    names = gathered.names
    usual = round(state.established, 1)
    last_five, _games = recent_minutes(state.minutes)

    mates = sorted((m for m in morning.rosters.get(state.team or "", ()) if m.player_id != pid),
                   key=lambda m: (-m.established, m.player_id))
    teammates = []
    for member in mates[:MAX_TEAMMATES]:
        info = inputs.known.get(member.player_id)
        verdict = gathered.feed.get(member.player_id)
        entry = {"player_id": member.player_id, "name": names.get(member.player_id, f"player {member.player_id}"),
                 "minutes": round(member.established, 1), "out": member_out(member, inputs.known)}
        if verdict is not None and verdict.verdict:
            entry["status"] = verdict.verdict
        if info is not None and info.return_date is not None:
            entry["expected_return"] = info.return_date.isoformat()
        teammates.append(entry)
    vacated = sum(m.established for m in mates if member_out(m, inputs.known))

    body: dict = {
        "season": gathered.season,
        "as_of": gathered.as_of.isoformat(),
        "player_id": pid,
        "name": names.get(pid, f"player {pid}"),
        "team": state.team,
        "reasons": [r.text for r in candidate.reasons],
        "projection": {
            "out_now": model_out(known),
            "return_date": known.return_date.isoformat() if (known and known.return_date) else None,
            "out_for_season": bool(known and known.out_for_season),
            "missed_in_a_row": state.streak,
            "games_played": state.n,
            "minutes_usual": usual,
            "minutes_last_five": last_five,
            "minutes": round(projection.minutes, 1),
            "games_expected": round(projection.games, 1),
            "team_games_left": len(morning.ahead.get(state.team or "", ())),
            "timeline": projection.timeline()[:MAX_RUNS],
        },
        "recent_games": recent_games(player_lines if player_lines is not None else inputs.lines, pid),
        "teammates": teammates,
        "items": [item.as_packet() for item in gathered.items.get(pid, [])],
        "bounds": bounds(gathered.as_of, usual, last_five, vacated, gathered.season_end),
    }
    if feed is not None and feed.verdict:
        body["availability"] = {
            k: v for k, v in {
                "verdict": feed.verdict, "for_game_on": feed.game_date.isoformat() if feed.game_date else None,
                "reason": feed.reason, "before": feed.before,
                "expected_return": feed.expected_return.isoformat() if feed.expected_return else None,
            }.items() if v is not None
        }
    own = gathered.analyst_notes.get(pid)
    if own is not None:
        body["own_note"] = note_packet(own, note_standing(own, gathered.as_of, inputs.last_played), names)
    owner = gathered.owner_notes.get(pid)
    if owner is not None:
        body["owner_note"] = note_packet(owner, note_standing(owner, gathered.as_of, inputs.last_played), names)
    return body


def fingerprint(body: Mapping, own: Optional[list], owner_note_id: Optional[int]) -> str:
    """A hash of the facts a read turns on.

    His newest news item and newest report (an item ageing out of the window
    is not news), the feed's verdict, whether the projection has him out and
    until when, how the analyst's own note stands (`own_state`), which note
    the owner has written, and which of his teammates are out. Not the
    morning, not the box scores, not a rank: those move every day.
    """
    refs = [item["ref"] for item in body.get("items", ())]
    availability = body.get("availability") or {}
    projection = body.get("projection") or {}
    facts = {
        "news": next((r for r in refs if r.startswith("news:")), None),
        "report": next((r for r in refs if r.startswith("report:")), None),
        "feed": [availability.get("verdict"), availability.get("expected_return")],
        "model": [projection.get("out_now"), projection.get("return_date"), projection.get("out_for_season")],
        "own": own,
        "owner": owner_note_id,
        "out": sorted(t["player_id"] for t in body.get("teammates", ()) if t.get("out")),
    }
    return hashlib.sha256(json.dumps(facts, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
