"""
Availability pipeline: who is out, doubtful or available for each game, from
every free source, as one verdict per player and date
(docs/NEWS_AND_INJURY_PIPELINES_PLAN.md § 2).

cron-runner's `availability` job asks every 2 minutes. The route decides from
the clock and the date's nba.games which sources are due (`due_sources`) and
skips the tick, with no run row, when none is:

    espn_injuries  ESPN's league-wide injury list (extractors/espn_injuries.py):
                   every 10 minutes, and every tick from 90 minutes before any
                   tip until that tip (the plan's 3 minutes, on a 2-minute tick)
    espn_fantasy   ESPN fantasy's injuryStatus for ~1,500 players, the call
                   espn_injury_status makes: every 15 minutes on a game day from
                   11 AM ET, hourly otherwise
    nba_boxscore   a game's live box score from 100 minutes before its tip until
                   the tip, every tick: its inactive list is the one final
                   source. From the tip the live-stats pipeline reads the same
                   box score every poll and hands it here (`record_boxscore`),
                   which is also how DNPs reach the post-game view.

The NBA's official report PDF joins once the spike has real files to parse.

Each poll is compared with what that source last said about the player that
date: a report row (nba.availability_reports) is written only when its status,
reason or wording changed, so a quiet poll writes nothing. ESPN's lists name only the
hurt, so a player a list stops naming gets an `available` report; that is the
only healthy row an ESPN source writes. Every player with a new report is
re-resolved (pipelines/transformers/availability.py) into his
nba.player_availability row. The writes run under one transaction-scoped
advisory lock, so this pipeline and live-stats never interleave a read of the
latest reports with the writes that depend on it.

Dark for now (build step 2): a verdict change the late-scratch reactor would
hear about (out, doubtful or available, for a game not yet tipped) is logged
and counted as skipped `reaction_not_posted`; nothing goes to the backend.

nba.player_injuries keeps its readers: each run copies the date's verdicts
there (`write_injuries_compat`, settings.availability_writes_injuries), which
gives off days and the offseason a status. The PRE_GAME espn_injury_status
pipeline is retired after the parallel week (decision 9).

Counters: records = report rows written. Skipped = an ESPN entry matched to no
NBA player, a box score not up yet, a held reaction. Failed = a source that
could not be read; the run goes on with the others.
"""

from __future__ import annotations

from collections import Counter
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta, timezone
from typing import Iterable, Iterator, Mapping, Optional, Sequence

from core.locks import NAMESPACE, lock_key
from core.nba_calendar import nba_date_et
from core.settings import settings
from db.base import db
from db.models.nba import (
    AvailabilityReport,
    Game,
    Player,
    PlayerAvailability,
    PlayerInjury,
    PlayerProfile,
)
from pipelines.base import BasePipeline
from pipelines.config import PipelineCategory, PipelineConfig
from pipelines.context import PipelineContext
from pipelines.extractors.espn import ESPNExtractor
from pipelines.extractors.espn_injuries import ESPNInjuriesExtractor, EspnInjury
from pipelines.extractors.nba_api import NBAApiExtractor
from pipelines.transformers import normalize_name
from pipelines.transformers.availability import (
    CONFIDENCE,
    ET,
    BoxPlayer,
    GameContext,
    Report,
    SOURCE_RANK,
    StoredVerdict,
    boxscore_players,
    boxscore_tipped,
    fantasy_status,
    is_reaction_event,
    next_row,
    resolve,
    should_record,
    team_day_words,
)

# ---- when each source is due ----------------------------------------------------------

# cron-runner fires `availability` at every even minute (*/2 * * * *). The slot
# rule below assumes it: change both together.
TICK_MINUTES = 2
INJURIES_EVERY = 10
INJURIES_PRE_TIP = timedelta(minutes=90)
FANTASY_GAME_DAY_EVERY = 15
FANTASY_EVERY = 60
FANTASY_GAME_DAY_FROM = time(11, 0)
# How long before its tip a game's box score is read. The spike (plan § 6.1)
# measures when the inactive list first shows; until then, the plan's 100.
BOXSCORE_PRE_TIP = timedelta(minutes=100)


def _slot_due(now_et: datetime, every: int) -> bool:
    """True on the one tick in each `every`-minute slot of the ET day.

    With a tick every TICK_MINUTES, exactly one tick lands in the first
    TICK_MINUTES of a slot (every 15: :00, :16, :30, :46). A tick the runner
    skips costs that slot's poll; the next slot polls again.
    """
    return (now_et.hour * 60 + now_et.minute) % every < TICK_MINUTES


@dataclass(frozen=True)
class SlateGame:
    game_id: str
    home: str
    away: str
    tip: Optional[datetime]  # aware, ET; None while nba.games has no start time

    def opponent(self, team: str) -> str:
        return self.away if team == self.home else self.home


@dataclass(frozen=True)
class Slate:
    """One NBA date's games, as nba.games has them."""

    game_date: date
    games: tuple[SlateGame, ...] = ()

    @classmethod
    def load(cls, game_date: date) -> "Slate":
        """Blocking DB: one read of nba.games."""
        rows = (
            Game.select(Game.game_id, Game.home_team, Game.away_team, Game.start_time_et)
            .where(Game.game_date == game_date)
            .order_by(Game.game_id)
        )
        return cls(game_date, tuple(
            SlateGame(
                game_id=g.game_id,
                home=g.home_team_id,
                away=g.away_team_id,
                tip=datetime.combine(game_date, g.start_time_et, tzinfo=ET) if g.start_time_et else None,
            )
            for g in rows
        ))

    def game_of(self, team: Optional[str]) -> Optional[SlateGame]:
        return next((g for g in self.games if team in (g.home, g.away)), None) if team else None

    def by_id(self, game_id: Optional[str]) -> Optional[SlateGame]:
        return next((g for g in self.games if g.game_id == game_id), None) if game_id else None

    def context(self, team: Optional[str], *, game_id: Optional[str] = None) -> GameContext:
        """What the resolver needs about this player's game on the date.

        `game_id` is the box score's, for a game nba.games does not have (an
        unscheduled playoff round): the player has that game, opponent unknown.
        """
        game = self.game_of(team)
        if game is None:
            return GameContext(self.game_date, has_game=game_id is not None, game_id=game_id)
        return GameContext(
            self.game_date, has_game=True, game_id=game.game_id,
            day_words=team_day_words(game.opponent(team)),
        )

    def tipped(self, game_id: Optional[str], now: datetime) -> bool:
        """Whether the game has tipped; a game we cannot place in time counts as tipped."""
        game = self.by_id(game_id)
        return game is None or game.tip is None or now >= game.tip


@dataclass(frozen=True)
class Due:
    """What one tick polls."""

    espn_injuries: bool = False
    espn_fantasy: bool = False
    boxscore_games: tuple[str, ...] = ()

    @property
    def any(self) -> bool:
        return self.espn_injuries or self.espn_fantasy or bool(self.boxscore_games)

    def options(self) -> dict:
        """As the pipeline's run options."""
        return {
            "sources": [s for s, on in (("espn_injuries", self.espn_injuries),
                                        ("espn_fantasy", self.espn_fantasy)) if on],
            "boxscore_games": list(self.boxscore_games),
        }

    @classmethod
    def from_options(cls, options: Mapping) -> Optional["Due"]:
        """The due set the route passed, or None for a run started without one."""
        if "sources" not in options:
            return None
        sources = set(options.get("sources") or ())
        return cls(
            espn_injuries="espn_injuries" in sources,
            espn_fantasy="espn_fantasy" in sources,
            boxscore_games=tuple(options.get("boxscore_games") or ()),
        )


def due_sources(now: datetime, slate: Slate) -> Due:
    """The plan's due rules (§ 2) for a tick at `now`: pure, from the clock and the slate."""
    now_et = now.astimezone(ET)
    tips = [g.tip for g in slate.games if g.tip is not None]
    pre_tip = any(tip - INJURIES_PRE_TIP <= now_et < tip for tip in tips)
    fantasy_every = (
        FANTASY_GAME_DAY_EVERY if slate.games and now_et.time() >= FANTASY_GAME_DAY_FROM else FANTASY_EVERY
    )
    return Due(
        espn_injuries=pre_tip or _slot_due(now_et, INJURIES_EVERY),
        espn_fantasy=_slot_due(now_et, fantasy_every),
        boxscore_games=tuple(
            g.game_id for g in slate.games
            if g.tip is not None and g.tip - BOXSCORE_PRE_TIP <= now_et < g.tip
        ),
    )


def poll_everything(now: datetime, slate: Slate) -> Due:
    """The dashboard's Run: every source, and the box score of every game not yet tipped."""
    now_et = now.astimezone(ET)
    return Due(
        espn_injuries=True,
        espn_fantasy=True,
        boxscore_games=tuple(g.game_id for g in slate.games if g.tip is None or now_et < g.tip),
    )


def due_now(force: bool = False, now: Optional[datetime] = None) -> Due:
    """What this tick should poll. Blocking DB (one read of nba.games)."""
    now = now or datetime.now(timezone.utc)
    slate = Slate.load(nba_date_et(now))
    return poll_everything(now, slate) if force else due_sources(now, slate)


# ---- who is who ------------------------------------------------------------------------


@dataclass
class PlayerIndex:
    """ESPN ids and names to NBA ids, and each player's current team."""

    by_espn_id: dict[int, int] = field(default_factory=dict)
    by_name: dict[str, int] = field(default_factory=dict)
    team_of: dict[int, str] = field(default_factory=dict)

    @classmethod
    def load(cls) -> "PlayerIndex":
        """Blocking DB. Names are current players' only, and a name two of them
        share maps to neither (as in espn_injury_status)."""
        by_espn_id = {
            p.espn_id: p.id
            for p in Player.select(Player.id, Player.espn_id).where(Player.espn_id.is_null(False))
        }
        names: dict[str, Optional[int]] = {}
        for p in Player.select(Player.id, Player.name_normalized).where(Player.in_play()):
            if p.name_normalized:
                names[p.name_normalized] = None if p.name_normalized in names else p.id
        team_of = {
            row.player_id: row.team_id
            for row in PlayerProfile.select(PlayerProfile.player, PlayerProfile.team)
            .where(PlayerProfile.team.is_null(False))
        }
        return cls(by_espn_id, {n: pid for n, pid in names.items() if pid is not None}, team_of)

    def match(self, espn_id: Optional[int], name: Optional[str]) -> Optional[int]:
        """The NBA id for an ESPN player: by ESPN id, else by name."""
        if espn_id is not None and espn_id in self.by_espn_id:
            return self.by_espn_id[espn_id]
        return self.by_name.get(normalize_name(name)) if name else None


# ---- the report log --------------------------------------------------------------------

_REPORT_COLUMNS = (
    "player_id, source, status, reason, detail, game_id, expected_return, "
    "source_time, observed_at, confidence"
)
# The latest row per (player, source) on a date: what `should_record` compares with.
LATEST_REPORTS_SQL = f"""
    SELECT DISTINCT ON (player_id, source) {_REPORT_COLUMNS}
    FROM nba.availability_reports
    WHERE game_date = %s AND source = ANY(%s)
    ORDER BY player_id, source, observed_at DESC, id DESC
"""
REPORTS_FOR_SQL = f"""
    SELECT {_REPORT_COLUMNS}
    FROM nba.availability_reports
    WHERE game_date = %s AND player_id = ANY(%s)
"""
_REASON_MAX = 100  # nba.availability_reports.reason is varchar(100)


def _aware(value: Optional[datetime]) -> Optional[datetime]:
    """timestamptz comes back aware; a timestamp column (tests' tables) does not."""
    if value is None or value.tzinfo is not None:
        return value
    return value.replace(tzinfo=timezone.utc)


def _reason(text: Optional[str]) -> Optional[str]:
    return text[:_REASON_MAX] if text else None


def _stored_report(row: Sequence) -> tuple[int, Report]:
    (player_id, source, status, reason, detail, game_id, expected_return,
     source_time, observed_at, confidence) = row
    return player_id, Report(
        source=source, status=status, observed_at=_aware(observed_at),
        source_time=_aware(source_time), game_id=game_id, reason=reason, detail=detail,
        expected_return=expected_return, confidence=confidence,
    )


class LatestReports:
    """What each source last said about each player on one date, as stored."""

    def __init__(self, reports: Optional[Mapping[tuple[int, str], Report]] = None):
        self._by_key: dict[tuple[int, str], Report] = dict(reports or {})

    @classmethod
    def load(cls, game_date: date, sources: Iterable[str] = SOURCE_RANK) -> "LatestReports":
        """Blocking DB."""
        rows = db.execute_sql(LATEST_REPORTS_SQL, (game_date, list(sources))).fetchall()
        return cls({(pid, report.source): report for pid, report in map(_stored_report, rows)})

    def get(self, player_id: int, source: str) -> Optional[Report]:
        return self._by_key.get((player_id, source))

    def put(self, player_id: int, report: Report) -> None:
        self._by_key[(player_id, report.source)] = report

    def of(self, source: str) -> dict[int, Report]:
        return {pid: report for (pid, s), report in self._by_key.items() if s == source}


@dataclass(frozen=True)
class Observation:
    """One source's word on one player, before the log decides whether it is news."""

    player_id: int
    report: Report
    team: Optional[str] = None  # NBA tricode, when the source says (the box score, ESPN's list)


# ESPN leaves a team with nobody hurt off its list, so a player whose whole team
# is missing may only have been dropped with a short list. From this many teams
# up, the list is taken as whole and anyone it stops naming is healthy.
WHOLE_LIST_TEAMS = 20


def espn_injury_observations(
    entries: Sequence[EspnInjury],
    latest: Mapping[int, Report],
    index: PlayerIndex,
    now: datetime,
    skips: Counter,
) -> list[Observation]:
    """ESPN's list as observations: one per listed player, and an `available` one
    for each player it named earlier today and no longer does."""
    observations: list[Observation] = []
    listed: set[int] = set()
    for entry in entries:
        player_id = index.match(entry.espn_id, entry.name)
        if player_id is None:
            skips["unmatched_player"] += 1
            continue
        if player_id in listed:
            continue
        listed.add(player_id)
        observations.append(Observation(player_id, Report(
            source="espn_injuries",
            status=entry.status,
            observed_at=now,
            source_time=entry.source_time,
            reason=_reason(entry.reason),
            detail=entry.detail,
            expected_return=entry.expected_return,
        ), team=entry.team))

    teams_listed = {entry.team for entry in entries if entry.team}
    whole_list = len(teams_listed) >= WHOLE_LIST_TEAMS
    for player_id, report in latest.items():
        if player_id in listed or report.status == "available":
            continue
        team = index.team_of.get(player_id)
        if not whole_list and team not in teams_listed:
            skips["espn_injuries_team_missing"] += 1
            continue
        observations.append(Observation(
            player_id, Report(source="espn_injuries", status="available", observed_at=now), team=team,
        ))
    return observations


def fantasy_observations(
    players: Mapping[str, dict],
    latest: Mapping[int, Report],
    index: PlayerIndex,
    now: datetime,
    skips: Counter,
) -> list[Observation]:
    """ESPN fantasy's statuses (ESPNExtractor.get_player_data: normalized name ->
    espn_id, injured, injury_status) as observations. Healthy is the default, so a
    healthy player is observed only when he was not healthy earlier today."""
    observations: list[Observation] = []
    seen: set[int] = set()
    for name, info in players.items():
        raw = info.get("injury_status")
        status = fantasy_status(bool(info.get("injured")), raw)
        if status is None:
            skips["espn_fantasy_unknown_status"] += 1
            continue
        player_id = index.match(info.get("espn_id"), name)
        if player_id is None:
            if status != "available":
                skips["unmatched_player"] += 1
            continue
        if player_id in seen:
            continue
        seen.add(player_id)
        if status == "available":
            earlier = latest.get(player_id)
            if earlier is None or earlier.status == "available":
                continue
        observations.append(Observation(player_id, Report(
            source="espn_fantasy",
            status=status,
            observed_at=now,
            reason="Suspension" if str(raw or "").upper() == "SUSPENSION" else None,
        )))
    return observations


def boxscore_observations(
    game_id: str,
    players: Sequence[BoxPlayer],
    tipped: bool,
    latest: Mapping[int, Report],
    now: datetime,
) -> list[Observation]:
    """A box score's list as observations.

    INACTIVE is news at any time. Before the tip an ACTIVE may only mean the
    inactive list is not in yet (the spike says when it is), so it is observed
    only to take back an INACTIVE; from the tip, ACTIVE is final.
    """
    observations: list[Observation] = []
    for p in players:
        if p.status == "active" and not tipped:
            earlier = latest.get(p.player_id)
            if earlier is None or earlier.status != "inactive":
                continue
        observations.append(Observation(p.player_id, Report(
            source="nba_boxscore",
            status=p.status,
            observed_at=now,
            game_id=game_id,
            reason=_reason(p.reason),
            detail=p.detail,
        ), team=p.team))
    return observations


# ---- writing ---------------------------------------------------------------------------

WRITE_LOCK = "availability_write"


@contextmanager
def write_transaction() -> Iterator[None]:
    """One transaction holding the availability write lock until it commits.

    This pipeline and live-stats both write the log and the verdicts; a read of
    the latest reports and the writes decided from it must not interleave with
    the other's. Transaction-scoped, so the lock can never ride a pooled
    connection past its transaction.
    """
    with db.atomic():
        db.execute_sql("SELECT pg_advisory_xact_lock(%s, %s)", (NAMESPACE, lock_key(WRITE_LOCK)))
        yield


@dataclass(frozen=True)
class VerdictChange:
    """A verdict row this write produced, and whether its verdict moved."""

    player_id: int
    verdict: str
    previous_verdict: Optional[str]
    decided_by: str
    game_id: Optional[str]
    changed: bool


@dataclass
class WriteResult:
    reports: int = 0                 # report rows written
    unknown_players: int = 0         # observations for an id nba.players does not have
    verdicts: list[VerdictChange] = field(default_factory=list)

    @property
    def changes(self) -> list[VerdictChange]:
        return [v for v in self.verdicts if v.changed]


_VERDICT_FIELDS = (
    "game_id", "verdict", "final", "decided_by", "reason", "detail", "expected_return",
    "confidence", "previous_verdict", "changed_at", "updated_at",
)


def write_observations(
    game_date: date,
    observations: Sequence[Observation],
    *,
    latest: LatestReports,
    slate: Slate,
    now: datetime,
    run_id,
    team_of: Optional[Mapping[int, str]] = None,
) -> WriteResult:
    """Append the observations that are news, then re-resolve every player they
    touched. Call inside `write_transaction()`, with `latest` read inside it."""
    result = WriteResult()
    known = _known_players({o.player_id for o in observations})
    teams: dict[int, str] = dict(team_of or {})
    rows: list[dict] = []
    for o in observations:
        if o.player_id not in known:
            result.unknown_players += 1
            continue
        if o.team:
            teams[o.player_id] = o.team
        if not should_record(latest.get(o.player_id, o.report.source), o.report):
            continue
        latest.put(o.player_id, o.report)
        rows.append(_report_row(o, game_date, run_id))
    if rows:
        AvailabilityReport.insert_many(rows).execute()
    result.reports = len(rows)
    touched = {row["player"] for row in rows}
    if touched:
        result.verdicts = _resolve_verdicts(game_date, touched, teams=teams, slate=slate, now=now)
    return result


def _known_players(player_ids: set[int]) -> set[int]:
    if not player_ids:
        return set()
    return {p.id for p in Player.select(Player.id).where(Player.id.in_(sorted(player_ids)))}


def _report_row(o: Observation, game_date: date, run_id) -> dict:
    r = o.report
    return {
        "player": o.player_id,
        "game_id": r.game_id,
        "game_date": game_date,
        "source": r.source,
        "status": r.status,
        "reason": r.reason,
        "detail": r.detail,
        "expected_return": r.expected_return,
        "source_time": r.source_time,
        "observed_at": r.observed_at,
        "confidence": r.confidence if r.confidence is not None else CONFIDENCE[r.source],
        "pipeline_run_id": run_id,
    }


def _resolve_verdicts(
    game_date: date,
    player_ids: set[int],
    *,
    teams: Mapping[int, str],
    slate: Slate,
    now: datetime,
) -> list[VerdictChange]:
    ids = sorted(player_ids)
    reports: dict[int, list[Report]] = {}
    for pid, report in map(_stored_report, db.execute_sql(REPORTS_FOR_SQL, (game_date, ids)).fetchall()):
        reports.setdefault(pid, []).append(report)
    current = {
        row.player_id: StoredVerdict(row.verdict, row.previous_verdict, _aware(row.changed_at))
        for row in PlayerAvailability.select(
            PlayerAvailability.player, PlayerAvailability.verdict,
            PlayerAvailability.previous_verdict, PlayerAvailability.changed_at,
        ).where((PlayerAvailability.game_date == game_date) & PlayerAvailability.player.in_(ids))
    }
    previous = _previous_game_verdicts(game_date, [pid for pid in ids if pid not in current], teams)

    rows: list[dict] = []
    out: list[VerdictChange] = []
    for pid in ids:
        mine = reports.get(pid, [])
        box_game = next((r.game_id for r in mine if r.source == "nba_boxscore" and r.game_id), None)
        verdict = resolve(mine, slate.context(teams.get(pid), game_id=box_game))
        if verdict is None:
            continue
        before, before_at = previous.get(pid, (None, None))
        row, changed = next_row(
            current.get(pid), verdict, now,
            verdict_at_previous_game=before, previous_game_changed_at=before_at,
        )
        rows.append({"player": pid, "game_date": game_date, **row})
        out.append(VerdictChange(pid, verdict.verdict, row["previous_verdict"], verdict.decided_by,
                                 verdict.game_id, changed))
    if rows:
        (PlayerAvailability.insert_many(rows)
         .on_conflict(
             conflict_target=[PlayerAvailability.player, PlayerAvailability.game_date],
             preserve=[getattr(PlayerAvailability, name) for name in _VERDICT_FIELDS],
         )
         .execute())
    return out


# Each team's last game date before `day`, within the lookback.
PREVIOUS_GAME_DATES_SQL = """
    SELECT team, MAX(game_date) FROM (
        SELECT home_team_id AS team, game_date FROM nba.games
        WHERE game_date < %(day)s AND game_date >= %(since)s AND home_team_id = ANY(%(teams)s)
        UNION ALL
        SELECT away_team_id AS team, game_date FROM nba.games
        WHERE game_date < %(day)s AND game_date >= %(since)s AND away_team_id = ANY(%(teams)s)
    ) played
    GROUP BY team
"""
PREVIOUS_GAME_LOOKBACK = timedelta(days=365)


def _previous_game_verdicts(
    game_date: date, player_ids: Sequence[int], teams: Mapping[int, str],
) -> dict[int, tuple[str, Optional[datetime]]]:
    """Each player's verdict at his team's previous game and when it last moved;
    ("available", None) when he had no row there. Missing for a player whose team
    is unknown or played no game in the lookback."""
    wanted = {pid: teams[pid] for pid in player_ids if teams.get(pid)}
    if not wanted:
        return {}
    last = dict(db.execute_sql(PREVIOUS_GAME_DATES_SQL, {
        "day": game_date,
        "since": game_date - PREVIOUS_GAME_LOOKBACK,
        "teams": sorted(set(wanted.values())),
    }).fetchall())
    on = {pid: last[team] for pid, team in wanted.items() if team in last}
    if not on:
        return {}
    stored = {
        (row.player_id, row.game_date): (row.verdict, _aware(row.changed_at))
        for row in PlayerAvailability.select(
            PlayerAvailability.player, PlayerAvailability.game_date,
            PlayerAvailability.verdict, PlayerAvailability.changed_at,
        ).where(PlayerAvailability.player.in_(sorted(on)) & PlayerAvailability.game_date.in_(sorted(set(on.values()))))
    }
    return {pid: stored.get((pid, day), ("available", None)) for pid, day in on.items()}


def reaction_events(verdicts: Iterable[VerdictChange], slate: Slate, now: datetime) -> list[VerdictChange]:
    """The verdict changes the late-scratch reactor would hear about: out, doubtful
    or available, for a game that has not tipped (plan § 2, Events)."""
    return [
        v for v in verdicts
        if is_reaction_event(v.changed, v.verdict) and v.game_id and not slate.tipped(v.game_id, now)
    ]


def record_boxscore(
    game_date: date,
    games: Sequence[tuple[str, Mapping, bool]],
    *,
    now: datetime,
    run_id,
) -> WriteResult:
    """The live-stats pipeline's hand-off: each (game_id, box score, tipped) it read
    this poll, as nba_boxscore reports. Blocking DB; a quiet poll is three reads."""
    slate = Slate.load(game_date)
    with write_transaction():
        latest = LatestReports.load(game_date, ("nba_boxscore",))
        box_latest = latest.of("nba_boxscore")
        observations: list[Observation] = []
        for game_id, game, tipped in games:
            observations += boxscore_observations(
                game_id, boxscore_players(game), tipped or boxscore_tipped(game), box_latest, now,
            )
        return write_observations(game_date, observations, latest=latest, slate=slate, now=now, run_id=run_id)


# ---- nba.player_injuries ---------------------------------------------------------------

INJURY_LABELS: Mapping[str, str] = {
    "out": "Out",
    "doubtful": "Doubtful",
    "questionable": "Questionable",
    "probable": "Probable",
    "available": "Available",
}
# The backend reads a player's latest record from the last 7 days; an older one
# is not his status anyway.
INJURY_LOOKBACK = timedelta(days=8)
HURT_ON_RECORD_SQL = """
    SELECT DISTINCT ON (player_id) player_id, status
    FROM nba.player_injuries
    WHERE report_date <= %s AND report_date > %s
    ORDER BY player_id, report_date DESC
"""
SOURCES_REPORTED_SQL = "SELECT DISTINCT source FROM nba.availability_reports WHERE game_date = %s"


def write_injuries_compat(game_date: date, run_id) -> int:
    """Today's verdicts as nba.player_injuries rows (report_date = the NBA date),
    for that table's readers. Writes only rows that differ; returns how many.

    Hurt players get their verdict. A player whose latest record says hurt is
    cleared with `Available` once today says he is healthy: an `available`
    verdict, or no verdict at all after both ESPN feeds have reported today (so
    one feed failing never clears a player the other has out).
    """
    verdicts = {
        row.player_id: row
        for row in PlayerAvailability.select().where(PlayerAvailability.game_date == game_date)
    }
    wanted: dict[int, tuple] = {}
    for pid, v in verdicts.items():
        label = INJURY_LABELS.get(v.verdict)
        if label is None or label == "Available":
            continue
        box = v.decided_by == "nba_boxscore"  # its reason is a code (INACTIVE_INJURY), not an injury
        wanted[pid] = (label, None if box else v.reason, v.detail, v.expected_return)

    on_record = dict(db.execute_sql(HURT_ON_RECORD_SQL, (game_date, game_date - INJURY_LOOKBACK)).fetchall())
    hurt_on_record = {pid for pid, status in on_record.items() if status != "Available"}
    both_feeds = {"espn_injuries", "espn_fantasy"} <= {
        row[0] for row in db.execute_sql(SOURCES_REPORTED_SQL, (game_date,)).fetchall()
    }
    for pid in hurt_on_record - set(wanted):
        verdict = verdicts.get(pid)
        if (verdict is not None and verdict.verdict == "available") or (verdict is None and both_feeds):
            wanted[pid] = ("Available", None, None, None)

    today = {
        row.player_id: (row.status, row.injury_type, row.injury_detail, row.expected_return)
        for row in PlayerInjury.select().where(PlayerInjury.report_date == game_date)
    }
    written = 0
    for pid, values in sorted(wanted.items()):
        if today.get(pid) == values:
            continue
        status, injury_type, injury_detail, expected_return = values
        PlayerInjury.upsert_injury(
            player_id=pid, report_date=game_date, status=status, injury_type=injury_type,
            injury_detail=injury_detail, expected_return=expected_return, pipeline_run_id=run_id,
        )
        written += 1
    return written


# ---- the pipeline ----------------------------------------------------------------------


class AvailabilityPipeline(BasePipeline):
    """
    One availability tick: read the due sources, log what changed, re-resolve
    the verdicts it touched, hold the reactions (dark), refresh player_injuries.
    """

    config = PipelineConfig(
        name="availability",
        display_name="Player Availability",
        description="Who is out, doubtful or available for each game, from ESPN's injury list, "
                    "ESPN fantasy and the NBA box score",
        target_table="nba.player_availability",
        category=PipelineCategory.SCHEDULED,
        trigger_slug="availability",
        cron_job="availability",
        # The route skips a tick with no source due; the dashboard's Run polls them all.
        force_on_run=True,
        timeout_seconds=120,
    )

    def __init__(self):
        super().__init__()
        self.injuries_extractor = ESPNInjuriesExtractor()
        self.espn_extractor = ESPNExtractor()
        self.nba_extractor = NBAApiExtractor()

    def execute(self, ctx: PipelineContext) -> None:
        now = datetime.now(timezone.utc)
        game_date = ctx.game_date()
        slate = Slate.load(game_date)
        due = Due.from_options(ctx.options) or poll_everything(now, slate)
        ctx.log.info(
            "availability_due",
            game_date=str(game_date),
            espn_injuries=due.espn_injuries,
            espn_fantasy=due.espn_fantasy,
            boxscore_games=list(due.boxscore_games),
        )

        # Network first, outside the write transaction.
        injuries = self._read(ctx, "espn_injuries", self.injuries_extractor.extract) if due.espn_injuries else None
        fantasy = self._read(ctx, "espn_fantasy", self.espn_extractor.get_player_data) if due.espn_fantasy else None
        boxes: list[tuple[str, dict]] = []
        for game_id in due.boxscore_games:
            game = self._read(ctx, "nba_boxscore", self.nba_extractor.get_live_box_score, game_id)
            if game is None:
                continue
            if not game:
                ctx.increment_skipped(1, "boxscore_not_up")
                continue
            boxes.append((game_id, game))
        index = PlayerIndex.load() if injuries is not None or fantasy is not None else PlayerIndex()

        skips: Counter = Counter()
        with write_transaction():
            latest = LatestReports.load(game_date)
            observations: list[Observation] = []
            if injuries is not None:
                entries, unparsed = injuries
                skips["espn_injuries_unparsed"] += unparsed
                observations += espn_injury_observations(entries, latest.of("espn_injuries"), index, now, skips)
            if fantasy is not None:
                observations += fantasy_observations(fantasy, latest.of("espn_fantasy"), index, now, skips)
            box_latest = latest.of("nba_boxscore")
            for game_id, game in boxes:
                # Last, so the box score's team wins for the players it lists.
                observations += boxscore_observations(
                    game_id, boxscore_players(game), boxscore_tipped(game), box_latest, now,
                )
            result = write_observations(
                game_date, observations, latest=latest, slate=slate, now=now,
                run_id=ctx.run_id, team_of=index.team_of,
            )

        ctx.increment_records(result.reports)
        skips["unknown_player"] += result.unknown_players
        for reason, count in sorted(skips.items()):
            if count:
                ctx.increment_skipped(count, reason)

        held = reaction_events(result.verdicts, slate, now)
        for event in held:
            ctx.log.info(
                "availability_reaction_held",
                player_id=event.player_id,
                verdict=event.verdict,
                previous_verdict=event.previous_verdict,
                decided_by=event.decided_by,
                game_id=event.game_id,
            )
        if held:
            ctx.increment_skipped(len(held), "reaction_not_posted")

        injuries_written = (
            write_injuries_compat(game_date, ctx.run_id) if settings.availability_writes_injuries else 0
        )
        ctx.log.info(
            "availability_complete",
            game_date=str(game_date),
            observations=len(observations),
            reports_written=result.reports,
            verdicts_written=len(result.verdicts),
            verdicts_changed=len(result.changes),
            reactions_held=len(held),
            injuries_written=injuries_written,
        )

    @staticmethod
    def _read(ctx: PipelineContext, source: str, fetch, *args):
        """One source's fetch; a failure is counted and the run goes on without it."""
        try:
            data = fetch(*args)
        except Exception as exc:
            ctx.increment_failed(1, f"{source}_unavailable")
            ctx.log.warning(
                "availability_source_failed", source=source, args=[str(a) for a in args],
                error=f"{type(exc).__name__}: {exc}",
            )
            return None
        if data is None and source == "nba_boxscore":
            return {}  # not up yet: not a failure
        return data
