"""
What the live projection is built from, loaded once (services.live_projection_model).

- **box scores**: this season's regular-season lines before the morning
  (nba.player_game_stats);
- **the schedule**: every team's regular-season games from the morning on
  (nba.games);
- **the preseason projection**: the latest source='cv' snapshot, ESPN's where
  Court Vision has none (nba.player_projections). The live projection is
  written to the same table under its own source and never reads itself;
- **where a player is now**: the latest player-profiles run, for a projected
  player who has not played yet and for one traded since his last game;
- **ages**: last season's nba.player_history row plus a year, the birthdate
  where there is none;
- **what is known about an absence**: the availability pipeline's newest verdict
  for each player (nba.player_availability: ruled out, and ESPN's expected
  return date when it gives one), and the curated return dates and seasons
  written off in nba.projection_adjustments;
- **the season's notes**: each player's live `scope='season'` row of
  nba.projection_adjustments, and how it stands this morning
  (services.live_notes). The games part of a note in force is believed over
  the feed's return date; its role part goes to the model as it is.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field, replace
from datetime import date, datetime, time, timedelta
from functools import lru_cache
from pathlib import Path
from typing import Iterable, Mapping, Optional

import pytz
from peewee import fn

from db.models.nba import (
    Game,
    PlayerAvailability,
    PlayerGameStats,
    PlayerHistory,
    PlayerProfile,
    PlayerProjection,
    ProjectionAdjustment,
)
from db.models.nba.projection_adjustments import SCOPE_SEASON
from services.live_notes import Note, RoleNote, Standing, role_note, standing
from services.live_projection_model import LINE_KEYS, Availability, BoxLine, Coefficients, Prior
from services.projection_inputs import PROFILE_RUN_SLACK
from services.projection_model import season_label, season_start

COEFFICIENTS_PATH = Path(__file__).resolve().parent.parent / "static" / "live_projection_coefficients.json"
PRIOR_SOURCES = ("cv", "espn")              # the preseason projection, in order of preference
# A verdict older than this says nothing about today
VERDICT_FRESH = timedelta(days=2)
EASTERN = pytz.timezone("US/Eastern")
# When a game with no start time on the schedule is taken to have tipped, ET
DEFAULT_TIP = time(19, 0)


@lru_cache(maxsize=1)
def load_coefficients(path: str = str(COEFFICIENTS_PATH)) -> Coefficients:
    return Coefficients.from_json(json.loads(Path(path).read_text()))


@dataclass
class LiveInputs:
    """Everything `services.live_projection_model` needs for one morning."""

    season: str
    as_of: date
    lines: list[BoxLine]
    ahead: dict[str, list[date]]
    priors: dict[int, Prior]
    prior_source: Optional[str]
    prior_as_of: Optional[date]
    teams: dict[int, str]
    ages: dict[int, float]
    known: dict[int, Availability] = field(default_factory=dict)
    # Every live season note, how each stands this morning, and the role part of those in force
    notes: dict[int, Note] = field(default_factory=dict)
    standings: dict[int, Standing] = field(default_factory=dict)
    role_notes: dict[int, RoleNote] = field(default_factory=dict)
    # `known` before the notes were laid over it, and each player's last tip: what
    # the editor needs to try a note on (services.projection_notes)
    known_before_notes: dict[int, Availability] = field(default_factory=dict)
    last_played: dict[int, datetime] = field(default_factory=dict)

    def with_note(self, player_id: int, note: Optional[Note]) -> "LiveInputs":
        """These inputs with one player's season note replaced by `note` (None: with none)."""
        notes = {pid: n for pid, n in self.notes.items() if pid != player_id}
        if note is not None:
            notes[player_id] = note
        return replace(self, **noted_inputs(self.known_before_notes, notes, self.as_of, self.last_played))

    @property
    def newest_line(self) -> Optional[date]:
        return max((l.game_date for l in self.lines), default=None)


def box_lines(season: str, before: date) -> list[BoxLine]:
    G = PlayerGameStats
    columns = [getattr(G, k) for k in LINE_KEYS]
    query = (
        G.select(G.player, G.team, G.game, G.game_date, G.min, *columns)
        .where((G.season == season) & (G.season_type == "regular") & (G.game_date < before)
               & G.game.is_null(False) & G.team.is_null(False))
        .tuples()
    )
    return [
        BoxLine(player_id=pid, team=team, game_id=game_id, game_date=day, minutes=float(minutes or 0),
                stats={k: float(v or 0) for k, v in zip(LINE_KEYS, stats)})
        for pid, team, game_id, day, minutes, *stats in query
    ]


def games_ahead(season: str, from_day: date) -> dict[str, list[date]]:
    # Imported here: the pipelines package imports this module on its way up
    from pipelines.readiness import REGULAR_SEASON_PREFIX

    out: dict[str, list[date]] = {}
    query = (
        Game.select(Game.game_date, Game.home_team, Game.away_team)
        .where((Game.season == season) & (Game.game_date >= from_day)
               & Game.game_id.startswith(REGULAR_SEASON_PREFIX))
        .tuples()
    )
    for day, home, away in query:
        for team in (home, away):
            if team:
                out.setdefault(team, []).append(day)
    return {team: sorted(days) for team, days in out.items()}


def preseason_priors(season: str) -> tuple[Optional[str], Optional[date], dict[int, Prior]]:
    """The newest preseason projection: (its source, its date, each player's)."""
    for source in PRIOR_SOURCES:
        rows = PlayerProjection.latest_for_season(season, source=source)
        priors = {
            r.player_id: Prior(
                # No games is not zero games: the model then leans neither way
                minutes=float(r.min), games=float(r.projected_gp) if r.projected_gp is not None else None,
                per_game={k: float(getattr(r, k) or 0) for k in LINE_KEYS},
            )
            for r in rows if r.min is not None and float(r.min) > 0
        }
        if priors:
            return source, max(r.as_of_date for r in rows), priors
    return None, None, {}


def current_teams() -> dict[int, str]:
    """Player -> team, from the profiles the latest player-profiles run touched."""
    profiles = list(PlayerProfile.select(PlayerProfile.player, PlayerProfile.team, PlayerProfile.updated_at).tuples())
    latest = max((updated for _pid, _team, updated in profiles if updated), default=None)
    if latest is None:
        return {}
    return {pid: team for pid, team, updated in profiles if team and updated and updated >= latest - PROFILE_RUN_SLACK}


def player_ages(season: str, as_of: date) -> dict[int, float]:
    last = season_label(season_start(season) - 1)
    ages = {
        pid: float(age) + 1.0
        for pid, age in PlayerHistory.select(PlayerHistory.player_id, PlayerHistory.age)
        .where((PlayerHistory.season == last) & PlayerHistory.age.is_null(False)).tuples()
    }
    opening = date(season_start(season), 10, 20)        # an age is "as of the season", as nba.player_history counts it
    for pid, born in (PlayerProfile.select(PlayerProfile.player, PlayerProfile.birthdate)
                      .where(PlayerProfile.birthdate.is_null(False)).tuples()):
        if pid not in ages:
            ages[pid] = (opening - born).days / 365.25
    return ages


def known_absences(season: str, as_of: date, played: set[int]) -> dict[int, Availability]:
    """What the availability feed and the curated notes say beyond the box scores.

    The newest verdict of the last two days per player: "out" rules him out of
    his team's next game, and its expected return date is kept when it is still
    to come. A preseason adjustment's return date stands in where the feed
    gives none, and its season of no games is believed until he plays. The
    season's own notes are laid over this afterwards (`with_notes`).
    """
    out: dict[int, Availability] = {}
    A = PlayerAvailability
    newest = (
        A.select(A.player, fn.MAX(A.game_date).alias("day"))
        .where((A.game_date <= as_of) & (A.game_date >= as_of - VERDICT_FRESH))
        .group_by(A.player)
    )
    rows = (
        A.select(A.player, A.verdict, A.expected_return)
        .join(newest, on=((A.player == newest.c.player_id) & (A.game_date == newest.c.day)))
        .where(A.verdict == "out")
        .tuples()
    )
    for pid, _verdict, back in rows:
        out[pid] = Availability(out_next=True, return_date=back if (back is not None and back >= as_of) else None)
    for adj in ProjectionAdjustment.active_for(season):
        pid = adj.player_id
        have = out.get(pid)
        gone = adj.games is not None and int(adj.games) == 0 and pid not in played
        back = adj.return_date if (adj.return_date is not None and adj.return_date >= as_of) else None
        if not gone and back is None:
            continue
        out[pid] = Availability(
            out_next=have.out_next if have else False,
            return_date=(have.return_date if (have and have.return_date) else back),
            out_for_season=gone,
        )
    return out


def eastern(utc_naive: datetime) -> datetime:
    """A stored UTC timestamp as Eastern wall-clock time."""
    return pytz.utc.localize(utc_naive).astimezone(EASTERN).replace(tzinfo=None)


def note_of(record: ProjectionAdjustment) -> Note:
    """A stored season note as the model reads it."""
    return Note(
        id=record.id, player_id=record.player_id, kind=record.kind,
        written=eastern(record.created_at), review_date=record.review_date,
        minutes=float(record.minutes) if record.minutes is not None else None,
        usage=float(record.usage) if record.usage is not None else None,
        games=int(record.games) if record.games is not None else None,
        return_date=record.return_date, until_date=record.until_date,
        depends_on=record.depends_on_player_id,
    )


def season_notes(season: str) -> dict[int, Note]:
    """Every player's live season note."""
    return {rec.player_id: note_of(rec) for rec in ProjectionAdjustment.active_for(season, SCOPE_SEASON)}


def last_tips(lines: Iterable[BoxLine], season: str, before: date) -> dict[int, datetime]:
    """Each player's most recent game before the morning, as the Eastern time it tipped."""
    latest: dict[int, tuple[date, str]] = {}
    for line in lines:
        have = latest.get(line.player_id)
        if have is None or line.game_date > have[0]:
            latest[line.player_id] = (line.game_date, line.game_id)
    return _tips(latest, season, before)


def tips_of(player_ids: Iterable[int], season: str, before: date) -> dict[int, datetime]:
    """`last_tips` for a few players, without loading the season's box scores:
    what the editor reads to say how a note stands."""
    wanted = sorted(set(player_ids))
    if not wanted:
        return {}
    G = PlayerGameStats
    latest: dict[int, tuple[date, str]] = {}
    rows = (
        G.select(G.player, G.game_date, G.game)
        .where(G.player.in_(wanted) & (G.season == season) & (G.season_type == "regular")
               & (G.game_date < before) & G.game.is_null(False))
        .tuples()
    )
    for pid, day, game_id in rows:
        have = latest.get(pid)
        if have is None or day > have[0]:
            latest[pid] = (day, game_id)
    return _tips(latest, season, before)


def _tips(latest: Mapping[int, tuple[date, str]], season: str, before: date) -> dict[int, datetime]:
    wanted = sorted({game_id for _day, game_id in latest.values()})
    starts = dict(
        Game.select(Game.game_id, Game.start_time_et)
        .where(Game.game_id.in_(wanted) & (Game.season == season) & (Game.game_date < before))
        .tuples()
    ) if wanted else {}
    return {pid: datetime.combine(day, starts.get(game_id) or DEFAULT_TIP) for pid, (day, game_id) in latest.items()}


def with_notes(
    known: Mapping[int, Availability], notes: Mapping[int, Note], standings: Mapping[int, Standing]
) -> dict[int, Availability]:
    """`known` with the games part of every note in force laid over it.

    A note is the considered reading ("re-evaluated in two weeks" is not "back
    in two weeks"), so its return date is believed over the feed's, and it says
    he is out now even when his last game was last night. The feed still rules
    him out of his team's next game on its own.
    """
    out = dict(known)
    for pid, note in notes.items():
        if not standings[pid].games:
            continue
        have = out.get(pid)
        out[pid] = Availability(
            out_next=have.out_next if have else False,
            return_date=None if note.out_for_season else note.return_date,
            out_for_season=note.out_for_season,
            out_now=True,
        )
    return out


def noted_inputs(
    known: Mapping[int, Availability], notes: Mapping[int, Note], as_of: date, last_played: Mapping[int, datetime]
) -> dict:
    """The fields of `LiveInputs` that follow from the season's notes."""
    standings = {pid: standing(note, as_of, last_played) for pid, note in notes.items()}
    return {
        "known": with_notes(known, notes, standings),
        "notes": dict(notes),
        "standings": standings,
        "role_notes": {pid: role_note(note) for pid, note in notes.items() if standings[pid].role},
    }


def load_inputs(season: str, as_of: date) -> LiveInputs:
    """One trip to the database for everything the live projection reads."""
    lines = box_lines(season, as_of)
    source, prior_as_of, priors = preseason_priors(season)
    tips = last_tips(lines, season, as_of)
    known = known_absences(season, as_of, {l.player_id for l in lines})
    return LiveInputs(
        season=season, as_of=as_of, lines=lines, ahead=games_ahead(season, as_of),
        priors=priors, prior_source=source, prior_as_of=prior_as_of,
        teams=current_teams(), ages=player_ages(season, as_of),
        known_before_notes=known, last_played=tips,
        **noted_inputs(known, season_notes(season), as_of, tips),
    )
