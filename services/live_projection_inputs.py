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
  written off in nba.projection_adjustments.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import date, timedelta
from functools import lru_cache
from pathlib import Path
from typing import Optional

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
from services.live_projection_model import LINE_KEYS, Availability, BoxLine, Coefficients, Prior
from services.projection_inputs import PROFILE_RUN_SLACK
from services.projection_model import season_label, season_start

COEFFICIENTS_PATH = Path(__file__).resolve().parent.parent / "static" / "live_projection_coefficients.json"
PRIOR_SOURCES = ("cv", "espn")              # the preseason projection, in order of preference
# A verdict older than this says nothing about today
VERDICT_FRESH = timedelta(days=2)


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
                minutes=float(r.min), games=float(r.projected_gp or 0),
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
    to come. A curated return date stands in where the feed gives none, and a
    curated season of no games is believed until he plays.
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


def load_inputs(season: str, as_of: date) -> LiveInputs:
    """One trip to the database for everything the live projection reads."""
    lines = box_lines(season, as_of)
    source, prior_as_of, priors = preseason_priors(season)
    return LiveInputs(
        season=season, as_of=as_of, lines=lines, ahead=games_ahead(season, as_of),
        priors=priors, prior_source=source, prior_as_of=prior_as_of,
        teams=current_teams(), ages=player_ages(season, as_of),
        known=known_absences(season, as_of, {l.player_id for l in lines}),
    )
