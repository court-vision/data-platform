"""
What the Court Vision projection is built from, loaded once.

Two callers need exactly the same inputs: the cv-projection pipeline, which
publishes the projection, and the dashboard's projections editor, which shows
how it was built and previews an edit before it is saved. Loading them in one
place is what makes a preview a promise — the editor's "after" line is the line
the pipeline will write.

- history: the last three seasons of nba.player_history;
- ESPN: the latest source='espn' projection snapshot (preseason-market);
- adjustments: the live nba.projection_adjustments rows;
- the roster: who is in the league now — players the latest player-profiles
  run saw with a team, plus anyone ESPN projects. A retired player keeps his
  history and gets no projection.
"""

from __future__ import annotations

import json
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from functools import lru_cache
from pathlib import Path
from typing import Mapping, Optional

from db.models.nba import PlayerHistory, PlayerProfile, PlayerProjection, ProjectionAdjustment
from services.projection_model import (
    LINE_KEYS,
    Adjustment,
    Coefficients,
    EspnLine,
    HistoryRow,
    season_label,
    season_start,
    team_games_after,
)

STATIC_DIR = Path(__file__).resolve().parent.parent / "static"
COEFFICIENTS_PATH = STATIC_DIR / "cv_projection_coefficients.json"
# A profile counts as current when the latest player-profiles run touched it.
PROFILE_RUN_SLACK = timedelta(hours=6)


@lru_cache(maxsize=1)
def load_coefficients(path: str = str(COEFFICIENTS_PATH)) -> Coefficients:
    return Coefficients.from_json(json.loads(Path(path).read_text()))


@lru_cache(maxsize=4)
def per_day_calendar(season: str) -> dict[date, frozenset[str]]:
    """Game date -> teams playing, from the season's static matchupsPerDay file ({} when absent)."""
    short = f"{season[2:4]}-{season[5:7]}"
    path = STATIC_DIR / f"matchupsPerDay{short}.json"
    if not path.exists():
        return {}
    raw = json.loads(path.read_text())
    out: dict[date, frozenset[str]] = {}
    for day, games in raw.items():
        teams: set[str] = set()
        for g in games or []:
            teams.add(str(g.get("homeTeam")))
            teams.add(str(g.get("awayTeam")))
        out[datetime.strptime(day, "%m/%d/%Y").date()] = frozenset(t for t in teams if t and t != "None")
    return out


def history_rows(records) -> dict[int, list[HistoryRow]]:
    out: dict[int, list[HistoryRow]] = defaultdict(list)
    for r in records:
        out[r.player_id].append(HistoryRow(
            player_id=r.player_id, season=season_start(r.season), gp=int(r.gp), minutes=float(r.min or 0),
            totals={k: float(getattr(r, k) or 0) for k in PlayerHistory.TOTAL_KEYS},
            age=float(r.age) if r.age is not None else None, from_year=r.from_year,
        ))
    return out


def espn_lines(records) -> dict[int, EspnLine]:
    return {
        r.player_id: EspnLine(
            per_game={k: float(getattr(r, k) or 0) for k in LINE_KEYS},
            minutes=float(r.min) if r.min is not None else None,
            games=float(r.projected_gp) if r.projected_gp else None,
        )
        for r in records
    }


def adjustment_of(
    record, team: Optional[str], per_day: Mapping[date, frozenset[str]]
) -> Adjustment:
    """A stored adjustment (or anything shaped like one) resolved to numbers:
    a return date becomes the games his team has left after it."""
    games_available = None
    if record.return_date is not None:
        games_available = team_games_after(record.return_date, per_day, team)
    return Adjustment(
        id=record.id,
        minutes=float(record.minutes) if record.minutes is not None else None,
        games=float(record.games) if record.games is not None else None,
        games_available=float(games_available) if games_available is not None else None,
        usage=float(record.usage) if record.usage is not None else None,
        rates={k: float(v) for k, v in (record.rates or {}).items()},
    )


@dataclass
class ProjectionInputs:
    """Everything `services.projection_model.project` needs, fully materialized."""

    season: str
    target: int                                         # the season's start year
    history: dict[int, list[HistoryRow]]
    espn: dict[int, EspnLine]
    espn_as_of: Optional[date]                          # the ESPN snapshot the lines came from
    current: dict[int, str]                             # player id -> current NBA team
    roster: set[int]
    per_day: Mapping[date, frozenset[str]]
    coeffs: Coefficients
    records: dict[int, ProjectionAdjustment] = field(default_factory=dict)   # live adjustment rows, by player

    @property
    def has_last_season(self) -> bool:
        """Whether the history reaches the season before the target: without it
        there is nothing to project from."""
        return any(r.season == self.target - 1 for rows in self.history.values() for r in rows)

    @property
    def adjustments(self) -> dict[int, Adjustment]:
        return {
            pid: adjustment_of(record, self.current.get(pid), self.per_day)
            for pid, record in self.records.items()
        }


def load_inputs(season: str) -> ProjectionInputs:
    """One trip to the database for everything the projection reads."""
    target = season_start(season)
    window = [season_label(target - k) for k in (1, 2, 3)]
    history = history_rows(PlayerHistory.for_seasons(window))

    espn_records = list(PlayerProjection.latest_for_season(season, source="espn"))
    espn = espn_lines(espn_records)
    espn_as_of = max((r.as_of_date for r in espn_records), default=None)

    profiles = list(PlayerProfile.select(PlayerProfile.player, PlayerProfile.team, PlayerProfile.updated_at))
    latest_run = max((p.updated_at for p in profiles if p.updated_at), default=None)
    current = {
        p.player_id: p.team_id for p in profiles
        if p.team_id and latest_run and p.updated_at and p.updated_at >= latest_run - PROFILE_RUN_SLACK
    }

    return ProjectionInputs(
        season=season,
        target=target,
        history=history,
        espn=espn,
        espn_as_of=espn_as_of,
        current=current,
        roster=set(current) | set(espn),
        per_day=per_day_calendar(season),
        coeffs=load_coefficients(),
        records={a.player_id: a for a in ProjectionAdjustment.active_for(season)},
    )
