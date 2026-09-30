"""
CV Projection Pipeline

Court Vision's own projection for the coming season, written to
nba.player_projections with source='cv' — the table's `source` column exists
for exactly this. The math is services.projection_model; this pipeline only
loads its four inputs and writes its output:

- history: the last three seasons of nba.player_history;
- ESPN: the latest source='espn' projection snapshot (preseason-market);
- adjustments: the live nba.projection_adjustments rows (the dashboard editor);
- the roster: who is in the league now — players in the latest player-profiles
  run with a team, plus anyone ESPN projects. A retired player keeps his
  history and gets no projection.

Runs daily after preseason-market (it wants the day's ESPN line), and on
demand when an adjustment is saved. Same Aug 15 - Oct 31 window as
preseason-market: in-season projections are a different problem. Publishes
the day's rows atomically, like preseason-market, so a reader resolving "the
latest snapshot" never sees half of one.
"""

from __future__ import annotations

import json
from collections import defaultdict
from datetime import date, datetime, timedelta
from functools import lru_cache
from pathlib import Path
from typing import Optional

from core.settings import settings
from db.base import db
from db.models.nba import (
    PlayerHistory,
    PlayerProfile,
    PlayerProjection,
    ProjectionAdjustment,
)
from pipelines.base import BasePipeline
from pipelines.config import PipelineCategory, PipelineConfig
from pipelines.context import PipelineContext
from pipelines.gates import preseason_market_window
from services.projection_model import (
    LINE_KEYS,
    Adjustment,
    Coefficients,
    EspnLine,
    HistoryRow,
    project,
    season_label,
    season_start,
    team_games_after,
)

COEFFICIENTS_PATH = Path(__file__).resolve().parent.parent / "static" / "cv_projection_coefficients.json"
STATIC_DIR = Path(__file__).resolve().parent.parent / "static"
SOURCE = "cv"
# A profile counts as current when the latest player-profiles run touched it.
PROFILE_RUN_SLACK = timedelta(hours=6)


@lru_cache(maxsize=1)
def load_coefficients(path: str = str(COEFFICIENTS_PATH)) -> Coefficients:
    return Coefficients.from_json(json.loads(Path(path).read_text()))


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
    record, team: Optional[str], per_day: dict[date, frozenset[str]]
) -> Adjustment:
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


class CVProjectionPipeline(BasePipeline):
    """Build and publish Court Vision's projection for the coming season."""

    config = PipelineConfig(
        name="cv_projection",
        display_name="CV Projection",
        description="Court Vision's own per-game projection and expected games (history + ESPN + adjustments)",
        target_table="nba.player_projections",
        category=PipelineCategory.SCHEDULED,
        trigger_slug="cv-projection",
        cron_job="preseason-market",    # chained after preseason-market and player-profiles
        timeout_seconds=300,
    )

    def execute(self, ctx: PipelineContext) -> None:
        as_of_date = ctx.game_date()
        season = settings.nba_season
        target = season_start(season)

        gate = preseason_market_window(as_of_date)
        if not gate.run and not (ctx.options or {}).get("force"):
            ctx.log.info("cv_projection_gated", reason=gate.reason, **gate.detail)
            return

        coeffs = load_coefficients()
        window = [season_label(target - k) for k in (1, 2, 3)]
        history = history_rows(PlayerHistory.for_seasons(window))
        if not any(r.season == target - 1 for rows in history.values() for r in rows):
            ctx.log.warning("cv_projection_no_history", missing_season=window[0])
            return

        espn = espn_lines(PlayerProjection.latest_for_season(season, source="espn"))

        profiles = list(PlayerProfile.select(PlayerProfile.player, PlayerProfile.team, PlayerProfile.updated_at))
        latest_run = max((p.updated_at for p in profiles if p.updated_at), default=None)
        current = {
            p.player_id: p.team_id for p in profiles
            if p.team_id and latest_run and p.updated_at and p.updated_at >= latest_run - PROFILE_RUN_SLACK
        }
        roster = set(current) | set(espn)

        per_day = per_day_calendar(season)
        adjustments = {
            a.player_id: adjustment_of(a, current.get(a.player_id), per_day)
            for a in ProjectionAdjustment.active_for(season)
        }

        projections = project(target, history, espn, adjustments, roster, coeffs)
        ctx.log.info(
            "cv_projection_built",
            players=len(projections), roster=len(roster), espn=len(espn),
            adjustments=len(adjustments), version=coeffs.version,
        )

        with db.atomic():
            for p in projections:
                PlayerProjection.record_projection(
                    player_id=p.player_id,
                    season=season,
                    as_of_date=as_of_date,
                    line=p.line(),
                    projected_gp=int(round(p.games)),
                    raw={
                        "version": coeffs.version,
                        "dd_rate": round(p.dd_rate, 4),
                        "td_rate": round(p.td_rate, 4),
                        "espn_weight": p.components.get("espn_weight"),
                        "adjustment_id": p.components.get("adjustment_id"),
                        "age": p.components.get("age"),
                        "seasons": p.components.get("seasons"),
                        "games": round(p.games, 1),
                    },
                    source=SOURCE,
                    pipeline_run_id=ctx.run_id,
                )
                ctx.increment_records()
