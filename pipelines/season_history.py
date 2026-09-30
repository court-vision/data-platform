"""
Season History Pipeline

One row per player per completed NBA regular season into nba.player_history:
totals, age, usage, first season. The Court Vision projection reads the last
three seasons; the projection's age curve is fitted on all of them
(scripts/fit_cv_projection.py).

Manual trigger, two uses:
- the one-time backfill: `options.seasons` = every season wanted (14 were used
  to fit the 2026-27 coefficients);
- the yearly append: no options = the season before `settings.nba_season`,
  run once after the regular season ends.

Two nba_api calls per season (Base and Advanced totals) plus one for career
starts, paced so a 14-season backfill stays polite to stats.nba.com.
"""

from __future__ import annotations

import time
from datetime import datetime
from typing import Any, Mapping, Optional

from core.season import previous_season
from core.settings import settings
from db.base import db
from db.models.nba import PlayerHistory
from pipelines.base import BasePipeline
from pipelines.config import PipelineCategory, PipelineConfig
from pipelines.context import PipelineContext
from pipelines.extractors import NBAApiExtractor

# nba_api column -> nba.player_history column, all season totals.
TOTAL_COLUMNS: dict[str, str] = {
    "pts": "PTS", "reb": "REB", "ast": "AST", "stl": "STL", "blk": "BLK", "tov": "TOV",
    "fgm": "FGM", "fga": "FGA", "fg3m": "FG3M", "fg3a": "FG3A", "ftm": "FTM", "fta": "FTA",
    "oreb": "OREB", "dreb": "DREB", "dd2": "DD2", "td3": "TD3",
}
PAUSE_BETWEEN_CALLS = 1.5  # seconds
BATCH_SIZE = 200


def _int(value: Any) -> int:
    try:
        return int(round(float(value)))
    except (TypeError, ValueError):
        return 0


def _float(value: Any) -> Optional[float]:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def history_record(
    season: str,
    total: Mapping[str, Any],
    advanced: Optional[Mapping[str, Any]],
    from_year: Optional[int],
    run_id=None,
) -> Optional[dict]:
    """One nba_api totals row as an nba.player_history row; None without a player or a game."""
    player_id = total.get("PLAYER_ID")
    gp = _int(total.get("GP"))
    if player_id is None or gp <= 0:
        return None
    usage = _float((advanced or {}).get("USG_PCT"))
    minutes = _float(total.get("MIN")) or 0.0
    now = datetime.utcnow()
    return {
        "player_id": int(player_id),
        "season": season,
        "player_name": str(total.get("PLAYER_NAME") or f"Player {player_id}")[:100],
        "team": (total.get("TEAM_ABBREVIATION") or None),
        "age": _float(total.get("AGE")),
        "from_year": from_year,
        "gp": gp,
        "min": round(minutes, 1),
        **{col: _int(total.get(src)) for col, src in TOTAL_COLUMNS.items()},
        "usg_pct": round(usage, 3) if usage is not None else None,
        "pipeline_run_id": run_id,
        "created_at": now,
        "updated_at": now,
    }


class SeasonHistoryPipeline(BasePipeline):
    """Backfill or append completed seasons into nba.player_history."""

    config = PipelineConfig(
        name="season_history",
        display_name="Season History",
        description="One row per player per completed regular season (totals, age, usage) for the CV projection",
        target_table="nba.player_history",
        category=PipelineCategory.SCHEDULED,
        trigger_slug="season-history",
        timeout_seconds=900,
    )

    def __init__(self):
        super().__init__()
        self.nba_extractor = NBAApiExtractor()

    def execute(self, ctx: PipelineContext) -> None:
        seasons = list((ctx.options or {}).get("seasons") or [previous_season(settings.nba_season)])
        ctx.log.info("season_history_start", seasons=seasons)
        starts = self.nba_extractor.get_career_starts()

        # Everything but the key and the first-written stamp is refreshed on a
        # re-run: nba_api corrects old box scores now and then.
        preserve = [
            field for name, field in PlayerHistory._meta.fields.items()
            if name not in ("player_id", "season", "created_at")
        ]
        for season in seasons:
            time.sleep(PAUSE_BETWEEN_CALLS)
            totals = self.nba_extractor.get_season_totals(season)
            time.sleep(PAUSE_BETWEEN_CALLS)
            advanced = {row.get("PLAYER_ID"): row for row in self.nba_extractor.get_advanced_stats(season)}
            records = [
                rec for rec in (
                    history_record(season, t, advanced.get(t.get("PLAYER_ID")), starts.get(t.get("PLAYER_ID")), ctx.run_id)
                    for t in totals
                ) if rec is not None
            ]
            with db.atomic():
                for i in range(0, len(records), BATCH_SIZE):
                    (
                        PlayerHistory.insert_many(records[i : i + BATCH_SIZE])
                        .on_conflict(
                            conflict_target=[PlayerHistory.player_id, PlayerHistory.season],
                            preserve=preserve,
                        )
                        .execute()
                    )
            ctx.increment_records(len(records))
            ctx.log.info("season_history_season_written", season=season, records=len(records))
