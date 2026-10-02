"""
CV Projection Pipeline

Court Vision's own projection for the coming season, written to
nba.player_projections with source='cv' — the table's `source` column exists
for exactly this. The math is services.projection_model and the inputs are
services.projection_inputs (history, ESPN's line, the live adjustments, the
current roster); this pipeline only runs the one over the other and writes
the result.

Runs daily as the last link of the preseason-market trigger's chain, after
player-profiles and preseason-market (it wants the day's rosters and ESPN
line), and on demand when an adjustment is saved. Same Aug 15 - Oct 31 window as
preseason-market: in-season projections are a different problem. Publishes
the day's rows atomically, like preseason-market, so a reader resolving "the
latest snapshot" never sees half of one.
"""

from __future__ import annotations

from core.settings import settings
from db.base import db
from db.models.nba import PlayerProjection
from pipelines.base import BasePipeline
from pipelines.config import PipelineCategory, PipelineConfig
from pipelines.context import PipelineContext
from pipelines.gates import preseason_market_window
from services.projection_inputs import load_inputs
from services.projection_model import project, season_label

SOURCE = "cv"
BATCH_SIZE = 500


class CVProjectionPipeline(BasePipeline):
    """Build and publish Court Vision's projection for the coming season."""

    config = PipelineConfig(
        name="cv_projection",
        display_name="CV Projection",
        description="Court Vision's own per-game projection and expected games (history + ESPN + adjustments)",
        target_table="nba.player_projections",
        category=PipelineCategory.SCHEDULED,
        trigger_slug="cv-projection",
        cron_job="preseason-market",    # last link of the chain: player-profiles, preseason-market, then this
        timeout_seconds=300,
    )

    def execute(self, ctx: PipelineContext) -> None:
        as_of_date = ctx.game_date()
        season = settings.nba_season

        gate = preseason_market_window(as_of_date)
        if not gate.run and not (ctx.options or {}).get("force"):
            ctx.log.info("cv_projection_gated", reason=gate.reason, **gate.detail)
            return

        inputs = load_inputs(season)
        coeffs = inputs.coeffs
        if not inputs.has_last_season:
            ctx.log.warning("cv_projection_no_history", missing_season=season_label(inputs.target - 1))
            return

        adjustments = inputs.adjustments
        projections = project(inputs.target, inputs.history, inputs.espn, adjustments, inputs.roster, coeffs)
        # `coefficients`, not `version`: the log pipeline stamps every line with
        # the service's own version under that name.
        ctx.log.info(
            "cv_projection_built",
            players=len(projections), roster=len(inputs.roster), espn=len(inputs.espn),
            adjustments=len(adjustments), coefficients=coeffs.version,
        )

        rows = [
            {
                "player": p.player_id,
                "season": season,
                "source": SOURCE,
                "as_of_date": as_of_date,
                **p.line(),
                "projected_gp": int(round(p.games)),
                "raw": {
                    "version": coeffs.version,
                    "dd_rate": round(p.dd_rate, 4),
                    "td_rate": round(p.td_rate, 4),
                    "espn_weight": p.components.get("espn_weight"),
                    "adjustment_id": p.components.get("adjustment_id"),
                    "age": p.components.get("age"),
                    "seasons": p.components.get("seasons"),
                    "games": round(p.games, 1),
                },
                "pipeline_run_id": ctx.run_id,
            }
            for p in projections
        ]
        # A re-run the same day replaces the day's snapshot: every stat, the
        # games and the provenance are refreshed, and a player the earlier run
        # projected and this one does not is taken out of it. One statement per
        # batch rather than a read and a write per player — the editor waits on
        # this when an adjustment is saved.
        snapshot = (
            (PlayerProjection.season == season)
            & (PlayerProjection.source == SOURCE)
            & (PlayerProjection.as_of_date == as_of_date)
        )
        preserve = [getattr(PlayerProjection, key) for key in PlayerProjection.STAT_KEYS] + [
            PlayerProjection.projected_gp, PlayerProjection.raw, PlayerProjection.pipeline_run_id,
        ]
        with db.atomic():
            stale = PlayerProjection.delete().where(snapshot)
            if rows:
                stale = stale.where(PlayerProjection.player.not_in([row["player"] for row in rows]))
            stale.execute()
            for i in range(0, len(rows), BATCH_SIZE):
                (
                    PlayerProjection.insert_many(rows[i : i + BATCH_SIZE])
                    .on_conflict(
                        conflict_target=[
                            PlayerProjection.player, PlayerProjection.season,
                            PlayerProjection.source, PlayerProjection.as_of_date,
                        ],
                        preserve=preserve,
                    )
                    .execute()
                )
        ctx.increment_records(len(rows))
