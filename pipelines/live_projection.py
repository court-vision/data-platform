"""
Live Projection Pipeline: every player's line for each game left, repriced
every morning (the Live rankings plan, step 2).

The preseason projection stops on Oct 31. Each morning of the regular season
this builds the live one (services.live_projection_model: skill per minute,
role by situation, games from the return curves and the availability feed) and
writes it to nba.player_projections under its own source, `cv_live`: one row
per player a day, his rest-of-season line when he plays, the games he is
expected to play from here (`projected_gp`, and unrounded in `raw.games`), and
in `raw` the timeline that prices any window of days and what each part read.

The season's notes are read with it (services.live_notes: a return date, or
minutes and a share of the offense with an end), and a note saved in the
projections editor rebuilds the day's snapshot at once.

The preseason snapshots (`cv`, `espn`) are untouched: rooms drafted before
opening night and their recaps still read them, and they are this model's
prior. The backend's in-season boards read the newest `cv_live` snapshot, and
fall back to the blend they used before when there is none.

It runs as the first link of the daily-rankings trigger, so the boards stored
that morning are built on it. One projection a day: a later tick keeps the
morning's, and only a forced run (`?force=true`, the dashboard's Run button)
rebuilds it. Like the boards it waits for last night: until every game of the
last game day has box scores, and the batch that writes them is done and
wrote nothing while they were being read, it raises `DataNotReady`, and from
`WAIT_UNTIL_HOUR_ET` it projects from what there is. And like the boards it stores nothing between midnight and
`DAY_TURNS_HOUR_ET`: the night's games are still coming in, and a projection
built then would be the one the morning keeps.

Everything is written in one transaction: a reader resolving "the latest
snapshot" never finds half of one.
"""

from __future__ import annotations

from datetime import date, datetime
from typing import Optional

from core.settings import settings
from db.base import db
from db.models.nba import Game, PlayerProjection
from pipelines.base import BasePipeline, DataNotReady
from pipelines.config import PipelineCategory, PipelineConfig
from pipelines.context import PipelineContext
from pipelines.daily_rankings import (
    WAIT_UNTIL_HOUR_ET,
    batch_mark,
    batch_unsettled,
    last_game_day,
    now_eastern,
    today_et,
)
from pipelines.readiness import REGULAR_SEASON_PREFIX
from services.live_projection_inputs import LiveInputs, load_coefficients, load_inputs
from services.live_projection_model import Coefficients, LiveProjection, build_morning, project

SOURCE = "cv_live"
BATCH_SIZE = 500
RATE_DECIMALS = 5

# The batch that writes the box scores this reads
BOX_SCORES_PIPELINE = "player_game_stats"
# The fantasy day (ESPN's, and the backend's boards') turns at this hour, ET
DAY_TURNS_HOUR_ET = 3


def stored_on(season: str, day: date) -> int:
    """How many players the day's live snapshot holds."""
    return (
        PlayerProjection.select()
        .where((PlayerProjection.season == season) & (PlayerProjection.source == SOURCE)
               & (PlayerProjection.as_of_date == day))
        .count()
    )


def games_missing(inputs: LiveInputs, last_night: Optional[date]) -> list[str]:
    """Regular-season games of the last game day that have no box score yet."""
    if last_night is None:
        return []
    landed = {l.game_id for l in inputs.lines if l.game_date == last_night}
    scheduled = [
        game_id for (game_id,) in Game.select(Game.game_id)
        .where((Game.game_date == last_night) & (Game.season == inputs.season)
               & Game.game_id.startswith(REGULAR_SEASON_PREFIX)).tuples()
    ]
    return sorted(g for g in scheduled if g not in landed)


def waiting_on(
    missing: list[str],
    last_night: Optional[date],
    now_et: datetime,
    unsettled: Optional[str] = None,
) -> Optional[str]:
    """Why the projection should not be built yet, or None when it can be.

    It can once every game of the last game day has box scores and the batch
    that writes them is done (`unsettled` is why it is not), or once the
    morning has gone on long enough that waiting costs the day's projection.
    """
    if last_night is None or now_et.hour >= WAIT_UNTIL_HOUR_ET:
        return None
    if missing:
        return f"{len(missing)} of the games of {last_night.isoformat()} have no box score yet"
    return unsettled


def row_of(p: LiveProjection, inputs: LiveInputs, coeffs: Coefficients, run_id) -> dict:
    """One player's projection as his nba.player_projections row."""
    return {
        "player": p.player_id,
        "season": inputs.season,
        "source": SOURCE,
        "as_of_date": inputs.as_of,
        **p.line(),
        "projected_gp": int(round(p.games)),
        "raw": {
            "version": coeffs.version,
            "games": round(p.games, 2),
            "team": p.team,
            "rates": {k: round(v, RATE_DECIMALS) for k, v in p.rates.items()},
            "timeline": p.timeline(),
            "parts": p.parts,
            "prior": inputs.prior_source,
        },
        "pipeline_run_id": run_id,
    }


class LiveProjectionPipeline(BasePipeline):
    """Build and publish the morning's live projection."""

    config = PipelineConfig(
        name="live_projection",
        display_name="Live Projection",
        description="Every player's line for each game left, repriced each morning (skill, role, games)",
        target_table="nba.player_projections",
        category=PipelineCategory.SCHEDULED,
        trigger_slug="live-projection",
        cron_job="daily-rankings",      # first link of the chain: this, then the day's boards
        timeout_seconds=180,
        force_on_run=True,
    )

    def execute(self, ctx: PipelineContext) -> None:
        force = bool((ctx.options or {}).get("force"))
        season = settings.nba_season
        day = today_et()
        now_et = now_eastern()
        if now_et.hour < DAY_TURNS_HOUR_ET:
            ctx.increment_skipped(1, "day_over")
            ctx.log.warning("live_projection_day_over", today=str(day), hour_et=now_et.hour, forced=force)
            return

        held = stored_on(season, day)
        if held and not force:
            ctx.log.info("live_projection_kept", as_of=str(day), rows=held)
            return

        # Before the read, so a box-score batch that writes under it shows afterwards
        box_scores = batch_mark(BOX_SCORES_PIPELINE)
        inputs = load_inputs(season, day)
        if not inputs.lines or not any(inputs.ahead.values()):
            ctx.log.info("live_projection_out_of_season", as_of=str(day), lines=len(inputs.lines))
            return
        if not inputs.priors:
            ctx.log.warning("live_projection_no_prior", season=season)

        last_night = last_game_day(day, season)
        reason = waiting_on(
            games_missing(inputs, last_night), last_night, now_et,
            batch_unsettled(BOX_SCORES_PIPELINE, "box-score", box_scores),
        )
        if reason and not force:
            raise DataNotReady(reason)

        coeffs = load_coefficients()
        morning = build_morning(inputs.lines, inputs.priors, inputs.ahead, day, teams=inputs.teams, ages=inputs.ages)
        projections = project(morning, coeffs, inputs.known, inputs.role_notes)
        for pid, note in inputs.notes.items():
            # Which note an absence was read from, for the reasons shown later
            if inputs.standings[pid].games and pid in projections:
                projections[pid].parts.setdefault("known", {})["note"] = note.id
        rows = [row_of(p, inputs, coeffs, ctx.run_id) for p in projections.values()]
        ctx.log.info(
            "live_projection_built",
            as_of=str(day), players=len(rows), lines=len(inputs.lines), projected=len(inputs.priors),
            prior=inputs.prior_source, prior_as_of=str(inputs.prior_as_of) if inputs.prior_as_of else None,
            known_absences=len(inputs.known), newest_line=str(inputs.newest_line), coefficients=coeffs.version,
            notes=len(inputs.notes), notes_in_force=sum(1 for s in inputs.standings.values() if s.in_force),
        )

        snapshot = (
            (PlayerProjection.season == season)
            & (PlayerProjection.source == SOURCE)
            & (PlayerProjection.as_of_date == day)
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
