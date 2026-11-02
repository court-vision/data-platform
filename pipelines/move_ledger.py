"""
Move Ledger Pipeline — asks the backend to rebuild the move ledger each night.

The move ledger (usr.move_outcomes, the Dime autopilot plan's step 1) is every
add and drop each team in a league made, read off the day-to-day differences in
usr.lineup_snapshots, and what it earned afterwards in the box scores, scored
with the league's own scoring. The backend owns it — the scoring engine, the
league settings and Court Vision's own writes (who made a move) all live there —
so this pipeline is one call: POST /v1/internal/jobs/ledger/run, which rebuilds
every league with a recent snapshot and answers one result per league. Each
league's rows are replaced whole, so a rerun (a forced Run, the next poll) is
always safe, and the first run back-fills the season from opening night.

Timing: POST_GAME after `lineup_snapshots` (the night's rosters) and
`player_game_stats` (the night's box scores); an unmet dependency is a silent
per-poll skip, so a WITHHOLD night that holds the snapshots back holds this
back too.

Counters: a league rebuilt is a processed record; a league the backend could
not rebuild is a failed record with its error; a backend that could not be
asked is one failed record (the next poll retries).
"""

from core.logging import get_correlation_id
from pipelines.base import BasePipeline
from pipelines.config import PipelineCategory, PipelineConfig
from pipelines.context import PipelineContext
from services.backend_client import backend_client_from_settings


class MoveLedgerPipeline(BasePipeline):
    """One backend call that rebuilds every active league's move ledger."""

    config = PipelineConfig(
        name="move_ledger",
        display_name="Move Ledger",
        description="Asks the backend to rebuild each league's adds and drops and what they earned",
        target_table="usr.move_outcomes",
        category=PipelineCategory.POST_GAME,
        trigger_slug="move-ledger",
        depends_on=("player_game_stats", "lineup_snapshots"),
        timeout_seconds=360,
    )

    def __init__(self):
        super().__init__()
        self.backend_client = backend_client_from_settings()

    def execute(self, ctx: PipelineContext) -> None:
        run = self.backend_client.run_ledger(correlation_id=get_correlation_id())
        if not run.ok:
            ctx.increment_failed(1, run.reason or "backend_unavailable")
            ctx.log.warning("move_ledger_backend_unavailable", reason=run.reason, error=run.error)
            return
        for league in run.leagues:
            if league.ok:
                ctx.increment_records(1)
            else:
                ctx.increment_failed(1, f"league_failed:{league.error or 'unknown'}")
            ctx.log.info(
                "move_ledger_league",
                league=league.provider_league_id,
                season=league.season,
                ok=league.ok,
                days=league.days,
                moves=league.moves,
                by_source=league.by_source,
                error=league.error,
            )
        if not run.leagues:
            ctx.log.info("move_ledger_no_active_leagues")
