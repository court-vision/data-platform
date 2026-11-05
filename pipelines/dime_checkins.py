"""
Dime Check-ins Pipeline — the tick that starts Dime's check-ins (the Dime
autopilot plan, step 3).

A check-in is a turn Dime starts itself, in shadow: the morning of a game day
(the weekly one on a matchup's first day), shortly before the first tip, and a
follow-up the next morning. Dime reads what the automations are about to do and
logs what it would keep or change; nothing is sent to ESPN. The backend owns all
of it — the schedule, the claim (one per team, kind and day), the model and the
spend — so this pipeline is one call: POST /v1/internal/jobs/checkins/run, which
starts what is due and answers at once (the turns run on in the backend).

cron-runner fires the trigger every fifteen minutes; the route gates on one
query (is any team switched on, usr.autopilot), so an idle tick never reaches
here.

Counters: a check-in started is a processed record; a missed or skipped one is a
skipped record with its reason. Only a backend that could not be asked is a
failure.
"""

from core.logging import get_correlation_id
from pipelines.base import BasePipeline
from pipelines.config import PipelineCategory, PipelineConfig
from pipelines.context import PipelineContext
from services.backend_client import backend_client_from_settings


class DimeCheckinsFailed(RuntimeError):
    """The backend could not be asked: the run is recorded as failed (the next tick retries)."""


class DimeCheckinsPipeline(BasePipeline):
    """One backend call that starts the due check-ins on every switched-on team."""

    config = PipelineConfig(
        name="dime_checkins",
        display_name="Dime Check-ins",
        description="Asks the backend to start Dime's due check-ins (in shadow) on the teams it runs on",
        target_table="usr.ai_checkins",
        category=PipelineCategory.SCHEDULED,
        trigger_slug="dime-checkins",
        cron_job="dime-checkins",
        # The route skips a tick when no team is switched on; the dashboard's Run runs anyway.
        force_on_run=True,
        timeout_seconds=120,
    )

    def __init__(self):
        super().__init__()
        self.backend_client = backend_client_from_settings()

    def execute(self, ctx: PipelineContext) -> None:
        run = self.backend_client.run_checkins(correlation_id=get_correlation_id())
        if not run.ok:
            ctx.increment_failed(1, run.reason or "backend_unavailable")
            ctx.log.warning("dime_checkins_backend_unavailable", reason=run.reason, error=run.error)
            raise DimeCheckinsFailed(f"the backend could not be asked ({run.reason or 'backend_unavailable'})")
        if run.off:
            ctx.log.info("dime_checkins_off")
            return
        for started in run.started:
            ctx.increment_records(1)
            ctx.log.info("dime_checkin_started", checkin_id=started.checkin_id, team_id=started.team_id,
                         kind=started.kind, date=started.date)
        if run.missed:
            ctx.increment_skipped(run.missed, "missed")
        if run.skipped:
            ctx.increment_skipped(run.skipped, "skipped")
        ctx.log.info("dime_checkins_tick", teams=run.teams, started=len(run.started), missed=run.missed,
                     skipped=run.skipped, stale=run.stale)
