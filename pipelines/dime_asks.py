"""
Dime Asks Pipeline — the tick that runs what users asked Dime to do at a time
(the Dime desk redesign, step 2).

In a chat a user can ask Dime for something later ("at 6:30 tonight, check
whether he plays and swap him out if he doesn't"). Dime saves it in
usr.ai_asks; when it falls due it runs once, as a check-in of its own (kind
`ask`): one Dime turn on the check-ins budget. The backend owns all of it, the
asks, the claim, the model and the spend, so this pipeline is one call: POST
/v1/internal/jobs/asks/run, which starts the due asks and answers at once (the
turns run on in the backend).

cron-runner fires the trigger every minute, so an ask starts in the minute it
was saved for; the route gates on one query (is any ask due, or an ask's
check-in left running by a process that died), so an idle tick never reaches
here.

Counters: an ask started is a processed record; one closed without running
(missed an hour past its time, or refused by the budget) is a skipped record.
One still waiting (its team had a check-in running) is neither: the next tick
takes it. Only a backend that could not be asked is a failure.
"""

from core.logging import get_correlation_id
from pipelines.base import BasePipeline
from pipelines.config import PipelineCategory, PipelineConfig
from pipelines.context import PipelineContext
from services.backend_client import backend_client_from_settings


class DimeAsksFailed(RuntimeError):
    """The backend could not be asked: the run is recorded as failed (the next tick retries)."""


class DimeAsksPipeline(BasePipeline):
    """One backend call that starts the asks that are due."""

    config = PipelineConfig(
        name="dime_asks",
        display_name="Dime Asks",
        description="Asks the backend to run what users asked Dime to do at a time, once each is due",
        target_table="usr.ai_asks",
        category=PipelineCategory.SCHEDULED,
        trigger_slug="dime-asks",
        cron_job="dime-asks",
        # The route skips a tick when no ask is due; the dashboard's Run runs anyway.
        force_on_run=True,
        timeout_seconds=120,
    )

    def __init__(self):
        super().__init__()
        self.backend_client = backend_client_from_settings()

    def execute(self, ctx: PipelineContext) -> None:
        run = self.backend_client.run_asks(correlation_id=get_correlation_id())
        if not run.ok:
            ctx.increment_failed(1, run.reason or "backend_unavailable")
            ctx.log.warning("dime_asks_backend_unavailable", reason=run.reason, error=run.error)
            raise DimeAsksFailed(f"the backend could not be asked ({run.reason or 'backend_unavailable'})")
        if run.off:
            ctx.log.info("dime_asks_off")
            return
        for started in run.started:
            ctx.increment_records(1)
            ctx.log.info("dime_ask_started", ask_id=started.ask_id, team_id=started.team_id,
                         checkin_id=started.checkin_id)
        if run.closed:
            ctx.increment_skipped(run.closed, "closed")
        ctx.log.info("dime_asks_tick", started=len(run.started), waiting=run.waiting, closed=run.closed,
                     stale=run.stale)
