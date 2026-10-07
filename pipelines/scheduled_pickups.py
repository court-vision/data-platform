"""
Scheduled Pickups Pipeline — the executor tick for usr.scheduled_pickups.

Asks the backend to attempt every due scheduled pickup (POST
/v1/internal/jobs/pickups/execute; the backend owns the ESPN reads, the retry
rules, the writes and the audit) and emails each user what became of theirs.
This pipeline owns the cadence — cron-runner fires its trigger every minute and
the route gates on one EXISTS over the table, so an idle tick never reaches
here — the run log, and the emails.

Counters: an executed pickup is a processed record; skipped / failed / expired /
deferred ones are skipped records with their reason. A business outcome is not a
pipeline failure (`pipeline_partial` would alert on one refused pickup
otherwise): only a backend that could not be asked counts as failed, and the ops
alert for that is deduped like lineup-alerts'.
"""

from datetime import timedelta

from core.logging import get_correlation_id
from core.settings import settings
from db.models.notifications import NotificationPreference, NotificationTeamPreference
from db.models.teams import Team
from db.models.users import User
from pipelines.base import BasePipeline
from pipelines.config import PipelineCategory, PipelineConfig
from pipelines.context import PipelineContext
from services.alert_service import AlertEvent, get_alert_service
from services.backend_client import PickupExecution, PickupsRun, backend_client_from_settings
from services.notification_service import NotificationService

BACKEND_UNAVAILABLE_ALERT_DEDUPE = timedelta(hours=6)
# Outcomes that end a row — and get an email. A deferred row waits silently.
SETTLED_OUTCOMES = frozenset({"executed", "skipped", "failed", "expired"})


def _lookup_recipient(user_id: int, team_id: int):
    """
    (user, team, prefs) for an email, or None when the row's owner is gone.

    The address is the one lineup alerts use for the team: the team's override
    email where it sets one (non-null wins, LineupAlertsPipeline._get_effective_prefs),
    else the global preference's, else (NotificationService._recipient) the account's.
    """
    user = User.get_or_none(User.user_id == user_id)
    team = Team.get_or_none(Team.team_id == team_id)
    if user is None or team is None:
        return None
    prefs = NotificationPreference.get_or_none(NotificationPreference.user == user_id)
    team_pref = NotificationTeamPreference.get_or_none(
        (NotificationTeamPreference.user == user_id) & (NotificationTeamPreference.team_id == team_id)
    )
    if team_pref is not None and team_pref.email is not None:
        prefs = team_pref  # the notification service reads only prefs.email
    return user, team, prefs


class ScheduledPickupsPipeline(BasePipeline):
    """
    One tick of the scheduled-pickup executor.

    1. Asks the backend to attempt the due rows (it claims them with a lease)
    2. Counts each outcome
    3. Emails the owner of every row that settled this tick
    """

    config = PipelineConfig(
        name="scheduled_pickups",
        display_name="Scheduled Pickups",
        description="Attempts due scheduled pickups through the backend and emails each outcome",
        target_table="usr.scheduled_pickups",
        category=PipelineCategory.SCHEDULED,
        trigger_slug="scheduled-pickups",
        cron_job="scheduled-pickups",
        timeout_seconds=180,
    )

    def __init__(self):
        super().__init__()
        self.backend_client = backend_client_from_settings()
        self.notification_service = NotificationService()

    def execute(self, ctx: PipelineContext) -> None:
        run = self.backend_client.execute_pickups(
            limit=settings.pickups_per_run, correlation_id=get_correlation_id(),
        )
        if not run.ok:
            ctx.increment_failed(1, run.reason or "backend_unavailable")
            ctx.log.warning("scheduled_pickups_backend_unavailable", reason=run.reason, error=run.error)
            self._alert_backend_unavailable(ctx, run)
            return

        ctx.log.info("scheduled_pickups_backend_run", due=run.due, results=len(run.results))
        for result in run.results:
            self._record(ctx, result)
            if result.outcome in SETTLED_OUTCOMES:
                self._email(ctx, result)

    # ---- per result ----

    @staticmethod
    def _record(ctx: PipelineContext, result: PickupExecution) -> None:
        if result.outcome == "executed":
            ctx.increment_records(1)
        else:
            ctx.increment_skipped(1, f"{result.outcome}:{result.reason or 'unknown'}")
        ctx.log.info(
            "scheduled_pickup_result",
            pickup_id=result.pickup_id,
            team_id=result.team_id,
            user_id=result.user_id,
            outcome=result.outcome,
            reason=result.reason,
            add=result.add.get("name"),
            drop=(result.drop or {}).get("name"),
            nba_date=result.nba_date,
            seated_slot=result.seated_slot,
            verified=result.verified,
            next_attempt_at=result.next_attempt_at,
        )

    def _email(self, ctx: PipelineContext, result: PickupExecution) -> None:
        found = _lookup_recipient(result.user_id, result.team_id)
        if found is None:
            ctx.log.warning("scheduled_pickup_email_no_recipient", pickup_id=result.pickup_id,
                            user_id=result.user_id, team_id=result.team_id)
            ctx.increment_skipped(1, "email_no_recipient")
            return
        user, team, prefs = found
        sent = self.notification_service.send_scheduled_pickup_result(user, team, result.as_dict(), prefs=prefs)
        if not sent.success:
            ctx.log.warning("scheduled_pickup_email_failed", pickup_id=result.pickup_id, error=sent.error)
            ctx.increment_skipped(1, "email_failed")

    # ---- ops ----

    def _alert_backend_unavailable(self, ctx: PipelineContext, run: PickupsRun) -> None:
        get_alert_service().notify(AlertEvent(
            key="scheduled_pickups_backend_unavailable",
            severity="warning",
            title="Scheduled pickups: backend unavailable",
            body=(
                f"The backend could not be asked to attempt the due pickups ({run.reason}); "
                f"the rows stay pending and the next tick retries."
            ),
            fields={
                "pipeline": self.config.name,
                "run_id": str(ctx.run_id),
                "reason": run.reason or "",
                "error": run.error or "",
                "backend_url": self.backend_client.base_url,
            },
            dedupe=BACKEND_UNAVAILABLE_ALERT_DEDUPE,
        ))
