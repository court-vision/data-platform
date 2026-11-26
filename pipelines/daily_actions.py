"""
Daily Actions Pipeline (the Daily actions row's Act level in the automations table)

Once a day before the first tip-off, asks the backend to make each team's daily
actions: the Week desk's IR moves, and the day's best free agent for each open
roster spot, never a drop (`POST /v1/internal/jobs/daily-actions/run`). The backend
owns the ESPN reads, the list, the add limit and the writes; this pipeline keeps
which teams are at Act, the day's dedup, the email and the log.

A team is at Act when its own `daily_actions_level` is act, or it has none and its
account's is. The account's lineup email has no say here: this is its own
automation. A team whose row is at AI (Dime does it) is answered `skipped` by the
backend, which knows the autopilot rows.

Runs on the pre-game tick (every 15 min) from when the pre-game window opens, 150
min before the night's first tip, until that tip. It sits before lineup_alerts in
the registry, so on the tick both run the pickup is already on the roster when
auto-lineup fills the slots. One `usr.notification_log` row per (user, team,
'daily_actions', day): applied, rejected, noop and skipped settle the day; failed
(the backend or ESPN couldn't be reached) is retried by the next tick and updated
in place.
"""

import json
import time as time_mod
from datetime import datetime

import pytz

from core.logging import get_correlation_id
from db.models.nba.games import Game
from db.models.notifications import NotificationLog, NotificationPreference, NotificationTeamPreference
from db.models.teams import Team
from db.models.users import User
from pipelines.base import BasePipeline
from pipelines.config import PipelineCategory, PipelineConfig
from pipelines.context import PipelineContext
from services.backend_client import OUTCOME_UNAVAILABLE, DailyActionsRun, backend_client_from_settings
from services.notification_service import NotificationService

NOTIFICATION_TYPE = "daily_actions"
ACT = "act"
# Log statuses that mean "done for today"; anything else is retried next tick.
SETTLED_STATUSES = ("sent", "skipped")
# Outcomes that email the user: something was made, or ESPN refused the first move.
EMAILED = ("applied", "rejected")


class DailyActionsPipeline(BasePipeline):
    """Make the Week desk's IR moves and fill open roster spots on every team at Act."""

    config = PipelineConfig(
        name="daily_actions",
        display_name="Daily Actions",
        description=(
            "Before the first tip, makes the Week desk's IR moves and fills open roster "
            "spots on every team whose Daily actions are at Act, and emails what it made"
        ),
        target_table="usr.notification_log",
        category=PipelineCategory.PRE_GAME,
        trigger_slug="daily-actions",
        skip_batch_dedup=True,
    )

    def __init__(self):
        super().__init__()
        self.backend_client = backend_client_from_settings()
        self.notification_service = NotificationService()

    def execute(self, ctx: PipelineContext) -> None:
        now_et = datetime.now(pytz.timezone("US/Eastern"))
        today = now_et.date()

        if ctx.date_override and ctx.date_override != today:
            # A pre-game backfill for another day: the moves are today's ESPN writes, never that day's
            ctx.log.info("backfill_skipped", date_override=str(ctx.date_override), today=str(today))
            return

        first_tip = Game.get_earliest_game_time_on_date(today)
        if not first_tip:
            ctx.log.info("no_games_today", date=str(today))
            return
        if now_et.time() >= first_tip:
            # A move after the first tip is next-day business on ESPN, and the list is about tonight
            ctx.log.info("after_first_tip", first_game=str(first_tip), current_time=str(now_et.time()))
            return
        if not self.backend_client.enabled:
            ctx.log.warning("backend_not_configured", detail="BACKEND_INTERNAL_URL is unset; no daily actions made")
            return

        teams = self._teams_at_act()
        ctx.log.info("teams_at_act", count=len(teams))
        for user, team, recipient in teams:
            try:
                self._process_team(ctx, user, team, recipient, today)
                time_mod.sleep(1)    # pace the backend: each call reads ESPN and may write
            except Exception as e:
                ctx.log.warning("daily_actions_team_error", user_id=user.user_id, team_id=team.team_id, error=str(e))
                ctx.increment_failed(1, type(e).__name__)

    def _process_team(self, ctx: PipelineContext, user: User, team: Team, recipient, today) -> None:
        if json.loads(team.league_info).get("provider", "espn") != "espn":
            return
        if self._already_settled_today(user, team, today):
            ctx.log.debug("already_settled", user_id=user.user_id, team_id=team.team_id)
            return

        run = self.backend_client.run_daily_actions(
            team_id=team.team_id, user_id=user.user_id, nba_date=today,
            correlation_id=get_correlation_id() or None,
        )
        fields = dict(user_id=user.user_id, team_id=team.team_id, outcome=run.outcome, reason=run.reason)

        if run.outcome in EMAILED:
            result = self.notification_service.send_daily_actions_result(user=user, team=team, result=run.as_dict(),
                                                                         prefs=recipient)
            # The moves stand whether or not the email went out, so today settles either way: a
            # `skipped` row keeps the email's error and leaves sent_at null
            self._upsert_log(user, team, today, status="sent" if result.success else "skipped",
                             alert_data=self._alert_data(run), resend_message_id=result.message_id,
                             error_message=result.error)
            ctx.increment_records()
            if not result.success:
                ctx.log.warning("daily_actions_email_failed", error=result.error, **fields)
            ctx.log.info("daily_actions_made" if run.outcome == "applied" else "daily_actions_refused",
                         ir_moves=len(run.ir_moves), adds=len(run.adds), held=run.held, **fields)
            return

        if run.outcome in ("noop", "skipped"):
            self._upsert_log(user, team, today, status="skipped", alert_data=self._alert_data(run),
                             error_message=run.reason)
            ctx.increment_skipped(1, run.reason or run.outcome)
            ctx.log.debug("daily_actions_nothing_made", held=run.held, **fields)
            return

        # failed (the board or the writer couldn't be reached) or unavailable: the next tick retries
        self._upsert_log(user, team, today, status="failed", alert_data=self._alert_data(run),
                         error_message=run.reason or run.error or run.outcome)
        ctx.increment_failed(1, "backend_unavailable" if run.outcome == OUTCOME_UNAVAILABLE else "daily_actions_failed")
        ctx.log.warning("daily_actions_failed", error=run.error, **fields)

    @staticmethod
    def _alert_data(run: DailyActionsRun) -> str:
        data = {"outcome": run.outcome, "ir_moves": run.ir_moves, "adds": run.adds, "open_spots": run.open_spots}
        if run.reason:
            data["reason"] = run.reason
        if run.held:
            data["held"] = run.held
        return json.dumps(data)

    # ---- which teams ----------------------------------------------------------------

    @staticmethod
    def _teams_at_act() -> list[tuple]:
        """(user, team, recipient prefs) for every team whose effective Daily actions level is act:
        the team's override, else its account's. `recipient` carries the address the email goes to
        (the team's own, else the account's, else none: the user's)."""
        account = {int(p.user_id): p for p in NotificationPreference.select()}
        overrides = {(int(o.user_id), int(o.team_id)): o for o in NotificationTeamPreference.select()}
        users = {u for u, p in account.items() if p.daily_actions_level == ACT}
        users |= {u for (u, _), o in overrides.items() if o.daily_actions_level == ACT}
        if not users:
            return []

        class _Recipient:
            def __init__(self, email):
                self.email = email

        out = []
        people = {int(u.user_id): u for u in User.select().where(User.user_id.in_(sorted(users)))}
        for team in Team.select().where(Team.user_id.in_(sorted(users))).order_by(Team.team_id):
            user_id = int(team.user_id_id)
            user = people.get(user_id)
            if user is None:
                continue
            override = overrides.get((user_id, int(team.team_id)))
            prefs = account.get(user_id)
            own = override.daily_actions_level if override is not None else None
            level = own if own is not None else (prefs.daily_actions_level if prefs is not None else None)
            if level != ACT:
                continue
            email = (override.email if override is not None and override.email else None) or \
                (prefs.email if prefs is not None and prefs.email else None)
            out.append((user, team, _Recipient(email)))
        return out

    # ---- notification_log -----------------------------------------------------------

    @staticmethod
    def _already_settled_today(user: User, team: Team, today) -> bool:
        return (
            NotificationLog.select()
            .where(
                (NotificationLog.user == user.user_id)
                & (NotificationLog.team_id == team.team_id)
                & (NotificationLog.notification_type == NOTIFICATION_TYPE)
                & (NotificationLog.notification_date == today)
                & (NotificationLog.status.in_(list(SETTLED_STATUSES)))
            )
            .exists()
        )

    @staticmethod
    def _upsert_log(user: User, team: Team, today, *, status: str, alert_data=None, resend_message_id=None,
                    error_message=None) -> NotificationLog:
        """Today's row for the team: updated in place when one exists (the unique index on user, team,
        type and day means a retry after `failed` must update, never insert)."""
        sent_at = datetime.utcnow() if status == "sent" else None
        existing = NotificationLog.get_or_none(
            (NotificationLog.user == user.user_id)
            & (NotificationLog.team_id == team.team_id)
            & (NotificationLog.notification_type == NOTIFICATION_TYPE)
            & (NotificationLog.notification_date == today)
        )
        if existing is not None:
            existing.status = status
            existing.alert_data = alert_data
            existing.resend_message_id = resend_message_id
            existing.error_message = error_message
            existing.sent_at = sent_at
            existing.save()
            return existing
        return NotificationLog.create(user=user.user_id, team_id=team.team_id, notification_type=NOTIFICATION_TYPE,
                                      notification_date=today, alert_data=alert_data, status=status,
                                      resend_message_id=resend_message_id, error_message=error_message,
                                      sent_at=sent_at)
