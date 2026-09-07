"""
Lineup Alerts Pipeline

Asks the backend for each opted-in team's fill plan for today and either emails
it to the user or — for users who turned on auto-lineup — lets the backend
apply it to ESPN and emails what changed. The backend
(`POST /v1/internal/jobs/lineup/evaluate`) owns the ESPN roster read, slot
eligibility, game locks and the planner; this pipeline keeps the schedule
gating, opt-in, per-team overrides, daily dedup, emails and logging.

Self-gates in two stages:
  1. Broad gate: only enters the user loop when within the configured outer window
     (LINEUP_ALERT_WINDOW_MINUTES env var, default 150 min) before first tip-off.
  2. Per-user gate: each team is only evaluated when within that team's configured
     alert_minutes_before window.

Safe to call frequently (every 15 min): one `usr.notification_log` row per
(user, team, type, day) dedups the sent and skipped outcomes, while a `failed`
row (backend unreachable, email bounced) is retried by the next poll and
updated in place. Three or more `unavailable` answers in one run raise the
`lineup_alerts_backend_unavailable` ops alert.
"""

import json
import time as time_mod
from datetime import datetime, timedelta, time

import pytz

from core.logging import get_correlation_id
from core.settings import settings
from db.models.users import User
from db.models.teams import Team
from db.models.nba.games import Game
from db.models.notifications import NotificationPreference, NotificationLog, NotificationTeamPreference
from pipelines.base import BasePipeline
from pipelines.config import PipelineConfig, PipelineCategory
from pipelines.context import PipelineContext
from services.alert_service import AlertEvent, get_alert_service
from services.backend_client import OUTCOME_UNAVAILABLE, LineupEvaluation, backend_client_from_settings
from services.notification_service import NotificationService

# Runs with this many `unavailable` backend answers post the ops alert.
BACKEND_UNAVAILABLE_ALERT_THRESHOLD = 3
BACKEND_UNAVAILABLE_ALERT_DEDUPE = timedelta(hours=6)

# The two notification types that share the one-row-per-team-day dedup.
NOTIFICATION_TYPES = ("lineup_alert", "auto_lineup")
# Log statuses that mean "done for today" — anything else is retried next poll.
SETTLED_STATUSES = ("sent", "skipped")

# A rejected/failed reason that mentions one of these gets the reconnect hint.
_AUTH_REASON_MARKERS = ("auth", "expired", "provider_auth")


class LineupAlertsPipeline(BasePipeline):
    """
    Evaluate opted-in users' lineups before games start.

    This pipeline:
    1. Checks if we're within the notification window (before first game)
    2. Fetches all opted-in users with ESPN teams
    3. For each team, asks the backend for today's fill plan (applying it when
       the user opted into auto-lineup)
    4. Emails the plan (or the applied-moves summary)
    5. Logs one row per team-day for dedup and auditing
    """

    config = PipelineConfig(
        name="lineup_alerts",
        display_name="Lineup Alerts",
        description=(
            "Asks the backend for each opted-in team's fill plan; emails it, "
            "or applies it for auto-lineup users"
        ),
        target_table="usr.notification_log",
        category=PipelineCategory.PRE_GAME,
        skip_batch_dedup=True,
    )

    def __init__(self):
        super().__init__()
        self.backend_client = backend_client_from_settings()
        self.notification_service = NotificationService()
        self._unavailable_count = 0

    def execute(self, ctx: PipelineContext) -> None:
        """Execute the lineup alerts pipeline."""
        self._unavailable_count = 0
        eastern = pytz.timezone("US/Eastern")
        now_et = datetime.now(eastern)
        today = now_et.date()

        # Step 1: Check if there are games today with start times
        earliest_game_time = Game.get_earliest_game_time_on_date(today)
        if not earliest_game_time:
            ctx.log.info("no_games_today", date=str(today))
            return

        # Step 2: Check if we're in the notification window
        now_et_time = now_et.time()
        if not self._in_notification_window(now_et_time, earliest_game_time):
            ctx.log.info(
                "outside_notification_window",
                current_time=str(now_et_time),
                first_game=str(earliest_game_time),
            )
            return

        ctx.log.info(
            "notification_window_active",
            first_game=str(earliest_game_time),
            current_time=str(now_et_time),
        )

        # Step 3: Get teams playing today
        teams_playing = Game.get_teams_playing_on_date(today)
        if not teams_playing:
            ctx.log.info("no_teams_playing", date=str(today))
            return

        # Step 4: Without a backend URL there is nothing this pipeline can do
        if not self.backend_client.enabled:
            ctx.log.warning(
                "backend_not_configured",
                detail="BACKEND_INTERNAL_URL is unset; no lineups evaluated",
            )
            return

        # Step 5: Get all eligible users
        users_with_prefs = self._get_eligible_users()
        ctx.log.info("eligible_users", count=len(users_with_prefs))

        # Step 6: Process each user's teams
        for user, prefs in users_with_prefs:
            teams = list(Team.select().where(Team.user_id == user.user_id))

            for team in teams:
                try:
                    self._process_team(ctx, user, team, prefs, teams_playing, today, now_et_time, earliest_game_time)
                    # Pace the backend (each call may hit ESPN and the writer)
                    time_mod.sleep(1)
                except Exception as e:
                    ctx.log.warning(
                        "team_alert_error",
                        user_id=user.user_id,
                        team_id=team.team_id,
                        error=str(e),
                    )
                    ctx.increment_failed(1, type(e).__name__)
                    continue

        self._alert_if_backend_unavailable(ctx)

    def _process_team(
        self,
        ctx: PipelineContext,
        user: User,
        team: Team,
        prefs,
        teams_playing: set[str],
        today,
        now_et_time: time,
        earliest_game_time,
    ) -> None:
        """Evaluate one team: gate, dedup, ask the backend, email, log.

        Order: provider check -> effective prefs -> team alerts disabled ->
        per-team window -> dedup -> backend evaluate -> outcome branch.
        """
        # Provider comes from the stored JSON only; the backend does the ESPN
        # read, so no credentials are decrypted here.
        league_info = json.loads(team.league_info)
        provider = league_info.get("provider", "espn")

        # ESPN only for now
        if provider != "espn":
            return

        # Look up team-level override and merge with global prefs
        team_pref = (
            NotificationTeamPreference.select()
            .where(
                (NotificationTeamPreference.user == user.user_id)
                & (NotificationTeamPreference.team_id == team.team_id)
            )
            .first()
        )
        effective_prefs = self._get_effective_prefs(prefs, team_pref)

        # If team-level alerts are explicitly disabled, skip this team
        if effective_prefs.lineup_alerts_enabled is False:
            ctx.log.debug("team_alerts_disabled", user_id=user.user_id, team_id=team.team_id)
            return

        # Check if we're within this team's configured alert window
        game_dt = datetime.combine(today, earliest_game_time)
        now_dt = datetime.combine(today, now_et_time)
        user_window_start = game_dt - timedelta(minutes=effective_prefs.alert_minutes_before)
        user_window_end = game_dt - timedelta(minutes=15)
        if not (user_window_start <= now_dt <= user_window_end):
            ctx.log.debug(
                "outside_user_notification_window",
                user_id=user.user_id,
                team_id=team.team_id,
                alert_minutes_before=effective_prefs.alert_minutes_before,
            )
            return

        # Dedup: a sent or skipped row of either type settles the team for today.
        # A `failed` row does not — the next poll retries and updates it in place.
        if self._already_settled_today(user, team, today):
            ctx.log.debug(
                "already_notified",
                user_id=user.user_id,
                team_id=team.team_id,
            )
            return

        apply = bool(getattr(effective_prefs, "auto_lineup_enabled", False))
        evaluation = self.backend_client.evaluate_lineup(
            team_id=team.team_id,
            user_id=user.user_id,
            nba_date=today,
            apply=apply,
            correlation_id=get_correlation_id() or None,
        )

        self._handle_evaluation(ctx, user, team, today, earliest_game_time, effective_prefs, evaluation)

    def _handle_evaluation(
        self,
        ctx: PipelineContext,
        user: User,
        team: Team,
        today,
        earliest_game_time,
        effective_prefs,
        evaluation: LineupEvaluation,
    ) -> None:
        """Branch on the backend's outcome: email, log, count."""
        outcome = evaluation.outcome
        moves, unfilled = evaluation.moves, evaluation.unfilled
        log_fields = dict(user_id=user.user_id, team_id=team.team_id, outcome=outcome, reason=evaluation.reason)

        if outcome == "planned":
            result = self.notification_service.send_lineup_alert(
                user=user,
                team=team,
                moves=moves,
                unfilled=unfilled,
                first_game_time=earliest_game_time,
                prefs=effective_prefs,
            )
            self._record_email(ctx, user, team, today, "lineup_alert", result, evaluation)
            ctx.log.info("alert_sent", move_count=len(moves), success=result.success, **log_fields)
            return

        if outcome == "applied":
            result = self.notification_service.send_auto_lineup_summary(
                user=user,
                team=team,
                moves=moves,
                unfilled=unfilled,
                first_game_time=earliest_game_time,
                prefs=effective_prefs,
                verified=evaluation.verified,
            )
            self._upsert_log(
                user, team, today,
                notification_type="auto_lineup",
                status="sent" if result.success else "failed",
                alert_data=self._alert_data(evaluation),
                resend_message_id=result.message_id,
                error_message=result.error,
            )
            # The lineup was set whether or not the summary email went out.
            ctx.increment_records()
            if not result.success:
                ctx.log.warning("auto_lineup_summary_email_failed", error=result.error, **log_fields)
            ctx.log.info(
                "auto_lineup_applied",
                move_count=len(moves),
                verified=evaluation.verified,
                email_success=result.success,
                **log_fields,
            )
            return

        if outcome == "noop":
            self._upsert_log(
                user, team, today,
                notification_type="lineup_alert",
                status="skipped",
                alert_data=json.dumps({"reason": "nothing_actionable"}),
            )
            ctx.increment_skipped(1, "nothing_actionable")
            ctx.log.debug("nothing_actionable", **log_fields)
            return

        if outcome in ("rejected", "failed"):
            reason = evaluation.reason or evaluation.error or outcome
            extra_lines = [f"Auto-lineup could not apply these moves: {reason}"]
            if any(marker in reason.lower() for marker in _AUTH_REASON_MARKERS):
                extra_lines.append("Your ESPN connection may have expired — reconnect it in Settings.")
            result = self.notification_service.send_lineup_alert(
                user=user,
                team=team,
                moves=moves,
                unfilled=unfilled,
                first_game_time=earliest_game_time,
                prefs=effective_prefs,
                extra_lines=extra_lines,
            )
            self._record_email(ctx, user, team, today, "lineup_alert", result, evaluation)
            ctx.log.warning("auto_lineup_not_applied", move_count=len(moves), email_success=result.success, **log_fields)
            return

        # skipped: the backend answered and cannot act today (no credentials,
        # not the owner, already applied, ...) — a settled answer, so log it as
        # `skipped` and stop asking. unavailable / anything new: the backend could
        # not be asked — a `failed` row so the next poll retries.
        error_message = evaluation.reason or evaluation.error or f"unknown_outcome:{outcome}"
        self._upsert_log(
            user, team, today,
            notification_type="lineup_alert",
            status="skipped" if outcome == "skipped" else "failed",
            alert_data=self._alert_data(evaluation),
            error_message=error_message,
        )
        if outcome == OUTCOME_UNAVAILABLE:
            self._unavailable_count += 1
            ctx.increment_failed(1, "backend_unavailable")
            ctx.log.warning("backend_unavailable", error=evaluation.error, **log_fields)
        elif outcome == "skipped":
            ctx.increment_skipped(1, f"backend_skipped:{evaluation.reason or 'unknown'}")
            ctx.log.info("backend_skipped", **log_fields)
        else:
            ctx.increment_failed(1, "unknown_outcome")
            ctx.log.warning("unknown_backend_outcome", **log_fields)

    def _record_email(
        self,
        ctx: PipelineContext,
        user: User,
        team: Team,
        today,
        notification_type: str,
        result,
        evaluation: LineupEvaluation,
    ) -> None:
        """Log an alert email's outcome and count it (a bounced email is a failed record)."""
        self._upsert_log(
            user, team, today,
            notification_type=notification_type,
            status="sent" if result.success else "failed",
            alert_data=self._alert_data(evaluation),
            resend_message_id=result.message_id,
            error_message=result.error,
        )
        if result.success:
            ctx.increment_records()
        else:
            ctx.increment_failed(1, "email_failed")

    @staticmethod
    def _alert_data(evaluation: LineupEvaluation) -> str:
        data = {"moves": evaluation.moves, "unfilled": evaluation.unfilled}
        if evaluation.outcome not in ("planned", "applied"):
            data["outcome"] = evaluation.outcome
        if evaluation.reason:
            data["reason"] = evaluation.reason
        if evaluation.verified is not None:
            data["verified"] = evaluation.verified
        return json.dumps(data)

    def _alert_if_backend_unavailable(self, ctx: PipelineContext) -> None:
        """Ops alert when the backend was unreachable for three or more teams this run."""
        if self._unavailable_count < BACKEND_UNAVAILABLE_ALERT_THRESHOLD:
            return
        get_alert_service().notify(AlertEvent(
            key="lineup_alerts_backend_unavailable",
            severity="warning",
            title="Lineup alerts: backend unavailable",
            body=(
                f"{self._unavailable_count} team evaluation(s) got no usable answer from the backend "
                f"this run; those teams were logged as failed and will be retried next poll."
            ),
            fields={
                "pipeline": self.config.name,
                "run_id": str(ctx.run_id),
                "unavailable": self._unavailable_count,
                "backend_url": self.backend_client.base_url,
            },
            dedupe=BACKEND_UNAVAILABLE_ALERT_DEDUPE,
        ))

    def _get_effective_prefs(self, global_prefs, team_pref):
        """Merge team override on top of global prefs. Team wins where not None."""
        if team_pref is None:
            return global_prefs

        class _EffectivePrefs:
            pass

        ep = _EffectivePrefs()
        for field in [
            "lineup_alerts_enabled", "alert_benched_starters",
            "alert_active_non_playing", "alert_injured_active",
            "alert_minutes_before", "auto_lineup_enabled", "email",
        ]:
            team_val = getattr(team_pref, field, None)
            setattr(ep, field, team_val if team_val is not None else getattr(global_prefs, field))
        return ep

    def _in_notification_window(
        self,
        now_et: time,
        first_game_time: time,
    ) -> bool:
        """
        Check if current time is within the notification window.

        Window is [first_game - alert_window, first_game - 15 min].
        """
        window_minutes = settings.lineup_alert_window_minutes
        today = datetime.today().date()

        game_dt = datetime.combine(today, first_game_time)
        now_dt = datetime.combine(today, now_et)

        window_start = game_dt - timedelta(minutes=window_minutes)
        window_end = game_dt - timedelta(minutes=15)

        return window_start <= now_dt <= window_end

    def _get_eligible_users(self) -> list[tuple]:
        """
        Get users eligible for lineup alerts.

        Opt-in model: only processes users who have an explicit
        NotificationPreference row with lineup_alerts_enabled=True.
        Users without a row are not processed until they opt in via the UI.
        Auto-lineup is a sub-feature of alerts, so the same list covers it.

        Returns list of (User, NotificationPreference) tuples.
        """
        enabled_prefs = list(
            NotificationPreference.select()
            .where(NotificationPreference.lineup_alerts_enabled == True)
        )

        result = []
        for pref in enabled_prefs:
            user = User.select().where(User.user_id == pref.user_id).first()
            if user:
                result.append((user, pref))

        return result

    # ---- notification_log ---------------------------------------------------------

    @staticmethod
    def _already_settled_today(user: User, team: Team, today) -> bool:
        """True when a sent/skipped row of either notification type exists for today."""
        return (
            NotificationLog.select()
            .where(
                (NotificationLog.user == user.user_id)
                & (NotificationLog.team_id == team.team_id)
                & (NotificationLog.notification_type.in_(list(NOTIFICATION_TYPES)))
                & (NotificationLog.notification_date == today)
                & (NotificationLog.status.in_(list(SETTLED_STATUSES)))
            )
            .exists()
        )

    def _upsert_log(
        self,
        user: User,
        team: Team,
        today,
        notification_type: str = "lineup_alert",
        status: str = "pending",
        alert_data: str | None = None,
        resend_message_id: str | None = None,
        error_message: str | None = None,
    ) -> NotificationLog:
        """Write today's row for (user, team, type): update the existing one or create it.

        The unique index on (user, team_id, notification_type, notification_date)
        means a retry after a `failed` row must update, never insert.
        """
        sent_at = datetime.utcnow() if status == "sent" else None
        existing = NotificationLog.get_or_none(
            (NotificationLog.user == user.user_id)
            & (NotificationLog.team_id == team.team_id)
            & (NotificationLog.notification_type == notification_type)
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

        return NotificationLog.create(
            user=user.user_id,
            team_id=team.team_id,
            notification_type=notification_type,
            notification_date=today,
            alert_data=alert_data,
            status=status,
            resend_message_id=resend_message_id,
            error_message=error_message,
            sent_at=sent_at,
        )
