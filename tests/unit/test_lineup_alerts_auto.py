"""
`pipelines.lineup_alerts` after the backend took over detection: one
`evaluate_lineup` call per opted-in ESPN team, then a branch per outcome.

The Peewee models are bound to an in-memory SQLite database (schema stripped —
SQLite has no `usr.` namespace) so the dedup query and the `_upsert_log`
retry path run against the real unique index on
(user, team_id, notification_type, notification_date): a retry that inserted a
second row instead of updating the `failed` one would raise IntegrityError here
exactly as it would in Postgres. The backend client and the notification
service are fakes.
"""

import json
from datetime import date, time

import pytest
from freezegun import freeze_time
from peewee import SqliteDatabase

from db.models.notifications import NotificationLog, NotificationPreference, NotificationTeamPreference
from db.models.provider_connections import ProviderConnection
from db.models.teams import Team
from db.models.users import User
from pipelines import lineup_alerts as module
from pipelines.context import PipelineContext
from pipelines.lineup_alerts import LineupAlertsPipeline
from services.backend_client import LineupEvaluation
from services.notification_service import NotificationResult

MODELS = [User, ProviderConnection, Team, NotificationPreference, NotificationLog, NotificationTeamPreference]

TODAY = date(2026, 3, 4)
NOW_ET = time(18, 0)
FIRST_GAME = time(19, 30)  # 90-minute default window: [18:00, 19:15]

MOVES = [
    {"player_id": 1, "name": "Bench Guy", "from_slot_id": 12, "from_slot": "BE", "to_slot_id": 11,
     "to_slot": "UT", "role": "start", "note": "vs LAL · 7:30 PM"},
    {"player_id": 2, "name": "Idle Starter", "from_slot_id": 11, "from_slot": "UT", "to_slot_id": 12,
     "to_slot": "BE", "role": "bench", "note": "no game today"},
]
UNFILLED = [{"player_id": 3, "name": "Third Man", "slot": "BE", "reason": "no eligible open slot"}]


def evaluation(outcome, reason=None, moves=None, unfilled=None, verified=None, error=None):
    return LineupEvaluation(
        outcome=outcome,
        reason=reason,
        moves=MOVES if moves is None else moves,
        unfilled=UNFILLED if unfilled is None else unfilled,
        verified=verified,
        scoring_period_id=134,
        nba_date=TODAY.isoformat(),
        first_game_time_et="19:30",
        team_name="Test Team",
        error=error,
    )


# ---- fakes ---------------------------------------------------------------------------


class FakeBackendClient:
    def __init__(self, *evaluations, enabled=True):
        self.responses = list(evaluations)
        self.calls = []
        self.enabled = enabled
        self.base_url = "http://api.railway.internal:8080" if enabled else None

    def evaluate_lineup(self, **kwargs):
        self.calls.append(kwargs)
        if len(self.responses) > 1:
            return self.responses.pop(0)
        return self.responses[0]


class FakeNotificationService:
    def __init__(self, succeed=True):
        self.succeed = succeed
        self.alerts = []
        self.summaries = []

    def _result(self):
        if self.succeed:
            return NotificationResult(success=True, message_id="msg-1")
        return NotificationResult(success=False, error="resend down")

    def send_lineup_alert(self, user, team, moves, unfilled, first_game_time, prefs=None, extra_lines=None):
        self.alerts.append(dict(user=user, team=team, moves=moves, unfilled=unfilled,
                                first_game_time=first_game_time, prefs=prefs, extra_lines=extra_lines))
        return self._result()

    def send_auto_lineup_summary(self, user, team, moves, unfilled, first_game_time, prefs=None, verified=None):
        self.summaries.append(dict(user=user, team=team, moves=moves, unfilled=unfilled,
                                   first_game_time=first_game_time, prefs=prefs, verified=verified))
        return self._result()


# ---- fixtures ------------------------------------------------------------------------


@pytest.fixture
def sqlite_models():
    """Bind the real models to in-memory SQLite (schema stripped) for the test."""
    saved = {model: model._meta.schema for model in MODELS}
    for model in MODELS:
        model._meta.schema = None
    db = SqliteDatabase(":memory:")
    try:
        with db.bind_ctx(MODELS):
            db.create_tables(MODELS)
            yield db
    finally:
        for model, schema in saved.items():
            model._meta.schema = schema


@pytest.fixture
def pipeline(sqlite_models, monkeypatch):
    monkeypatch.setattr(module.time_mod, "sleep", lambda _s: None)
    p = LineupAlertsPipeline()
    p.backend_client = FakeBackendClient(evaluation("noop", moves=[], unfilled=[]))
    p.notification_service = FakeNotificationService()
    return p


def make_user(email="fan@example.com", **prefs):
    user = User.create(email=email)
    defaults = dict(user=user.user_id, lineup_alerts_enabled=True, alert_minutes_before=90)
    defaults.update(prefs)
    pref = NotificationPreference.create(**defaults)
    return user, pref


def make_team(user, provider="espn", team_name="Test Team"):
    return Team.create(
        user_id=user.user_id,
        team_identifier="t",
        league_info=json.dumps({"provider": provider, "league_id": 1, "team_name": team_name}),
    )


def process(pipeline, user, team, pref, ctx=None):
    ctx = ctx or PipelineContext("lineup_alerts")
    pipeline._process_team(ctx, user, team, pref, {"LAL", "BOS"}, TODAY, NOW_ET, FIRST_GAME)
    return ctx


def logs(team):
    return list(NotificationLog.select().where(NotificationLog.team_id == team.team_id))


# ---- outcome branches ----------------------------------------------------------------


@pytest.mark.unit
def test_planned_emails_the_plan_and_logs_lineup_alert_sent(pipeline):
    user, pref = make_user()
    team = make_team(user)
    pipeline.backend_client = FakeBackendClient(evaluation("planned"))

    ctx = process(pipeline, user, team, pref)

    call = pipeline.backend_client.calls[0]
    assert call == dict(team_id=team.team_id, user_id=user.user_id, nba_date=TODAY, apply=False, correlation_id=None)

    alert = pipeline.notification_service.alerts[0]
    assert alert["moves"] == MOVES and alert["unfilled"] == UNFILLED
    assert alert["first_game_time"] == FIRST_GAME and alert["extra_lines"] is None
    assert pipeline.notification_service.summaries == []

    (row,) = logs(team)
    assert (row.notification_type, row.status) == ("lineup_alert", "sent")
    assert row.resend_message_id == "msg-1" and row.sent_at is not None
    assert json.loads(row.alert_data) == {"moves": MOVES, "unfilled": UNFILLED}
    assert (ctx.records_processed, ctx.records_failed) == (1, 0)


@pytest.mark.unit
def test_planned_with_bounced_email_logs_failed_and_counts_it(pipeline):
    user, pref = make_user()
    team = make_team(user)
    pipeline.backend_client = FakeBackendClient(evaluation("planned"))
    pipeline.notification_service = FakeNotificationService(succeed=False)

    ctx = process(pipeline, user, team, pref)

    (row,) = logs(team)
    assert (row.status, row.error_message, row.sent_at) == ("failed", "resend down", None)
    assert (ctx.records_processed, ctx.records_failed) == (0, 1)
    assert ctx.failure_reasons == {"email_failed": 1}


@pytest.mark.unit
def test_applied_sends_summary_logs_auto_lineup_and_counts_a_record(pipeline):
    user, pref = make_user(auto_lineup_enabled=True)
    team = make_team(user)
    pipeline.backend_client = FakeBackendClient(evaluation("applied", verified=True))

    ctx = process(pipeline, user, team, pref)

    assert pipeline.backend_client.calls[0]["apply"] is True
    assert pipeline.notification_service.alerts == []
    summary = pipeline.notification_service.summaries[0]
    assert summary["moves"] == MOVES and summary["verified"] is True

    (row,) = logs(team)
    assert (row.notification_type, row.status) == ("auto_lineup", "sent")
    assert json.loads(row.alert_data) == {"moves": MOVES, "unfilled": UNFILLED, "verified": True}
    assert ctx.records_processed == 1


@pytest.mark.unit
def test_applied_counts_the_record_even_when_the_summary_email_fails(pipeline):
    user, pref = make_user(auto_lineup_enabled=True)
    team = make_team(user)
    pipeline.backend_client = FakeBackendClient(evaluation("applied", verified=False))
    pipeline.notification_service = FakeNotificationService(succeed=False)

    ctx = process(pipeline, user, team, pref)

    (row,) = logs(team)
    assert (row.notification_type, row.status, row.error_message) == ("auto_lineup", "failed", "resend down")
    assert ctx.records_processed == 1  # the lineup was set regardless of the email
    assert pipeline.notification_service.summaries[0]["verified"] is False


@pytest.mark.unit
def test_team_override_turns_auto_lineup_on(pipeline):
    user, pref = make_user(auto_lineup_enabled=False)
    team = make_team(user)
    NotificationTeamPreference.create(user=user.user_id, team_id=team.team_id, auto_lineup_enabled=True)
    pipeline.backend_client = FakeBackendClient(evaluation("noop", moves=[], unfilled=[]))

    process(pipeline, user, team, pref)

    assert pipeline.backend_client.calls[0]["apply"] is True


@pytest.mark.unit
def test_noop_logs_skipped_and_sends_nothing(pipeline):
    user, pref = make_user()
    team = make_team(user)
    pipeline.backend_client = FakeBackendClient(evaluation("noop", moves=[], unfilled=[]))

    ctx = process(pipeline, user, team, pref)

    assert pipeline.notification_service.alerts == [] and pipeline.notification_service.summaries == []
    (row,) = logs(team)
    assert (row.notification_type, row.status) == ("lineup_alert", "skipped")
    assert json.loads(row.alert_data) == {"reason": "nothing_actionable"}
    assert (ctx.records_processed, ctx.records_failed, ctx.records_skipped) == (0, 0, 1)


@pytest.mark.unit
def test_rejected_emails_the_plan_with_the_reason_line(pipeline):
    user, pref = make_user(auto_lineup_enabled=True)
    team = make_team(user)
    pipeline.backend_client = FakeBackendClient(evaluation("rejected", reason="Player is locked"))

    ctx = process(pipeline, user, team, pref)

    alert = pipeline.notification_service.alerts[0]
    assert alert["moves"] == MOVES
    assert alert["extra_lines"] == ["Auto-lineup could not apply these moves: Player is locked"]
    (row,) = logs(team)
    assert (row.notification_type, row.status) == ("lineup_alert", "sent")
    assert json.loads(row.alert_data)["outcome"] == "rejected"
    assert json.loads(row.alert_data)["reason"] == "Player is locked"
    assert ctx.records_processed == 1


@pytest.mark.unit
@pytest.mark.parametrize("reason", ["PROVIDER_AUTH_EXPIRED", "espn cookies expired", "Auth rejected by ESPN"])
def test_auth_failures_add_the_reconnect_hint(pipeline, reason):
    user, pref = make_user(auto_lineup_enabled=True)
    team = make_team(user)
    pipeline.backend_client = FakeBackendClient(evaluation("failed", reason=reason))

    process(pipeline, user, team, pref)

    extra = pipeline.notification_service.alerts[0]["extra_lines"]
    assert extra[0] == f"Auto-lineup could not apply these moves: {reason}"
    assert extra[1] == "Your ESPN connection may have expired — reconnect it in Settings."


@pytest.mark.unit
def test_unavailable_logs_failed_without_email_and_counts_toward_the_ops_alert(pipeline):
    user, pref = make_user()
    team = make_team(user)
    pipeline.backend_client = FakeBackendClient(
        evaluation("unavailable", reason="timeout", moves=[], unfilled=[], error="read timed out")
    )

    ctx = process(pipeline, user, team, pref)

    assert pipeline.notification_service.alerts == [] and pipeline.notification_service.summaries == []
    (row,) = logs(team)
    assert (row.notification_type, row.status, row.error_message) == ("lineup_alert", "failed", "timeout")
    assert pipeline._unavailable_count == 1
    assert ctx.records_failed == 1 and ctx.failure_reasons == {"backend_unavailable": 1}


@pytest.mark.unit
def test_skipped_settles_the_day_as_skipped_and_is_not_a_failure(pipeline):
    user, pref = make_user()
    team = make_team(user)
    pipeline.backend_client = FakeBackendClient(
        evaluation("skipped", reason="can_write:no_credentials", moves=[], unfilled=[])
    )

    ctx = process(pipeline, user, team, pref)

    (row,) = logs(team)
    assert (row.status, row.error_message) == ("skipped", "can_write:no_credentials")
    assert pipeline.notification_service.alerts == []
    assert pipeline._unavailable_count == 0
    assert ctx.records_failed == 0
    assert ctx.skip_reasons == {"backend_skipped:can_write:no_credentials": 1}


# ---- gating before the backend call --------------------------------------------------


@pytest.mark.unit
def test_non_espn_teams_never_reach_the_backend(pipeline):
    user, pref = make_user()
    team = make_team(user, provider="yahoo")
    process(pipeline, user, team, pref)
    assert pipeline.backend_client.calls == [] and logs(team) == []


@pytest.mark.unit
def test_team_level_disable_skips_the_team(pipeline):
    user, pref = make_user()
    team = make_team(user)
    NotificationTeamPreference.create(user=user.user_id, team_id=team.team_id, lineup_alerts_enabled=False)
    process(pipeline, user, team, pref)
    assert pipeline.backend_client.calls == [] and logs(team) == []


@pytest.mark.unit
def test_outside_the_team_window_skips_the_team(pipeline):
    user, pref = make_user(alert_minutes_before=30)  # window [19:00, 19:15]; now is 18:00
    team = make_team(user)
    process(pipeline, user, team, pref)
    assert pipeline.backend_client.calls == []


@pytest.mark.unit
def test_effective_prefs_merge_includes_auto_lineup(pipeline):
    user, pref = make_user(auto_lineup_enabled=False, alert_minutes_before=90)
    override = NotificationTeamPreference(user=user.user_id, team_id=1, auto_lineup_enabled=True, alert_minutes_before=None)
    merged = pipeline._get_effective_prefs(pref, override)
    assert merged.auto_lineup_enabled is True
    assert merged.alert_minutes_before == 90
    assert merged.lineup_alerts_enabled is True


# ---- dedup + upsert ------------------------------------------------------------------


@pytest.mark.unit
@pytest.mark.parametrize("notification_type,status", [
    ("lineup_alert", "sent"),
    ("lineup_alert", "skipped"),
    ("auto_lineup", "sent"),
])
def test_a_settled_row_of_either_type_dedups_the_team(pipeline, notification_type, status):
    user, pref = make_user()
    team = make_team(user)
    NotificationLog.create(user=user.user_id, team_id=team.team_id, notification_type=notification_type,
                           notification_date=TODAY, status=status)
    pipeline.backend_client = FakeBackendClient(evaluation("planned"))

    process(pipeline, user, team, pref)

    assert pipeline.backend_client.calls == []
    assert pipeline.notification_service.alerts == []
    assert len(logs(team)) == 1


@pytest.mark.unit
def test_a_failed_row_is_retried_and_updated_in_place(pipeline):
    user, pref = make_user()
    team = make_team(user)
    NotificationLog.create(user=user.user_id, team_id=team.team_id, notification_type="lineup_alert",
                           notification_date=TODAY, status="failed", error_message="timeout")
    pipeline.backend_client = FakeBackendClient(evaluation("planned"))

    process(pipeline, user, team, pref)

    assert len(pipeline.backend_client.calls) == 1
    assert len(pipeline.notification_service.alerts) == 1
    (row,) = logs(team)  # one row, not two — the unique index would have raised otherwise
    assert (row.status, row.error_message, row.resend_message_id) == ("sent", None, "msg-1")
    assert row.sent_at is not None
    assert json.loads(row.alert_data) == {"moves": MOVES, "unfilled": UNFILLED}


@pytest.mark.unit
def test_yesterdays_row_does_not_dedup_today(pipeline):
    user, pref = make_user()
    team = make_team(user)
    NotificationLog.create(user=user.user_id, team_id=team.team_id, notification_type="lineup_alert",
                           notification_date=date(2026, 3, 3), status="sent")
    pipeline.backend_client = FakeBackendClient(evaluation("planned"))

    process(pipeline, user, team, pref)

    assert len(pipeline.backend_client.calls) == 1
    assert len(logs(team)) == 2


@pytest.mark.unit
def test_two_unavailable_polls_then_success_leave_one_row(pipeline):
    user, pref = make_user()
    team = make_team(user)
    pipeline.backend_client = FakeBackendClient(
        evaluation("unavailable", reason="ConnectionError", moves=[], unfilled=[]),
        evaluation("unavailable", reason="http_503", moves=[], unfilled=[]),
        evaluation("noop", moves=[], unfilled=[]),
    )

    for _ in range(3):
        process(pipeline, user, team, pref)
    process(pipeline, user, team, pref)  # settled now: no fourth call

    assert len(pipeline.backend_client.calls) == 3
    (row,) = logs(team)
    assert (row.status, row.error_message) == ("skipped", None)


# ---- execute(): outer gating, backend_not_configured, ops alert ------------------------


@pytest.fixture
def in_window(monkeypatch):
    """18:00 ET on 2026-03-04 (EST): inside the 150-minute outer window before a 19:30 tip."""
    monkeypatch.setattr(module.Game, "get_earliest_game_time_on_date", classmethod(lambda cls, d: FIRST_GAME))
    monkeypatch.setattr(module.Game, "get_teams_playing_on_date", classmethod(lambda cls, d: {"LAL", "BOS"}))
    with freeze_time("2026-03-04T23:00:00Z"):
        yield


@pytest.mark.unit
def test_execute_returns_before_the_user_loop_when_backend_is_not_configured(pipeline, in_window, monkeypatch):
    user, pref = make_user()
    make_team(user)
    pipeline.backend_client = FakeBackendClient(enabled=False)
    monkeypatch.setattr(pipeline, "_get_eligible_users", lambda: pytest.fail("user loop entered"))

    ctx = PipelineContext("lineup_alerts")
    pipeline.execute(ctx)

    assert pipeline.backend_client.calls == []
    assert NotificationLog.select().count() == 0


@pytest.mark.unit
def test_execute_processes_every_espn_team_of_every_opted_in_user(pipeline, in_window):
    user, _ = make_user()
    make_team(user)
    make_team(user, provider="yahoo")
    other, _ = make_user(email="other@example.com", lineup_alerts_enabled=False)
    make_team(other)
    pipeline.backend_client = FakeBackendClient(evaluation("planned"))

    ctx = PipelineContext("lineup_alerts")
    pipeline.execute(ctx)

    assert [c["user_id"] for c in pipeline.backend_client.calls] == [user.user_id]
    assert ctx.records_processed == 1


@pytest.mark.unit
def test_three_unavailable_teams_raise_the_ops_alert(pipeline, in_window, alerts):
    user, _ = make_user()
    for _ in range(3):
        make_team(user)
    pipeline.backend_client = FakeBackendClient(
        evaluation("unavailable", reason="ConnectionError", moves=[], unfilled=[], error="refused")
    )

    ctx = PipelineContext("lineup_alerts")
    pipeline.execute(ctx)

    assert pipeline._unavailable_count == 3
    assert ctx.records_failed == 3
    assert alerts.keys() == ["lineup_alerts_backend_unavailable"]
    event = alerts.events[0]
    assert event.severity == "warning"
    assert event.dedupe.total_seconds() == 6 * 3600
    assert event.fields["unavailable"] == 3
    assert event.fields["backend_url"] == "http://api.railway.internal:8080"


@pytest.mark.unit
def test_two_unavailable_teams_do_not_alert_and_the_counter_resets_per_run(pipeline, in_window, alerts):
    user, _ = make_user()
    for _ in range(2):
        make_team(user)
    pipeline.backend_client = FakeBackendClient(
        evaluation("unavailable", reason="timeout", moves=[], unfilled=[])
    )

    pipeline.execute(PipelineContext("lineup_alerts"))
    assert pipeline._unavailable_count == 2
    assert alerts.events == []

    pipeline.backend_client = FakeBackendClient(evaluation("noop", moves=[], unfilled=[]))
    pipeline.execute(PipelineContext("lineup_alerts"))
    assert pipeline._unavailable_count == 0


@pytest.mark.unit
def test_a_team_that_raises_is_counted_and_the_run_continues(pipeline, in_window):
    user, _ = make_user()
    make_team(user)
    good = make_team(user)
    calls = []

    def flaky(**kwargs):
        calls.append(kwargs)
        if len(calls) == 1:
            raise RuntimeError("boom")
        return evaluation("noop", moves=[], unfilled=[])

    pipeline.backend_client.evaluate_lineup = flaky

    ctx = PipelineContext("lineup_alerts")
    pipeline.execute(ctx)

    assert len(calls) == 2
    assert ctx.failure_reasons == {"RuntimeError": 1}
    assert [r.status for r in logs(good)] == ["skipped"]
