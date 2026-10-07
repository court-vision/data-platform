"""
`pipelines.scheduled_pickups`: one backend call per tick, then a counter and (for
a settled row) an email per result. The backend client, the recipient lookup and
the notification service are fakes; the context is real so the counters are the
ones the run record and `pipeline_partial` read. The recipient lookup itself runs
against the real models bound to in-memory SQLite (schema stripped).
"""

import json
from types import SimpleNamespace

import pytest
from peewee import SqliteDatabase

from db.models.notifications import NotificationPreference, NotificationTeamPreference
from db.models.provider_connections import ProviderConnection
from db.models.teams import Team
from db.models.users import User
from pipelines import scheduled_pickups as module
from pipelines.context import PipelineContext
from pipelines.scheduled_pickups import ScheduledPickupsPipeline
from services.backend_client import PickupExecution, PickupsRun
from services.notification_service import NotificationResult, NotificationService

MODELS = [User, ProviderConnection, Team, NotificationPreference, NotificationTeamPreference]


def execution(pickup_id, outcome, reason=None, **over):
    fields = dict(pickup_id=pickup_id, team_id=21, user_id=11, outcome=outcome, reason=reason, team_name="GloatingSoap369",
                  add={"player_id": 6450, "name": "Kawhi Leonard", "team": "LAC"},
                  drop={"player_id": 4594268, "name": "Anthony Edwards", "team": "MIN"},
                  nba_date="2026-10-22", scoring_period_id=3)
    fields.update(over)
    return PickupExecution(**fields)


class FakeBackendClient:
    def __init__(self, run):
        self.run = run
        self.calls = []
        self.base_url = "http://api.railway.internal:8080"

    def execute_pickups(self, **kwargs):
        self.calls.append(kwargs)
        return self.run


class FakeNotificationService:
    def __init__(self, succeed=True):
        self.succeed = succeed
        self.sent = []

    def send_scheduled_pickup_result(self, user, team, result, prefs=None):
        self.sent.append(dict(user=user, team=team, result=result, prefs=prefs))
        return NotificationResult(success=True, message_id="m") if self.succeed else NotificationResult(success=False, error="resend down")


@pytest.fixture
def pipeline(monkeypatch):
    monkeypatch.setattr(module.settings, "pickups_per_run", 4)
    monkeypatch.setattr(module, "get_correlation_id", lambda: "corr-1")
    user, team, prefs = SimpleNamespace(email="u@x.dev"), SimpleNamespace(league_info="{}"), SimpleNamespace(email=None)
    monkeypatch.setattr(module, "_lookup_recipient", lambda user_id, team_id: (user, team, prefs) if user_id == 11 else None)
    p = ScheduledPickupsPipeline()
    p.notification_service = FakeNotificationService()
    p.recipient = (user, team, prefs)
    return p


def run(pipeline, backend_run):
    pipeline.backend_client = FakeBackendClient(backend_run)
    ctx = PipelineContext("scheduled_pickups")
    pipeline.execute(ctx)
    return ctx


@pytest.mark.unit
def test_each_settled_result_is_counted_and_emailed_once(pipeline):
    backend = PickupsRun(ok=True, due=4, results=[
        execution(1, "executed", seated_slot="PG", verified=True),
        execution(2, "skipped", "unavailable"),
        execution(3, "failed", "espn_rejected", detail="Roster is full."),
        execution(4, "deferred", "add_locked", next_attempt_at="2026-10-22T06:00:00+00:00"),
    ])
    ctx = run(pipeline, backend)

    assert pipeline.backend_client.calls == [{"limit": 4, "correlation_id": "corr-1"}]
    assert (ctx.records_processed, ctx.records_skipped, ctx.records_failed) == (1, 3, 0)
    sent = pipeline.notification_service.sent
    assert [s["result"]["pickup_id"] for s in sent] == [1, 2, 3]          # the deferred row waits silently
    assert sent[0]["user"] is pipeline.recipient[0] and sent[0]["prefs"] is pipeline.recipient[2]
    assert sent[0]["result"]["seated_slot"] == "PG" and sent[2]["result"]["detail"] == "Roster is full."


@pytest.mark.unit
def test_a_business_failure_is_never_a_pipeline_failure(pipeline, alerts):
    ctx = run(pipeline, PickupsRun(ok=True, due=1, results=[execution(1, "failed", "auth_expired")]))
    assert (ctx.records_processed, ctx.records_skipped, ctx.records_failed) == (0, 1, 0)
    assert alerts.keys() == []


@pytest.mark.unit
def test_a_missing_recipient_or_a_bounced_email_is_counted_not_raised(pipeline):
    pipeline.notification_service = FakeNotificationService(succeed=False)
    ctx = run(pipeline, PickupsRun(ok=True, due=2, results=[
        execution(1, "executed"), execution(2, "executed", user_id=99),
    ]))
    assert (ctx.records_processed, ctx.records_skipped) == (2, 2)   # email_failed + email_no_recipient
    assert len(pipeline.notification_service.sent) == 1


@pytest.mark.unit
def test_an_unreachable_backend_fails_the_run_and_alerts_once(pipeline, alerts):
    ctx = run(pipeline, PickupsRun.unavailable("timeout", error="read timed out"))
    assert (ctx.records_processed, ctx.records_failed) == (0, 1)
    assert pipeline.notification_service.sent == []
    assert alerts.keys() == ["scheduled_pickups_backend_unavailable"]
    event = alerts.events[0]
    assert event.severity == "warning" and event.fields["reason"] == "timeout"
    run(pipeline, PickupsRun.unavailable("timeout"))
    assert len(alerts.events) == 1                                      # deduped


@pytest.mark.unit
def test_an_empty_tick_does_nothing(pipeline):
    ctx = run(pipeline, PickupsRun(ok=True, due=0, results=[]))
    assert (ctx.records_processed, ctx.records_skipped, ctx.records_failed) == (0, 0, 0)
    assert pipeline.notification_service.sent == []


# ---- the recipient lookup, against the real models -------------------------------------


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


def make_team(user):
    return Team.create(user_id=user.user_id, team_identifier="t",
                       league_info=json.dumps({"provider": "espn", "league_id": 1, "team_name": "GloatingSoap369"}))


def address(user, team):
    """Where the pickup email for `team` goes: the lookup, then the service's own fallback."""
    found_user, found_team, prefs = module._lookup_recipient(user.user_id, team.team_id)
    assert (found_user.user_id, found_team.team_id) == (user.user_id, team.team_id)
    return NotificationService._recipient(found_user, prefs)


@pytest.mark.unit
def test_the_teams_own_email_wins_over_the_global_override_and_the_account(sqlite_models):
    user = User.create(email="fan@example.com")
    team = make_team(user)
    NotificationPreference.create(user=user.user_id, email="global@example.com")
    NotificationTeamPreference.create(user=user.user_id, team_id=team.team_id, email="team@example.com")

    assert address(user, team) == "team@example.com"


@pytest.mark.unit
def test_without_a_team_email_the_global_override_then_the_account_email_is_used(sqlite_models):
    user = User.create(email="fan@example.com")
    team, other = make_team(user), make_team(user)
    NotificationTeamPreference.create(user=user.user_id, team_id=other.team_id, email="other@example.com")
    NotificationTeamPreference.create(user=user.user_id, team_id=team.team_id, auto_lineup_enabled=True)  # no email
    assert address(user, team) == "fan@example.com"

    NotificationPreference.create(user=user.user_id, email="global@example.com")
    assert address(user, team) == "global@example.com"
    assert address(user, other) == "other@example.com"


@pytest.mark.unit
def test_a_row_whose_owner_or_team_is_gone_has_no_recipient(sqlite_models):
    user = User.create(email="fan@example.com")
    team = make_team(user)
    assert module._lookup_recipient(user.user_id + 1, team.team_id) is None
    assert module._lookup_recipient(user.user_id, team.team_id + 1) is None
