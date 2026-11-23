"""
`pipelines.daily_actions` (the Daily actions row's Act level): which teams are at Act,
one `run_daily_actions` call per team before the first tip, then a branch per outcome,
plus the backend client's parse and the email.

The Peewee models are bound to an in-memory SQLite database (schema stripped), so the
team selection, the dedup query and the `_upsert_log` retry path run against the real
unique index on (user, team_id, notification_type, notification_date). The backend
client and the notification service are fakes.
"""

import json
from datetime import date, datetime, time

import pytest
import pytz
import requests
from freezegun import freeze_time
from peewee import SqliteDatabase

from db.models.notifications import NotificationLog, NotificationPreference, NotificationTeamPreference
from db.models.provider_connections import ProviderConnection
from db.models.teams import Team
from db.models.users import User
from pipelines import daily_actions as module
from pipelines.context import PipelineContext
from pipelines.daily_actions import DailyActionsPipeline
from services import backend_client as bc
from services.backend_client import BackendClient, DailyActionsRun
from services.notification_service import NotificationResult, NotificationService

MODELS = [User, ProviderConnection, Team, NotificationPreference, NotificationLog, NotificationTeamPreference]

TODAY = date(2026, 11, 24)
FIRST_GAME = time(19, 30)
IR_MOVE = {"player_id": 4, "name": "Hurt Guard", "from_slot_id": 12, "from_slot": "BE", "to_slot_id": 13,
           "to_slot": "IR", "role": "shift", "note": "OUT"}
ADD = {"player": {"player_id": 90, "name": "Free Agent", "team": "LAL"},
       "detail": "Open roster seat · Free Agent plays tonight vs BOS · 7:30 PM · 25.0 avg", "verified": True,
       "audit_id": 201}


def run(outcome, *, reason=None, ir_moves=None, adds=None, held=None, error=None):
    return DailyActionsRun(outcome=outcome, reason=reason, ir_moves=[IR_MOVE] if ir_moves is None else ir_moves,
                           adds=[ADD] if adds is None else adds, held=held, open_spots=0,
                           nba_date=TODAY.isoformat(), first_game_time_et="19:30", team_name="Test Team", error=error)


class FakeBackendClient:
    def __init__(self, *runs, enabled=True):
        self.responses = list(runs)
        self.calls = []
        self.enabled = enabled
        self.base_url = "http://api.railway.internal:8080" if enabled else None

    def run_daily_actions(self, **kwargs):
        self.calls.append(kwargs)
        return self.responses.pop(0) if len(self.responses) > 1 else self.responses[0]


class FakeNotificationService:
    def __init__(self, succeed=True):
        self.succeed = succeed
        self.sent = []

    def send_daily_actions_result(self, user, team, result, prefs=None):
        self.sent.append(dict(user=user, team=team, result=result, prefs=prefs))
        return (NotificationResult(success=True, message_id="msg-1") if self.succeed
                else NotificationResult(success=False, error="resend down"))


@pytest.fixture
def sqlite_models():
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
    p = DailyActionsPipeline()
    p.backend_client = FakeBackendClient(run("noop", ir_moves=[], adds=[]))
    p.notification_service = FakeNotificationService()
    return p


def make_user(email="fan@example.com", prefs_email=None, **prefs):
    user = User.create(email=email)
    if prefs_email is not None:
        prefs["email"] = prefs_email
    pref = NotificationPreference.create(user=user.user_id, **prefs) if prefs else None
    return user, pref


def make_team(user, provider="espn", name="Test Team"):
    return Team.create(user_id=user.user_id, team_identifier=name,
                       league_info=json.dumps({"provider": provider, "league_id": 1, "team_name": name}))


def logs(team):
    return list(NotificationLog.select().where(NotificationLog.team_id == team.team_id))


def at_act(pipeline):
    return [(u.user_id, t.team_id, r.email) for u, t, r in pipeline._teams_at_act()]


# ---- which teams -------------------------------------------------------------------


@pytest.mark.unit
def test_a_team_is_at_act_by_its_own_level_else_its_accounts(pipeline):
    user, _ = make_user(daily_actions_level="act", prefs_email="account@example.com")
    first, second, third = make_team(user, name="A"), make_team(user, name="B"), make_team(user, name="C")
    NotificationTeamPreference.create(user=user.user_id, team_id=second.team_id, daily_actions_level="off")
    NotificationTeamPreference.create(user=user.user_id, team_id=third.team_id, email="team@example.com")

    other, _ = make_user(email="other@example.com", lineup_alerts_enabled=False)    # account off, lineup email off
    own = make_team(other, name="D")
    make_team(other, name="E")
    NotificationTeamPreference.create(user=other.user_id, team_id=own.team_id, daily_actions_level="act")

    assert at_act(pipeline) == [(user.user_id, first.team_id, "account@example.com"),
                                (user.user_id, third.team_id, "team@example.com"),
                                (other.user_id, own.team_id, None)]


@pytest.mark.unit
def test_nobody_at_act_asks_nothing(pipeline):
    user, _ = make_user()
    make_team(user)
    assert pipeline._teams_at_act() == []


# ---- the day's gate ----------------------------------------------------------------


def execute_at(pipeline, monkeypatch, et_clock, first_game=FIRST_GAME):
    monkeypatch.setattr(module.Game, "get_earliest_game_time_on_date", staticmethod(lambda d: first_game))
    at = pytz.timezone("US/Eastern").localize(datetime.combine(TODAY, et_clock))
    ctx = PipelineContext("daily_actions")
    with freeze_time(at):
        pipeline.execute(ctx)
    return ctx


@pytest.mark.unit
def test_before_the_first_tip_every_team_at_act_is_asked(pipeline, monkeypatch):
    user, _ = make_user(daily_actions_level="act")
    team = make_team(user)
    execute_at(pipeline, monkeypatch, time(17, 30))
    assert pipeline.backend_client.calls == [dict(team_id=team.team_id, user_id=user.user_id, nba_date=TODAY,
                                                  correlation_id=None)]


@pytest.mark.unit
@pytest.mark.parametrize("clock, first_game", [(time(19, 30), FIRST_GAME), (time(17, 30), None)])
def test_after_the_tip_or_without_games_nothing_is_asked(pipeline, monkeypatch, clock, first_game):
    user, _ = make_user(daily_actions_level="act")
    make_team(user)
    execute_at(pipeline, monkeypatch, clock, first_game)
    assert pipeline.backend_client.calls == []


# ---- outcome branches --------------------------------------------------------------


def process(pipeline, user, team, recipient=None):
    ctx = PipelineContext("daily_actions")
    pipeline._process_team(ctx, user, team, recipient, TODAY)
    return ctx


@pytest.mark.unit
def test_applied_emails_what_was_made_and_settles_the_day(pipeline):
    user, _ = make_user(daily_actions_level="act")
    team = make_team(user)
    pipeline.backend_client = FakeBackendClient(run("applied", held="An open spot stays open: adds used"))

    ctx = process(pipeline, user, team)

    (sent,) = pipeline.notification_service.sent
    assert sent["result"]["ir_moves"] == [IR_MOVE] and sent["result"]["adds"] == [ADD]
    (row,) = logs(team)
    assert (row.notification_type, row.status, row.resend_message_id) == ("daily_actions", "sent", "msg-1")
    data = json.loads(row.alert_data)
    assert (data["outcome"], data["held"], len(data["adds"])) == ("applied", "An open spot stays open: adds used", 1)
    assert ctx.records_processed == 1

    process(pipeline, user, team)                    # settled: the next tick asks nothing
    assert len(pipeline.backend_client.calls) == 1


@pytest.mark.unit
def test_a_bounced_email_still_settles_the_day(pipeline):
    user, _ = make_user(daily_actions_level="act")
    team = make_team(user)
    pipeline.backend_client = FakeBackendClient(run("applied"))
    pipeline.notification_service = FakeNotificationService(succeed=False)
    process(pipeline, user, team)
    (row,) = logs(team)
    assert (row.status, row.error_message, row.sent_at) == ("skipped", "resend down", None)


@pytest.mark.unit
@pytest.mark.parametrize("outcome, reason", [("noop", None), ("skipped", "at_ai")])
def test_nothing_made_settles_the_day_without_an_email(pipeline, outcome, reason):
    user, _ = make_user(daily_actions_level="act")
    team = make_team(user)
    pipeline.backend_client = FakeBackendClient(run(outcome, reason=reason, ir_moves=[], adds=[]))
    ctx = process(pipeline, user, team)
    assert pipeline.notification_service.sent == []
    (row,) = logs(team)
    assert row.status == "skipped" and ctx.records_skipped == 1


@pytest.mark.unit
def test_a_failure_is_retried_next_tick_and_updates_the_same_row(pipeline):
    user, _ = make_user(daily_actions_level="act")
    team = make_team(user)
    pipeline.backend_client = FakeBackendClient(DailyActionsRun.unavailable("timeout"), run("applied"))

    ctx = process(pipeline, user, team)
    (row,) = logs(team)
    assert row.status == "failed" and ctx.failure_reasons == {"backend_unavailable": 1}

    process(pipeline, user, team)
    (row,) = logs(team)
    assert row.status == "sent" and len(pipeline.backend_client.calls) == 2


@pytest.mark.unit
def test_a_yahoo_team_is_not_asked(pipeline):
    user, _ = make_user(daily_actions_level="act")
    team = make_team(user, provider="yahoo")
    process(pipeline, user, team)
    assert pipeline.backend_client.calls == [] and logs(team) == []


# ---- the backend client ------------------------------------------------------------


class _Response:
    def __init__(self, status, payload):
        self.status_code = status
        self._payload = payload
        self.text = json.dumps(payload)

    def json(self):
        return self._payload


@pytest.mark.unit
def test_the_client_posts_one_team_and_parses_its_answer(monkeypatch):
    seen = {}

    def post(url, json=None, headers=None, timeout=None):
        seen.update(url=url, body=json, timeout=timeout)
        return _Response(200, {"status": "success", "data": {
            "outcome": "applied", "ir_moves": [IR_MOVE], "adds": [ADD], "held": None, "open_spots": 1,
            "nba_date": "2026-11-24", "first_game_time_et": "19:30", "team_name": "Test Team"}})

    monkeypatch.setattr(bc.requests, "post", post)
    client = BackendClient("http://api", "tok", 45.0, pickups_timeout_seconds=120.0)
    out = client.run_daily_actions(team_id=22, user_id=7, nba_date=TODAY)
    assert seen["url"] == "http://api/v1/internal/jobs/daily-actions/run"
    assert seen["body"] == {"team_id": 22, "user_id": 7, "nba_date": "2026-11-24"}
    assert seen["timeout"] == (bc.CONNECT_TIMEOUT_SECONDS, 120.0)
    assert (out.outcome, out.ir_moves, out.adds, out.open_spots) == ("applied", [IR_MOVE], [ADD], 1)


@pytest.mark.unit
@pytest.mark.parametrize("status, payload, outcome, reason", [
    (200, {"data": {"outcome": "planned"}}, "unavailable", "malformed_response"),
    (404, {"error_code": "TEAM_NOT_FOUND", "message": "Team not found"}, "skipped", "team_not_found"),
    (401, {"detail": "bad token"}, "unavailable", "unauthorized"),
    (502, {"message": "bad gateway"}, "unavailable", "http_502"),
])
def test_the_client_maps_what_isnt_an_answer(monkeypatch, status, payload, outcome, reason):
    monkeypatch.setattr(bc.requests, "post", lambda *a, **k: _Response(status, payload))
    out = BackendClient("http://api", "tok", 45.0).run_daily_actions(team_id=22, user_id=7, nba_date=TODAY)
    assert (out.outcome, out.reason) == (outcome, reason)


@pytest.mark.unit
def test_the_client_never_raises_on_a_timeout(monkeypatch):
    def post(*a, **k):
        raise requests.Timeout("read timed out")

    monkeypatch.setattr(bc.requests, "post", post)
    out = BackendClient("http://api", "tok", 45.0).run_daily_actions(team_id=22, user_id=7, nba_date=TODAY)
    assert (out.outcome, out.reason) == ("unavailable", "timeout")


# ---- the email ---------------------------------------------------------------------


@pytest.mark.unit
def test_the_email_says_what_was_made_and_that_nobody_was_dropped():
    subject, body = NotificationService._build_daily_actions_email("Test Team", run("applied").as_dict())
    assert subject == "Court Vision moved Hurt Guard to IR and picked up Free Agent for Test Team"
    assert "First game today: 7:30 PM ET" in body
    assert "  - Moved Hurt Guard to IR (OUT)" in body
    assert "  - Picked up Free Agent (LAL) for an open roster spot" in body
    assert "plays tonight vs BOS" in body
    assert "Nobody was dropped" in body and "Lineup automation decides whether he starts" in body
    assert "Not made" not in body


@pytest.mark.unit
def test_the_email_says_what_was_left_and_an_unconfirmed_pickup():
    unconfirmed = {**ADD, "verified": False}
    out = run("applied", ir_moves=[], adds=[unconfirmed], held="An open spot stays open: Every add this matchup "
              "allows is used (2 of 2)").as_dict()
    subject, body = NotificationService._build_daily_actions_email("Test Team", out)
    assert subject == "Court Vision picked up Free Agent for Test Team"
    assert "not yet confirmed by ESPN" in body
    assert "Not made: An open spot stays open: Every add this matchup allows is used (2 of 2)." in body


@pytest.mark.unit
def test_the_email_for_a_first_move_espn_refused():
    out = run("rejected", reason="PROVIDER_AUTH_EXPIRED: ESPN no longer accepts espn_s2", ir_moves=[],
              adds=[]).as_dict()
    subject, body = NotificationService._build_daily_actions_email("Test Team", out)
    assert subject == "Court Vision couldn't make today's daily actions for Test Team"
    assert "nothing was changed: ESPN no longer accepts espn_s2" in body
    assert "PROVIDER_AUTH_EXPIRED" not in body
