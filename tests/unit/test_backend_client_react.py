"""
`BackendClient.react_lineup`: one verdict change posted to the late-scratch swap
(POST /v1/internal/jobs/lineup/react). A 200 envelope parses into each team's
outcome; any other status, a malformed body or a timeout is a reaction that is
not `ok`, and nothing raises.
"""

import json
from datetime import date, datetime, timezone

import pytest
import requests
import responses

from services.backend_client import REACT_LINEUP_PATH, BackendClient, ReactTeam, backend_client_from_settings

BASE = "http://api.railway.internal:8080"
URL = BASE + REACT_LINEUP_PATH
TOKEN = "pipeline-secret-token"
SWAP = [{"player_id": 3, "name": "Josh Hart", "from_slot_id": 12, "from_slot": "BE", "to_slot_id": 11,
         "to_slot": "UT", "role": "start", "note": "vs BOS · 7:30 PM"}]


def _react(client=None, **overrides):
    kwargs = dict(player_id=1628973, game_date=date(2026, 11, 10), verdict="out", previous_verdict="questionable",
                  reason="Left Ankle; Sprain", source="espn_injuries",
                  changed_at=datetime(2026, 11, 10, 21, 30, tzinfo=timezone.utc), correlation_id="corr-1")
    kwargs.update(overrides)
    return (client or BackendClient(base_url=BASE, token=TOKEN, timeout_seconds=45.0)).react_lineup(**kwargs)


def _team(**kw):
    team = {"team_id": 22, "user_id": 7, "level": "act", "outcome": "applied", "reason": None,
            "player_name": "Jalen Brunson", "moves": SWAP, "verified": True, "audit_id": 9,
            "nba_date": "2026-11-10", "first_game_time_et": "19:30", "team_name": "Test"}
    team.update(kw)
    return team


@pytest.mark.unit
@responses.activate
def test_a_200_is_each_teams_outcome_and_the_change_is_sent_whole():
    responses.post(URL, json={"status": "success", "message": "1 of 2 team(s) swapped", "data": {
        "player_id": 1628973, "verdict": "out",
        "teams": [_team(), _team(team_id=23, level="notify", outcome="planned", reason="notify", verified=None)]}})

    reaction = _react()

    assert reaction.ok
    assert [(t.team_id, t.level, t.outcome, t.emails) for t in reaction.teams] == [
        (22, "act", "applied", True), (23, "notify", "planned", True)]
    assert reaction.teams[0].moves == SWAP
    sent = responses.calls[0].request
    assert sent.headers["Authorization"] == f"Bearer {TOKEN}" and sent.headers["X-Correlation-ID"] == "corr-1"
    assert json.loads(sent.body) == {
        "player_id": 1628973, "game_date": "2026-11-10", "verdict": "out", "previous_verdict": "questionable",
        "reason": "Left Ankle; Sprain", "source": "espn_injuries", "changed_at": "2026-11-10T21:30:00+00:00"}


@pytest.mark.unit
def test_only_a_swap_made_or_one_to_make_is_emailed():
    def emails(**kw):
        return ReactTeam.from_payload(_team(**kw)).emails
    assert emails() and emails(outcome="planned") and emails(outcome="rejected")
    assert not emails(outcome="noop", moves=[]) and not emails(outcome="skipped", moves=[])
    assert not emails(outcome="failed", moves=[])                          # a board that couldn't be read
    assert not emails(outcome="skipped", reason="daily_cap")                # the cap: moves, but not made or due


@pytest.mark.unit
@responses.activate
@pytest.mark.parametrize("status,body,reason", [
    (401, {"detail": "bad token"}, "unauthorized"),
    (500, {"message": "boom"}, "http_500"),
    (200, {"data": {"teams": [{"team_id": 1}]}}, "malformed_response"),
    (200, {"data": None}, "malformed_response"),
])
def test_anything_but_a_good_200_is_not_ok(status, body, reason):
    responses.post(URL, json=body, status=status)
    reaction = _react()
    assert (reaction.ok, reaction.reason, reaction.teams) == (False, reason, [])


@pytest.mark.unit
def test_a_timeout_or_no_backend_is_not_ok_and_never_raises(monkeypatch):
    assert _react(BackendClient(base_url=None, token=TOKEN, timeout_seconds=45.0)).reason == "backend_not_configured"

    captured = {}

    def slow(url, **kwargs):
        captured.update(kwargs)
        raise requests.Timeout("read timed out")

    monkeypatch.setattr(requests, "post", slow)
    client = BackendClient(base_url=BASE, token=TOKEN, timeout_seconds=45.0, react_timeout_seconds=60.0)
    assert (_react(client).ok, _react(client).reason) == (False, "timeout")
    assert captured["timeout"] == (5.0, 60.0)


@pytest.mark.unit
def test_the_settings_give_the_reaction_its_own_timeout():
    from core.settings import settings
    client = backend_client_from_settings(settings)
    assert client._react_timeout == (5.0, settings.react_backend_timeout_seconds)
