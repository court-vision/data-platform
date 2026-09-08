"""
`services.notification_service` body rendering: the suggested-moves alert and
the auto-lineup summary, grouped swap-by-swap, with the unfilled block, the
extra lines and the unverified note in the right places. Sending goes through
the log stub (no Resend key in tests).
"""

import json
from datetime import time
from types import SimpleNamespace

import pytest

from services.notification_service import (
    AUTO_LINEUP_DISCLAIMER,
    NotificationResult,
    NotificationService,
)

MOVES = [
    {"player_id": 1, "name": "Bench Guy", "from_slot": "BE", "to_slot": "UT", "role": "start", "note": "vs LAL · 7:30 PM"},
    {"player_id": 2, "name": "Idle Starter", "from_slot": "UT", "to_slot": "BE", "role": "bench", "note": "no game today"},
    {"player_id": 4, "name": "Second Sub", "from_slot": "BE", "to_slot": "G", "role": "start", "note": None},
    {"player_id": 5, "name": "Point Guard", "from_slot": "PG", "to_slot": "BE", "role": "bench", "note": "OUT"},
    {"player_id": 6, "name": "Shifter", "from_slot": "G", "to_slot": "PG", "role": "shift", "note": None},
]
UNFILLED = [{"player_id": 3, "name": "Third Man", "slot": "BE", "reason": "no eligible open slot"}]

USER = SimpleNamespace(email="fan@example.com")
TEAM = SimpleNamespace(league_info=json.dumps({"team_name": "Hoop Dreams"}))


@pytest.fixture
def service(monkeypatch):
    from core.settings import settings

    monkeypatch.setattr(settings, "resend_api_key", None)
    svc = NotificationService()
    sent = []

    def capture(to, subject, body):
        sent.append((to, subject, body))
        return NotificationResult(success=True, message_id="stub")

    monkeypatch.setattr(svc, "_send_email", capture)
    svc.sent = sent
    return svc


@pytest.mark.unit
def test_alert_body_groups_swaps_and_lists_unfilled(service):
    result = service.send_lineup_alert(USER, TEAM, MOVES, UNFILLED, time(19, 30))

    assert result.success
    to, subject, body = service.sent[0]
    assert to == "fan@example.com"
    assert subject == "Court Vision: 5 suggested lineup move(s) before today's games"
    assert body == "\n".join([
        "Team: Hoop Dreams",
        "First game today: 07:30 PM ET",
        "",
        "Suggested lineup moves (5):",
        "  1. Start Bench Guy at UT (vs LAL · 7:30 PM)",
        "     Bench Idle Starter (no game today)",
        "  2. Start Second Sub at G",
        "     Bench Point Guard (OUT)",
        "     Shift Shifter G→PG",
        "",
        "Still on the bench (no eligible open slot):",
        "  - Third Man (BE): no eligible open slot",
        "",
        "-- Court Vision",
    ])


@pytest.mark.unit
def test_alert_extra_lines_sit_before_the_signature(service):
    service.send_lineup_alert(
        USER, TEAM, MOVES[:2], [], time(19, 30),
        extra_lines=["Auto-lineup could not apply these moves: locked", "Reconnect ESPN."],
    )
    body = service.sent[0][2]
    assert body.endswith(
        "  1. Start Bench Guy at UT (vs LAL · 7:30 PM)\n"
        "     Bench Idle Starter (no game today)\n"
        "\n"
        "Auto-lineup could not apply these moves: locked\n"
        "Reconnect ESPN.\n"
        "\n"
        "-- Court Vision"
    )
    assert "Still on the bench" not in body


@pytest.mark.unit
def test_prefs_email_overrides_user_email(service):
    prefs = SimpleNamespace(email="alerts@example.com")
    service.send_lineup_alert(USER, TEAM, MOVES[:2], [], time(19, 30), prefs=prefs)
    assert service.sent[0][0] == "alerts@example.com"


@pytest.mark.unit
def test_summary_body_verified(service):
    service.send_auto_lineup_summary(USER, TEAM, MOVES[:2], UNFILLED, time(19, 30), verified=True)

    _, subject, body = service.sent[0]
    assert subject == "Court Vision set your lineup for Hoop Dreams: 2 move(s)"
    assert body == "\n".join([
        "Team: Hoop Dreams",
        "First game today: 07:30 PM ET",
        "",
        "Applied to ESPN:",
        "  1. Start Bench Guy at UT (vs LAL · 7:30 PM)",
        "     Bench Idle Starter (no game today)",
        "",
        "Still on the bench (no eligible open slot):",
        "  - Third Man (BE): no eligible open slot",
        "",
        AUTO_LINEUP_DISCLAIMER,
        "",
        "-- Court Vision",
    ])


@pytest.mark.unit
def test_summary_body_flags_unverified_writes(service):
    service.send_auto_lineup_summary(USER, TEAM, MOVES[:2], [], time(19, 30), verified=False)
    body = service.sent[0][2]
    assert "Applied to ESPN: (not yet confirmed by ESPN — please check your roster)" in body
    assert "Still on the bench" not in body


@pytest.mark.unit
def test_summary_says_nothing_about_verification_when_unknown(service):
    service.send_auto_lineup_summary(USER, TEAM, MOVES[:2], [], time(19, 30), verified=None)
    assert "not yet confirmed" not in service.sent[0][2]


@pytest.mark.unit
def test_header_tolerates_missing_team_name_and_string_times(service):
    team = SimpleNamespace(league_info="not json")
    service.send_lineup_alert(USER, team, MOVES[:2], [], "7:30 PM ET")
    body = service.sent[0][2]
    assert body.startswith("Team: Your Team\nFirst game today: 7:30 PM ET\n")

    service.send_lineup_alert(USER, team, MOVES[:2], [], None)
    assert "First game today: TBD" in service.sent[1][2]


@pytest.mark.unit
def test_stub_send_reports_success_without_a_resend_key(monkeypatch):
    from core.settings import settings

    monkeypatch.setattr(settings, "resend_api_key", None)
    result = NotificationService().send_lineup_alert(USER, TEAM, MOVES[:2], [], time(19, 30))
    assert result.success and result.message_id == "stub-fan@example.com"
