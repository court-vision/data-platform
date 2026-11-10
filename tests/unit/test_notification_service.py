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
    SIGNATURE,
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


# ---- stream lanes ----------------------------------------------------------------------------


def _pickup(outcome="executed", reason=None, lane=None):
    return {"outcome": outcome, "reason": reason, "add": {"name": "Kawhi Leonard"}, "drop": {"name": "Anthony Edwards"},
            "nba_date": "2026-11-17", "seated_slot": "UT", "verified": True, "lane": lane}


@pytest.mark.unit
@pytest.mark.parametrize("projected, why, line", [
    ("Kawhi Leonard", "as_projected", "This was the move the lane had projected."),
    ("Joel Embiid", "projected_gone", "The lane had projected Joel Embiid for this day, but Joel Embiid was no longer "
                                      "available, so it picked Kawhi Leonard."),
    ("Joel Embiid", "replanned", "The lane had projected Joel Embiid for this day; with the latest projections "
                                 "Kawhi Leonard was the better pick."),
    ("Anthony Edwards", "replanned", "The lane had projected keeping Anthony Edwards for this day; with the latest "
                                     "projections Kawhi Leonard was the better pick."),
])
def test_a_lane_pickup_says_how_it_compares_with_the_projection(projected, why, line):
    subject, body = NotificationService._build_pickup_email(
        "Hoop Dreams", _pickup(lane={"lane_id": 7, "notify": "email", "projected": {"name": projected}, "why": why}))
    assert subject == "Stream lane picked up Kawhi Leonard for Hoop Dreams"
    assert "Your stream lane made this move" in body and line in body


@pytest.mark.unit
def test_a_lane_pickup_that_did_not_go_through_and_a_users_own_pickup():
    subject, body = NotificationService._build_pickup_email(
        "Hoop Dreams", _pickup("skipped", "unavailable", lane={"lane_id": 7, "notify": "email"}))
    assert subject == "Stream lane skipped Kawhi Leonard for Hoop Dreams"
    assert "re-plans before the next day" in body and "made this move" not in body
    subject, body = NotificationService._build_pickup_email("Hoop Dreams", _pickup())
    assert subject == "Court Vision picked up Kawhi Leonard for Hoop Dreams" and "stream lane" not in body


@pytest.mark.unit
def test_lane_notices(service):
    holder = {"name": "Anthony Edwards"}
    result = service.send_stream_lane_notice(USER, TEAM, {"action": "paused", "reason": "holder_gone", "holder": holder})
    assert result.success
    _, subject, body = service.sent[0]
    assert subject == "Stream lane paused for Hoop Dreams"
    assert "Anthony Edwards is no longer on your roster" in body and "won't change anything else" in body
    _, _, body = service.sent[-1] if service.send_stream_lane_notice(
        USER, TEAM, {"action": "paused", "reason": "auth_expired", "holder": holder}) else (None, None, None)
    assert "reconnect it on Court Vision" in body and "until you resume it" in body
    service.send_stream_lane_notice(USER, TEAM, {"action": "blocked", "reason": "missed", "nba_date": "2026-11-17",
                                                 "holder": holder})
    _, subject, body = service.sent[-1]
    assert subject == "Stream lane missed a day for Hoop Dreams" and "Tue, Nov 17" in body


@pytest.mark.unit
@pytest.mark.parametrize("outcome, reason, line", [
    ("executed", None, "Your stream lane made this move for the matchup's last day: the lane ends with the matchup."),
    ("expired", "deadline", "This was your stream lane's pickup for the matchup's last day: the lane ends with the "
                            "matchup."),
])
def test_a_lane_pickup_for_the_matchups_last_day_says_the_lane_ends(outcome, reason, line):
    _, body = NotificationService._build_pickup_email(
        "Hoop Dreams", _pickup(outcome, reason, lane={"lane_id": 7, "notify": "email", "last_day": True}))
    assert line in body
    assert "carries on" not in body and "before each day" not in body


@pytest.mark.unit
@pytest.mark.parametrize("ended, line", [
    (True, "The matchup has no day left, so the lane has ended."),
    (False, "The lane carries on with the next day."),
])
def test_a_missed_day_says_whether_the_lane_ended_with_it(service, ended, line):
    service.send_stream_lane_notice(USER, TEAM, {"action": "blocked", "reason": "missed", "nba_date": "2026-11-22",
                                                 "holder": {"name": "Anthony Edwards"}, "ended": ended})
    _, subject, body = service.sent[-1]
    assert subject == "Stream lane missed a day for Hoop Dreams"
    assert line in body and ("carries on" in body) is not ended


# ---- late scratches ---------------------------------------------------------------


SWAP = [
    {"player_id": 3, "name": "Josh Hart", "from_slot": "BE", "to_slot": "UT", "role": "start", "note": "vs BOS · 7:30 PM"},
    {"player_id": 2, "name": "Jalen Brunson", "from_slot": "UT", "to_slot": "BE", "role": "bench", "note": "OUT"},
]


def _scratch(**kw):
    result = {"outcome": "applied", "level": "act", "reason": None, "player_name": "Jalen Brunson",
              "verdict": "out", "injury": "Left Ankle; Sprain", "source": "espn_injuries",
              "changed_at": "2026-11-10T21:30:00+00:00", "moves": SWAP, "verified": True}
    result.update(kw)
    return result


@pytest.mark.unit
def test_a_late_scratch_swap_made_says_why_and_what_moved(service):
    service.send_late_scratch_result(USER, TEAM, _scratch())

    _, subject, body = service.sent[0]
    assert subject == "Jalen Brunson ruled out: Court Vision benched him for Hoop Dreams"
    assert body == "\n".join([
        "Team: Hoop Dreams",
        "",
        "Because Jalen Brunson was ruled out (Left Ankle; Sprain, per ESPN's injury list) at 4:30 PM ET:",
        "",
        "Applied to ESPN:",
        "  1. Start Josh Hart at UT (vs BOS · 7:30 PM)",
        "     Bench Jalen Brunson (OUT)",
        "",
        "Only this swap was made; the rest of your lineup is as you set it.",
        "If Jalen Brunson is upgraded again before his game, this swap is not undone.",
        "",
        SIGNATURE,
    ])


@pytest.mark.unit
def test_at_notify_the_swap_is_suggested_and_nothing_changed(service):
    service.send_late_scratch_result(USER, TEAM, _scratch(outcome="planned", level="notify", reason="notify",
                                                          verdict="doubtful", verified=None))

    _, subject, body = service.sent[0]
    assert subject == "Jalen Brunson ruled doubtful: a lineup swap for Hoop Dreams"
    assert "Suggested swap:" in body and "Applied to ESPN:" not in body
    assert "didn't make it" not in body
    assert "Nothing was changed on ESPN. Make the swap in Court Vision's Week desk or on ESPN." in body


@pytest.mark.unit
@pytest.mark.parametrize("outcome,reason,line", [
    ("planned", "writes_disabled", "Court Vision didn't make it: its lineup writes are switched off right now."),
    ("planned", "can_write:espn_locked",
     "Court Vision didn't make it: ESPN isn't taking lineup changes for this team right now."),
    ("rejected", "ESPN says the roster is locked",
     "Court Vision tried to make it, but ESPN didn't take it (ESPN says the roster is locked)."),
])
def test_at_act_a_swap_not_made_says_why(service, outcome, reason, line):
    service.send_late_scratch_result(USER, TEAM, _scratch(outcome=outcome, reason=reason, verified=None))
    _, subject, body = service.sent[0]
    assert subject == "Jalen Brunson ruled out: a lineup swap for Hoop Dreams"
    assert line in body and "Suggested swap:" in body


@pytest.mark.unit
def test_a_bench_player_cleared_to_play_and_an_unverified_write(service):
    service.send_late_scratch_result(USER, TEAM, _scratch(verdict="available", injury=None, source="nba_boxscore",
                                                          changed_at=None, verified=False))
    _, subject, body = service.sent[0]
    assert subject == "Jalen Brunson is available: Court Vision started him for Hoop Dreams"
    assert "Because Jalen Brunson was upgraded to available (per the NBA box score):" in body
    assert "upgraded again" not in body
    assert body.splitlines()[4].startswith("Applied to ESPN:") and body.splitlines()[4] != "Applied to ESPN:"
