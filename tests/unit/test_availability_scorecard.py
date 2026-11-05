"""
The Availability page's scorecard (services/availability_scorecard.py): one
player's reports for a date in, his outcome out. Pure: the reads are tested in
tests/integration/test_availability_scorecard_reads.py.

WEDNESDAY's game tips at 7 PM ET (00:00 UTC Thursday). Both ESPN sources first
polled the date at 6 AM ET (11:00 UTC).
"""

from datetime import date, datetime, timezone

import pytest

from pipelines.transformers.availability import ET, GameContext, Report
from schemas.availability import AvailabilityCall
from pipelines.availability import Slate, SlateGame
from services.availability_scorecard import game_context, out_since, score_call, summarize

pytestmark = pytest.mark.unit

WEDNESDAY = date(2026, 11, 4)
GAME = GameContext(game_date=WEDNESDAY, game_id="0022600150", day_words=("Celtics", "Boston"))
TIP = datetime(2026, 11, 4, 19, 0, tzinfo=ET)
OPEN = "2026-11-04T11:00"
OPENING = {"espn_injuries": datetime(2026, 11, 4, 11, 0, tzinfo=timezone.utc),
           "espn_fantasy": datetime(2026, 11, 4, 11, 0, tzinfo=timezone.utc)}
AFTER = datetime(2026, 11, 5, 6, 0, tzinfo=timezone.utc)
BEFORE = datetime(2026, 11, 4, 21, 0, tzinfo=timezone.utc)


def _utc(at: str) -> datetime:
    return datetime.fromisoformat(at).replace(tzinfo=timezone.utc)


def rep(source, status, at, **kw) -> Report:
    return Report(source=source, status=status, observed_at=_utc(at), **kw)


def injuries(status, at, **kw):
    return rep("espn_injuries", status, at, **kw)


def fantasy(status, at):
    return rep("espn_fantasy", status, at)


def box(status, at="2026-11-05T00:01"):
    return rep("nba_boxscore", status, at, game_id=GAME.game_id)


def score(*reports, now=AFTER, opening=OPENING):
    return score_call(list(reports), GAME, TIP, opening=opening, now=now, player_id=1, name="Jalen Brunson", team="NYK")


# ---- the race ----------------------------------------------------------------------


def test_we_called_him_out_two_hours_before_espn_fantasy_flipped():
    call = score(injuries("out", "2026-11-04T20:10", reason="Ankle"), fantasy("out", "2026-11-04T22:15"), box("inactive"))

    assert call.outcome == "us_first"
    assert call.ours_by == "espn_injuries"
    assert call.ours_at == _utc("2026-11-04T20:10")
    assert call.espn_at == _utc("2026-11-04T22:15")
    assert call.lead_minutes == 125.0
    assert call.reason == "Ankle"
    assert (call.verdict_at_tip, call.verdict_by, call.box) == ("out", "espn_injuries", "inactive")
    assert call.tip == _utc("2026-11-05T00:00")


def test_espn_fantasy_first_is_a_negative_lead():
    call = score(fantasy("out", "2026-11-04T20:00"), injuries("out", "2026-11-04T21:30"), box("inactive"))

    assert call.outcome == "espn_first"
    assert call.lead_minutes == -90.0


def test_calls_in_the_same_tick_are_a_tie():
    call = score(injuries("out", "2026-11-04T22:00"), fantasy("out", "2026-11-04T22:01"), box("inactive"))

    assert call.outcome == "same_poll"


def test_calls_a_tick_apart_are_a_tie_and_two_ticks_are_not():
    def race(ours, espn):
        return score(injuries("out", ours), fantasy("out", espn), box("inactive")).outcome

    assert race("2026-11-04T22:00:00", "2026-11-04T22:02:00") == "same_poll"
    assert race("2026-11-04T22:00:00", "2026-11-04T22:02:20") == "same_poll"   # the next run started late
    assert race("2026-11-04T22:00:00", "2026-11-04T22:04:00") == "us_first"


def test_only_we_had_him_out_at_the_tip():
    call = score(injuries("out", "2026-11-04T20:00"), fantasy("questionable", "2026-11-04T20:00"), box("inactive"))

    assert call.outcome == "us_only"
    assert call.espn_at is None
    assert call.espn_at_tip == "questionable"
    assert call.lead_minutes is None


def test_only_espn_fantasy_had_him_out_and_the_floor_made_the_verdict():
    call = score(injuries("questionable", "2026-11-04T15:00"), fantasy("out", "2026-11-04T23:15"), box("inactive"))

    assert call.outcome == "espn_only"
    assert call.ours_at is None
    assert (call.verdict_at_tip, call.verdict_by) == ("out", "espn_fantasy")


def test_an_out_we_took_back_starts_the_clock_again():
    call = score(
        injuries("out", "2026-11-04T15:00"),
        injuries("available", "2026-11-04T17:00"),
        fantasy("out", "2026-11-04T20:00"),
        injuries("out", "2026-11-04T22:00"),
        box("inactive"),
    )

    assert call.ours_at == _utc("2026-11-04T22:00")
    assert call.outcome == "espn_first"


def test_an_espn_status_cleared_before_the_tip_is_not_a_call():
    call = score(injuries("out", "2026-11-04T18:00"), fantasy("out", "2026-11-04T16:00"),
                 fantasy("available", "2026-11-04T23:00"), box("inactive"))

    assert call.outcome == "us_only"
    assert call.espn_at_tip == "available"


def test_what_came_after_the_tip_does_not_count():
    call = score(fantasy("questionable", "2026-11-04T15:00"), injuries("out", "2026-11-05T00:10"), box("inactive"))

    assert call.outcome == "unannounced"
    assert call.ours_at is None


# ---- not races ---------------------------------------------------------------------


def test_out_at_both_first_reads_of_the_date_is_a_long_absence():
    call = score(injuries("out", OPEN, reason="Knee"), fantasy("out", OPEN), box("inactive"))

    assert call.outcome == "carried"


def test_out_at_our_first_read_and_later_at_espns_is_still_a_race():
    call = score(injuries("out", OPEN), fantasy("questionable", OPEN), fantasy("out", "2026-11-04T19:00"), box("inactive"))

    assert call.outcome == "us_first"
    assert call.lead_minutes == 480.0


def test_an_out_player_who_was_active_is_a_false_out():
    call = score(injuries("out", "2026-11-04T20:00"), box("active"))

    assert call.outcome == "false_out"
    assert (call.verdict_at_tip, call.verdict_by) == ("out", "espn_injuries")
    assert call.box == "active"


def test_a_false_out_only_espn_fantasy_made_is_the_floors():
    call = score(injuries("questionable", "2026-11-04T15:00"), fantasy("out", "2026-11-04T20:00"), box("active"))

    assert call.outcome == "false_out"
    assert call.verdict_by == "espn_fantasy"
    assert call.espn_at_tip == "out"


def test_players_the_scorecard_leaves_out():
    # On the inactive list and named by nobody else: a two-way player.
    assert score(box("inactive")) is None
    # Listed questionable, played: nobody called him out.
    assert score(injuries("questionable", "2026-11-04T15:00"), box("active")) is None
    # Listed questionable, game not played yet: nothing to judge.
    assert score(injuries("questionable", "2026-11-04T15:00"), now=BEFORE) is None


def test_a_call_for_a_game_not_tipped_is_pending_and_one_never_on_the_box_score_is_not_in_box():
    reports = (injuries("out", "2026-11-04T20:00"), fantasy("out", "2026-11-04T20:30"))

    assert score(*reports, now=BEFORE).outcome == "pending"
    tipped = score(*reports, now=AFTER)
    assert tipped.outcome == "not_in_box"
    assert tipped.lead_minutes == 30.0


def test_a_pre_tip_inactive_is_not_graded_before_the_tip():
    call = score(box("inactive", "2026-11-04T23:40"), now=BEFORE.replace(hour=23, minute=45))

    assert call.outcome == "pending"
    assert call.ours_by == "nba_boxscore"


def test_a_pre_tip_inactive_taken_back_by_the_box_score_is_a_false_out():
    call = score(box("inactive", "2026-11-04T23:40"), box("active", "2026-11-05T00:02"))

    assert call.outcome == "false_out"
    assert call.verdict_by == "nba_boxscore"


# ---- the timeline ------------------------------------------------------------------


def test_out_since_is_the_start_of_the_last_stretch_of_out():
    t = [_utc(f"2026-11-04T1{h}:00") for h in range(5)]

    assert out_since([(t[0], "out"), (t[1], "out"), (t[2], "questionable"), (t[3], "out"), (t[4], "out")]) == t[3]
    assert out_since([(t[0], "out"), (t[1], None)]) is None
    assert out_since([]) is None


# ---- the summary -------------------------------------------------------------------


def _calls(*outcomes):
    return [
        AvailabilityCall(
            game_date=WEDNESDAY, player_id=i, name=str(i), tip=TIP, outcome=outcome, lead_minutes=lead,
            ours_by=by, verdict_by=verdict_by, espn_at_tip=espn_at_tip,
        )
        for i, (outcome, lead, by, verdict_by, espn_at_tip) in enumerate(outcomes)
    ]


def test_the_gate_reads_first_share_and_our_own_false_outs():
    races = [
        ("us_first", 30.0, "espn_injuries", "espn_injuries", "out"),
        ("us_first", 90.0, "espn_injuries", "espn_injuries", "out"),
        ("us_only", None, "espn_injuries", "espn_injuries", "questionable"),
        ("espn_first", -10.0, "espn_injuries", "espn_injuries", "out"),
        ("same_poll", 0.0, "espn_injuries", "espn_injuries", "out"),
        ("carried", None, "espn_injuries", "espn_injuries", "out"),
        ("false_out", None, None, "espn_fantasy", "out"),
    ]
    summary = summarize(_calls(*races))

    assert (summary.races, summary.us_first, summary.us_only, summary.carried) == (5, 2, 1, 1)
    assert summary.first_share == 0.6
    assert summary.median_lead_minutes == 15.0
    assert summary.first_by == {"espn_injuries": 3}
    assert (summary.false_outs, summary.false_outs_floor, summary.espn_false_outs) == (1, 1, 1)
    assert summary.gate_met

    ours = summarize(_calls(*races, ("false_out", None, None, "espn_injuries", None)))
    assert (ours.false_outs, ours.false_outs_floor) == (2, 1)
    assert not ours.gate_met


def test_no_races_is_no_gate():
    summary = summarize([])

    assert summary.races == 0
    assert summary.first_share is None
    assert summary.median_lead_minutes is None
    assert not summary.gate_met


# ---- which game ------------------------------------------------------------------


SLATE = Slate(WEDNESDAY, (
    SlateGame("0022600150", home="NYK", away="BOS", tip=TIP),
    SlateGame("0022600153", home="DAL", away="PHX", tip=datetime(2026, 11, 4, 21, 0, tzinfo=ET)),
))


def test_the_box_scores_game_wins_over_a_traded_players_current_team():
    # Listed in Knicks-Celtics as a Knick, now a Mav: still the 7 PM game.
    game = game_context(SLATE, "DAL", "0022600150")

    assert game.game_id == "0022600150"
    assert game.day_words == ()
    assert game_context(SLATE, "NYK", "0022600150").day_words == ("Boston", "Celtics")


def test_without_a_box_score_the_current_teams_game_is_his():
    assert game_context(SLATE, "PHX", None).game_id == "0022600153"
    assert not game_context(SLATE, "LAL", None).has_game
