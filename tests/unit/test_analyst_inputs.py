"""
The pure parts of what a read is shown (services.analyst_inputs): the bounds a
note is held to, when the analyst's own note comes due, and the fingerprint
that decides whether a player is read again. No database: a note is an unsaved
row. The loaders, the screen's subjects and the packet are exercised against
Postgres in tests/integration/test_analyst_pipeline.py.
"""

from datetime import date, datetime, timedelta, timezone

import pytest

from db.models.nba import ProjectionAdjustment
from services import analyst_inputs as inputs
from services.analyst_inputs import Item, bounds, fingerprint, note_due, note_packet, note_standing, own_state
from services.live_projection_model import Availability, Member

pytestmark = pytest.mark.unit

TODAY = date(2026, 11, 16)
STAR, BACKUP = 201, 202


def note(**over) -> ProjectionAdjustment:
    fields = {
        "id": 5, "player": BACKUP, "season": "2026-27", "scope": "analyst", "kind": "injury_current",
        "return_date": date(2026, 12, 8), "review_date": date(2026, 11, 30), "confidence": "likely",
        "note": "Re-evaluated in two weeks.", "author": "analyst", "created_at": datetime(2026, 11, 15, 15, 0),
        "evidence": [{"type": "news", "id": 80, "headline": "Calf strain"}, {"type": "report", "id": 7}],
    }
    return ProjectionAdjustment(**{**fields, **over})


def body(**over) -> dict:
    base = {
        "player_id": BACKUP,
        "projection": {"out_now": False, "return_date": None, "out_for_season": False, "minutes": 24.0},
        "availability": {"verdict": "questionable", "expected_return": None},
        "teammates": [{"player_id": STAR, "out": True}, {"player_id": 203, "out": False}],
        "items": [{"ref": "report:7"}, {"ref": "news:91"}, {"ref": "news:80"}],
        "recent_games": [{"date": "2026-11-15", "min": 22.0}],
        "bounds": {"minutes_max": 38.0},
    }
    return {**base, **over}


class TestWhatAReadIsShown:
    @pytest.mark.parametrize("verdict, expected", [
        ("out", "out"), ("doubtful", "out"), ("questionable", "questionable"),
        ("probable", "in"), ("available", "in"), (None, "in"),
    ])
    def test_a_verdict_is_out_a_question_or_in(self, verdict, expected):
        assert inputs.status_class(verdict) == expected

    def test_the_projection_has_him_out_on_any_of_the_three_things_it_can_be_told(self):
        assert inputs.model_out(None) is False and inputs.model_out(Availability()) is False
        assert inputs.model_out(Availability(out_next=True)) is True
        assert inputs.model_out(Availability(return_date=TODAY, out_now=True)) is True
        assert inputs.model_out(Availability(out_for_season=True)) is True

    def test_a_teammate_is_out_on_a_missed_game_or_on_what_the_projection_was_told(self):
        healthy, missing = Member(STAR, 34.0, 20.0, 0, False), Member(STAR, 34.0, 20.0, 2, False)
        assert inputs.member_out(healthy, {}) is False and inputs.member_out(missing, {}) is True
        assert inputs.member_out(healthy, {STAR: Availability(out_next=True)}) is True

    def test_recent_minutes_are_his_last_five_played(self):
        assert inputs.recent_minutes([10, 20, 30, 30, 30, 30, 30]) == (30.0, 5)
        assert inputs.recent_minutes([12.0, 25.5]) == (18.8, 2)
        assert inputs.recent_minutes([]) == (None, 0)

    def test_an_item_reads_in_eastern_time_and_long_text_is_cut(self):
        seen = datetime(2026, 11, 16, 3, 30)                # stored without a zone: UTC
        item = Item(ref="news:9", kind="injury", citable=True, source="espn_player_news", at=None, seen=seen,
                    text=inputs._clip("word " * 400))
        shown = item.as_packet()
        assert shown["at"] == "2026-11-15 22:30" and set(shown) == {"ref", "kind", "citable", "source", "at", "text"}
        assert len(shown["text"]) == inputs.TEXT_CHARS and shown["text"].endswith("…")
        assert inputs._clip("  two   spaces\nand a line ") == "two spaces and a line"
        assert inputs.aware(seen).tzinfo is timezone.utc and inputs.eastern_label(None) is None

    def test_a_note_as_a_read_sees_it(self):
        record = note(minutes=30, usage=1.1, depends_on_player=STAR, until_date=date(2026, 11, 28))
        shown = note_packet(record, note_standing(record, TODAY, {}), {STAR: "Star"})

        assert shown == {
            "written": "2026-11-15", "kind": "injury_current", "return_date": "2026-12-08",
            "out_for_season": False, "minutes": 30.0, "usage": 1.1, "until_date": "2026-11-28",
            "until_player_id": STAR, "until_player": "Star", "confidence": "likely", "review_date": "2026-11-30",
            "reason": "Re-evaluated in two weeks.", "cites": ["news:80", "report:7"],
            "standing": {"state": "in_force", "games_part_applies": True, "role_part_applies": True, "why": []},
        }
        off = note_packet(note(games=0, return_date=None), note_standing(note(games=0, return_date=None), TODAY, {}), {})
        assert off["out_for_season"] is True and "return_date" not in off and "minutes" not in off


class TestBounds:
    def test_a_backup_gets_what_he_plays_plus_what_the_absent_left_behind(self):
        found = bounds(TODAY, usual=14.0, recent=18.0, vacated=10.0, season_end=date(2027, 4, 11))
        assert found == {"minutes_max": 32.0, "usage_min": 0.6, "usage_max": 1.4, "review_by": "2026-12-07",
                         "season_end": "2027-04-11"}

    def test_nobody_is_given_more_than_a_starters_night(self):
        assert bounds(TODAY, 30.0, 33.0, 34.0, None)["minutes_max"] == 38.0

    def test_with_nobody_out_there_is_only_a_little_room(self):
        found = bounds(TODAY, usual=20.0, recent=None, vacated=0.0, season_end=None)
        assert found["minutes_max"] == 24.0 and "season_end" not in found


class TestWhenItsOwnNoteComesDue:
    def test_a_note_in_force_and_not_yet_due_is_left_alone(self):
        record = note()
        assert note_due(record, note_standing(record, TODAY, {}), TODAY) is None
        assert own_state(record, TODAY, {}) == ["in_force", True, False, False]

    def test_on_its_review_date_it_is_read_again_while_it_still_applies(self):
        record = note(review_date=TODAY)
        assert note_due(record, note_standing(record, TODAY, {}), TODAY) == "His note is due to be read again today"
        assert own_state(record, TODAY, {}) == ["in_force", True, False, True]

    def test_past_its_review_date_it_is_no_longer_applied(self):
        record = note(review_date=TODAY - timedelta(days=1))
        assert note_due(record, note_standing(record, TODAY, {}), TODAY) == (
            "His note passed its review date, 2026-11-15, and is no longer applied")
        assert own_state(record, TODAY, {}) == ["review_due", False, False, True]

    def test_a_note_he_played_through_has_ended(self):
        played = {BACKUP: datetime(2026, 11, 15, 19, 0)}        # tipped after the note was written that morning
        record = note(created_at=datetime(2026, 11, 15, 14, 0))
        assert note_due(record, note_standing(record, TODAY, played), TODAY) == (
            "His note has ended: he played on 2026-11-15")
        assert own_state(record, TODAY, played) == ["ended", False, False, True]

    def test_a_note_ended_in_part_says_so(self):
        played = {STAR: datetime(2026, 11, 15, 19, 0)}
        record = note(created_at=datetime(2026, 11, 15, 14, 0), minutes=30, depends_on_player=STAR)
        assert note_due(record, note_standing(record, TODAY, played), TODAY) == (
            "His note has ended in part: the teammate it depended on played on 2026-11-15")
        assert own_state(record, TODAY, played) == ["in_force", True, False, True]

    def test_no_note_is_no_state(self):
        assert own_state(None, TODAY, {}) is None


class TestTheFingerprint:
    def test_it_is_the_same_for_the_same_facts(self):
        first = fingerprint(body(), ["in_force", True, False, False], None)
        assert first == fingerprint(body(), ["in_force", True, False, False], None) and len(first) == 64

    @pytest.mark.parametrize("moved", [
        {"recent_games": [{"date": "2026-11-16", "min": 31.0}]},              # a new box score
        {"bounds": {"minutes_max": 30.0}},
        {"projection": {"out_now": False, "return_date": None, "out_for_season": False, "minutes": 27.5}},
        {"items": [{"ref": "report:7"}, {"ref": "news:91"}]},                # an old item aged out of the window
        {"reasons": ["Ranked 12 here and 60 by ESPN"]},
        {"as_of": "2026-11-17"},
    ])
    def test_what_moves_every_day_does_not_move_it(self, moved):
        assert fingerprint(body(**moved), None, None) == fingerprint(body(), None, None)

    @pytest.mark.parametrize("moved", [
        {"items": [{"ref": "report:7"}, {"ref": "news:95"}, {"ref": "news:91"}]},        # a newer news item
        {"items": [{"ref": "report:8"}, {"ref": "report:7"}, {"ref": "news:91"}]},       # a newer report
        # Stored since his last read with an older date on it, so it sorts below what he has seen
        {"items": [{"ref": "report:7"}, {"ref": "news:91"}, {"ref": "news:95"}, {"ref": "news:80"}]},
        {"items": [{"ref": "report:7"}, {"ref": "report:9"}, {"ref": "news:91"}, {"ref": "news:80"}]},
        {"availability": {"verdict": "out", "expected_return": None}},
        {"availability": {"verdict": "questionable", "expected_return": "2026-11-20"}},
        {"projection": {"out_now": True, "return_date": None, "out_for_season": False}},
        {"projection": {"out_now": False, "return_date": "2026-11-20", "out_for_season": False}},
        {"teammates": [{"player_id": STAR, "out": False}, {"player_id": 203, "out": False}]},
        {"teammates": [{"player_id": STAR, "out": True}, {"player_id": 203, "out": True}]},
    ])
    def test_a_fact_a_read_turns_on_does(self, moved):
        assert fingerprint(body(**moved), None, None) != fingerprint(body(), None, None)

    def test_the_order_the_items_are_shown_in_does_not_move_it(self):
        shuffled = {"items": [{"ref": "news:80"}, {"ref": "news:91"}, {"ref": "report:7"}]}
        assert fingerprint(body(**shuffled), None, None) == fingerprint(body(), None, None)

    def test_its_own_note_and_the_owners_are_facts_too(self):
        plain = fingerprint(body(), None, None)
        noted = fingerprint(body(), ["in_force", True, False, False], None)
        assert len({plain, noted, fingerprint(body(), ["in_force", True, False, True], None),
                    fingerprint(body(), ["ended", False, False, True], None),
                    fingerprint(body(), None, 12), fingerprint(body(), None, 13)}) == 6

    def test_a_player_with_no_report_and_no_items_still_has_one(self):
        bare = {"player_id": BACKUP, "projection": {"out_now": False}, "teammates": [], "items": []}
        assert len(fingerprint(bare, None, None)) == 64
