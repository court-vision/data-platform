"""
What becomes of a read (services.analyst_notes.judge): the checks a proposal
has to pass before it is a note. No source, no note; bounds; every note
expires; and a note kept is the parts of it that still hold. No database: the
analyst's own note is an unsaved row.
"""

from datetime import date, datetime

import pytest

from db.models.nba import ProjectionAdjustment
from services.analyst_inputs import Item
from services.analyst_notes import KEEP, NOTHING, REFUSED, RETIRE, WRITE, judge, parse, same_note

pytestmark = pytest.mark.unit

TODAY = date(2026, 11, 16)
STAR, BACKUP = 201, 202


def item(ref="news:91", kind="injury", citable=True, url="https://example.com/91") -> Item:
    kind_, item_id = ref.split(":")
    return Item(ref=ref, kind=kind, citable=citable, source="espn_player_news", at=None, seen=None, text="x",
                evidence={"type": kind_, "id": int(item_id), "headline": "Out two weeks", "url": url})


ITEMS = [item(), item("news:92", kind="rumor", citable=False, url=None), item("report:7", kind="availability", url=None)]


def body(**over) -> dict:
    base = {
        "player_id": BACKUP,
        "teammates": [{"player_id": STAR, "name": "Star", "minutes": 34.0, "out": True},
                      {"player_id": 203, "name": "Healthy", "minutes": 28.0, "out": False}],
        "bounds": {"minutes_max": 34.0, "usage_min": 0.6, "usage_max": 1.4, "review_by": "2026-12-07",
                   "season_end": "2027-04-11"},
    }
    return {**base, **over}


def proposal(**over) -> dict:
    base = {
        "decision": "write", "reason": "ESPN on Nov 15: re-evaluated in two weeks.", "kind": "injury_current",
        "return_date": "2026-12-08", "return_earliest": "2026-11-30", "return_latest": "2026-12-20",
        "out_for_season": False, "minutes": None, "usage": None, "until_date": None, "until_player_id": None,
        "confidence": "likely", "review_date": "2026-11-30", "cites": ["news:91"], "against_owner_note": None,
    }
    return {**base, **over}


def role(**over) -> dict:
    return proposal(**{"kind": "role", "return_date": None, "return_earliest": None, "return_latest": None,
                       "minutes": 30, "until_player_id": STAR, **over})


def own_note(**over) -> ProjectionAdjustment:
    fields = {
        "id": 5, "player": BACKUP, "season": "2026-27", "scope": "analyst", "kind": "injury_current",
        "return_date": date(2026, 12, 8), "review_date": date(2026, 11, 20), "confidence": "likely",
        "note": "Re-evaluated in two weeks.", "author": "analyst", "created_at": datetime(2026, 11, 6, 15, 0),
        "evidence": [{"type": "news", "id": 80, "headline": "Calf strain"}],
    }
    return ProjectionAdjustment(**{**fields, **over})


def problems(raw, **kwargs) -> tuple[str, ...]:
    found = judge(raw, kwargs.pop("body", body()), kwargs.pop("items", ITEMS), TODAY, **kwargs)
    assert found.action == REFUSED and found.summary.startswith("Refused: ")
    return found.problems


class TestWrite:
    def test_a_return_date_with_its_range_and_its_source_becomes_the_row(self):
        found = judge(proposal(), body(), ITEMS, TODAY)

        assert found.action == WRITE and found.problems == ()
        assert found.summary == "ESPN on Nov 15: re-evaluated in two weeks."
        assert found.fields == {
            "scope": "analyst", "kind": "injury_current", "minutes": None, "usage": None, "games": None,
            "return_date": date(2026, 12, 8), "return_earliest": date(2026, 11, 30),
            "return_latest": date(2026, 12, 20), "until_date": None, "depends_on_player": None,
            "evidence": [{"type": "news", "id": 91, "headline": "Out two weeks", "url": "https://example.com/91"}],
            "confidence": "likely", "review_date": date(2026, 11, 30),
            "note": "ESPN on Nov 15: re-evaluated in two weeks.", "source_url": "https://example.com/91",
            "author": "analyst",
        }

    def test_a_role_that_waits_on_the_teammate_who_is_out(self):
        found = judge(role(usage=1.153, minutes=30.04, cites=["report:7", "news:92"]), body(), ITEMS, TODAY)

        assert found.action == WRITE
        assert (found.fields["minutes"], found.fields["usage"], found.fields["depends_on_player"]) == (30.0, 1.153, STAR)
        # The rumor it also cited backs nothing and is not kept
        assert [e["type"] for e in found.fields["evidence"]] == ["report"]
        assert found.fields["source_url"] is None

    def test_a_season_written_off_is_no_games(self):
        found = judge(proposal(out_for_season=True, return_date=None, return_earliest=None, return_latest=None),
                      body(), ITEMS, TODAY)
        assert found.action == WRITE and found.fields["games"] == 0 and found.fields["return_date"] is None

    def test_with_no_review_date_it_is_read_again_in_two_weeks(self):
        assert judge(proposal(review_date=None), body(), ITEMS, TODAY).fields["review_date"] == date(2026, 11, 30)
        soon = body()
        soon["bounds"]["review_by"] = "2026-11-25"
        assert judge(proposal(review_date=None), soon, ITEMS, TODAY).fields["review_date"] == date(2026, 11, 25)

    def test_a_range_that_starts_in_the_past_starts_today(self):
        found = judge(proposal(return_earliest="2026-11-10"), body(), ITEMS, TODAY)
        assert found.fields["return_earliest"] == TODAY

    def test_an_end_means_nothing_without_a_role(self):
        found = judge(proposal(until_date="2026-11-10", until_player_id=999), body(), ITEMS, TODAY)
        assert found.action == WRITE
        assert found.fields["until_date"] is None and found.fields["depends_on_player"] is None

    @pytest.mark.parametrize("change, said", [
        (dict(cites=[]), "it cites no stored injury, availability, lineup or transaction item"),
        (dict(cites=["news:92"]), "it cites no stored injury, availability, lineup or transaction item"),
        (dict(cites=["news:91", "news:404"]), "it cites items the read was not shown: news:404"),
    ])
    def test_no_source_no_note(self, change, said):
        assert problems(proposal(**change)) == (said,)

    @pytest.mark.parametrize("change, said", [
        (dict(return_date=None, return_earliest=None, return_latest=None),
         "it changes nothing: no return date, no minutes and no share of the offense"),
        (dict(out_for_season=True), "a season written off has no return date"),
        (dict(return_date=None, minutes=20), "a range needs the likely return date too"),
        (dict(return_date="2026-11-15", return_earliest=None), "its return date, 2026-11-15, is before this morning"),
        (dict(return_date="2027-04-20", return_latest=None),
         "its return date, 2027-04-20, is past the regular season: that is a season written off"),
        (dict(return_earliest="2026-12-10"), "its range does not hold its return date"),
        (dict(return_latest="2026-12-01"), "its range does not hold its return date"),
        (dict(review_date="2026-11-16"), "its review date, 2026-11-16, is not after this morning"),
        (dict(review_date="2026-12-08"),
         "its review date, 2026-12-08, is past the latest it may be, 2026-12-07"),
    ])
    def test_dates_are_held_to_the_morning_the_season_and_the_review_window(self, change, said):
        assert problems(proposal(**change)) == (said,)

    @pytest.mark.parametrize("change, said", [
        (dict(minutes=36), "36 minutes is over its bound of 34"),
        (dict(minutes=0), "minutes must be more than none"),
        (dict(usage=1.6), "a share of the offense of 1.6 is outside 0.6 to 1.4"),
        (dict(usage=0.4), "a share of the offense of 0.4 is outside 0.6 to 1.4"),
        (dict(until_player_id=203), "the teammate it waits on, Healthy, is not out"),
        (dict(until_player_id=999), "the teammate it waits on is not in his team's rotation"),
        (dict(until_date="2026-11-10"), "its end, 2026-11-10, is before this morning"),
    ])
    def test_a_role_is_held_to_its_bounds(self, change, said):
        assert problems(role(**change)) == (said,)

    def test_every_problem_is_named(self):
        found = problems(role(minutes=44, usage=2.5, cites=[]))
        assert len(found) == 3 and "44 minutes" in found[0]


class TestKeepRetireAndNone:
    def test_a_kept_note_is_itself_with_a_new_review_date_and_what_it_newly_rests_on(self):
        own = own_note()
        found = judge(proposal(decision="keep", reason="Still out.", review_date="2026-11-28", cites=["news:91"]),
                      body(), ITEMS, TODAY, own=own, last_played={})

        assert found.action == KEEP and found.summary == "Still out."
        assert found.fields["return_date"] == date(2026, 12, 8) and found.fields["review_date"] == date(2026, 11, 28)
        # The note keeps its own words, and its old evidence with the new
        assert found.fields["note"] == "Re-evaluated in two weeks."
        assert [e["id"] for e in found.fields["evidence"]] == [80, 91]
        assert found.fields["scope"] == "analyst" and found.fields["author"] == "analyst"

    def test_a_note_past_its_review_date_can_be_kept(self):
        own = own_note(review_date=date(2026, 11, 14))
        found = judge(proposal(decision="keep", cites=[]), body(), ITEMS, TODAY, own=own, last_played={})
        assert found.action == KEEP and found.fields["return_date"] == date(2026, 12, 8)

    def test_keeping_does_not_bring_back_a_part_that_has_ended(self):
        # He played after the note was written: its return date is spent, its minutes still hold
        own = own_note(minutes=24, until_date=date(2026, 11, 30))
        played = {BACKUP: datetime(2026, 11, 14, 19, 0)}
        found = judge(proposal(decision="keep", cites=[]), body(), ITEMS, TODAY, own=own, last_played=played)

        assert found.action == KEEP
        assert found.fields["return_date"] is None and found.fields["minutes"] == 24
        assert found.fields["until_date"] == date(2026, 11, 30)

    def test_a_note_that_has_ended_cannot_be_kept(self):
        played = {BACKUP: datetime(2026, 11, 14, 19, 0)}
        said = problems(proposal(decision="keep", cites=[]), own=own_note(), last_played=played)
        assert said == ("the note it would keep has ended (he played on 2026-11-14)",)

    def test_there_must_be_a_note_to_keep(self):
        assert problems(proposal(decision="keep")) == ("there is no note of its own to keep",)

    def test_keep_is_held_to_the_review_window_and_to_what_it_was_shown(self):
        said = problems(proposal(decision="keep", review_date="2027-01-01", cites=["news:404"]),
                        own=own_note(), last_played={})
        assert said == ("its review date, 2027-01-01, is past the latest it may be, 2026-12-07",
                        "it cites items the read was not shown: news:404")

    def test_retire_withdraws_its_note_and_is_nothing_without_one(self):
        raw = proposal(decision="retire", reason="He played last night.")
        assert judge(raw, body(), ITEMS, TODAY, own=own_note()).action == RETIRE
        nothing = judge(raw, body(), ITEMS, TODAY)
        assert (nothing.action, nothing.summary) == (NOTHING, "He played last night.")

    def test_none_writes_nothing(self):
        found = judge(proposal(decision="none", reason="The items say nothing new."), body(), ITEMS, TODAY)
        assert (found.action, found.fields, found.summary) == (NOTHING, {}, "The items say nothing new.")


class TestTheOwnersNote:
    def test_what_it_says_against_a_note_written_by_hand_is_carried_whatever_it_decided(self):
        against = "Newer reports say January, not December 1."
        with_owner = body(owner_note={"return_date": "2026-12-01"})
        for decision in ("write", "none"):
            found = judge(proposal(decision=decision, against_owner_note=against), with_owner, ITEMS, TODAY)
            assert found.disagrees == against
        refused = judge(proposal(cites=[], against_owner_note=against), with_owner, ITEMS, TODAY)
        assert refused.action == REFUSED and refused.disagrees == against

    def test_with_no_note_written_by_hand_there_is_nothing_to_disagree_with(self):
        assert judge(proposal(against_owner_note="x"), body(), ITEMS, TODAY).disagrees is None


class TestParsing:
    def test_an_answer_that_is_not_a_proposal_is_refused(self):
        assert "could not be read" in problems({"decision": "promote"})[0]
        assert "could not be read" in problems(proposal(return_date="early December"))[0]
        assert "could not be read" in problems(None)[0]

    def test_a_field_this_side_does_not_know_is_ignored(self):
        assert parse({**proposal(), "rank": 3}).return_date == date(2026, 12, 8)


def test_the_same_note_is_the_same_whatever_its_review_date_and_evidence():
    own = own_note(return_earliest=date(2026, 11, 30), return_latest=date(2026, 12, 20))
    fields = judge(proposal(), body(), ITEMS, TODAY).fields
    assert same_note(own, fields)
    assert not same_note(own, {**fields, "return_date": date(2026, 12, 15)})
    assert not same_note(own, {**fields, "minutes": 24.0})
