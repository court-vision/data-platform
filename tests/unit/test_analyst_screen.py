"""
The analyst's morning screen (services.analyst_screen): which players get a
read, why, and in what order. Fixed rules over what changed for a player; no
model and no database.
"""

import pytest

from services.analyst_screen import (
    MINUTES_GAP,
    NEW_ITEM,
    NOTE_DUE,
    RANK_GAP,
    STATUS_CHANGED,
    TEAMMATE_CHANGE,
    StatusChange,
    Subject,
    TeammateChange,
    minutes_gap,
    rank_gap,
    screen,
    widest_gaps,
)

pytestmark = pytest.mark.unit


def subject(pid=1, **over) -> Subject:
    fields = {"player_id": pid, "name": f"P{pid}", "minutes_usual": 30.0, "citable_items": 1, "cv_rank": 100}
    return Subject(**{**fields, **over})


def rules(candidates) -> dict[int, tuple[str, ...]]:
    return {c.player_id: c.rules for c in candidates}


def test_a_player_nothing_changed_for_is_not_read():
    assert screen([subject(), subject(2, cv_rank=40, espn_rank=45)]) == []


def test_each_rule_picks_its_player_and_says_why_in_words():
    picked = screen([
        subject(1, has_note=True, note_due="His note is due to be read again today"),
        subject(2, status_change=StatusChange(was="available", now="out")),
        subject(3, new_items=("injury", "injury", "lineup")),
        subject(4, teammate_changes=(TeammateChange(name="Star", minutes=34.0, out=True),)),
        subject(5, recent_minutes=31.2, recent_games=5, projected_minutes=24.0),
        subject(6, cv_rank=45, espn_rank=110),
    ])

    assert rules(picked) == {1: (NOTE_DUE,), 2: (STATUS_CHANGED,), 3: (NEW_ITEM,), 4: (TEAMMATE_CHANGE,),
                             5: (MINUTES_GAP,), 6: (RANK_GAP,)}
    # In the rules' own order: the strongest reason is read first
    assert [c.player_id for c in picked] == [1, 2, 3, 4, 5, 6]
    said = {c.player_id: c.reasons[0].text for c in picked}
    assert said[1] == "His note is due to be read again today"
    assert said[2] == "The availability feed moved him from available to out"
    assert said[3] == "3 new items since his last read: injury, lineup"
    assert said[4] == "Star, who plays 34 minutes a game, is now out"
    assert said[5] == "He played 31.2 minutes a game over his last 5; the projection has him at 24"
    assert said[6] == "Ranked 45 here and 110 by ESPN"


def test_a_first_report_and_a_return_read_as_what_they_are():
    first, back = screen([
        subject(1, status_change=StatusChange(was=None, now="out")),
        subject(2, teammate_changes=(TeammateChange(name="Star", minutes=34.5, out=False),)),
    ])
    assert first.reasons[0].text == "The availability feed now has him out; it had no report on him before"
    assert back.reasons[0].text == "Star, who plays 34.5 minutes a game, is back"


def test_every_rule_that_picks_him_is_a_reason_and_the_first_places_him():
    both = subject(1, new_items=("injury",), status_change=StatusChange(was="available", now="out"),
                   cv_rank=80)
    item_only = subject(2, new_items=("transaction",), cv_rank=10)

    picked = screen([item_only, both])

    assert rules(picked)[1] == (STATUS_CHANGED, NEW_ITEM)
    # A status change outranks a better-ranked player with only an item
    assert [c.player_id for c in picked] == [1, 2]
    assert picked[0].reasons[0].as_dict() == {
        "rule": STATUS_CHANGED, "text": "The availability feed moved him from available to out"}


def test_within_a_rule_the_better_ranked_player_is_read_first_then_the_one_who_plays_more():
    # The unranked two are read at all only for the notes they carry
    picked = screen([
        subject(1, new_items=("injury",), cv_rank=None, has_note=True, minutes_usual=20.0),
        subject(2, new_items=("injury",), cv_rank=None, has_note=True, minutes_usual=34.0),
        subject(3, new_items=("injury",), cv_rank=150),
        subject(4, new_items=("injury",), cv_rank=12),
    ])
    assert [c.player_id for c in picked] == [4, 3, 2, 1]


def test_without_a_citable_item_there_is_nothing_to_write():
    change = StatusChange(was="available", now="out")
    assert screen([subject(1, status_change=change, citable_items=0)]) == []
    # ... but a note he already carries can still be kept or retired
    picked = screen([subject(1, status_change=change, citable_items=0, has_note=True)])
    assert rules(picked) == {1: (STATUS_CHANGED,)}


def test_the_screen_is_for_players_the_board_ranks_in_its_top_200():
    news = {"new_items": ("injury",)}
    # The board ranks him, so he matters whatever he has played
    assert rules(screen([subject(1, cv_rank=200, minutes_usual=8.0, **news)])) == {1: (NEW_ITEM,)}
    # Past that he waits for the stats-only board to lift him in, however much he plays
    assert screen([subject(1, cv_rank=201, minutes_usual=34.0, **news)]) == []
    assert screen([subject(1, cv_rank=None, minutes_usual=34.0, **news)]) == []


def test_a_note_of_the_analysts_own_is_read_again_wherever_he_ranks():
    low = {"cv_rank": 260, "minutes_usual": 8.0, "has_note": True}
    due = subject(1, note_due="His note has ended: he played on 2026-11-14", **low)
    assert rules(screen([due])) == {1: (NOTE_DUE,)}
    # ... and so is whatever else changes for him while the note stands
    back = subject(1, status_change=StatusChange(was="out", now="available"), **low)
    assert rules(screen([back])) == {1: (STATUS_CHANGED,)}
    assert screen([subject(1, **low)]) == []


def test_a_gap_to_espn_is_no_reason_for_a_player_below_the_floor():
    # 201 here against 40 at ESPN is among the day's widest, but he is not who the screen is for
    assert screen([subject(1, cv_rank=201, espn_rank=40)]) == []
    assert rules(screen([subject(1, cv_rank=200, espn_rank=40)])) == {1: (RANK_GAP,)}


def test_a_due_reason_without_a_note_is_not_a_reason():
    assert screen([subject(1, note_due="stale", has_note=False)]) == []


@pytest.mark.parametrize("recent, games, projected, out_now, gap", [
    (31.2, 5, 24.0, False, 7.2),
    (18.0, 5, 26.0, False, -8.0),
    (29.0, 5, 24.0, False, None),       # under six minutes is noise
    (31.2, 4, 24.0, False, None),       # not five games yet
    (31.2, 5, 24.0, True, None),        # he is out: his last five are not his next five
    (None, 0, 24.0, False, None),
])
def test_the_minutes_gap_is_his_last_five_against_the_projection(recent, games, projected, out_now, gap):
    found = minutes_gap(subject(recent_minutes=recent, recent_games=games, projected_minutes=projected,
                                out_now=out_now))
    assert found == (pytest.approx(gap) if gap is not None else None)


@pytest.mark.parametrize("cv, espn, gap", [
    (45, 110, 65), (110, 45, -65),
    (45, 70, None),         # under thirty places
    (160, 240, None),       # neither rank is high enough to matter
    (140, 240, 100),
    (None, 40, None), (40, None, None),
])
def test_the_rank_gap_is_espns_rank_less_ours(cv, espn, gap):
    assert rank_gap(subject(cv_rank=cv, espn_rank=espn)) == gap


def test_only_the_days_widest_gaps_are_read():
    pool = [subject(pid, cv_rank=10 + pid, espn_rank=10 + pid + 30 + pid) for pid in range(1, 13)]
    widest = widest_gaps(pool)
    assert widest == {5, 6, 7, 8, 9, 10, 11, 12}
    assert widest_gaps(pool, top=2) == {11, 12}
    assert {c.player_id for c in screen(pool)} == widest
