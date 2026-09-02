"""Preseason-market pipeline: window gate and the pure ESPN-payload transforms."""

from datetime import date

import pytest

from pipelines.extractors.espn import parse_draft_market_players
from pipelines.gates import preseason_market_window
from pipelines.preseason_market import projected_gp, projection_line


# ---- window gate -----------------------------------------------------------


@pytest.mark.parametrize("today,should_run", [
    (date(2026, 8, 14), False),   # day before it opens
    (date(2026, 8, 15), True),    # opening day
    (date(2026, 9, 20), True),    # mid draft season
    (date(2026, 10, 31), True),   # closing day
    (date(2026, 11, 1), False),   # season underway
    (date(2026, 7, 4), False),    # deep offseason
])
def test_preseason_window_boundaries(today, should_run):
    decision = preseason_market_window(today)
    assert decision.run is should_run
    assert decision.reason == ("preseason_window_open" if should_run else "outside_preseason_window")


# ---- parse_draft_market_players -------------------------------------------


def _espn_player(**overrides):
    base = {
        "id": 3112335,
        "fullName": "Nikola Jokic",
        "defaultPositionId": 5,
        "eligibleSlots": [4, 9, 10, 11, 12, 13],
        "injuryStatus": "ACTIVE",
        "draftRanksByRankType": {
            "STANDARD": {"rank": 1, "auctionValue": 65, "rankType": "STANDARD"},
            "ROTO": {"rank": 2, "auctionValue": 65},
        },
        "ownership": {"percentOwned": 99.88, "averageDraftPosition": 1.79, "auctionValueAverage": 68.59},
        "stats": [
            {"id": "002027", "appliedTotal": 0},
            {"id": "102027", "appliedTotal": 5067.0, "appliedAverage": 68.47,
             "averageStats": {"0": 28.1, "6": 12.6, "3": 10.1}},
        ],
    }
    base.update(overrides)
    return base


def test_parse_reads_rank_adp_and_projected_split():
    rows = parse_draft_market_players([_espn_player()], projected_split_id="102027")
    assert len(rows) == 1
    row = rows[0]
    assert row["espn_id"] == 3112335 and row["normalized_name"] == "nikola jokic"
    assert row["overall_rank"] == 1 and row["auction_value"] == 65
    assert row["adp"] == 1.79 and row["auction_value_avg"] == 68.59
    assert row["projected_total"] == 5067.0 and row["projected_stats"] == {"0": 28.1, "6": 12.6, "3": 10.1}


def test_parse_without_projected_split_is_market_only():
    # The 2027 preseason state: ranks live, only last season's 102026 split present.
    player = _espn_player(stats=[{"id": "102026", "appliedTotal": None, "averageStats": {"0": 27.0}}])
    row = parse_draft_market_players([player], projected_split_id="102027")[0]
    assert row["overall_rank"] == 1
    assert row["projected_total"] is None and row["projected_stats"] is None


def test_parse_handles_missing_blocks_and_drops_anonymous_entries():
    bare = {"id": 99, "fullName": "Deep Bencher", "stats": []}
    rows = parse_draft_market_players(
        [bare, {"id": None, "fullName": "No Id"}, {"id": 5}, None],
        projected_split_id="102027",
    )
    assert len(rows) == 1
    row = rows[0]
    assert row["espn_id"] == 99
    assert row["overall_rank"] is None and row["adp"] is None and row["projected_stats"] is None
    assert row["default_position_id"] is None and row["eligible_slot_ids"] is None
    assert row["injury_status"] is None


def test_parse_unwraps_nothing_but_accepts_empty_average_stats():
    player = _espn_player(stats=[{"id": "102027", "appliedTotal": 100.0, "appliedAverage": 2.0,
                                  "averageStats": {}}])
    row = parse_draft_market_players([player], projected_split_id="102027")[0]
    assert row["projected_stats"] is None            # empty dict normalized to None
    assert row["projected_total"] == 100.0


def test_parse_captures_position_eligibility_and_injury():
    """The three fields the draft room counts caps and eligibility from."""
    row = parse_draft_market_players([_espn_player()], projected_split_id="102027")[0]
    # Native id spaces, never mixed: defaultPositionId is 1-based (5 = C),
    # eligibleSlots are 0-based lineup slots (4 = C, 9 = PF/C, 11 = UT).
    assert row["default_position_id"] == 5
    assert row["eligible_slot_ids"] == [4, 9, 10, 11, 12, 13]
    assert row["injury_status"] == "ACTIVE"
    # ESPN eligibility is authoritative and not inferred: a pure centre is not
    # F-eligible, so slot 6 is absent even though slot 9 (PF/C) is present.
    assert 6 not in row["eligible_slot_ids"]


def test_parse_injury_status_rides_through_verbatim():
    row = parse_draft_market_players([_espn_player(injuryStatus="DAY_TO_DAY")],
                                     projected_split_id="102027")[0]
    assert row["injury_status"] == "DAY_TO_DAY"


@pytest.mark.parametrize("slots,expected", [
    ([], None),                                  # empty list is no eligibility
    (None, None),                                # absent
    ("0,5", None),                               # not a list
    ([0, "5", 11], [0, 5, 11]),                  # string ids coerced
    ([0, "junk", 5], [0, 5]),                    # unusable entries dropped
])
def test_parse_normalizes_eligible_slots(slots, expected):
    row = parse_draft_market_players([_espn_player(eligibleSlots=slots)],
                                     projected_split_id="102027")[0]
    assert row["eligible_slot_ids"] == expected


@pytest.mark.parametrize("raw,expected", [("3", 3), (3.0, 3), (None, None), ("PG", None)])
def test_parse_normalizes_default_position_id(raw, expected):
    row = parse_draft_market_players([_espn_player(defaultPositionId=raw)],
                                     projected_split_id="102027")[0]
    assert row["default_position_id"] == expected


# ---- projection_line / projected_gp ----------------------------------------


def test_projection_line_maps_espn_ids_to_stat_keys():
    line = projection_line({
        "0": 28.147, "6": 12.6, "3": 10.1, "11": 3.2,    # pts, reb, ast, tov
        "13": 10.0, "14": 17.4, "40": 34.6,              # fgm, fga, min
    })
    assert line == {"pts": 28.15, "reb": 12.6, "ast": 10.1, "tov": 3.2,
                    "fgm": 10.0, "fga": 17.4, "min": 34.6}


def test_projection_line_drops_rates_composites_and_junk():
    line = projection_line({
        "19": 0.58,      # fg_pct — recomputed at read, never stored
        "42": 79,        # gp — carried separately as projected_gp
        "37": 60,        # double-doubles — not a stored stat
        "abc": 5, "0": None,
    })
    assert line == {}


def test_projected_gp_derives_from_applied_totals():
    assert projected_gp(5067.0, 68.47) == 74
    assert projected_gp(None, 68.47) is None
    assert projected_gp(100.0, 0) is None
    assert projected_gp("bad", 1.0) is None
