"""
The news feed's sources and rules against real payloads (tests/fixtures/news/, fetched
2026-11-05; ESPN's injury list is the availability tests' 2026-10-15 copy): what each
parser keeps, the kind table, the cross-source hash, the name scan and the watch set.
"""

import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

import pytest

from pipelines.extractors.espn_injuries import parse_espn_injuries
from pipelines.extractors.news_feeds import (
    items_from_injuries,
    parse_espn_player_news,
    parse_realgm_rss,
    parse_rotowire_rss,
    parse_rss_date,
    watch_slice,
)
from pipelines.availability import PlayerIndex
from pipelines.news_feed import Stored, choose_watch, event_items, link_players
from pipelines.transformers import normalize_name
from pipelines.transformers.news import NameScanner, classify, content_hash, kind_for, linked_teams

pytestmark = pytest.mark.unit

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures"
ROTOWIRE = (FIXTURES / "news" / "rotowire_2026-11-05.xml").read_bytes()
REALGM = (FIXTURES / "news" / "realgm_2026-11-05.xml").read_bytes()
BALL = json.loads((FIXTURES / "news" / "espn_player_news_4432816.json").read_text())
INJURIES = json.loads((FIXTURES / "espn_injuries_2026-10-15.json").read_text())


def _utc(at: str) -> datetime:
    return datetime.fromisoformat(at).replace(tzinfo=timezone.utc)


# ---- the sources -----------------------------------------------------------------------


def test_rotowire_items_name_their_player_and_drop_the_feeds_tail():
    items = parse_rotowire_rss(ROTOWIRE)

    assert [i.names for i in items] == [
        ("Devin Booker",), ("Victor Wembanyama",), ("LaMelo Ball",), ("Keyonte George",), ("Vit Krejci",),
    ]
    krejci = items[-1]
    assert krejci.source == "rotowire_rss" and krejci.source_item_id == "nba537626"
    assert krejci.headline == "Vit Krejci: Suffers leg fracture"
    assert krejci.published_at == _utc("2026-11-05T05:17:00")       # 9:17 PM PST
    assert krejci.url == "https://www.rotowire.com/basketball/player/vit-krejci-5233"
    assert "Visit RotoWire" not in krejci.blurb and krejci.blurb.endswith("reports.")
    assert krejci.body == krejci.blurb and not krejci.scan_text


def test_realgm_items_keep_the_first_paragraph_and_are_scanned_for_names():
    items = parse_realgm_rss(REALGM)

    assert len(items) == 8
    knueppel = next(i for i in items if i.headline == "Kon Knueppel Leaves Game With Leg Soreness")
    assert knueppel.scan_text and knueppel.names == () and knueppel.attribution == "RealGM"
    assert knueppel.published_at == _utc("2026-11-05T06:39:38")    # 01:39:38 EST
    assert knueppel.body.startswith("Kon Knueppel left the Charlotte Hornets' 114-104 loss")
    assert "<" not in knueppel.body
    assert knueppel.url and knueppel.source_item_id


def test_espn_player_news_keeps_rotowire_items_and_leaves_video_out():
    items = parse_espn_player_news(BALL, 4432816)

    assert [i.source_item_id for i in items] == ["53762846", "53731246"]
    first = items[0]
    assert first.espn_ids == (4432816,)
    assert first.headline.startswith("Ball totaled 20 points (6-11 FG")
    assert first.blurb == first.headline
    assert first.body.startswith("Ball did not miss during the final frame")
    assert first.published_at == _utc("2026-11-05T05:38:52")


def test_the_same_blurb_from_espn_and_rotowire_hashes_the_same():
    espn = parse_espn_player_news(BALL, 4432816)[0]
    rotowire = next(i for i in parse_rotowire_rss(ROTOWIRE) if i.names == ("LaMelo Ball",))

    assert content_hash(espn.blurb) == content_hash(rotowire.blurb)
    assert content_hash(espn.blurb) != content_hash(parse_espn_player_news(BALL, 4432816)[1].blurb)
    assert content_hash("Jokić  (ankle) is OUT.") == content_hash("jokic (ankle) is out")


def test_an_injury_note_is_an_item_whose_id_carries_the_notes_time():
    entries, _ = parse_espn_injuries(INJURIES)
    items = items_from_injuries(entries)

    assert len(items) == len([e for e in entries if e.detail])
    gilbert = next(i for i in items if i.espn_ids == (4585618,))
    assert gilbert.source_item_id == "534140:20261012T2136"
    assert gilbert.headline.startswith("Gilbert (knee) won't play in Monday's preseason game")
    assert gilbert.body.startswith("Gilbert continues to deal with patella tendinitis")
    assert gilbert.teams == ("ATL",) and gilbert.raw["fantasy_status"] == "GTD"


@pytest.mark.parametrize("value,expected", [
    ("Wed, 04 Nov 2026 10:00:00 PM PST", "2026-11-05T06:00:00"),
    ("Wed, 04 Nov 2026 12:05:00 AM EST", "2026-11-04T05:05:00"),
    ("Wed, 04 Nov 2026 12:30:00 PM EDT", "2026-11-04T16:30:00"),
    ("Thu, 05 Nov 2026 01:39:38 EST", "2026-11-05T06:39:38"),
    ("Thu, 05 Nov 2026 06:39:38 +0000", "2026-11-05T06:39:38"),
])
def test_rss_dates(value, expected):
    assert parse_rss_date(value) == _utc(expected)


@pytest.mark.parametrize("value", [
    None, "", "yesterday", "Wed, 04 Nov 2026 10:00:00 PM XYZ",
    # Rotowire's 12-hour shape with an impossible clock or calendar date
    "Wed, 04 Nov 2026 10:99:00 PM EST", "Mon, 31 Nov 2026 10:00:00 PM EST",
    "Wed, 04 Nov 2026 13:00:00 PM EST", "Wed, 04 Nov 2026 00:30:00 AM EST",
])
def test_an_rss_date_that_cannot_be_read_is_none(value):
    assert parse_rss_date(value) is None


def test_an_injury_note_names_its_player_and_the_name_is_tried_only_without_a_known_espn_id():
    entries, _ = parse_espn_injuries(INJURIES)
    gilbert = next(i for i in items_from_injuries(entries) if i.espn_ids == (4585618,))
    assert gilbert.names == ("Keshon Gilbert",)

    by_name = {normalize_name("Keshon Gilbert"): 77}
    skips = Counter()
    # ESPN id known: linked by it, and the name is not tried (no second, weaker link)
    known = PlayerIndex(by_espn_id={4585618: 11}, by_name=by_name)
    assert link_players(gilbert, known, NameScanner(known.by_name), skips) == [(11, "espn_id")]
    # A player new to nba.players (no ESPN id on file yet): the name still finds him
    new = PlayerIndex(by_name=by_name)
    assert link_players(gilbert, new, NameScanner(new.by_name), skips) == [(77, "name")]
    assert not skips


# ---- kinds -----------------------------------------------------------------------------


@pytest.mark.parametrize("headline,kind", [
    ("Smith (ankle) has been ruled out for Friday's game against Boston.", "availability"),
    ("Brunson (ankle) is questionable for Friday's game.", "availability"),
    ("Gilbert (knee) won't play in Monday's preseason game against the Thunder.", "availability"),
    ("Kingston Flemings Suspended 1 Game For Elbowing Bryce McGowens", "availability"),
    ("Vit Krejci Suffers Fractured Lower Leg", "injury"),
    ("Kon Knueppel Leaves Game With Leg Soreness", "injury"),
    ("Hukporti has sustained a season-ending ruptured right Achilles tendon, per Shams Charania.", "other"),
    ("Norchad Omier To Sign With Cavs After Agreeing To Two-Way Deal With Wolves", "transaction"),
    ("Dalen Terry, Thunder Agree To Two-Year Contract", "transaction"),
    ("Jazz Decline $4.99M Option On Isaiah Collier", "transaction"),
    ("Kristaps Porzingis Could Be Traded Before The Deadline", "rumor"),
    ("Mitchell will start Friday with Garland out.", "lineup"),
    ("Ball will come off the bench Thursday.", "lineup"),
    ("Ball totaled 20 points (6-11 FG, 4-7 3Pt, 4-5 FT), two rebounds and six assists in 31 minutes.", "performance"),
    ("Posts monster double-double in win", "performance"),
    ("WNBA Finals Game 7 Averages 3.28M Viewers", "other"),
])
def test_the_kind_table(headline, kind):
    assert classify(headline) == kind


def test_the_headline_decides_and_the_body_speaks_only_when_it_says_nothing():
    recap = "Ball totaled 20 points (6-11 FG, 4-7 3Pt, 4-5 FT) in Wednesday's win."
    assert classify(recap, "He was questionable before the game with an ankle sprain.") == "performance"
    assert classify("Warriors Withholding Payments", "He is considering a buyout once traded.") == "transaction"


def test_a_rotowire_title_is_read_without_its_name_and_an_injury_note_is_never_a_move():
    assert kind_for("rotowire_rss", "Vit Krejci: Suffers leg fracture") == "injury"
    note = "The Hornets recalled Williams (hamstring) from the G League's Greensboro Swarm on Wednesday."
    assert classify(note) == "transaction"
    assert kind_for("espn_injuries", note) == "injury"
    assert kind_for("espn_injuries", "Sharpe registered 15 points (5-7 FG, 1-1 3Pt) in Sunday's win.") == "performance"


# ---- names and teams -------------------------------------------------------------------


NAMES = {
    "kon knueppel": 1642000,
    "jaren jackson jr.": 1628991,
    "victor wembanyama": 1641705,
    "kristaps porzingis": 204001,
    "shai gilgeous-alexander": 1628983,
    "d'angelo russell": 1626156,
}


def test_the_scanner_finds_full_names_in_order():
    scanner = NameScanner(NAMES)
    found = scanner.find(
        "Kon Knueppel Leaves Game With Leg Soreness",
        "Knueppel left the loss; Victor Wembanyama's 52 led the Spurs, and Shai Gilgeous-Alexander sat.",
    )
    assert found == [1642000, 1641705, 1628983]


def test_a_suffix_is_optional_and_a_full_stop_is_not_part_of_a_name():
    scanner = NameScanner(NAMES)
    assert scanner.find("The Grizzlies said Jaren Jackson will start.") == [1628991]
    assert scanner.find("A big night for Jaren Jackson Jr.") == [1628991]
    assert scanner.find("Traded: D’Angelo Russell.") == [1626156]
    assert scanner.find("Kon scored, and Knueppel did not.") == []


def test_a_short_name_two_players_share_matches_neither():
    scanner = NameScanner({"gary payton ii": 1, "gary payton": 2})
    assert scanner.find("Gary Payton II is out.") == [1]
    assert scanner.find("Gary Payton is out.") == [2]
    clash = NameScanner({"marcus morris sr.": 1, "marcus morris jr.": 2})
    assert clash.find("Marcus Morris is out.") == []


def test_an_items_teams_are_its_own_and_its_players_known_ones():
    team_of = {1: "BOS", 2: "NYK", 3: "XXX"}
    assert linked_teams(["atl"], [1, 2, 3, 4], team_of, {"ATL", "BOS", "NYK"}) == ["ATL", "BOS", "NYK"]
    assert linked_teams([], [3], team_of, {"ATL"}) == []


# ---- the watch set and what Dime hears -------------------------------------------------


def test_the_watch_set_is_the_rostered_by_rank_then_the_most_rostered_others():
    ranked = [10, 11, 12, 13, 14, 15]
    assert choose_watch({14, 12, 99}, ranked, free_agents=2, cap=10) == [12, 14, 99, 10, 11]
    # The cap keeps room for the free agents and cuts the least-rostered of the rostered.
    assert choose_watch({14, 12, 99}, ranked, free_agents=2, cap=4) == [12, 14, 10, 11]
    assert choose_watch(set(), ranked, free_agents=3, cap=10) == [10, 11, 12]


def test_each_tick_reads_its_slice_and_every_id_is_read_once_per_round():
    ids = list(range(1, 11))
    slices = [watch_slice(ids, tick, 3) for tick in range(3)]
    assert sorted(i for s in slices for i in s) == ids
    assert watch_slice(ids, 3, 3) == slices[0]
    assert watch_slice(ids, 7, 1) == ids


def test_dime_hears_of_new_items_about_players_of_the_kinds_it_reacts_to():
    now = _utc("2026-11-05T20:00:00")
    stored = [
        Stored(1, "availability", (101,), _utc("2026-11-05T19:30:00")),
        Stored(2, "performance", (101,), _utc("2026-11-05T19:30:00")),
        Stored(3, "injury", (), _utc("2026-11-05T19:30:00")),              # about a team only
        Stored(4, "transaction", (102,), _utc("2026-11-05T12:00:00")),      # 8 hours old
        Stored(5, "lineup", (103,), None),                                 # undated: new now
    ]
    assert event_items(stored, now) == [1, 5]
