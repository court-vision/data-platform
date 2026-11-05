"""
The news feed against Postgres (pipelines/news_feed.py): what a tick stores in
nba.news_items and its link tables, what a second tick leaves alone, the cross-source
dedup, the watch set, what goes to Dime and the `raw` prune.

The sources are the fixtures in tests/fixtures/news/ (fetched 2026-11-05) and two injury
notes; the HTTP calls are faked, everything under them runs for real under freeze_time.
07:00Z is a tick RealGM is read on (one in three); 07:05Z is not.
"""

from __future__ import annotations

import json
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pytest
from freezegun import freeze_time

from core.settings import settings
from db.models.lineup_snapshots import LineupSnapshot, LineupSnapshotPlayer
from db.models.nba.news import NewsItem, NewsItemPlayer, NewsItemTeam
from db.models.nba.player_ownership import PlayerOwnership
from db.models.nba.player_profiles import PlayerProfile
from db.models.nba.players import Player
from pipelines.extractors.espn_injuries import EspnInjury
from pipelines.extractors.news_feeds import parse_espn_player_news, parse_realgm_rss, parse_rotowire_rss
from pipelines.news_feed import NewsFeedPipeline, prune_raw, watch_set
from pipelines.transformers import normalize_name
from schemas.common import ApiStatus
from services.backend_client import EventsPost

pytestmark = pytest.mark.integration

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "news"
ROTOWIRE = (FIXTURES / "rotowire_2026-11-05.xml").read_bytes()
REALGM = (FIXTURES / "realgm_2026-11-05.xml").read_bytes()
BALL_NEWS = json.loads((FIXTURES / "espn_player_news_4432816.json").read_text())

BALL, KREJCI, KNUEPPEL, GILBERT, YOUNG = 1630163, 1630249, 1642851, 1642354, 1629027
PLAYERS = {
    BALL: ("LaMelo Ball", 4432816, "CHA"),
    KREJCI: ("Vit Krejci", 4592402, "ATL"),
    KNUEPPEL: ("Kon Knueppel", 5104157, "CHA"),
    GILBERT: ("Keshon Gilbert", 4585618, "ATL"),
    YOUNG: ("Trae Young", 4277905, "WAS"),
}
RG_TICK, PLAIN_TICK = "2026-11-05T07:00:00", "2026-11-05T07:05:00"


def _utc(at: str) -> datetime:
    return datetime.fromisoformat(at).replace(tzinfo=timezone.utc)


def note(pid: int, detail: str, written: str, item_id: str = None) -> EspnInjury:
    name, espn_id, team = PLAYERS[pid]
    return EspnInjury(
        item_id=item_id or f"note-{pid}", espn_id=espn_id, name=name, team=team, status="out",
        espn_status="Out", fantasy_status="OUT", reason="Knee", detail=detail, long_comment=None,
        expected_return=None, source_time=_utc(written),
    )


class FakeFetcher:
    def __init__(self):
        self.player_reads: list[int] = []
        self.down: set[str] = set()

    def _check(self, source):
        if source in self.down:
            raise ConnectionError(f"{source} is down")

    def espn_player_news(self, espn_id, limit=5):
        self._check("espn_player_news")
        self.player_reads.append(espn_id)
        return parse_espn_player_news(BALL_NEWS, espn_id) if espn_id == 4432816 else []

    def rotowire(self):
        self._check("rotowire_rss")
        return parse_rotowire_rss(ROTOWIRE)

    def realgm(self):
        self._check("realgm_rss")
        return parse_realgm_rss(REALGM)


class FakeClient:
    def __init__(self):
        self.news: list[list[int]] = []

    def post_events(self, *, availability=(), news=(), correlation_id=None):
        self.news.append(list(news))
        return EventsPost(ok=True, recorded=len(news))


@pytest.fixture
def feeds(integration_db, monkeypatch):
    for pid, (name, espn_id, team) in PLAYERS.items():
        Player.create(id=pid, name=name, name_normalized=normalize_name(name), espn_id=espn_id)
        PlayerProfile.create(player=pid, team=team, to_year=None)
    # Ball is on a tracked roster; Young is the one most-rostered other.
    snap = LineupSnapshot.create(provider="espn", provider_league_id="552315826", season=2027, provider_team_id=1,
                                 team_name="Test", scoring_period_id=15, nba_date=date(2026, 11, 4), player_count=1)
    LineupSnapshotPlayer.create(snapshot=snap, player_id=4432816, player_name="LaMelo Ball", lineup_slot_id=0)
    for pid, pct in ((YOUNG, 99.1), (BALL, 99.5), (KREJCI, 2.0)):
        PlayerOwnership.create(player=pid, snapshot_date=date(2026, 11, 4), rost_pct=pct)
    monkeypatch.setattr(settings, "news_free_agents", 1)
    monkeypatch.setattr(settings, "news_watch_slices", 1)
    monkeypatch.setattr(settings, "post_dime_events", True)
    return Feeds()


class Feeds:
    def __init__(self):
        self.injuries: list[EspnInjury] = []
        self.fetcher = FakeFetcher()
        self.client = FakeClient()

    def tick(self, at: str):
        pipeline = NewsFeedPipeline()
        pipeline.injuries_extractor.extract = lambda: (list(self.injuries), 0)
        pipeline.fetcher = self.fetcher
        pipeline.backend_client = self.client
        seen = []
        pipeline.after_execute = seen.append
        with freeze_time(at):
            result = pipeline._run_sync()
        assert result.status == ApiStatus.SUCCESS, result.error
        return result, dict(seen[0].skip_reasons), dict(seen[0].failure_reasons)


def items() -> dict[tuple[str, str], NewsItem]:
    return {(i.source, i.source_item_id): i for i in NewsItem.select()}


def links(item: NewsItem) -> tuple[list[tuple[int, str]], list[str]]:
    players = sorted((l.player_id, l.linked_by) for l in NewsItemPlayer.select().where(NewsItemPlayer.news_item == item))
    teams = sorted(t.team_id for t in NewsItemTeam.select().where(NewsItemTeam.news_item == item))
    return players, teams


def test_a_tick_stores_each_sources_items_linked_to_their_players(feeds):
    feeds.injuries = [note(GILBERT, "Gilbert (knee) has been ruled out for Thursday's game against the Hornets.",
                           "2026-11-05T06:30:00")]

    result, skips, failures = feeds.tick(RG_TICK)

    stored = items()
    assert failures == {}
    assert sorted(feeds.fetcher.player_reads) == [4277905, 4432816]
    gilbert = stored[("espn_injuries", "note-1642354:20261105T0630")]
    assert (gilbert.kind, links(gilbert)) == ("availability", ([(GILBERT, "espn_id")], ["ATL"]))
    ball = stored[("espn_player_news", "53762846")]
    assert ball.kind == "performance" and links(ball) == ([(BALL, "espn_id")], ["CHA"])
    krejci = stored[("rotowire_rss", "nba537626")]
    assert (krejci.kind, krejci.attribution) == ("injury", "Rotowire")
    assert links(krejci) == ([(KREJCI, "name")], ["ATL"])
    knueppel = next(i for i in stored.values() if i.headline == "Kon Knueppel Leaves Game With Leg Soreness")
    assert (knueppel.source, knueppel.kind, links(knueppel)) == ("realgm_rss", "injury", ([(KNUEPPEL, "name")], ["CHA"]))

    # Ball's Rotowire RSS item is ESPN's item in other words' clothing: the ESPN one is kept.
    assert ("rotowire_rss", "nba537628") not in stored
    assert skips["duplicate"] == 1
    # Booker, Wembanyama and George are no one here; RealGM's league stories name no one either.
    assert skips["name_unmatched"] == 3
    assert skips["unlinked"] >= 3 + 4
    assert result.records_processed == len(stored)

    # Dime hears about Gilbert's ruling, Krejci's fracture (Rotowire's words and RealGM's) and
    # Knueppel's exit: new, about players, of its kinds. Not Ball's stat lines.
    krejci_realgm = next(i for i in stored.values() if i.headline == "Vit Krejci Suffers Fractured Lower Leg")
    assert feeds.client.news == [sorted(i.id for i in (gilbert, krejci, knueppel, krejci_realgm))]


def test_a_second_tick_stores_nothing_and_tells_dime_nothing(feeds):
    feeds.tick(RG_TICK)
    before = len(items())

    result, skips, _ = feeds.tick("2026-11-05T07:15:00")

    assert len(items()) == before and result.records_processed == 0
    assert skips["seen"] >= before
    assert len(feeds.client.news) == 1


def test_a_note_espn_rewrites_is_a_new_item(feeds):
    feeds.injuries = [note(GILBERT, "Gilbert (knee) is questionable for Thursday.", "2026-11-05T06:30:00")]
    feeds.tick(PLAIN_TICK)
    feeds.injuries = [note(GILBERT, "Gilbert (knee) has been ruled out for Thursday.", "2026-11-05T07:20:00")]

    feeds.tick("2026-11-05T07:25:00")

    notes = sorted(i.headline for i in NewsItem.select().where(NewsItem.source == "espn_injuries"))
    assert notes == ["Gilbert (knee) has been ruled out for Thursday.", "Gilbert (knee) is questionable for Thursday."]


def test_realgm_waits_for_its_tick_and_a_source_that_is_down_is_counted(feeds):
    feeds.fetcher.down = {"rotowire_rss"}

    _, _, failures = feeds.tick(PLAIN_TICK)

    assert failures == {"rotowire_rss_unavailable": 1}
    assert {s for s, _ in items()} == {"espn_player_news"}


def test_the_watch_set_reads_tracked_rosters_then_the_most_rostered_others(feeds):
    assert watch_set(free_agents=1, cap=10) == [4432816, 4277905]
    assert watch_set(free_agents=2, cap=10) == [4432816, 4277905, 4592402]
    assert watch_set(free_agents=1, cap=1) == [4277905]


def test_raw_is_cleared_after_thirty_days(feeds):
    feeds.tick(PLAIN_TICK)
    old = NewsItem.select().first()
    NewsItem.update(fetched_at=_utc(PLAIN_TICK) - timedelta(days=31)).where(NewsItem.id == old.id).execute()

    assert prune_raw(_utc(PLAIN_TICK)) == 1
    assert NewsItem.get_by_id(old.id).raw is None
    assert NewsItem.select().where(NewsItem.raw.is_null(False)).count() == len(items()) - 1
