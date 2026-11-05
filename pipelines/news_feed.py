"""
News feed pipeline: what was written about players, from free sources, as items linked to
the players and teams they are about (docs/NEWS_AND_INJURY_PIPELINES_PLAN.md § 3).

cron-runner's `news` job asks every 5 minutes. Each tick reads:

    espn_injuries     ESPN's league-wide injury list (the availability pipeline's call):
                      each entry's note, by ESPN id
    rotowire_rss      Rotowire's five newest items, by the "Name:" in the title
    espn_player_news  ESPN fantasy's per-player feed for a third of the watch set, so each
                      watched player is read every 15 minutes (`watch_set`: the players on
                      Court Vision leagues' latest lineup snapshots, then the most-rostered
                      others, settings.news_*)
    realgm_rss        RealGM's Wiretap, one tick in three: names found in its text

The sources and their parsing are extractors/news_feeds.py; the kind, the hash and the name
scan are transformers/news.py. An item is stored once per (source, source item id); one
whose text another source already gave in the last three days is a duplicate and not
stored again (the injury list's note, ESPN's Rotowire item and Rotowire's own are the same
words, and the ESPN sources are read first, so an ESPN id link wins over a name). An item that names no
player or team Court Vision knows is not stored. `raw` is cleared after 30 days.

New items about players, of the kinds Dime reacts to (injury, availability, lineup,
transaction) and written in the last three hours, are posted to the backend
(`post_dime_events`), which starts a check-in on each team Dime runs that rosters one of
the players, behind its own DIME_EVENTS_ENABLED switch. A backend that can't be reached is
logged, never a failed run.

Counters: records = items stored. Skipped = an item already stored (`seen`), the same text
from another source (`duplicate`), an item naming no one (`unlinked`), a Rotowire name that
matched no player or two (`name_unmatched`). Failed = a source that could not be read; the
run goes on with the others.
"""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Iterable, Optional, Sequence

from peewee import fn

from core.logging import get_correlation_id
from core.settings import settings
from db.base import db
from db.models import LineupSnapshot, LineupSnapshotPlayer
from db.models.nba import NBATeam, NewsItem, NewsItemPlayer, NewsItemTeam, Player, PlayerOwnership
from pipelines.availability import PlayerIndex
from pipelines.base import BasePipeline
from pipelines.config import PipelineCategory, PipelineConfig
from pipelines.context import PipelineContext
from pipelines.extractors.espn_injuries import ESPNInjuriesExtractor
from pipelines.extractors.news_feeds import NewsFetcher, RawItem, items_from_injuries, watch_slice
from pipelines.transformers.news import NameScanner, content_hash, kind_for, linked_teams
from services.backend_client import backend_client_from_settings

TICK_SECONDS = 300
DEDUP_WINDOW = timedelta(days=3)
RAW_KEPT = timedelta(days=30)
# What Dime reacts to, and how new an item must be to be news to it (the backend checks both too)
EVENT_KINDS = frozenset({"injury", "availability", "lineup", "transaction"})
EVENT_FRESH = timedelta(hours=3)
SNAPSHOT_DAYS = 2


def tick_of(now: datetime) -> int:
    return int(now.timestamp() // TICK_SECONDS)


# ---- the watch set ---------------------------------------------------------------------


def watch_set(free_agents: int, cap: int) -> list[int]:
    """ESPN ids to read ESPN's per-player news for: everyone on a Court Vision league's latest
    lineup snapshots (most-rostered first), then the `free_agents` most-rostered others, at
    most `cap` in all. Blocking DB."""
    latest = (LineupSnapshot.select(fn.MAX(LineupSnapshot.nba_date))
              .where(LineupSnapshot.provider == "espn").scalar())
    rostered: set[int] = set()
    if latest is not None:
        rostered = {
            pid for (pid,) in (LineupSnapshotPlayer
                               .select(LineupSnapshotPlayer.player_id)
                               .join(LineupSnapshot)
                               .where(LineupSnapshot.provider == "espn",
                                      LineupSnapshot.nba_date >= latest - timedelta(days=SNAPSHOT_DAYS))
                               .distinct()
                               .tuples())
        }
    ranked: list[int] = []
    day = PlayerOwnership.select(fn.MAX(PlayerOwnership.snapshot_date)).scalar()
    if day is not None:
        ranked = [
            espn_id for (espn_id,) in (PlayerOwnership
                                       .select(Player.espn_id)
                                       .join(Player, on=(PlayerOwnership.player == Player.id))
                                       .where(PlayerOwnership.snapshot_date == day, Player.espn_id.is_null(False))
                                       .order_by(PlayerOwnership.rost_pct.desc(), Player.espn_id)
                                       .tuples())
        ]
    return choose_watch(rostered, ranked, free_agents, cap)


def choose_watch(rostered: Iterable[int], ranked: Sequence[int], free_agents: int, cap: int) -> list[int]:
    """Pure: the rostered players by how rostered they are league-wide (unranked last), cut to
    leave room for the free agents, then the most-rostered players nobody here rosters."""
    rostered = set(rostered)
    rank = {pid: i for i, pid in enumerate(ranked)}
    mine = sorted(rostered, key=lambda pid: (rank.get(pid, len(rank)), pid))
    mine = mine[: max(0, cap - free_agents)]
    others = [pid for pid in ranked if pid not in rostered][: max(0, min(free_agents, cap - len(mine)))]
    return mine + others


# ---- storing ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Stored:
    id: int
    kind: str
    player_ids: tuple[int, ...]
    published_at: Optional[datetime]


@dataclass
class StoreResult:
    stored: list[Stored] = field(default_factory=list)
    skips: Counter = field(default_factory=Counter)


def _aware(value: Optional[datetime]) -> Optional[datetime]:
    if value is None or value.tzinfo is not None:
        return value
    return value.replace(tzinfo=timezone.utc)


def _stored_keys(items: Sequence[RawItem]) -> set[tuple[str, str]]:
    keys: set[tuple[str, str]] = set()
    by_source: dict[str, list[str]] = {}
    for item in items:
        by_source.setdefault(item.source, []).append(item.source_item_id)
    for source, ids in by_source.items():
        keys |= {(source, sid) for (sid,) in (NewsItem.select(NewsItem.source_item_id)
                                              .where(NewsItem.source == source, NewsItem.source_item_id.in_(ids))
                                              .tuples())}
    return keys


def _known_hashes(hashes: Iterable[str], now: datetime) -> set[str]:
    hashes = sorted(set(hashes))
    if not hashes:
        return set()
    return {h for (h,) in (NewsItem.select(NewsItem.content_hash)
                           .where(NewsItem.content_hash.in_(hashes), NewsItem.fetched_at >= now - DEDUP_WINDOW)
                           .tuples())}


def link_players(item: RawItem, index: PlayerIndex, scanner: NameScanner,
                 skips: Counter) -> list[tuple[int, str]]:
    """(NBA player_id, linked_by) for the players the item is about."""
    links: list[tuple[int, str]] = []
    for espn_id in item.espn_ids:
        pid = index.by_espn_id.get(espn_id)
        if pid is not None:
            links.append((pid, "espn_id"))
    for name in item.names:
        pid = index.match(None, name)
        if pid is None:
            skips["name_unmatched"] += 1
        else:
            links.append((pid, "name"))
    if item.scan_text:
        links += [(pid, "name") for pid in scanner.find(item.headline, item.body)]
    seen: set[int] = set()
    return [(pid, by) for pid, by in links if not (pid in seen or seen.add(pid))]


def store_items(items: Sequence[RawItem], index: PlayerIndex, scanner: NameScanner, known_teams: set[str],
                now: datetime, run_id=None) -> StoreResult:
    """Blocking DB: store the items not already stored, with their players and teams."""
    result = StoreResult()
    stored_keys = _stored_keys(items)
    fresh = [i for i in items if (i.source, i.source_item_id) not in stored_keys]
    result.skips["seen"] += len(items) - len(fresh)
    known = _known_hashes((content_hash(i.blurb) for i in fresh), now)
    batch_keys: set[tuple[str, str]] = set()
    with db.atomic():
        for item in fresh:
            key = (item.source, item.source_item_id)
            digest = content_hash(item.blurb)
            if key in batch_keys:
                result.skips["seen"] += 1
                continue
            if digest in known:
                result.skips["duplicate"] += 1
                continue
            players = link_players(item, index, scanner, result.skips)
            teams = linked_teams(item.teams, [pid for pid, _ in players], index.team_of, known_teams)
            if not players and not teams:
                result.skips["unlinked"] += 1
                continue
            kind = kind_for(item.source, item.headline, item.body)
            news_id = (NewsItem.insert(
                source=item.source, source_item_id=item.source_item_id, published_at=item.published_at,
                fetched_at=now, headline=item.headline, body=item.body, url=item.url,
                attribution=item.attribution, kind=kind, content_hash=digest,
                raw=dict(item.raw or {}, run_id=str(run_id) if run_id else None),
            ).on_conflict_ignore().execute())
            batch_keys.add(key)
            if not news_id:
                result.skips["seen"] += 1
                continue
            known.add(digest)
            if players:
                NewsItemPlayer.insert_many(
                    [{"news_item": news_id, "player": pid, "linked_by": by} for pid, by in players]
                ).on_conflict_ignore().execute()
            if teams:
                NewsItemTeam.insert_many(
                    [{"news_item": news_id, "team": team} for team in teams]
                ).on_conflict_ignore().execute()
            result.stored.append(Stored(id=int(news_id), kind=kind, player_ids=tuple(p for p, _ in players),
                                        published_at=item.published_at))
    return result


def event_items(stored: Iterable[Stored], now: datetime) -> list[int]:
    """The stored items Dime hears about: about a player, of a kind it reacts to, and new."""
    return sorted(
        s.id for s in stored
        if s.player_ids and s.kind in EVENT_KINDS
        and (s.published_at is None or _aware(s.published_at) >= now - EVENT_FRESH)
    )


def prune_raw(now: datetime) -> int:
    """Blocking DB: clear `raw` on items older than 30 days."""
    return (NewsItem.update(raw=None)
            .where(NewsItem.raw.is_null(False), NewsItem.fetched_at < now - RAW_KEPT)
            .execute())


def known_team_ids() -> set[str]:
    return {t for (t,) in NBATeam.select(NBATeam.id).tuples()}


class NewsFeedPipeline(BasePipeline):
    """One news tick: read the due sources, store what is new, tell Dime about it."""

    config = PipelineConfig(
        name="news_feed",
        display_name="News Feed",
        description="Player news from ESPN's injury notes and player news, Rotowire and RealGM, "
                    "linked to the players and teams it is about",
        target_table="nba.news_items",
        category=PipelineCategory.SCHEDULED,
        trigger_slug="news",
        cron_job="news",
        timeout_seconds=240,
    )

    def __init__(self):
        super().__init__()
        self.injuries_extractor = ESPNInjuriesExtractor()
        self.fetcher = NewsFetcher()
        self.backend_client = backend_client_from_settings()

    def execute(self, ctx: PipelineContext) -> None:
        now = datetime.now(timezone.utc)
        tick = tick_of(now)
        items: list[RawItem] = []

        # The ESPN-id sources first: when two sources carry the same words, the first is kept.
        injuries = self._read(ctx, "espn_injuries", self.injuries_extractor.extract)
        if injuries is not None:
            items += items_from_injuries(injuries[0])
        watch = watch_set(settings.news_free_agents, settings.news_watch_max)
        due_ids = watch_slice(watch, tick, max(1, settings.news_watch_slices))
        for espn_id in due_ids:
            items += self._read(ctx, "espn_player_news", self.fetcher.espn_player_news, espn_id,
                                settings.news_player_items) or []
        items += self._read(ctx, "rotowire_rss", self.fetcher.rotowire) or []
        realgm_due = tick % max(1, settings.news_realgm_every) == 0
        if realgm_due:
            items += self._read(ctx, "realgm_rss", self.fetcher.realgm) or []

        index = PlayerIndex.load()
        result = store_items(items, index, NameScanner(index.by_name), known_team_ids(), now, ctx.run_id)
        ctx.increment_records(len(result.stored))
        for reason, count in sorted(result.skips.items()):
            if count:
                ctx.increment_skipped(count, reason)
        pruned = prune_raw(now)

        events = event_items(result.stored, now)
        if events:
            self._post_events(ctx, events)
        ctx.log.info(
            "news_feed_complete",
            items=len(items),
            stored=len(result.stored),
            by_kind=dict(Counter(s.kind for s in result.stored)),
            watch=len(watch),
            player_reads=len(due_ids),
            realgm=realgm_due,
            events=len(events),
            raw_pruned=pruned,
        )

    def _post_events(self, ctx: PipelineContext, news_ids: Sequence[int]) -> None:
        """Hand this tick's new items to Dime's event check-ins (backend; plan step 6)."""
        if not settings.post_dime_events:
            return
        post = self.backend_client.post_events(news=list(news_ids), correlation_id=get_correlation_id())
        ctx.log.info(
            "news_events_posted",
            ok=post.ok, off=post.off, sent=len(news_ids), recorded=post.recorded, started=len(post.started),
            reason=post.reason,
        )

    @staticmethod
    def _read(ctx: PipelineContext, source: str, fetch, *args):
        """One source's fetch; a failure is counted and the run goes on without it."""
        try:
            return fetch(*args)
        except Exception as exc:
            ctx.increment_failed(1, f"{source}_unavailable")
            ctx.log.warning(
                "news_source_failed", source=source, args=[str(a) for a in args],
                error=f"{type(exc).__name__}: {exc}",
            )
            return None
