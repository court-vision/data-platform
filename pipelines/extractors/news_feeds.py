"""
The news feed's sources (docs/NEWS_AND_INJURY_PIPELINES_PLAN.md § 3, Sources):

    espn_injuries     ESPN's league-wide injury list, the call the availability
                      pipeline makes (extractors/espn_injuries.py): each entry's
                      Rotowire-written note (`shortComment`, `longComment`) is an item
                      about that player, by ESPN id
    espn_player_news  ESPN fantasy's per-player feed
                      (site.api.espn.com/apis/fantasy/v2/games/fba/news/players):
                      its `Rotowire` items, the same blurbs the injury list carries
                      plus the healthy players', by ESPN id. Video clips (`Media`)
                      are not news and are left out
    rotowire_rss      Rotowire's NBA feed (its five newest items): "Name: headline"
                      and a blurb, by name
    realgm_rss        RealGM's Wiretap: league news (signings, trades, injuries) with
                      players named in the text

Each parser is pure and fixture-tested (tests/fixtures/news/). An item keeps a
headline and one paragraph, never a full article, with its link (the plan's
decision 10). `blurb` is the text an item is deduplicated on across sources: the
injury list's note, ESPN's Rotowire item and Rotowire's own are the same words.
"""

from __future__ import annotations

import html
import re
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from typing import Any, Iterable, Optional, Sequence

import requests

from core.settings import settings
from pipelines.extractors.espn_injuries import EspnInjury

ESPN_PLAYER_NEWS_URL = "https://site.api.espn.com/apis/fantasy/v2/games/fba/news/players"
ROTOWIRE_RSS_URL = "https://www.rotowire.com/rss/news.php?sport=NBA"
REALGM_RSS_URL = "https://basketball.realgm.com/rss/wiretap/0/0.xml"
_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
)
# One paragraph: what an item keeps of a source's text
BODY_CHARS = 600
_ROTOWIRE_TAIL = re.compile(r"\s*Visit RotoWire\.com for more analysis on this update\.?\s*$", re.I)
_TAG = re.compile(r"<[^>]+>")
_PARAGRAPH = re.compile(r"</p\s*>", re.I)
# Rotowire writes "Wed, 04 Nov 2026 10:00:00 PM PST": a 12-hour clock RFC 2822 doesn't allow
_TWELVE_HOUR = re.compile(
    r"^\w{3},\s+(\d{1,2})\s+(\w{3})\s+(\d{4})\s+(\d{1,2}):(\d{2}):(\d{2})\s+([AP]M)\s+([A-Z]{2,4})$")
_MONTHS = {m: i for i, m in enumerate(
    ("Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"), start=1)}
_ZONES = {"EST": -5, "EDT": -4, "CST": -6, "CDT": -5, "MST": -7, "MDT": -6, "PST": -8, "PDT": -7,
          "UTC": 0, "GMT": 0}


@dataclass(frozen=True)
class RawItem:
    """One item as a source gave it, before it is linked and stored."""

    source: str
    source_item_id: str
    published_at: Optional[datetime]
    headline: str
    body: Optional[str]
    url: Optional[str]
    attribution: Optional[str]
    blurb: str                                  # what the cross-source hash is of
    espn_ids: tuple[int, ...] = ()              # players the source names by ESPN id
    names: tuple[str, ...] = ()                 # players the source names outright
    scan_text: bool = False                     # look for player names in the headline and body
    teams: tuple[str, ...] = ()                 # NBA tricodes the source names
    raw: Optional[dict] = None


def _clip(text: Optional[str], limit: int = BODY_CHARS) -> Optional[str]:
    if not text:
        return None
    text = re.sub(r"\s+", " ", text).strip()
    if len(text) <= limit:
        return text or None
    cut = text[:limit]
    end = max(cut.rfind(". "), cut.rfind("! "), cut.rfind("? "))
    return (cut[: end + 1] if end > limit // 2 else cut.rstrip() + "…") or None


def _iso(value: Any) -> Optional[datetime]:
    if not value:
        return None
    try:
        out = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return out if out.tzinfo else out.replace(tzinfo=timezone.utc)


def parse_rss_date(value: Optional[str]) -> Optional[datetime]:
    """An RSS pubDate: RFC 2822, or Rotowire's 12-hour variant with a US zone name."""
    if not value:
        return None
    value = value.strip()
    match = _TWELVE_HOUR.match(value)
    if match:
        day, mon, year, hh, mm, ss, ampm, zone = match.groups()
        if mon not in _MONTHS or zone not in _ZONES:
            return None
        hour = int(hh) % 12 + (12 if ampm == "PM" else 0)
        local = datetime(int(year), _MONTHS[mon], int(day), hour, int(mm), int(ss))
        return (local - timedelta(hours=_ZONES[zone])).replace(tzinfo=timezone.utc)
    try:
        out = parsedate_to_datetime(value)
    except (TypeError, ValueError, IndexError):
        return None
    if out.tzinfo is None:
        out = out.replace(tzinfo=timezone.utc)
    return out.astimezone(timezone.utc)


# ---- ESPN's injury list ----------------------------------------------------------------


def items_from_injuries(entries: Iterable[EspnInjury]) -> list[RawItem]:
    """Each entry's note as an item about its player. A note ESPN rewrites is a new item (its id
    carries the note's time); an entry with no note says nothing new."""
    out: list[RawItem] = []
    for e in entries:
        if not e.detail:
            continue
        stamp = e.source_time.strftime("%Y%m%dT%H%M") if e.source_time else "undated"
        out.append(RawItem(
            source="espn_injuries",
            source_item_id=f"{e.item_id or e.espn_id or e.name}:{stamp}",
            published_at=e.source_time,
            headline=_clip(e.detail, 400) or e.detail,
            body=_clip(e.long_comment),
            url=None,
            attribution=None,
            blurb=e.detail,
            espn_ids=(e.espn_id,) if e.espn_id else (),
            names=(e.name,) if e.name and not e.espn_id else (),
            teams=(e.team,) if e.team else (),
            raw={"status": e.espn_status, "fantasy_status": e.fantasy_status, "reason": e.reason,
                 "expected_return": e.expected_return.isoformat() if e.expected_return else None},
        ))
    return out


# ---- ESPN fantasy's per-player news ----------------------------------------------------

# The feed's item types that are news: Rotowire's blurbs (and ESPN's own written ones)
ESPN_NEWS_TYPES = frozenset({"Rotowire", "HeadlineNews", "Story"})


def parse_espn_player_news(payload: dict, espn_id: Optional[int] = None) -> list[RawItem]:
    out: list[RawItem] = []
    for item in (payload or {}).get("feed") or []:
        if item.get("type") not in ESPN_NEWS_TYPES or not item.get("headline") or item.get("id") is None:
            continue
        pid = item.get("playerId") or espn_id
        web = ((item.get("links") or {}).get("web") or {}).get("href")
        authors = [a.get("name") for a in item.get("authors") or [] if isinstance(a, dict) and a.get("name")]
        out.append(RawItem(
            source="espn_player_news",
            source_item_id=str(item["id"]),
            published_at=_iso(item.get("published")),
            headline=_clip(item["headline"], 400) or item["headline"],
            body=_clip(item.get("story") or (item.get("description") if item.get("description") != item.get("headline")
                                             else None)),
            url=web,
            attribution=", ".join(authors) or None,
            blurb=item["headline"],
            espn_ids=(int(pid),) if pid else (),
            raw={k: item.get(k) for k in ("id", "type", "published", "lastModified", "playerId")},
        ))
    return out


# ---- RSS -------------------------------------------------------------------------------


def _text(node: Optional[ET.Element]) -> str:
    return (node.text or "").strip() if node is not None else ""


def _paragraph(markup: str) -> str:
    """The first paragraph of an HTML description, as text."""
    first = _PARAGRAPH.split(markup, maxsplit=1)[0] if _PARAGRAPH.search(markup) else markup
    return re.sub(r"\s+", " ", html.unescape(_TAG.sub(" ", first))).strip()


def parse_rotowire_rss(xml: bytes | str) -> list[RawItem]:
    out: list[RawItem] = []
    root = ET.fromstring(xml)
    for item in root.iter("item"):
        title, guid = _text(item.find("title")), _text(item.find("guid"))
        if not title or not guid:
            continue
        name, _, headline = title.partition(":")
        blurb = _ROTOWIRE_TAIL.sub("", html.unescape(_text(item.find("description")))).strip()
        out.append(RawItem(
            source="rotowire_rss",
            source_item_id=guid,
            published_at=parse_rss_date(_text(item.find("pubDate"))),
            headline=title,
            body=_clip(blurb),
            url=re.sub(r"(?<!:)//+", "/", _text(item.find("link"))) or None,
            attribution="Rotowire",
            blurb=blurb or title,
            names=(name.strip(),) if headline else (),
            scan_text=not headline,
            raw={"guid": guid, "title": title},
        ))
    return out


def parse_realgm_rss(xml: bytes | str) -> list[RawItem]:
    out: list[RawItem] = []
    root = ET.fromstring(xml)
    for item in root.iter("item"):
        title = html.unescape(_text(item.find("title")))
        link = _text(item.find("link"))
        guid = _text(item.find("guid")) or link
        if not title or not guid:
            continue
        body = _clip(_paragraph(_text(item.find("description"))))
        out.append(RawItem(
            source="realgm_rss",
            source_item_id=guid[-200:],
            published_at=parse_rss_date(_text(item.find("pubDate"))),
            headline=title,
            body=body,
            url=link or None,
            attribution="RealGM",
            blurb=title,
            scan_text=True,
            raw={"guid": guid, "title": title},
        ))
    return out


# ---- fetching --------------------------------------------------------------------------


class NewsFetcher:
    """The HTTP calls; each raises on a failure, which the pipeline counts and goes on without."""

    def __init__(self, session: Optional[requests.Session] = None):
        self.session = session or requests.Session()

    def _get(self, url: str, **params: Any) -> requests.Response:
        response = self.session.get(url, params=params or None, timeout=settings.http_timeout,
                                    headers={"User-Agent": _USER_AGENT, "Accept": "*/*"})
        response.raise_for_status()
        return response

    def espn_player_news(self, espn_id: int, limit: int = 5) -> list[RawItem]:
        payload = self._get(ESPN_PLAYER_NEWS_URL, playerId=espn_id, limit=limit).json()
        return parse_espn_player_news(payload, espn_id)

    def rotowire(self) -> list[RawItem]:
        return parse_rotowire_rss(self._get(ROTOWIRE_RSS_URL).content)

    def realgm(self) -> list[RawItem]:
        return parse_realgm_rss(self._get(REALGM_RSS_URL).content)


def watch_slice(ids: Sequence[int], tick: int, slices: int) -> list[int]:
    """The part of the watch set this tick reads: every `slices`-th id, so each is read once per
    `slices` ticks."""
    ordered = sorted(set(ids))
    return ordered[tick % slices::slices] if slices > 1 else ordered
