"""
NBA static CDN (cdn.nba.com) — the league schedule feed.

``scheduleLeagueV2_1.json`` is the source of every game's tip-off time for the
season (preseason, regular season, Cup placeholders, play-in/playoffs once
known). A plain ``requests.get`` is answered 403 by the CDN's bot check; the
Chrome-131 header set below is accepted without a proxy. Keep the values
verbatim — ``utils.patches`` reuses the same set for stats.nba.com.

The same feed is also checked in as ``static/schedule_raw{YYYY}-{YYYY+1}.json``
(regenerated each season; see backend/scripts/build_season_calendar.py --fetch)
so the game_start_times pipeline can fall back when the CDN is unreachable.
"""

from __future__ import annotations

import json
from pathlib import Path
from urllib.parse import urlsplit

import requests

from core.season import start_year, validate_season

STATIC_DIR = Path(__file__).resolve().parent.parent / "static"
NBA_CDN_SCHEDULE_URL = "https://cdn.nba.com/static/json/staticData/scheduleLeagueV2_1.json"


def nba_cdn_headers(host: str = "cdn.nba.com") -> dict[str, str]:
    """Browser-shaped headers accepted by cdn.nba.com and stats.nba.com; ``Host`` set to ``host``.

    ``Accept-Encoding`` advertises ``br``: if the CDN ever answers with brotli the
    ``brotli`` package must be installed for ``requests`` to decode it (today it
    answers gzip).
    """
    return {
        "Accept": "application/json, text/plain, */*",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept-Language": "en-US,en;q=0.9",
        "Cache-Control": "no-cache",
        "Connection": "keep-alive",
        "Host": host,
        "Origin": "https://www.nba.com",
        "Pragma": "no-cache",
        "Referer": "https://www.nba.com/",
        "Sec-Ch-Ua": '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
        "Sec-Ch-Ua-Mobile": "?0",
        "Sec-Ch-Ua-Platform": '"Windows"',
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-site",
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
        ),
    }


def headers_for_url(url: str) -> dict[str, str]:
    """The header set with ``Host`` derived from the request URL's netloc."""
    return nba_cdn_headers(urlsplit(url).netloc)


def _check_season(data: dict, season: str, source: str) -> dict:
    feed_season = (data.get("leagueSchedule") or {}).get("seasonYear")
    if feed_season != season:
        raise ValueError(f"{source} is the {feed_season!r} schedule, expected {season!r}")
    return data


def fetch_league_schedule(season: str, timeout: int = 30) -> dict:
    """GET the live league schedule feed and verify it is for ``season`` ("2026-27").

    Raises ``requests`` errors on transport/HTTP failures and ``ValueError`` when
    the feed's ``leagueSchedule.seasonYear`` is a different season (e.g. the CDN
    still serving last season in August).
    """
    validate_season(season)
    response = requests.get(
        NBA_CDN_SCHEDULE_URL, headers=headers_for_url(NBA_CDN_SCHEDULE_URL), timeout=timeout
    )
    response.raise_for_status()
    return _check_season(response.json(), season, source=NBA_CDN_SCHEDULE_URL)


def static_schedule_path(season: str) -> Path:
    """``static/schedule_raw2026-2027.json`` for "2026-27"."""
    year = start_year(season)
    return STATIC_DIR / f"schedule_raw{year}-{year + 1}.json"


def load_static_schedule(season: str) -> dict:
    """Read the checked-in copy of the feed for ``season`` (same shape as the CDN)."""
    path = static_schedule_path(season)
    if not path.exists():
        raise FileNotFoundError(
            f"No static schedule feed for season {season}: expected {path.name} in {STATIC_DIR} "
            "(backend/scripts/build_season_calendar.py --fetch writes it)"
        )
    with open(path, "r") as f:
        data = json.load(f)
    return _check_season(data, season, source=path.name)
