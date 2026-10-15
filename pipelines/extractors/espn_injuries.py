"""
ESPN's league-wide NBA injury list (docs/NEWS_AND_INJURY_PIPELINES_PLAN.md, Sources).

`site.api.espn.com/apis/site/v2/sports/basketball/nba/injuries` returns every
team's current injury list in one ~1.5 MB call, no auth: per entry a `status`
(Out / Day-To-Day), `details` (fantasyStatus GTD / OUT / OFS, body part `type`,
`returnDate`), the Rotowire-written `shortComment` / `longComment`, the note's
`date`, and the athlete's recent `notes.items[]` headlines. `athlete.id` is
absent; the ESPN id is in the player-card link (`/id/4585618/`).

`parse_espn_injuries` is pure and fixture-tested against a real payload
(tests/fixtures/espn_injuries_2026-10-15.json). Statuses are mapped into the
availability vocabulary the resolver reads (pipelines/transformers/availability.py):
Out -> out, Day-To-Day -> questionable; the resolver decides per game whether a
won't-play blurb makes a Day-To-Day player out for that game.

A player missing from the list is not injured. The list carries no "Available"
rows, so the pipeline records a return to health itself when a listed player
drops off.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Optional

import requests

from core.resilience import (
    NetworkError,
    RateLimitError,
    ServerError,
    espn_api_circuit,
    with_retry,
)
from core.settings import settings
from pipelines.extractors.base import BaseExtractor

ESPN_INJURIES_URL = "https://site.api.espn.com/apis/site/v2/sports/basketball/nba/injuries"
_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
)
_ESPN_ID = re.compile(r"/id/(\d+)")

# ESPN team abbreviations that differ from NBA tricodes (every other one matches).
ESPN_TO_NBA_TRICODE: dict[str, str] = {
    "GS": "GSW",
    "NO": "NOP",
    "NY": "NYK",
    "SA": "SAS",
    "UTAH": "UTA",
    "WSH": "WAS",
}

# ESPN `status` -> availability report status.
STATUS_MAP: dict[str, str] = {
    "out": "out",
    "day-to-day": "questionable",
}


@dataclass(frozen=True)
class EspnInjury:
    item_id: str                     # ESPN's injury note id
    espn_id: int
    name: str
    team: Optional[str]              # NBA tricode
    status: str                      # out | questionable (the availability vocabulary)
    espn_status: str                 # "Out" / "Day-To-Day" as ESPN wrote it
    fantasy_status: Optional[str]    # GTD / OUT / OFS
    reason: Optional[str]            # body part, "Rest", "Illness", ...
    detail: Optional[str]            # the short comment: the blurb the resolver reads
    long_comment: Optional[str]
    expected_return: Optional[date]
    source_time: Optional[datetime]  # when ESPN wrote the note


def _espn_id(athlete: dict) -> Optional[int]:
    """The id from the player-card link, else from the headshot (`.../4585618.png`)."""
    for link in athlete.get("links") or []:
        match = _ESPN_ID.search(link.get("href") or "")
        if match:
            return int(match.group(1))
    tail = re.search(r"/(\d+)\.png$", (athlete.get("headshot") or {}).get("href") or "")
    return int(tail.group(1)) if tail else None


def _date(value: Any) -> Optional[date]:
    try:
        return date.fromisoformat(str(value)[:10]) if value else None
    except ValueError:
        return None


def _datetime(value: Any) -> Optional[datetime]:
    """ESPN writes `2026-10-12T21:36Z`."""
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None


def nba_tricode(espn_abbreviation: Optional[str]) -> Optional[str]:
    if not espn_abbreviation:
        return None
    return ESPN_TO_NBA_TRICODE.get(espn_abbreviation, espn_abbreviation)


def parse_espn_injuries(payload: dict) -> tuple[list[EspnInjury], int]:
    """Every parseable entry, and how many entries were skipped (no ESPN id or an
    unknown status) so the pipeline can count them rather than drop them silently."""
    out: list[EspnInjury] = []
    skipped = 0
    for team_block in payload.get("injuries") or []:
        for entry in team_block.get("injuries") or []:
            athlete = entry.get("athlete") or {}
            espn_id = _espn_id(athlete)
            status = STATUS_MAP.get(str(entry.get("status") or "").strip().lower())
            if espn_id is None or status is None:
                skipped += 1
                continue
            details = entry.get("details") or {}
            out.append(EspnInjury(
                item_id=str(entry.get("id") or ""),
                espn_id=espn_id,
                name=athlete.get("displayName") or "",
                team=nba_tricode((athlete.get("team") or {}).get("abbreviation")),
                status=status,
                espn_status=entry.get("status") or "",
                fantasy_status=(details.get("fantasyStatus") or {}).get("abbreviation"),
                reason=details.get("type"),
                detail=entry.get("shortComment") or None,
                long_comment=entry.get("longComment") or None,
                expected_return=_date(details.get("returnDate")),
                source_time=_datetime(entry.get("date")),
            ))
    return out, skipped


class ESPNInjuriesExtractor(BaseExtractor):
    """Fetches the league-wide injury list."""

    def __init__(self):
        super().__init__("espn_injuries")

    def extract(self, **kwargs: Any) -> tuple[list[EspnInjury], int]:
        return parse_espn_injuries(self.fetch())

    @with_retry(
        max_attempts=settings.retry_max_attempts,
        base_delay=settings.retry_base_delay,
        max_delay=settings.retry_max_delay,
    )
    @espn_api_circuit
    def fetch(self) -> dict:
        self.log.debug("request_start", endpoint=ESPN_INJURIES_URL)
        try:
            response = requests.get(
                ESPN_INJURIES_URL,
                headers={"User-Agent": _USER_AGENT, "Accept": "application/json"},
                timeout=settings.http_timeout,
            )
            if response.status_code == 429:
                retry_after = int(response.headers.get("Retry-After", 60))
                raise RateLimitError("ESPN rate limited", retry_after=retry_after)
            if response.status_code >= 500:
                raise ServerError("ESPN server error", status_code=response.status_code)
            response.raise_for_status()
            data = response.json()
        except requests.exceptions.Timeout:
            raise NetworkError("ESPN request timed out")
        except requests.exceptions.ConnectionError:
            raise NetworkError("ESPN connection failed")
        self.log.info("request_complete", teams=len(data.get("injuries") or []))
        return data
