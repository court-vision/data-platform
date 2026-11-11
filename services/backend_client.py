"""
Backend internal API client — the lineup-alerts pipeline's one call per team, the
scheduled-pickups pipeline's one call per tick, the move-ledger pipeline's one
call per night, the dime-checkins pipeline's one call per tick, and the events
the availability and news pipelines hand to Dime.

    POST {BACKEND_INTERNAL_URL}/v1/internal/jobs/lineup/evaluate
    Authorization: Bearer <PIPELINE_API_TOKEN>      (the token both services share)
    X-Correlation-ID: <the triggering request's id, when there is one>
    {"team_id": int, "user_id": int, "nba_date": "YYYY-MM-DD", "apply": bool}

The backend owns the ESPN roster read, slot eligibility, game locks and the fill
planner; with `apply=true` it also writes the plan to ESPN. It answers a 200
envelope whose `data.outcome` is one of `planned | applied | noop | rejected |
failed | skipped` — every business result is a 200, so this client only has to
map transport problems:

- 404 `TEAM_NOT_FOUND`  -> `skipped` / `team_not_found` (nothing to retry)
- 401, any other status, malformed body, timeout, connection error
                        -> `unavailable` (the pipeline records `failed` so the
                           next 15-minute poll retries; three or more in one run
                           raise the ops alert)

One request per call, **no retry**: the pipeline already re-polls, and an
`apply=true` call that timed out may have written to ESPN — the backend's
`already_applied_today` guard, not a client retry, is what makes that safe.
Runs in the pipeline thread (`BasePipeline.run` uses `asyncio.to_thread`), so
the synchronous `requests` call is fine here. The token is never logged.

`execute_pickups` is the second route, POST /v1/internal/jobs/pickups/execute with
`{"limit": int}`: the backend attempts up to that many due scheduled pickups and
answers one `PickupResult` per row (`executed | skipped | failed | expired |
deferred`). Same contract — one request, no retry (the backend's claim lease and
its re-read before every attempt make a lost answer safe), transport problems
come back as a `PickupsRun` that is not `ok`.

`run_ledger` is the third, POST /v1/internal/jobs/ledger/run with `{}`: the backend
rebuilds the move ledger (usr.move_outcomes) of every league with a recent lineup
snapshot and answers one result per league. A rebuild replaces a league's rows
whole, so a lost answer is safe to repeat; still one request, no retry (the
post-game batch's next poll is the retry). It reads a season of snapshots per
league, so it has its own read timeout.

`run_checkins` is the fourth, POST /v1/internal/jobs/checkins/run with `{}`: the
backend starts the Dime check-ins that are due (the Dime autopilot plan, step 3)
and answers what it started. The check-ins run on in the backend after the
answer, so the call is quick; one request, no retry (each check-in is claimed
once per team, kind and day, and the next tick fifteen minutes later is the retry).

`post_events` is the fifth, POST /v1/internal/jobs/checkins/events with
`{"availability": [{"player_id", "game_date"}], "news": [news_item_id]}`: what came
in about players this tick (the Dime autopilot plan, step 6). Keys only; the
backend reads the verdicts and items itself, records an event on each team Dime
runs that rosters the player, and starts a check-in about it. One request, no
retry: an event is recorded once per verdict change or item, so a repeat is
harmless, and a lost one is still read by the next scheduled check-in's tools.
"""

from __future__ import annotations

import time
from dataclasses import asdict, dataclass, field
from datetime import date, datetime
from typing import Any, Optional

import requests

from core.logging import get_logger
from core.settings import settings as default_settings

EVALUATE_PATH = "/v1/internal/jobs/lineup/evaluate"
EXECUTE_PICKUPS_PATH = "/v1/internal/jobs/pickups/execute"
RUN_LEDGER_PATH = "/v1/internal/jobs/ledger/run"
RUN_CHECKINS_PATH = "/v1/internal/jobs/checkins/run"
POST_EVENTS_PATH = "/v1/internal/jobs/checkins/events"
REACT_LINEUP_PATH = "/v1/internal/jobs/lineup/react"
CONNECT_TIMEOUT_SECONDS = 5.0

# Outcomes the backend returns for a 200.
BACKEND_OUTCOMES = frozenset({"planned", "applied", "noop", "rejected", "failed", "skipped"})
# Client-side outcome: the backend could not be asked (or did not answer usably).
OUTCOME_UNAVAILABLE = "unavailable"
# Outcomes the backend returns per scheduled pickup.
PICKUP_OUTCOMES = frozenset({"executed", "skipped", "failed", "expired", "deferred"})

_ERROR_EXCERPT_CHARS = 300


@dataclass(frozen=True)
class LineupEvaluation:
    """The backend's answer for one team on one NBA date (or why there is none)."""

    outcome: str
    reason: Optional[str]
    moves: list[dict] = field(default_factory=list)
    unfilled: list[dict] = field(default_factory=list)
    verified: Optional[bool] = None
    scoring_period_id: Optional[int] = None
    nba_date: Optional[str] = None
    first_game_time_et: Optional[str] = None
    team_name: str = ""
    error: Optional[str] = None

    @classmethod
    def unavailable(cls, reason: str, error: Optional[str] = None) -> "LineupEvaluation":
        return cls(outcome=OUTCOME_UNAVAILABLE, reason=reason, error=error)

    @classmethod
    def from_payload(cls, data: Any) -> "LineupEvaluation":
        """Build from the envelope's `data`; anything malformed is `unavailable`."""
        if not isinstance(data, dict) or data.get("outcome") not in BACKEND_OUTCOMES:
            return cls.unavailable(
                reason="malformed_response",
                error=f"unexpected data: {_excerpt(repr(data))}",
            )
        return cls(
            outcome=data["outcome"],
            reason=data.get("reason"),
            moves=[m for m in (data.get("moves") or []) if isinstance(m, dict)],
            unfilled=[u for u in (data.get("unfilled") or []) if isinstance(u, dict)],
            verified=data.get("verified"),
            scoring_period_id=data.get("scoring_period_id"),
            nba_date=data.get("nba_date"),
            first_game_time_et=data.get("first_game_time_et"),
            team_name=data.get("team_name") or "",
        )


def _excerpt(text: Optional[str]) -> str:
    text = (text or "").strip()
    return text if len(text) <= _ERROR_EXCERPT_CHARS else text[: _ERROR_EXCERPT_CHARS - 1] + "…"


@dataclass(frozen=True)
class PickupExecution:
    """One scheduled pickup's outcome this run — the backend's PickupResult."""

    pickup_id: int
    team_id: int
    user_id: int
    outcome: str
    reason: Optional[str] = None
    detail: Optional[str] = None
    team_name: str = ""
    add: dict = field(default_factory=dict)       # {player_id, name, team}
    drop: Optional[dict] = None
    nba_date: Optional[str] = None                 # ISO date the pickup is for
    scoring_period_id: Optional[int] = None
    seated_slot: Optional[str] = None              # executed: "UT", "PG", ...; None = bench
    verified: Optional[bool] = None
    audit_id: Optional[int] = None
    next_attempt_at: Optional[str] = None          # deferred: ISO datetime
    lane: Optional[dict] = None                    # a stream lane's pickup: {lane_id, notify, projected, why, last_day}

    def as_dict(self) -> dict:
        return asdict(self)

    @property
    def desk_only(self) -> bool:
        """A stream lane's pickup whose lane doesn't email."""
        return bool(self.lane) and self.lane.get("notify") == "desk"

    @classmethod
    def from_payload(cls, item: Any) -> Optional["PickupExecution"]:
        """Build from one `results` entry; None when it is not a usable result."""
        if not isinstance(item, dict) or not isinstance(item.get("outcome"), str) or item["outcome"] not in PICKUP_OUTCOMES:
            return None
        try:
            return cls(
                pickup_id=int(item["pickup_id"]),
                team_id=int(item["team_id"]),
                user_id=int(item["user_id"]),
                outcome=item["outcome"],
                reason=item.get("reason"),
                detail=item.get("detail"),
                team_name=item.get("team_name") or "",
                add=item["add"] if isinstance(item.get("add"), dict) else {},
                drop=item["drop"] if isinstance(item.get("drop"), dict) else None,
                nba_date=item.get("nba_date"),
                scoring_period_id=item.get("scoring_period_id"),
                seated_slot=item.get("seated_slot"),
                verified=item.get("verified"),
                audit_id=item.get("audit_id"),
                next_attempt_at=item.get("next_attempt_at"),
                lane=_pickup_lane(item.get("lane")),
            )
        except (KeyError, TypeError, ValueError):
            return None


def _pickup_lane(value: Any) -> Optional[dict]:
    """A pickup's `lane`, or None. A `projected` that is not a player is dropped, so the email
    renders without the projection rather than not at all."""
    if not isinstance(value, dict):
        return None
    if value.get("projected") is not None and not isinstance(value["projected"], dict):
        return {**value, "projected": None}
    return value


LANE_NOTICE_ACTIONS = frozenset({"paused", "blocked"})


@dataclass(frozen=True)
class LaneNotice:
    """A stream lane decision the user hears about — the backend's LaneNotice: the lane paused
    (`reason`: holder_gone, auth_expired, ...), or a day passed without a decision (blocked, missed)."""

    lane_id: int
    team_id: int
    user_id: int
    action: str
    reason: str
    notify: str = "email"                           # desk: no email
    detail: Optional[str] = None
    team_name: str = ""
    holder: dict = field(default_factory=dict)      # {player_id, name, team}
    nba_date: Optional[str] = None
    scoring_period_id: Optional[int] = None
    ended: bool = False                             # the lane ended with it: its matchup has no day left

    def as_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_payload(cls, item: Any) -> Optional["LaneNotice"]:
        """Build from one `lanes` entry; None when it is not a usable notice."""
        if not isinstance(item, dict) or not isinstance(item.get("action"), str) or item["action"] not in LANE_NOTICE_ACTIONS:
            return None
        try:
            return cls(
                lane_id=int(item["lane_id"]),
                team_id=int(item["team_id"]),
                user_id=int(item["user_id"]),
                action=item["action"],
                reason=str(item["reason"]),
                notify=item.get("notify") or "email",
                detail=item.get("detail"),
                team_name=item.get("team_name") or "",
                holder=item["holder"] if isinstance(item.get("holder"), dict) else {},
                nba_date=item.get("nba_date"),
                scoring_period_id=item.get("scoring_period_id"),
                ended=bool(item.get("ended")),       # absent from a backend that predates it
            )
        except (KeyError, TypeError, ValueError):
            return None


@dataclass(frozen=True)
class PickupsRun:
    """The backend's answer to one executor tick, or why there is none (`ok` False)."""

    ok: bool
    due: int = 0
    results: list[PickupExecution] = field(default_factory=list)
    lanes: list[LaneNotice] = field(default_factory=list)   # stream lane decisions to tell
    reason: Optional[str] = None
    error: Optional[str] = None

    @classmethod
    def unavailable(cls, reason: str, error: Optional[str] = None) -> "PickupsRun":
        return cls(ok=False, reason=reason, error=error)

    @classmethod
    def from_payload(cls, data: Any) -> "PickupsRun":
        """Build from the envelope's `data`; anything malformed is unavailable."""
        if not isinstance(data, dict) or not isinstance(data.get("results"), list):
            return cls.unavailable(reason="malformed_response", error=f"unexpected data: {_excerpt(repr(data))}")
        results: list[PickupExecution] = []
        for item in data["results"]:
            parsed = PickupExecution.from_payload(item)
            if parsed is None:
                return cls.unavailable(reason="malformed_response", error=f"unexpected result: {_excerpt(repr(item))}")
            results.append(parsed)
        raw_lanes = data.get("lanes") or []        # absent from a backend that predates stream lanes
        if not isinstance(raw_lanes, list):
            return cls.unavailable(reason="malformed_response", error=f"unexpected lane notices: {_excerpt(repr(raw_lanes))}")
        lanes: list[LaneNotice] = []
        for item in raw_lanes:
            notice = LaneNotice.from_payload(item)
            if notice is None:
                return cls.unavailable(reason="malformed_response", error=f"unexpected lane notice: {_excerpt(repr(item))}")
            lanes.append(notice)
        try:
            due = int(data.get("due") or 0)
        except (TypeError, ValueError):
            due = len(results)
        return cls(ok=True, due=due, results=results, lanes=lanes)


@dataclass(frozen=True)
class LedgerLeague:
    """One league's rebuild: how many stored days it read and moves it found, by source."""

    provider_league_id: str
    season: int
    ok: bool
    days: int = 0
    moves: int = 0
    by_source: dict = field(default_factory=dict)
    error: Optional[str] = None

    @classmethod
    def from_payload(cls, item: Any) -> Optional["LedgerLeague"]:
        if not isinstance(item, dict) or not isinstance(item.get("ok"), bool):
            return None
        try:
            return cls(
                provider_league_id=str(item["provider_league_id"]),
                season=int(item["season"]),
                ok=item["ok"],
                days=int(item.get("days") or 0),
                moves=int(item.get("moves") or 0),
                by_source=item["by_source"] if isinstance(item.get("by_source"), dict) else {},
                error=item.get("error"),
            )
        except (KeyError, TypeError, ValueError):
            return None


@dataclass(frozen=True)
class LedgerRun:
    """The backend's answer to one ledger rebuild, or why there is none (`ok` False)."""

    ok: bool
    leagues: list[LedgerLeague] = field(default_factory=list)
    reason: Optional[str] = None
    error: Optional[str] = None

    @classmethod
    def unavailable(cls, reason: str, error: Optional[str] = None) -> "LedgerRun":
        return cls(ok=False, reason=reason, error=error)

    @classmethod
    def from_payload(cls, data: Any) -> "LedgerRun":
        if not isinstance(data, dict) or not isinstance(data.get("leagues"), list):
            return cls.unavailable(reason="malformed_response", error=f"unexpected data: {_excerpt(repr(data))}")
        leagues: list[LedgerLeague] = []
        for item in data["leagues"]:
            league = LedgerLeague.from_payload(item)
            if league is None:
                return cls.unavailable(reason="malformed_response", error=f"unexpected league: {_excerpt(repr(item))}")
            leagues.append(league)
        return cls(ok=True, leagues=leagues)


@dataclass(frozen=True)
class CheckinStarted:
    checkin_id: int
    team_id: int
    kind: str
    date: str

    @classmethod
    def from_payload(cls, item: Any) -> Optional["CheckinStarted"]:
        if not isinstance(item, dict):
            return None
        try:
            return cls(checkin_id=int(item["checkin_id"]), team_id=int(item["team_id"]), kind=str(item["kind"]),
                       date=str(item["date"]))
        except (KeyError, TypeError, ValueError):
            return None


@dataclass(frozen=True)
class CheckinsRun:
    """The backend's answer to one check-ins tick, or why there is none (`ok` False)."""

    ok: bool
    off: bool = False
    teams: int = 0
    started: list[CheckinStarted] = field(default_factory=list)
    missed: int = 0
    skipped: int = 0
    stale: int = 0
    reason: Optional[str] = None
    error: Optional[str] = None

    @classmethod
    def unavailable(cls, reason: str, error: Optional[str] = None) -> "CheckinsRun":
        return cls(ok=False, reason=reason, error=error)

    @classmethod
    def from_payload(cls, data: Any) -> "CheckinsRun":
        if not isinstance(data, dict) or not isinstance(data.get("started"), list):
            return cls.unavailable(reason="malformed_response", error=f"unexpected data: {_excerpt(repr(data))}")
        started: list[CheckinStarted] = []
        for item in data["started"]:
            one = CheckinStarted.from_payload(item)
            if one is None:
                return cls.unavailable(reason="malformed_response", error=f"unexpected check-in: {_excerpt(repr(item))}")
            started.append(one)
        try:
            return cls(ok=True, off=bool(data.get("off")), teams=int(data.get("teams") or 0), started=started,
                       missed=int(data.get("missed") or 0), skipped=int(data.get("skipped") or 0),
                       stale=int(data.get("stale") or 0))
        except (TypeError, ValueError):
            return cls.unavailable(reason="malformed_response", error=f"unexpected data: {_excerpt(repr(data))}")


@dataclass(frozen=True)
class EventsPost:
    """The backend's answer to a post of events, or why there is none (`ok` False)."""

    ok: bool
    off: bool = False
    teams: int = 0
    facts: int = 0
    recorded: int = 0
    started: list[CheckinStarted] = field(default_factory=list)
    reason: Optional[str] = None
    error: Optional[str] = None

    @classmethod
    def unavailable(cls, reason: str, error: Optional[str] = None) -> "EventsPost":
        return cls(ok=False, reason=reason, error=error)

    @classmethod
    def from_payload(cls, data: Any) -> "EventsPost":
        if not isinstance(data, dict) or not isinstance(data.get("started"), list):
            return cls.unavailable(reason="malformed_response", error=f"unexpected data: {_excerpt(repr(data))}")
        started = [CheckinStarted.from_payload(item) for item in data["started"]]
        if any(s is None for s in started):
            return cls.unavailable(reason="malformed_response", error=f"unexpected data: {_excerpt(repr(data))}")
        try:
            return cls(ok=True, off=bool(data.get("off")), teams=int(data.get("teams") or 0),
                       facts=int(data.get("facts") or 0), recorded=int(data.get("recorded") or 0),
                       started=[s for s in started if s is not None])
        except (TypeError, ValueError):
            return cls.unavailable(reason="malformed_response", error=f"unexpected data: {_excerpt(repr(data))}")


@dataclass(frozen=True)
class ReactTeam:
    """What one verdict change did to one team whose Late scratches level is notify or act
    (the backend's LineupReactTeam)."""

    team_id: int
    user_id: int
    outcome: str                     # applied | planned | noop | skipped | rejected | failed
    level: str = "act"               # notify | act
    reason: Optional[str] = None
    player_name: Optional[str] = None
    moves: list[dict] = field(default_factory=list)
    verified: Optional[bool] = None
    audit_id: Optional[int] = None
    nba_date: Optional[str] = None
    team_name: str = ""

    @property
    def emails(self) -> bool:
        """The owner hears about it: a swap made, or one to make (suggested at notify, or one
        act couldn't make)."""
        return bool(self.moves) and self.outcome in ("applied", "planned", "rejected", "failed")

    @classmethod
    def from_payload(cls, item: Any) -> Optional["ReactTeam"]:
        if not isinstance(item, dict) or item.get("outcome") not in BACKEND_OUTCOMES:
            return None
        try:
            return cls(
                team_id=int(item["team_id"]), user_id=int(item["user_id"]), outcome=item["outcome"],
                level=str(item.get("level") or "act"), reason=item.get("reason"),
                player_name=item.get("player_name"),
                moves=[m for m in (item.get("moves") or []) if isinstance(m, dict)],
                verified=item.get("verified"), audit_id=item.get("audit_id"), nba_date=item.get("nba_date"),
                team_name=item.get("team_name") or "",
            )
        except (KeyError, TypeError, ValueError):
            return None


@dataclass(frozen=True)
class LineupReaction:
    """The backend's answer to one verdict change, or why there is none (`ok` False)."""

    ok: bool
    teams: list[ReactTeam] = field(default_factory=list)
    reason: Optional[str] = None
    error: Optional[str] = None

    @classmethod
    def unavailable(cls, reason: str, error: Optional[str] = None) -> "LineupReaction":
        return cls(ok=False, reason=reason, error=error)

    @classmethod
    def from_payload(cls, data: Any) -> "LineupReaction":
        if not isinstance(data, dict) or not isinstance(data.get("teams"), list):
            return cls.unavailable(reason="malformed_response", error=f"unexpected data: {_excerpt(repr(data))}")
        teams = [ReactTeam.from_payload(item) for item in data["teams"]]
        if any(t is None for t in teams):
            return cls.unavailable(reason="malformed_response", error=f"unexpected data: {_excerpt(repr(data))}")
        return cls(ok=True, teams=[t for t in teams if t is not None])


class BackendClient:
    """Talks to the backend's pipeline-token routes. Disabled when no base URL is configured."""

    def __init__(self, base_url: Optional[str], token: str, timeout_seconds: float,
                 pickups_timeout_seconds: Optional[float] = None, ledger_timeout_seconds: Optional[float] = None,
                 react_timeout_seconds: Optional[float] = None):
        self._base_url = (base_url or "").strip().rstrip("/")
        self._token = token
        self._timeout = (CONNECT_TIMEOUT_SECONDS, float(timeout_seconds))
        # A pickups tick is several ESPN round trips per row, so it gets its own read timeout.
        self._pickups_timeout = (CONNECT_TIMEOUT_SECONDS, float(pickups_timeout_seconds or timeout_seconds))
        # A ledger rebuild reads a season of snapshots and box scores per league.
        self._ledger_timeout = (CONNECT_TIMEOUT_SECONDS, float(ledger_timeout_seconds or timeout_seconds))
        # A verdict change reads the board of every team at notify or act and may write one or more.
        self._react_timeout = (CONNECT_TIMEOUT_SECONDS, float(react_timeout_seconds or timeout_seconds))
        self._log = get_logger("backend_client")

    @property
    def enabled(self) -> bool:
        return bool(self._base_url)

    @property
    def base_url(self) -> Optional[str]:
        return self._base_url or None

    def evaluate_lineup(
        self,
        *,
        team_id: int,
        user_id: int,
        nba_date: date,
        apply: bool,
        correlation_id: Optional[str] = None,
    ) -> LineupEvaluation:
        """Ask the backend for `team_id`'s fill plan for `nba_date` (and to apply it when `apply`).

        Never raises: transport and protocol failures come back as `unavailable`.
        """
        if not self.enabled:
            return LineupEvaluation.unavailable(reason="backend_not_configured")

        headers = {
            "Authorization": f"Bearer {self._token}",
            "Accept": "application/json",
        }
        if correlation_id:
            headers["X-Correlation-ID"] = correlation_id
        body = {
            "team_id": team_id,
            "user_id": user_id,
            "nba_date": nba_date.isoformat(),
            "apply": bool(apply),
        }

        started = time.monotonic()
        status_code: Optional[int] = None
        try:
            response = requests.post(
                f"{self._base_url}{EVALUATE_PATH}",
                json=body,
                headers=headers,
                timeout=self._timeout,
            )
            status_code = response.status_code
            evaluation = self._parse_response(response)
        except requests.Timeout as exc:
            evaluation = LineupEvaluation.unavailable(reason="timeout", error=_excerpt(str(exc)))
        except requests.RequestException as exc:
            evaluation = LineupEvaluation.unavailable(reason=type(exc).__name__, error=_excerpt(str(exc)))
        except Exception as exc:  # a client bug must degrade the team, not the run
            evaluation = LineupEvaluation.unavailable(reason=type(exc).__name__, error=_excerpt(str(exc)))

        elapsed_ms = int((time.monotonic() - started) * 1000)
        log = self._log.warning if evaluation.outcome == OUTCOME_UNAVAILABLE else self._log.info
        log(
            "backend_evaluate_lineup",
            team_id=team_id,
            user_id=user_id,
            nba_date=body["nba_date"],
            apply=body["apply"],
            outcome=evaluation.outcome,
            reason=evaluation.reason,
            status_code=status_code,
            elapsed_ms=elapsed_ms,
            moves=len(evaluation.moves),
            error=evaluation.error,
        )
        return evaluation

    def execute_pickups(self, *, limit: int = 4, correlation_id: Optional[str] = None) -> PickupsRun:
        """Ask the backend to attempt up to `limit` due scheduled pickups.

        Never raises: transport and protocol failures come back as a run that is not `ok`.
        """
        if not self.enabled:
            return PickupsRun.unavailable(reason="backend_not_configured")

        headers = {
            "Authorization": f"Bearer {self._token}",
            "Accept": "application/json",
        }
        if correlation_id:
            headers["X-Correlation-ID"] = correlation_id
        body = {"limit": int(limit)}

        started = time.monotonic()
        status_code: Optional[int] = None
        try:
            response = requests.post(
                f"{self._base_url}{EXECUTE_PICKUPS_PATH}",
                json=body,
                headers=headers,
                timeout=self._pickups_timeout,
            )
            status_code = response.status_code
            run = self._parse_pickups_response(response)
        except requests.Timeout as exc:
            run = PickupsRun.unavailable(reason="timeout", error=_excerpt(str(exc)))
        except requests.RequestException as exc:
            run = PickupsRun.unavailable(reason=type(exc).__name__, error=_excerpt(str(exc)))
        except Exception as exc:  # a client bug must degrade the tick, not the run
            run = PickupsRun.unavailable(reason=type(exc).__name__, error=_excerpt(str(exc)))

        elapsed_ms = int((time.monotonic() - started) * 1000)
        log = self._log.info if run.ok else self._log.warning
        log(
            "backend_execute_pickups",
            limit=limit,
            ok=run.ok,
            due=run.due,
            results=len(run.results),
            reason=run.reason,
            status_code=status_code,
            elapsed_ms=elapsed_ms,
            error=run.error,
        )
        return run

    def run_ledger(self, *, correlation_id: Optional[str] = None) -> LedgerRun:
        """Ask the backend to rebuild the move ledger of every active league.

        Never raises: transport and protocol failures come back as a run that is not `ok`.
        """
        if not self.enabled:
            return LedgerRun.unavailable(reason="backend_not_configured")

        headers = {
            "Authorization": f"Bearer {self._token}",
            "Accept": "application/json",
        }
        if correlation_id:
            headers["X-Correlation-ID"] = correlation_id

        started = time.monotonic()
        status_code: Optional[int] = None
        try:
            response = requests.post(
                f"{self._base_url}{RUN_LEDGER_PATH}",
                json={},
                headers=headers,
                timeout=self._ledger_timeout,
            )
            status_code = response.status_code
            run = self._parse_ledger_response(response)
        except requests.Timeout as exc:
            run = LedgerRun.unavailable(reason="timeout", error=_excerpt(str(exc)))
        except requests.RequestException as exc:
            run = LedgerRun.unavailable(reason=type(exc).__name__, error=_excerpt(str(exc)))
        except Exception as exc:  # a client bug must degrade the run, not raise
            run = LedgerRun.unavailable(reason=type(exc).__name__, error=_excerpt(str(exc)))

        elapsed_ms = int((time.monotonic() - started) * 1000)
        log = self._log.info if run.ok else self._log.warning
        log(
            "backend_run_ledger",
            ok=run.ok,
            leagues=len(run.leagues),
            failed=sum(1 for league in run.leagues if not league.ok),
            moves=sum(league.moves for league in run.leagues),
            reason=run.reason,
            status_code=status_code,
            elapsed_ms=elapsed_ms,
            error=run.error,
        )
        return run

    def run_checkins(self, *, correlation_id: Optional[str] = None) -> CheckinsRun:
        """Ask the backend to start the Dime check-ins that are due.

        Never raises: transport and protocol failures come back as a run that is not `ok`.
        """
        if not self.enabled:
            return CheckinsRun.unavailable(reason="backend_not_configured")

        headers = {
            "Authorization": f"Bearer {self._token}",
            "Accept": "application/json",
        }
        if correlation_id:
            headers["X-Correlation-ID"] = correlation_id

        started = time.monotonic()
        status_code: Optional[int] = None
        try:
            response = requests.post(
                f"{self._base_url}{RUN_CHECKINS_PATH}",
                json={},
                headers=headers,
                timeout=self._timeout,
            )
            status_code = response.status_code
            run = self._parse_checkins_response(response)
        except requests.Timeout as exc:
            run = CheckinsRun.unavailable(reason="timeout", error=_excerpt(str(exc)))
        except requests.RequestException as exc:
            run = CheckinsRun.unavailable(reason=type(exc).__name__, error=_excerpt(str(exc)))
        except Exception as exc:  # a client bug must degrade the run, not raise
            run = CheckinsRun.unavailable(reason=type(exc).__name__, error=_excerpt(str(exc)))

        elapsed_ms = int((time.monotonic() - started) * 1000)
        log = self._log.info if run.ok else self._log.warning
        log(
            "backend_run_checkins",
            ok=run.ok,
            off=run.off,
            teams=run.teams,
            started=len(run.started),
            missed=run.missed,
            skipped=run.skipped,
            stale=run.stale,
            reason=run.reason,
            status_code=status_code,
            elapsed_ms=elapsed_ms,
            error=run.error,
        )
        return run

    def post_events(
        self,
        *,
        availability: list[tuple[int, date]] = (),
        news: list[int] = (),
        correlation_id: Optional[str] = None,
    ) -> EventsPost:
        """Hand the backend what came in this tick: verdict changes (player, game date) and news item ids.

        Never raises: transport and protocol failures come back as a post that is not `ok`.
        """
        if not self.enabled:
            return EventsPost.unavailable(reason="backend_not_configured")
        body = {
            "availability": [{"player_id": int(pid), "game_date": day.isoformat()} for pid, day in availability],
            "news": [int(i) for i in news],
        }
        headers = {
            "Authorization": f"Bearer {self._token}",
            "Accept": "application/json",
        }
        if correlation_id:
            headers["X-Correlation-ID"] = correlation_id

        started = time.monotonic()
        status_code: Optional[int] = None
        try:
            response = requests.post(
                f"{self._base_url}{POST_EVENTS_PATH}",
                json=body,
                headers=headers,
                timeout=self._timeout,
            )
            status_code = response.status_code
            post = self._parse_events_response(response)
        except requests.Timeout as exc:
            post = EventsPost.unavailable(reason="timeout", error=_excerpt(str(exc)))
        except requests.RequestException as exc:
            post = EventsPost.unavailable(reason=type(exc).__name__, error=_excerpt(str(exc)))
        except Exception as exc:  # a client bug must degrade the run, not raise
            post = EventsPost.unavailable(reason=type(exc).__name__, error=_excerpt(str(exc)))

        elapsed_ms = int((time.monotonic() - started) * 1000)
        log = self._log.info if post.ok else self._log.warning
        log(
            "backend_post_events",
            ok=post.ok,
            off=post.off,
            availability=len(body["availability"]),
            news=len(body["news"]),
            teams=post.teams,
            facts=post.facts,
            recorded=post.recorded,
            started=len(post.started),
            reason=post.reason,
            status_code=status_code,
            elapsed_ms=elapsed_ms,
            error=post.error,
        )
        return post

    def react_lineup(
        self,
        *,
        player_id: int,
        game_date: date,
        verdict: str,
        previous_verdict: Optional[str] = None,
        reason: Optional[str] = None,
        source: Optional[str] = None,
        changed_at: Optional[datetime] = None,
        correlation_id: Optional[str] = None,
    ) -> LineupReaction:
        """Hand the backend one verdict change (out, doubtful or available, game not tipped): it
        swaps or plans on every team whose Late scratches level is notify or act.

        Never raises: transport and protocol failures come back as a reaction that is not `ok`.
        """
        if not self.enabled:
            return LineupReaction.unavailable(reason="backend_not_configured")
        body = {
            "player_id": int(player_id),
            "game_date": game_date.isoformat(),
            "verdict": verdict,
            "previous_verdict": previous_verdict,
            "reason": (reason or "")[:100] or None,
            "source": (source or "")[:16] or None,
            "changed_at": changed_at.isoformat() if changed_at else None,
        }
        headers = {
            "Authorization": f"Bearer {self._token}",
            "Accept": "application/json",
        }
        if correlation_id:
            headers["X-Correlation-ID"] = correlation_id

        started = time.monotonic()
        status_code: Optional[int] = None
        try:
            response = requests.post(
                f"{self._base_url}{REACT_LINEUP_PATH}",
                json=body,
                headers=headers,
                timeout=self._react_timeout,
            )
            status_code = response.status_code
            reaction = self._parse_react_response(response)
        except requests.Timeout as exc:
            reaction = LineupReaction.unavailable(reason="timeout", error=_excerpt(str(exc)))
        except requests.RequestException as exc:
            reaction = LineupReaction.unavailable(reason=type(exc).__name__, error=_excerpt(str(exc)))
        except Exception as exc:  # a client bug must degrade the run, not raise
            reaction = LineupReaction.unavailable(reason=type(exc).__name__, error=_excerpt(str(exc)))

        elapsed_ms = int((time.monotonic() - started) * 1000)
        log = self._log.info if reaction.ok else self._log.warning
        log(
            "backend_react_lineup",
            ok=reaction.ok,
            player_id=body["player_id"],
            verdict=verdict,
            teams=len(reaction.teams),
            applied=sum(1 for t in reaction.teams if t.outcome == "applied"),
            reason=reaction.reason,
            status_code=status_code,
            elapsed_ms=elapsed_ms,
            error=reaction.error,
        )
        return reaction

    @staticmethod
    def _parse_react_response(response: requests.Response) -> LineupReaction:
        status = response.status_code
        try:
            payload = response.json()
        except ValueError:
            payload = None

        if status == 200:
            data = payload.get("data") if isinstance(payload, dict) else None
            return LineupReaction.from_payload(data)

        if status == 401:
            detail = payload.get("detail") if isinstance(payload, dict) else None
            return LineupReaction.unavailable(reason="unauthorized", error=_excerpt(str(detail or response.text)))

        detail: Optional[str] = None
        if isinstance(payload, dict):
            detail = str(payload.get("message") or payload.get("detail") or payload.get("error_code") or "")
        return LineupReaction.unavailable(reason=f"http_{status}", error=_excerpt(detail or response.text) or None)

    @staticmethod
    def _parse_events_response(response: requests.Response) -> EventsPost:
        status = response.status_code
        try:
            payload = response.json()
        except ValueError:
            payload = None

        if status == 200:
            data = payload.get("data") if isinstance(payload, dict) else None
            return EventsPost.from_payload(data)

        if status == 401:
            detail = payload.get("detail") if isinstance(payload, dict) else None
            return EventsPost.unavailable(reason="unauthorized", error=_excerpt(str(detail or response.text)))

        detail: Optional[str] = None
        if isinstance(payload, dict):
            detail = str(payload.get("message") or payload.get("detail") or payload.get("error_code") or "")
        return EventsPost.unavailable(reason=f"http_{status}", error=_excerpt(detail or response.text) or None)

    @staticmethod
    def _parse_checkins_response(response: requests.Response) -> CheckinsRun:
        status = response.status_code
        try:
            payload = response.json()
        except ValueError:
            payload = None

        if status == 200:
            data = payload.get("data") if isinstance(payload, dict) else None
            return CheckinsRun.from_payload(data)

        if status == 401:
            detail = payload.get("detail") if isinstance(payload, dict) else None
            return CheckinsRun.unavailable(reason="unauthorized", error=_excerpt(str(detail or response.text)))

        detail: Optional[str] = None
        if isinstance(payload, dict):
            detail = str(payload.get("message") or payload.get("detail") or payload.get("error_code") or "")
        return CheckinsRun.unavailable(reason=f"http_{status}", error=_excerpt(detail or response.text) or None)

    @staticmethod
    def _parse_ledger_response(response: requests.Response) -> LedgerRun:
        status = response.status_code
        try:
            payload = response.json()
        except ValueError:
            payload = None

        if status == 200:
            data = payload.get("data") if isinstance(payload, dict) else None
            return LedgerRun.from_payload(data)

        if status == 401:
            detail = payload.get("detail") if isinstance(payload, dict) else None
            return LedgerRun.unavailable(reason="unauthorized", error=_excerpt(str(detail or response.text)))

        detail: Optional[str] = None
        if isinstance(payload, dict):
            detail = str(payload.get("message") or payload.get("detail") or payload.get("error_code") or "")
        return LedgerRun.unavailable(reason=f"http_{status}", error=_excerpt(detail or response.text) or None)

    @staticmethod
    def _parse_pickups_response(response: requests.Response) -> PickupsRun:
        status = response.status_code
        try:
            payload = response.json()
        except ValueError:
            payload = None

        if status == 200:
            data = payload.get("data") if isinstance(payload, dict) else None
            return PickupsRun.from_payload(data)

        if status == 401:
            detail = payload.get("detail") if isinstance(payload, dict) else None
            return PickupsRun.unavailable(reason="unauthorized", error=_excerpt(str(detail or response.text)))

        detail: Optional[str] = None
        if isinstance(payload, dict):
            detail = str(payload.get("message") or payload.get("detail") or payload.get("error_code") or "")
        return PickupsRun.unavailable(reason=f"http_{status}", error=_excerpt(detail or response.text) or None)

    @staticmethod
    def _parse_response(response: requests.Response) -> LineupEvaluation:
        status = response.status_code
        try:
            payload = response.json()
        except ValueError:
            payload = None

        if status == 200:
            data = payload.get("data") if isinstance(payload, dict) else None
            return LineupEvaluation.from_payload(data)

        if status == 404 and isinstance(payload, dict) and payload.get("error_code") == "TEAM_NOT_FOUND":
            return LineupEvaluation(
                outcome="skipped",
                reason="team_not_found",
                error=_excerpt(str(payload.get("message") or "")) or None,
            )

        if status == 401:
            detail = payload.get("detail") if isinstance(payload, dict) else None
            return LineupEvaluation.unavailable(reason="unauthorized", error=_excerpt(str(detail or response.text)))

        detail: Optional[str] = None
        if isinstance(payload, dict):
            detail = str(payload.get("message") or payload.get("detail") or payload.get("error_code") or "")
        return LineupEvaluation.unavailable(
            reason=f"http_{status}",
            error=_excerpt(detail or response.text) or None,
        )


def backend_client_from_settings(settings: Any = default_settings) -> BackendClient:
    """The client the pipeline uses: BACKEND_INTERNAL_URL + the shared pipeline token."""
    return BackendClient(
        base_url=settings.backend_internal_url,
        token=settings.pipeline_api_token.get_secret_value(),
        timeout_seconds=settings.backend_timeout_seconds,
        pickups_timeout_seconds=getattr(settings, "pickups_backend_timeout_seconds", None),
        ledger_timeout_seconds=getattr(settings, "ledger_backend_timeout_seconds", None),
        react_timeout_seconds=getattr(settings, "react_backend_timeout_seconds", None),
    )
