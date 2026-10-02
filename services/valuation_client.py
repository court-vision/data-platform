"""
Backend valuation client — the projections editor's one call per board.

    POST {BACKEND_INTERNAL_URL}/v1/internal/jobs/valuation/standard
    Authorization: Bearer <PIPELINE_API_TOKEN>      (the token both services share)
    {"players": [{"player_id", "line", "games", "team", "dd_rate", "td_rate"}, ...]}

The valuation lives in the backend, where the draft board reads it; this
platform owns the projection and keeps no second copy of the engine. The editor
sends the whole pool — a rank is a place among the players sent — and gets back
each player's rank in standard points and standard 9-cat.

Never raises. The editor is still useful without ranks (the four lines are all
computed here), so every transport or protocol problem comes back as an
unavailable `Valuation` with the reason, and the page says so instead of
failing. The token is never logged.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any, Optional, Sequence

import requests

from core.logging import get_logger
from core.settings import settings as default_settings

VALUATION_PATH = "/v1/internal/jobs/valuation/standard"
CONNECT_TIMEOUT_SECONDS = 5.0
# The backend values ~520 players in well under a second; this is for a cold start.
READ_TIMEOUT_SECONDS = 20.0

_ERROR_EXCERPT_CHARS = 300


@dataclass(frozen=True)
class StandardRank:
    """One player's place in the standard league, both formats."""

    player_id: int
    points_rank: int
    points_value: float
    category_rank: int
    category_value: float


@dataclass(frozen=True)
class Valuation:
    """The backend's answer for one pool, or why there is none."""

    available: bool
    ranks: dict[int, StandardRank] = field(default_factory=dict)
    reason: Optional[str] = None
    league_size: Optional[int] = None
    rounds: Optional[int] = None
    playoff_weight: Optional[float] = None
    playoff_weeks: tuple[int, ...] = ()

    @classmethod
    def unavailable(cls, reason: str) -> "Valuation":
        return cls(available=False, reason=reason)

    @classmethod
    def from_payload(cls, data: Any) -> "Valuation":
        """Build from the envelope's `data`; anything malformed is unavailable."""
        try:
            ranks = {
                int(p["player_id"]): StandardRank(
                    player_id=int(p["player_id"]),
                    points_rank=int(p["points_rank"]),
                    points_value=float(p["points_value"]),
                    category_rank=int(p["category_rank"]),
                    category_value=float(p["category_value"]),
                )
                for p in data["players"]
            }
            return cls(
                available=True,
                ranks=ranks,
                league_size=int(data["league_size"]),
                rounds=int(data["rounds"]),
                playoff_weight=float(data["playoff_weight"]),
                playoff_weeks=tuple(int(w) for w in data.get("playoff_weeks") or ()),
            )
        except (KeyError, TypeError, ValueError):
            return cls.unavailable("malformed_response")


def _excerpt(text: Optional[str]) -> str:
    text = (text or "").strip()
    return text if len(text) <= _ERROR_EXCERPT_CHARS else text[: _ERROR_EXCERPT_CHARS - 1] + "…"


class ValuationClient:
    """Asks the backend to rank a pool. Disabled when no base URL is configured."""

    def __init__(self, base_url: Optional[str], token: str):
        self._base_url = (base_url or "").strip().rstrip("/")
        self._token = token
        self._log = get_logger("valuation_client")

    @property
    def enabled(self) -> bool:
        return bool(self._base_url)

    def standard(self, players: Sequence[dict], correlation_id: Optional[str] = None) -> Valuation:
        """Rank `players` (the request's `players` entries) in the standard league."""
        if not self.enabled:
            return Valuation.unavailable("backend_not_configured")
        if not players:
            return Valuation(available=True)

        headers = {"Authorization": f"Bearer {self._token}", "Accept": "application/json"}
        if correlation_id:
            headers["X-Correlation-ID"] = correlation_id

        started = time.monotonic()
        status_code: Optional[int] = None
        error: Optional[str] = None
        try:
            response = requests.post(
                f"{self._base_url}{VALUATION_PATH}",
                json={"players": list(players)},
                headers=headers,
                timeout=(CONNECT_TIMEOUT_SECONDS, READ_TIMEOUT_SECONDS),
            )
            status_code = response.status_code
            if status_code == 200:
                payload = response.json()
                valuation = Valuation.from_payload(payload.get("data") if isinstance(payload, dict) else None)
            elif status_code == 401:
                valuation = Valuation.unavailable("unauthorized")
            else:
                valuation = Valuation.unavailable(f"http_{status_code}")
                error = _excerpt(response.text)
        except requests.Timeout as exc:
            valuation, error = Valuation.unavailable("timeout"), _excerpt(str(exc))
        except requests.RequestException as exc:
            valuation, error = Valuation.unavailable(type(exc).__name__), _excerpt(str(exc))
        except Exception as exc:  # a client bug must cost the page its ranks, not the page
            valuation, error = Valuation.unavailable(type(exc).__name__), _excerpt(str(exc))

        log = self._log.info if valuation.available else self._log.warning
        log(
            "backend_standard_valuation",
            players=len(players),
            available=valuation.available,
            reason=valuation.reason,
            status_code=status_code,
            elapsed_ms=int((time.monotonic() - started) * 1000),
            error=error,
        )
        return valuation


def valuation_client_from_settings(settings: Any = default_settings) -> ValuationClient:
    """The client the editor uses: BACKEND_INTERNAL_URL + the shared pipeline token."""
    return ValuationClient(
        base_url=settings.backend_internal_url,
        token=settings.pipeline_api_token.get_secret_value(),
    )
