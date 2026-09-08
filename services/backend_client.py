"""
Backend internal API client — the lineup-alerts pipeline's one call per team.

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
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from datetime import date
from typing import Any, Optional

import requests

from core.logging import get_logger
from core.settings import settings as default_settings

EVALUATE_PATH = "/v1/internal/jobs/lineup/evaluate"
CONNECT_TIMEOUT_SECONDS = 5.0

# Outcomes the backend returns for a 200.
BACKEND_OUTCOMES = frozenset({"planned", "applied", "noop", "rejected", "failed", "skipped"})
# Client-side outcome: the backend could not be asked (or did not answer usably).
OUTCOME_UNAVAILABLE = "unavailable"

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


class BackendClient:
    """Talks to the backend's pipeline-token routes. Disabled when no base URL is configured."""

    def __init__(self, base_url: Optional[str], token: str, timeout_seconds: float):
        self._base_url = (base_url or "").strip().rstrip("/")
        self._token = token
        self._timeout = (CONNECT_TIMEOUT_SECONDS, float(timeout_seconds))
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
    )
