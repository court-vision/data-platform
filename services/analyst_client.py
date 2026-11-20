"""
Backend analyst client: the analyst pipeline's one call per read, and the
month's spend for the digest (the Live rankings plan, step 4).

    POST {BACKEND_INTERNAL_URL}/v1/internal/jobs/analyst/read
    Authorization: Bearer <PIPELINE_API_TOKEN>      (the token both services share)
    {the packet: services.analyst_inputs.packet}

The backend owns the model call, the prompt and the spend ledger; this
platform owns the facts sent and what is done with the answer. Every business
result is a 200 whose `data.outcome` says how the read went: `read` (a
proposal came back), `declined`, `malformed`, `budget` (the month's budget for
reads is used up: nothing was asked) or `off` (the analyst is switched off).
A provider failure is a 5xx.

    GET {BACKEND_INTERNAL_URL}/v1/internal/jobs/analyst/spend

Never raises: a transport or protocol problem comes back as `unavailable` with
the reason, the pipeline logs the read as failed, and the next run reads the
player again. One request per read, no retry here. A read that timed out may
still have been asked and paid for; the ledger on the backend is what counts
the cost. The token is never logged.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any, Mapping, Optional

import requests

from core.logging import get_logger
from core.settings import settings as default_settings

READ_PATH = "/v1/internal/jobs/analyst/read"
SPEND_PATH = "/v1/internal/jobs/analyst/spend"
CONNECT_TIMEOUT_SECONDS = 5.0
# The backend gives the model 75 seconds for a read; this waits a little longer than that
READ_TIMEOUT_SECONDS = 95.0
SPEND_TIMEOUT_SECONDS = 15.0

OUTCOMES = frozenset({"read", "declined", "malformed", "budget", "off"})
UNAVAILABLE = "unavailable"

_ERROR_EXCERPT_CHARS = 300


def _excerpt(text: Optional[str]) -> str:
    text = (text or "").strip()
    return text if len(text) <= _ERROR_EXCERPT_CHARS else text[: _ERROR_EXCERPT_CHARS - 1] + "…"


def _int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


@dataclass(frozen=True)
class ReadResult:
    """How one read went, or why the backend could not be asked."""

    outcome: str                            # read | declined | malformed | budget | off | unavailable
    proposal: Optional[dict] = None
    detail: Optional[str] = None            # why an answer could not be read, or why there is none
    model: Optional[str] = None
    prompt_version: Optional[str] = None
    input_tokens: int = 0
    output_tokens: int = 0
    cost_micros: int = 0

    @classmethod
    def unavailable(cls, reason: str) -> "ReadResult":
        return cls(outcome=UNAVAILABLE, detail=reason)

    @classmethod
    def from_payload(cls, data: Any) -> "ReadResult":
        """Build from the envelope's `data`; anything malformed is unavailable."""
        if not isinstance(data, dict) or data.get("outcome") not in OUTCOMES:
            return cls.unavailable("malformed_response")
        usage = data.get("usage") if isinstance(data.get("usage"), dict) else {}
        proposal = data.get("proposal") if isinstance(data.get("proposal"), dict) else None
        if data["outcome"] == "read" and proposal is None:
            return cls.unavailable("malformed_response")
        return cls(
            outcome=data["outcome"],
            proposal=proposal,
            detail=data.get("detail"),
            model=usage.get("model"),
            prompt_version=data.get("prompt_version"),
            input_tokens=_int(usage.get("input_tokens")) + _int(usage.get("cache_read_input_tokens"))
            + _int(usage.get("cache_creation_input_tokens")),
            output_tokens=_int(usage.get("output_tokens")),
            cost_micros=_int(usage.get("cost_micros")),
        )


@dataclass(frozen=True)
class Month:
    """What the reads have spent this month and may, or why the backend could not say."""

    available: bool
    reason: Optional[str] = None
    enabled: bool = False
    spent_micros: int = 0
    cap_micros: int = 0
    read_max_micros: int = 0
    model: Optional[str] = None
    prompt_version: Optional[str] = None

    @classmethod
    def unavailable(cls, reason: str) -> "Month":
        return cls(available=False, reason=reason)

    @classmethod
    def from_payload(cls, data: Any) -> "Month":
        if not isinstance(data, dict) or "enabled" not in data:
            return cls.unavailable("malformed_response")
        return cls(
            available=True,
            enabled=bool(data["enabled"]),
            spent_micros=_int(data.get("month_spent_micros")),
            cap_micros=_int(data.get("month_cap_micros")),
            read_max_micros=_int(data.get("read_max_micros")),
            model=data.get("model"),
            prompt_version=data.get("prompt_version"),
        )

    @property
    def room(self) -> bool:
        """Whether the month's budget can take one more read's ceiling."""
        return self.cap_micros - self.spent_micros >= max(self.read_max_micros, 1)


class AnalystClient:
    """Asks the backend for reads. Disabled when no base URL is configured."""

    def __init__(self, base_url: Optional[str], token: str):
        self._base_url = (base_url or "").strip().rstrip("/")
        self._token = token
        self._log = get_logger("analyst_client")

    @property
    def enabled(self) -> bool:
        return bool(self._base_url)

    def _headers(self, correlation_id: Optional[str]) -> dict:
        headers = {"Authorization": f"Bearer {self._token}", "Accept": "application/json"}
        if correlation_id:
            headers["X-Correlation-ID"] = correlation_id
        return headers

    def read(self, body: Mapping, correlation_id: Optional[str] = None) -> ReadResult:
        """One read of the player `body` is about."""
        if not self.enabled:
            return ReadResult.unavailable("backend_not_configured")
        started = time.monotonic()
        status_code: Optional[int] = None
        error: Optional[str] = None
        try:
            response = requests.post(
                f"{self._base_url}{READ_PATH}", json=dict(body), headers=self._headers(correlation_id),
                timeout=(CONNECT_TIMEOUT_SECONDS, READ_TIMEOUT_SECONDS),
            )
            status_code = response.status_code
            if status_code == 200:
                payload = response.json()
                result = ReadResult.from_payload(payload.get("data") if isinstance(payload, dict) else None)
            elif status_code == 401:
                result = ReadResult.unavailable("unauthorized")
            else:
                result = ReadResult.unavailable(f"http_{status_code}")
                error = _excerpt(response.text)
        except requests.Timeout as exc:
            result, error = ReadResult.unavailable("timeout"), _excerpt(str(exc))
        except requests.RequestException as exc:
            result, error = ReadResult.unavailable(type(exc).__name__), _excerpt(str(exc))
        except Exception as exc:  # a client bug must cost one read, not the morning
            result, error = ReadResult.unavailable(type(exc).__name__), _excerpt(str(exc))

        log = self._log.warning if result.outcome == UNAVAILABLE else self._log.info
        log(
            "backend_analyst_read",
            player_id=body.get("player_id"),
            outcome=result.outcome,
            detail=result.detail if result.outcome != "read" else None,
            status_code=status_code,
            cost_micros=result.cost_micros,
            elapsed_ms=int((time.monotonic() - started) * 1000),
            error=error,
        )
        return result

    def month(self, correlation_id: Optional[str] = None) -> Month:
        """This month's spend on reads and the line's cap."""
        if not self.enabled:
            return Month.unavailable("backend_not_configured")
        try:
            response = requests.get(
                f"{self._base_url}{SPEND_PATH}", headers=self._headers(correlation_id),
                timeout=(CONNECT_TIMEOUT_SECONDS, SPEND_TIMEOUT_SECONDS),
            )
            if response.status_code == 200:
                payload = response.json()
                month = Month.from_payload(payload.get("data") if isinstance(payload, dict) else None)
            elif response.status_code == 401:
                month = Month.unavailable("unauthorized")
            else:
                month = Month.unavailable(f"http_{response.status_code}")
        except requests.Timeout:
            month = Month.unavailable("timeout")
        except requests.RequestException as exc:
            month = Month.unavailable(type(exc).__name__)
        except Exception as exc:  # the digest still has its reads without the month
            month = Month.unavailable(type(exc).__name__)
        if not month.available:
            self._log.warning("backend_analyst_spend_unavailable", reason=month.reason)
        return month


def analyst_client_from_settings(settings: Any = default_settings) -> AnalystClient:
    """BACKEND_INTERNAL_URL + the shared pipeline token."""
    return AnalystClient(
        base_url=settings.backend_internal_url,
        token=settings.pipeline_api_token.get_secret_value(),
    )
