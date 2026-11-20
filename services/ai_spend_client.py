"""
Backend AI spend client: the AI spend page's two calls.

    GET {BACKEND_INTERNAL_URL}/v1/internal/jobs/ai/spend?days=N
    PUT {BACKEND_INTERNAL_URL}/v1/internal/jobs/ai/caps   {line, user_id, monthly_usd, author}
    Authorization: Bearer <PIPELINE_API_TOKEN>              (the token both services share)

The backend owns the ledger, the caps and every model call; this platform shows
them and passes a cap change on. A refusal the backend can name (a 4xx with its
envelope) comes back as a `BackendRefused` carrying its status, code and message,
so the page says what the backend said; anything else (no backend configured, a
timeout, a 5xx, a malformed answer) is a `BackendUnavailable`. One request each,
no retry. The token is never logged.
"""

from __future__ import annotations

from typing import Any, Mapping, Optional

import requests

from core.logging import get_logger

SPEND_PATH = "/v1/internal/jobs/ai/spend"
CAPS_PATH = "/v1/internal/jobs/ai/caps"
CONNECT_TIMEOUT_SECONDS = 5.0
READ_TIMEOUT_SECONDS = 20.0

_EXCERPT_CHARS = 300


class BackendUnavailable(Exception):
    """The backend could not be asked, or did not answer sensibly."""

    def __init__(self, reason: str):
        super().__init__(reason)
        self.reason = reason


class BackendRefused(Exception):
    """The backend refused the request and said why."""

    def __init__(self, status_code: int, message: str, error_code: Optional[str] = None):
        super().__init__(message)
        self.status_code = status_code
        self.message = message
        self.error_code = error_code


def _excerpt(text: Optional[str]) -> str:
    text = (text or "").strip()
    return text if len(text) <= _EXCERPT_CHARS else text[: _EXCERPT_CHARS - 1] + "…"


def _refusal(response: requests.Response) -> BackendRefused:
    message, code = f"The backend refused it ({response.status_code})", None
    try:
        body = response.json()
    except ValueError:
        body = None
    if isinstance(body, dict):
        detail = body.get("message") or body.get("detail")
        if isinstance(detail, str):
            message = detail
        elif isinstance(detail, list) and detail and isinstance(detail[0], dict):
            message = str(detail[0].get("msg") or message)  # FastAPI's validation errors
        code = body.get("error_code") if isinstance(body.get("error_code"), str) else None
    return BackendRefused(response.status_code, message, code)


class AiSpendClient:
    """Asks the backend for the report and passes cap changes to it."""

    def __init__(self, base_url: Optional[str], token: str):
        self._base_url = (base_url or "").strip().rstrip("/")
        self._token = token
        self._log = get_logger("ai_spend_client")

    def _headers(self, correlation_id: Optional[str]) -> dict:
        headers = {"Authorization": f"Bearer {self._token}", "Accept": "application/json"}
        if correlation_id:
            headers["X-Correlation-ID"] = correlation_id
        return headers

    def _call(self, method: str, path: str, correlation_id: Optional[str], **kwargs: Any) -> Any:
        if not self._base_url:
            raise BackendUnavailable("backend_not_configured")
        try:
            response = requests.request(method, f"{self._base_url}{path}", headers=self._headers(correlation_id),
                                        timeout=(CONNECT_TIMEOUT_SECONDS, READ_TIMEOUT_SECONDS), **kwargs)
        except requests.Timeout as exc:
            self._log.warning("backend_ai_spend_unavailable", path=path, reason="timeout", error=_excerpt(str(exc)))
            raise BackendUnavailable("timeout") from exc
        except requests.RequestException as exc:
            self._log.warning("backend_ai_spend_unavailable", path=path, reason=type(exc).__name__,
                              error=_excerpt(str(exc)))
            raise BackendUnavailable(type(exc).__name__) from exc

        if response.status_code == 200:
            try:
                payload = response.json()
            except ValueError as exc:
                raise BackendUnavailable("malformed_response") from exc
            data = payload.get("data") if isinstance(payload, dict) else None
            if not isinstance(data, dict):
                raise BackendUnavailable("malformed_response")
            return data
        if response.status_code == 401:
            # Our own token is wrong: the backend's and this platform's differ
            raise BackendUnavailable("unauthorized")
        if 400 <= response.status_code < 500:
            raise _refusal(response)
        self._log.warning("backend_ai_spend_unavailable", path=path, reason=f"http_{response.status_code}",
                          error=_excerpt(response.text))
        raise BackendUnavailable(f"http_{response.status_code}")

    def report(self, days: int, correlation_id: Optional[str] = None) -> dict:
        """The backend's report over the last `days` UTC days."""
        return self._call("GET", SPEND_PATH, correlation_id, params={"days": days})

    def set_cap(self, body: Mapping[str, Any], correlation_id: Optional[str] = None) -> dict:
        """Set or clear one cap; returns the cap as it now stands."""
        data = self._call("PUT", CAPS_PATH, correlation_id, json=dict(body))
        self._log.info("backend_ai_cap_set", line=body.get("line"), user_id=body.get("user_id"),
                       monthly_usd=body.get("monthly_usd"), author=body.get("author"))
        return data
