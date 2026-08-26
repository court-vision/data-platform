"""
Ops alert notifier — one Discord (or Slack) incoming webhook for #cv-alerts.

    from services.alert_service import AlertEvent, get_alert_service

    get_alert_service().notify(AlertEvent(
        key="pipeline_failed:live_game_stats",
        severity="critical",
        title="Pipeline failed: Live Game Stats",
        body="NetworkError: NBA live BoxScore timeout",
        fields={"run_id": "...", "pipeline": "live_game_stats"},
        dedupe=timedelta(hours=6),
    ))

Behaviour:

- **No-op without a webhook.** `ALERT_WEBHOOK_URL` is set on production only;
  locally, in tests and on staging nothing is sent. `ALERTS_ENABLED=false`
  silences a configured webhook.
- **Never raises.** A failed POST is one `alert_send_failed` warning; callers
  (pipeline threads, request handlers, the health endpoint) are never affected.
- **Dedupe is in-memory and per process.** `{key: last_attempt_at}`; an event
  whose `key` was sent less than `event.dedupe` ago is dropped. The private
  and public uvicorn processes keep separate maps, and the map is empty after
  a restart, so a crash-looping service can re-send once per restart —
  Railway's "crashed" webhook is the primary signal there. The dedupe stamp is
  recorded before the POST so a dead webhook cannot turn every event into a
  5 s stall.
- **Sync.** `notify` blocks for at most `WEBHOOK_TIMEOUT_S`; async callers use
  `notify_async` (`asyncio.to_thread`).
- `recovered(key, ...)` posts a green "recovered" note and clears `key`'s
  dedupe stamp so the next occurrence alerts again (cron streaks use it).

Tests swap the singleton: `monkeypatch.setattr(services.alert_service, "_service", fake)`.

The event catalogue (what fires, thresholds, dedupe windows) is in README.md
under "Alerting".
"""

from __future__ import annotations

import asyncio
import threading
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Literal, Mapping, Optional

import httpx

from core.logging import get_logger
from core.settings import settings as default_settings
from core.telemetry import scrub_string

Severity = Literal["info", "warning", "critical"]

WEBHOOK_TIMEOUT_S = 5.0

# Discord embed colours (decimal RGB): green / amber / red.
COLOURS: dict[str, int] = {"info": 0x2ECC71, "warning": 0xF39C12, "critical": 0xE74C3C}
LABELS: dict[str, str] = {"info": "[RECOVERED]", "warning": "[WARNING]", "critical": "[CRITICAL]"}

# Discord embed limits.
_TITLE_MAX = 256
_DESCRIPTION_MAX = 4096
_FIELD_NAME_MAX = 256
_FIELD_VALUE_MAX = 1024
_FOOTER_MAX = 2048
_FIELDS_MAX = 25

PostFn = Callable[[str, dict], None]


@dataclass(frozen=True)
class AlertEvent:
    """One alert. `key` identifies the condition for dedupe (e.g. `pipeline_failed:<name>`)."""

    key: str
    severity: Severity
    title: str
    body: str = ""
    fields: Mapping[str, Any] = field(default_factory=dict)
    dedupe: timedelta = timedelta(hours=1)


def _clip(text: str, limit: int) -> str:
    text = text or ""
    return text if len(text) <= limit else text[: limit - 1] + "…"


def _format_value(value: Any) -> str:
    if value is None or value == "":
        return "—"
    if isinstance(value, float):
        return f"{value:.3g}"
    if isinstance(value, (dict, list, tuple)):
        return ", ".join(f"{k}={v}" for k, v in value.items()) if isinstance(value, dict) else ", ".join(map(str, value))
    return str(value)


def discord_payload(event: AlertEvent, now: datetime, footer: str) -> dict:
    fields = [
        {"name": _clip(str(name), _FIELD_NAME_MAX), "value": _clip(scrub_string(_format_value(value)), _FIELD_VALUE_MAX), "inline": True}
        for name, value in list(event.fields.items())[:_FIELDS_MAX]
    ]
    embed = {
        "title": _clip(f"{LABELS[event.severity]} {event.title}", _TITLE_MAX),
        "description": _clip(scrub_string(event.body), _DESCRIPTION_MAX),
        "color": COLOURS[event.severity],
        "fields": fields,
        "timestamp": now.isoformat(),
        "footer": {"text": _clip(footer, _FOOTER_MAX)},
    }
    return {"embeds": [embed]}


def slack_payload(event: AlertEvent, now: datetime, footer: str) -> dict:
    lines = [f"*{LABELS[event.severity]} {event.title}*"]
    if event.body:
        lines.append(scrub_string(event.body))
    lines += [f"• {name}: {scrub_string(_format_value(value))}" for name, value in event.fields.items()]
    lines.append(f"_{footer} · {now:%Y-%m-%d %H:%M} UTC_")
    return {"text": "\n".join(lines)}


def _now() -> datetime:
    return datetime.now(timezone.utc)


class AlertService:
    """Sends `AlertEvent`s to the configured webhook with per-key dedupe."""

    def __init__(self, settings: Any = default_settings, post: Optional[PostFn] = None):
        self._settings = settings
        self._post: PostFn = post or self._post_webhook
        self._last_sent: dict[str, datetime] = {}
        self._lock = threading.Lock()
        self._log = get_logger("alerts")

    # ---- configuration -------------------------------------------------------

    @property
    def enabled(self) -> bool:
        return bool(self._settings.alerts_enabled and self._webhook_url())

    def _webhook_url(self) -> str:
        url = self._settings.alert_webhook_url
        return url.get_secret_value() if url else ""

    def _footer(self) -> str:
        return f"{self._settings.environment} · {self._settings.version}"

    def build_payload(self, event: AlertEvent, now: Optional[datetime] = None) -> dict:
        now = now or _now()
        if (self._settings.alert_webhook_format or "discord").lower() == "slack":
            return slack_payload(event, now, self._footer())
        return discord_payload(event, now, self._footer())

    # ---- sending -------------------------------------------------------------

    def notify(self, event: AlertEvent) -> bool:
        """Send `event` unless alerts are off or `event.key` is inside its dedupe window.

        Returns True when a webhook call was attempted. Never raises.
        """
        try:
            if not self.enabled:
                self._log.debug("alert_skipped", key=event.key, reason="alerts_disabled")
                return False

            now = _now()
            with self._lock:
                last = self._last_sent.get(event.key)
                if last is not None and now - last < event.dedupe:
                    self._log.info("alert_deduped", key=event.key, last_sent_at=last.isoformat(),
                                   dedupe_seconds=int(event.dedupe.total_seconds()))
                    return False
                self._last_sent[event.key] = now

            self._post(self._webhook_url(), self.build_payload(event, now))
            self._log.info("alert_sent", key=event.key, severity=event.severity, title=event.title)
            return True
        except Exception as exc:  # alerting must never break the caller
            self._log.warning("alert_send_failed", key=event.key, error=type(exc).__name__,
                              detail=scrub_string(str(exc))[:200])
            return False

    async def notify_async(self, event: AlertEvent) -> bool:
        return await asyncio.to_thread(self.notify, event)

    def recovered(self, key: str, title: str, body: str = "", fields: Optional[Mapping[str, Any]] = None) -> bool:
        """Post a green "recovered" note for `key` and clear its dedupe stamp."""
        with self._lock:
            self._last_sent.pop(key, None)
        return self.notify(AlertEvent(
            key=f"{key}:recovered",
            severity="info",
            title=title,
            body=body,
            fields=dict(fields or {}),
            dedupe=timedelta(0),
        ))

    def last_sent_at(self, key: str) -> Optional[datetime]:
        with self._lock:
            return self._last_sent.get(key)

    def reset(self) -> None:
        """Forget every dedupe stamp (tests)."""
        with self._lock:
            self._last_sent.clear()

    @staticmethod
    def _post_webhook(url: str, payload: dict) -> None:
        with httpx.Client(timeout=WEBHOOK_TIMEOUT_S) as client:
            response = client.post(url, json=payload)
            response.raise_for_status()


# ---- cron streak thresholds ----------------------------------------------------


def cron_streak_threshold(job_name: str, settings: Any = default_settings) -> int:
    """Consecutive cron failures that trigger `cron_failure_streak` for `job_name`."""
    return int(settings.alert_cron_streak_thresholds.get(job_name, settings.alert_cron_streak_default_threshold))


# ---- module-level accessor -----------------------------------------------------

_service: Optional[AlertService] = None


def get_alert_service() -> AlertService:
    """The process-wide notifier (lazily created; tests replace `_service`)."""
    global _service
    if _service is None:
        _service = AlertService()
    return _service
