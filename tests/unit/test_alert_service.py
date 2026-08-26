"""
`services.alert_service`: a no-op without a webhook, per-key dedupe windows,
Discord embeds vs Slack text, a failing POST is swallowed, "recovered" clears
the dedupe stamp.
"""

from datetime import timedelta

import httpx
import pytest
from freezegun import freeze_time
from pydantic import SecretStr

from core.settings import settings
from services import alert_service as alert_module
from services.alert_service import AlertEvent, AlertService, cron_streak_threshold, get_alert_service

HOOK = "https://discord.test/api/webhooks/1/abc"


class Recorder:
    def __init__(self):
        self.calls = []

    def __call__(self, url, payload):
        self.calls.append((url, payload))


@pytest.fixture
def webhook(monkeypatch):
    monkeypatch.setattr(settings, "alert_webhook_url", SecretStr(HOOK))
    monkeypatch.setattr(settings, "alert_webhook_format", "discord")
    monkeypatch.setattr(settings, "alerts_enabled", True)


def _event(key="pipeline_failed:demo", dedupe=timedelta(hours=6), severity="critical"):
    return AlertEvent(
        key=key,
        severity=severity,
        title="Pipeline failed: Demo",
        body="ValueError: bad row",
        fields={"run_id": "r-1", "records_processed": 0, "date_override": None},
        dedupe=dedupe,
    )


@pytest.mark.unit
def test_noop_without_webhook_url(monkeypatch):
    monkeypatch.setattr(settings, "alert_webhook_url", None)
    monkeypatch.setattr(settings, "alerts_enabled", True)
    recorder = Recorder()
    service = AlertService(post=recorder)

    assert service.enabled is False
    assert service.notify(_event()) is False
    assert recorder.calls == []


@pytest.mark.unit
def test_noop_when_alerts_disabled(webhook, monkeypatch):
    monkeypatch.setattr(settings, "alerts_enabled", False)
    recorder = Recorder()
    assert AlertService(post=recorder).notify(_event()) is False
    assert recorder.calls == []


@pytest.mark.unit
def test_dedupes_the_same_key_inside_its_window(webhook):
    recorder = Recorder()
    service = AlertService(post=recorder)

    with freeze_time("2026-08-26T00:00:00Z") as clock:
        assert service.notify(_event()) is True
        assert service.notify(_event()) is False
        clock.tick(timedelta(hours=5, minutes=59))
        assert service.notify(_event()) is False
        clock.tick(timedelta(minutes=2))
        assert service.notify(_event()) is True

    assert len(recorder.calls) == 2


@pytest.mark.unit
def test_dedupe_is_per_key(webhook):
    recorder = Recorder()
    service = AlertService(post=recorder)
    assert service.notify(_event(key="pipeline_failed:a")) is True
    assert service.notify(_event(key="pipeline_failed:b")) is True
    assert len(recorder.calls) == 2


@pytest.mark.unit
def test_discord_embed_payload(webhook):
    recorder = Recorder()
    with freeze_time("2026-08-26T01:02:03Z"):
        AlertService(post=recorder).notify(_event())

    url, payload = recorder.calls[0]
    assert url == HOOK
    embed = payload["embeds"][0]
    assert embed["title"] == "[CRITICAL] Pipeline failed: Demo"
    assert embed["description"] == "ValueError: bad row"
    assert embed["color"] == 0xE74C3C
    assert embed["timestamp"] == "2026-08-26T01:02:03+00:00"
    assert embed["footer"] == {"text": "development · dev"}
    assert {"name": "run_id", "value": "r-1", "inline": True} in embed["fields"]
    assert {"name": "records_processed", "value": "0", "inline": True} in embed["fields"]
    assert {"name": "date_override", "value": "—", "inline": True} in embed["fields"]


@pytest.mark.unit
def test_warning_colour_and_footer_carry_environment_and_version(webhook, monkeypatch):
    monkeypatch.setattr(settings, "railway_environment_name", "production")
    monkeypatch.setattr(settings, "app_version", "abc1234def")
    recorder = Recorder()
    AlertService(post=recorder).notify(_event(severity="warning"))

    embed = recorder.calls[0][1]["embeds"][0]
    assert embed["title"].startswith("[WARNING]")
    assert embed["color"] == 0xF39C12
    assert embed["footer"]["text"] == "production · abc1234"


@pytest.mark.unit
def test_slack_text_payload(webhook, monkeypatch):
    monkeypatch.setattr(settings, "alert_webhook_format", "slack")
    recorder = Recorder()
    with freeze_time("2026-08-26T01:02:03Z"):
        AlertService(post=recorder).notify(_event())

    payload = recorder.calls[0][1]
    assert list(payload) == ["text"]
    text = payload["text"]
    assert text.startswith("*[CRITICAL] Pipeline failed: Demo*\nValueError: bad row\n")
    assert "• run_id: r-1" in text
    assert "development · dev · 2026-08-26 01:02 UTC" in text


@pytest.mark.unit
def test_secrets_in_the_body_are_scrubbed(webhook):
    recorder = Recorder()
    event = AlertEvent(key="k", severity="critical", title="t",
                       body="GET https://x/?espn_s2=SECRET&swid=ALSO failed", dedupe=timedelta(0))
    AlertService(post=recorder).notify(event)
    description = recorder.calls[0][1]["embeds"][0]["description"]
    assert "SECRET" not in description and "ALSO" not in description
    assert "espn_s2=[Filtered]" in description


@pytest.mark.unit
def test_post_failure_is_swallowed(webhook, monkeypatch):
    def boom(self, *args, **kwargs):
        raise httpx.ConnectError("connection refused")

    monkeypatch.setattr(httpx.Client, "post", boom)
    service = AlertService()  # the real httpx transport

    assert service.notify(_event()) is False  # no exception escapes
    assert service.last_sent_at("pipeline_failed:demo") is not None  # the attempt still counts for dedupe


@pytest.mark.unit
def test_real_transport_posts_json_and_raises_on_http_error(webhook, monkeypatch):
    seen = {}

    class FakeResponse:
        status_code = 204

        def raise_for_status(self):
            seen["raised"] = True

    def fake_post(self, url, json=None, **kwargs):
        seen["url"], seen["json"] = url, json
        return FakeResponse()

    monkeypatch.setattr(httpx.Client, "post", fake_post)
    assert AlertService().notify(_event()) is True
    assert seen["url"] == HOOK
    assert seen["json"]["embeds"][0]["title"] == "[CRITICAL] Pipeline failed: Demo"
    assert seen["raised"] is True


@pytest.mark.unit
def test_recovered_posts_green_and_clears_the_dedupe(webhook):
    recorder = Recorder()
    service = AlertService(post=recorder)
    key = "cron_failure_streak:deploy"

    assert service.notify(_event(key=key)) is True
    assert service.notify(_event(key=key)) is False  # deduped
    assert service.recovered(key, title="Cron job recovered: deploy", body="ok after 1 failure") is True
    assert service.last_sent_at(key) is None
    assert service.notify(_event(key=key)) is True  # a new streak alerts again

    recovered_embed = recorder.calls[1][1]["embeds"][0]
    assert recovered_embed["title"] == "[RECOVERED] Cron job recovered: deploy"
    assert recovered_embed["color"] == 0x2ECC71


@pytest.mark.unit
def test_get_alert_service_is_a_process_singleton(monkeypatch):
    monkeypatch.setattr(alert_module, "_service", None)
    assert get_alert_service() is get_alert_service()
    assert isinstance(get_alert_service(), AlertService)


@pytest.mark.unit
def test_cron_streak_thresholds():
    assert cron_streak_threshold("live-stats") == 3
    assert cron_streak_threshold("pre-game") == 2
    assert cron_streak_threshold("schedule-sync") == 1
    assert cron_streak_threshold("deploy") == 1
    assert cron_streak_threshold("alert-test") == 2  # unknown jobs
