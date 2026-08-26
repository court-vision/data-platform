"""
Partial success: `increment_failed` makes a successful run `partial`, the
result carries the counters, and `pipeline_partial` alerts only when nothing
succeeded or more than 20 % of attempts failed. A raising pipeline goes
through `BasePipeline._run_sync` to `mark_failed` -> Sentry + `pipeline_failed`.
"""

import threading
import uuid
from datetime import timedelta

import pytest

from pipelines import base as base_module
from pipelines import context as context_module
from pipelines.base import BasePipeline
from pipelines.config import PipelineCategory, PipelineConfig
from pipelines.context import PipelineContext


@pytest.mark.unit
def test_increment_failed_marks_the_run_partial(alerts):
    ctx = PipelineContext("demo")
    ctx.increment_records(8)
    ctx.increment_failed(2, "team_processing_error")
    ctx.increment_skipped(1, "no_matchup_data")

    result = ctx.mark_success()

    assert result.status == "success"
    assert result.partial is True
    assert (result.records_processed, result.records_failed, result.records_skipped) == (8, 2, 1)
    assert result.message == "demo completed successfully — 2 of 10 records failed (team_processing_error=2)"
    assert alerts.events == []  # exactly 20 % is not "more than 20 %"


@pytest.mark.unit
def test_clean_run_is_not_partial(alerts):
    ctx = PipelineContext("demo")
    ctx.increment_records(3)
    result = ctx.mark_success()
    assert result.partial is False
    assert result.records_failed == 0 and result.records_skipped == 0
    assert result.message == "demo completed successfully"
    assert alerts.events == []


@pytest.mark.unit
def test_alerts_when_nothing_succeeded(alerts):
    ctx = PipelineContext("demo")
    ctx.increment_failed(3, "RuntimeError")

    result = ctx.mark_success()

    assert result.partial is True
    assert alerts.keys() == ["pipeline_partial:demo"]
    event = alerts.events[0]
    assert event.severity == "warning"
    assert event.dedupe == timedelta(hours=24)
    assert "nothing succeeded" in event.body
    assert event.fields["failed"] == 3 and event.fields["processed"] == 0


@pytest.mark.unit
def test_alerts_when_more_than_a_fifth_failed(alerts):
    ctx = PipelineContext("demo")
    ctx.increment_records(7)
    ctx.increment_failed(3, "upsert_failed")

    ctx.mark_success()

    assert alerts.keys() == ["pipeline_partial:demo"]
    assert "30% of attempts" in alerts.events[0].body
    assert alerts.events[0].fields["reasons"] == "upsert_failed=3"


@pytest.mark.unit
def test_partial_alert_is_deduped_per_pipeline(alerts):
    for _ in range(2):
        ctx = PipelineContext("demo")
        ctx.increment_failed(1, "x")
        ctx.mark_success()
    other = PipelineContext("other")
    other.increment_failed(1, "x")
    other.mark_success()

    assert alerts.keys() == ["pipeline_partial:demo", "pipeline_partial:other"]


# ---- full lifecycle through BasePipeline._run_sync (DB stubbed) ---------------


class StubDB:
    def __init__(self):
        self._closed = True
        self.events = []

    def is_closed(self):
        return self._closed

    def connect(self):
        self._closed = False
        self.events.append("connect")

    def close(self):
        self._closed = True
        self.events.append("close")


class FakePipelineRun:
    instances = []

    def __init__(self, name):
        self.id = uuid.uuid4()
        self.pipeline_name = name
        self.status = "running"
        self.records_processed = 0
        self.error_message = None

    @classmethod
    def start_run(cls, pipeline_name):
        run = cls(pipeline_name)
        cls.instances.append(run)
        return run

    def mark_success(self, records_processed=0):
        self.status, self.records_processed = "success", records_processed

    def mark_failed(self, error_message):
        self.status, self.error_message = "failed", error_message


def _config(name="demo"):
    return PipelineConfig(name=name, display_name="Demo Pipeline", description="test",
                          target_table="nba.demo", category=PipelineCategory.POST_GAME)


@pytest.fixture
def lifecycle(monkeypatch):
    FakePipelineRun.instances = []
    stub_db = StubDB()
    monkeypatch.setattr(base_module, "db", stub_db)
    monkeypatch.setattr(context_module, "PipelineRun", FakePipelineRun)
    return stub_db


@pytest.mark.unit
def test_mark_failed_reports_sentry_and_alerts(alerts, lifecycle, monkeypatch):
    captured = []
    monkeypatch.setattr(context_module.sentry_sdk, "is_initialized", lambda: True)
    monkeypatch.setattr(context_module.sentry_sdk, "capture_exception", lambda exc: captured.append(exc))

    class Boom(BasePipeline):
        config = _config()

        def execute(self, ctx):
            ctx.increment_records(4)
            raise ValueError("bad row")

    result = Boom()._run_sync()

    assert result.status == "error"
    assert "ValueError: bad row" in result.error
    assert [type(e).__name__ for e in captured] == ["ValueError"]
    assert FakePipelineRun.instances[0].status == "failed"
    assert lifecycle.events == ["connect", "close"]

    assert alerts.keys() == ["pipeline_failed:demo"]
    event = alerts.events[0]
    assert event.severity == "critical"
    assert event.title == "Pipeline failed: Demo Pipeline"
    assert event.body == "ValueError: bad row"
    assert event.dedupe == timedelta(hours=6)
    assert event.fields["run_id"] == str(FakePipelineRun.instances[0].id)
    assert event.fields["records_processed"] == 4
    assert event.fields["category"] == "post_game"


@pytest.mark.unit
def test_repeated_failures_alert_once_per_window(alerts, lifecycle, monkeypatch):
    monkeypatch.setattr(context_module.sentry_sdk, "is_initialized", lambda: False)

    class Boom(BasePipeline):
        config = _config("live_game_stats")

        def execute(self, ctx):
            raise RuntimeError("boxscore down")

    for _ in range(5):
        assert Boom()._run_sync().status == "error"

    assert alerts.keys() == ["pipeline_failed:live_game_stats"]
    assert len(FakePipelineRun.instances) == 5  # every run is still recorded


@pytest.mark.unit
def test_successful_lifecycle_returns_partial_counters(alerts, lifecycle):
    class Mostly(BasePipeline):
        config = _config()

        def execute(self, ctx):
            ctx.increment_records(9)
            ctx.increment_failed(1, "upsert_failed")

    result = Mostly()._run_sync()

    assert result.status == "success" and result.partial is True
    assert result.records_failed == 1
    assert FakePipelineRun.instances[0].records_processed == 9
    assert alerts.events == []  # 10 % failed: partial but below the alert threshold


@pytest.mark.unit
def test_alerting_runs_on_the_pipeline_thread(alerts, lifecycle):
    """notify() is sync and runs inside _run_sync's worker thread — no event loop needed."""
    seen = {}

    class Boom(BasePipeline):
        config = _config()

        def execute(self, ctx):
            seen["thread"] = threading.get_ident()
            raise RuntimeError("x")

    worker = threading.Thread(target=lambda: Boom()._run_sync())
    worker.start()
    worker.join()

    assert seen["thread"] != threading.get_ident()
    assert alerts.keys() == ["pipeline_failed:demo"]
