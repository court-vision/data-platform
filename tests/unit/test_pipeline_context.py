"""
`PipelineContext` carries the triggering request's correlation id on every
pipeline log line and reports failures to Sentry (when it is initialised).
"""

import pytest
from structlog.testing import capture_logs

from core.logging import set_correlation_id
from pipelines import context as context_module
from pipelines.context import PipelineContext


@pytest.mark.unit
def test_pipeline_logs_carry_the_correlation_id():
    set_correlation_id("cid-42")
    try:
        with capture_logs() as logs:
            ctx = PipelineContext("demo")
            ctx.log.info("step")
    finally:
        set_correlation_id("")

    entry = logs[0]
    assert entry["event"] == "step"
    assert entry["pipeline"] == "demo"
    assert entry["correlation_id"] == "cid-42"
    assert entry["run_id"]


@pytest.mark.unit
def test_no_correlation_id_outside_a_request():
    set_correlation_id("")
    with capture_logs() as logs:
        PipelineContext("demo").log.info("step")
    assert "correlation_id" not in logs[0]


@pytest.mark.unit
def test_mark_failed_reports_the_exception_to_sentry(monkeypatch):
    captured = []
    monkeypatch.setattr(context_module.sentry_sdk, "is_initialized", lambda: True)
    monkeypatch.setattr(context_module.sentry_sdk, "capture_exception", lambda exc: captured.append(exc))

    ctx = PipelineContext("demo")
    try:
        raise ValueError("bad row")
    except ValueError as exc:
        error = exc
        result = ctx.mark_failed(exc)

    assert captured == [error]
    assert result.status == "error"
    assert "ValueError: bad row" in result.error


@pytest.mark.unit
def test_mark_failed_is_silent_without_sentry(monkeypatch):
    monkeypatch.setattr(context_module.sentry_sdk, "is_initialized", lambda: False)
    monkeypatch.setattr(context_module.sentry_sdk, "capture_exception",
                        lambda exc: (_ for _ in ()).throw(AssertionError("must not capture")))

    ctx = PipelineContext("demo")
    try:
        raise ValueError("bad row")
    except ValueError as exc:
        result = ctx.mark_failed(exc)

    assert result.status == "error"
