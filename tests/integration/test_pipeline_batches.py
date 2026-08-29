"""
`nba.pipeline_batches` — the durable record of what a batch decided.

Against a real database, because two of the properties that matter here are
properties of the storage: that the sweep latch survives a process restart, and
that an unwritable audit table degrades to no record rather than to a failed
pipeline.
"""

from datetime import date, datetime, timedelta

import pytest

from db.models.nba.pipeline_batch import (
    ALL_SKIPPED,
    DISPATCHED,
    WINDOW_CLOSED,
    PipelineBatch,
)
from db.models.pipeline_run import PipelineRun

NBA_DATE = date(2026, 3, 4)


@pytest.mark.integration
class TestRecordingDecisions:
    def test_a_dispatch_is_recorded_open_and_closed_with_outcomes(self, integration_db):
        batch = PipelineBatch.open(
            "post_game",
            NBA_DATE,
            DISPATCHED,
            "dispatched",
            {"player_game_stats": {"decision": "run", "reason": "due"}},
            job_id="job-1",
        )
        assert batch is not None
        # Still in flight: no completion stamp yet.
        assert batch.completed_at is None

        PipelineBatch.close(
            batch.id, {"player_game_stats": {"status": "success", "records": 412}}
        )

        stored = PipelineBatch.get_by_id(batch.id)
        assert stored.completed_at is not None
        entry = stored.pipelines["player_game_stats"]
        # The decision and the outcome are merged, not one overwriting the other:
        # "we meant to run it and it processed 412 rows" is the whole record.
        assert entry["decision"] == "run"
        assert entry["reason"] == "due"
        assert entry["status"] == "success"
        assert entry["records"] == 412

    def test_a_skip_is_recorded_complete_immediately(self, integration_db):
        batch = PipelineBatch.open(
            "post_game", NBA_DATE, ALL_SKIPPED, "already_complete",
            {"player_game_stats": {"decision": "skip", "reason": "already_ran"}},
        )
        assert batch.completed_at is not None

    def test_closing_an_unknown_batch_is_a_no_op(self, integration_db):
        PipelineBatch.close(None, {"x": {"status": "success"}})  # must not raise
        import uuid
        PipelineBatch.close(uuid.uuid4(), {"x": {"status": "success"}})


@pytest.mark.integration
class TestTheSweepLatch:
    def test_swept_is_false_until_a_window_closed_row_exists(self, integration_db):
        assert PipelineBatch.swept("post_game", NBA_DATE) is False
        PipelineBatch.open("post_game", NBA_DATE, WINDOW_CLOSED, "incomplete")
        assert PipelineBatch.swept("post_game", NBA_DATE) is True

    def test_other_decisions_do_not_latch_it(self, integration_db):
        PipelineBatch.open("post_game", NBA_DATE, DISPATCHED, "dispatched")
        PipelineBatch.open("post_game", NBA_DATE, ALL_SKIPPED, "already_complete")
        assert PipelineBatch.swept("post_game", NBA_DATE) is False

    def test_the_latch_is_per_date_and_per_category(self, integration_db):
        PipelineBatch.open("post_game", NBA_DATE, WINDOW_CLOSED, "complete")
        assert PipelineBatch.swept("post_game", NBA_DATE + timedelta(days=1)) is False
        assert PipelineBatch.swept("pre_game", NBA_DATE) is False


@pytest.mark.integration
class TestAuditFailuresAreNotPipelineFailures:
    """Observability must never be load-bearing for the data."""

    def test_an_unwritable_table_returns_none_instead_of_raising(
        self, integration_db, monkeypatch
    ):
        def boom(cls, **fields):
            raise RuntimeError("relation nba.pipeline_batches does not exist")

        monkeypatch.setattr(PipelineBatch, "create", classmethod(boom))
        # This is the state between the two services' deploys: data-platform
        # ships the writer before the backend has applied migration 0009.
        assert PipelineBatch.open("post_game", NBA_DATE, DISPATCHED, "dispatched") is None

    def test_an_unreadable_table_claims_the_sweep_already_happened(
        self, integration_db, monkeypatch
    ):
        """Better a missed alert than one fired on information we do not have."""
        def boom(cls):
            raise RuntimeError("relation nba.pipeline_batches does not exist")

        monkeypatch.setattr(PipelineBatch, "select", classmethod(boom))
        assert PipelineBatch.swept("post_game", NBA_DATE) is True


@pytest.mark.integration
class TestRetryBudget:
    """`PipelineRun.count_since` is what bounds a night's retries.

    `was_successful_on_date` cannot: under an ESPN outage no run ever succeeds,
    so a budget keyed on success would never stop the pipeline retrying every
    15 minutes until the window closed.
    """

    def test_counts_failures_as_well_as_successes(self, integration_db):
        cutoff = datetime.utcnow() - timedelta(hours=1)
        for _ in range(2):
            PipelineRun.start_run("budget_test").mark_failed("ESPN 503")
        PipelineRun.start_run("budget_test").mark_success(records_processed=5)

        assert PipelineRun.count_since("budget_test", cutoff) == 3
        assert PipelineRun.was_successful_on_date(
            "budget_test", NBA_DATE, after=cutoff
        ) is True

    def test_ignores_runs_before_the_cutoff(self, integration_db):
        run = PipelineRun.start_run("budget_test")
        run.started_at = datetime.utcnow() - timedelta(days=1)
        run.save()

        assert PipelineRun.count_since("budget_test", datetime.utcnow() - timedelta(hours=1)) == 0

    def test_is_scoped_to_one_pipeline(self, integration_db):
        cutoff = datetime.utcnow() - timedelta(hours=1)
        PipelineRun.start_run("budget_test")
        PipelineRun.start_run("other_pipeline")

        assert PipelineRun.count_since("budget_test", cutoff) == 1
