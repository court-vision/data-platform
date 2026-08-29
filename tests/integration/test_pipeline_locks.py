"""
Execution locks (`core/locks.py`).

`PipelineRun.is_running()` is a check-then-act, so two triggers arriving
together both read "not running" and both start. These tests use a real
PostgreSQL advisory lock on a second connection to prove the second run is
turned away, and — just as importantly — that the lock is released afterwards
rather than riding the pooled connection to its next user.
"""

import threading

import psycopg2
import pytest

from core.locks import NAMESPACE, LockNotAcquired, lock_key, pipeline_lock
from core.settings import settings
from db.models.pipeline_run import PipelineRun
from pipelines.base import BasePipeline
from pipelines.config import PipelineCategory, PipelineConfig
from pipelines.context import PipelineContext
from schemas.common import ApiStatus


class _CountingPipeline(BasePipeline):
    """Records every execution, and blocks in the middle when asked to."""

    config = PipelineConfig(
        name="test_lock_pipeline",
        display_name="Test Lock Pipeline",
        description="Counts executions",
        target_table="nba.pipeline_runs",
        category=PipelineCategory.POST_GAME,
    )

    executions = 0
    entered = None
    release = None

    def execute(self, ctx: PipelineContext) -> None:
        type(self).executions += 1
        if self.entered is not None:
            self.entered.set()
            self.release.wait(timeout=10)


class _ConcurrentPipeline(_CountingPipeline):
    """Same, but opts out of the lock."""

    config = PipelineConfig(
        name="test_lock_pipeline_concurrent",
        display_name="Test Concurrent Pipeline",
        description="Counts executions",
        target_table="nba.pipeline_runs",
        category=PipelineCategory.POST_GAME,
        allow_concurrent=True,
    )


@pytest.fixture
def rival(integration_db):
    """A second database session, standing in for a second process."""
    conn = psycopg2.connect(settings.database_url)
    conn.autocommit = True
    yield conn
    conn.close()


def _hold(conn, name: str) -> bool:
    with conn.cursor() as cur:
        cur.execute("SELECT pg_try_advisory_lock(%s, %s)", (NAMESPACE, lock_key(name)))
        return cur.fetchone()[0]


def _is_held(conn, name: str) -> bool:
    """Is anyone holding this lock? Asked from a session that does not hold it."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT count(*) FROM pg_locks WHERE locktype = 'advisory' "
            "AND classid = %s AND objid = %s",
            (NAMESPACE, lock_key(name) % 2**32),
        )
        return cur.fetchone()[0] > 0


@pytest.fixture(autouse=True)
def reset_counter():
    _CountingPipeline.executions = 0
    _ConcurrentPipeline.executions = 0
    _CountingPipeline.entered = None
    _CountingPipeline.release = None
    yield


@pytest.mark.integration
class TestTheLockItself:
    def test_a_second_holder_is_refused_immediately(self, rival, integration_db):
        assert _hold(rival, "solo") is True
        with pytest.raises(LockNotAcquired):
            with pipeline_lock("solo"):
                pytest.fail("acquired a lock another session holds")

    def test_release_happens_even_when_the_body_raises(self, rival, integration_db):
        with pytest.raises(ValueError):
            with pipeline_lock("boom"):
                raise ValueError("pipeline exploded")
        # The rival can now take it, so the failed run did not leak the lock
        # into the pooled connection it was using.
        assert _hold(rival, "boom") is True

    def test_different_pipelines_do_not_contend(self, rival, integration_db):
        assert _hold(rival, "other_pipeline") is True
        with pipeline_lock("mine"):
            pass  # different key, so no contention


@pytest.mark.integration
class TestPipelineRunsUnderTheLock:
    def test_a_contended_run_is_skipped_not_failed(self, rival, integration_db):
        """A losing trigger is a no-op, not an error and not an alert."""
        assert _hold(rival, _CountingPipeline.config.name) is True

        import asyncio

        result = asyncio.run(_CountingPipeline().run())

        assert result.status == ApiStatus.SKIPPED
        assert "execution lock" in result.message
        assert _CountingPipeline.executions == 0
        # No audit row: nothing ran, and a `failed` row here would look like a
        # data problem in the dashboard's error streak.
        assert (
            PipelineRun.select()
            .where(PipelineRun.pipeline_name == _CountingPipeline.config.name)
            .count()
            == 0
        )

    def test_two_simultaneous_runs_execute_once(self, integration_db):
        """The race the check-then-act gate cannot win."""
        import asyncio

        _CountingPipeline.entered = threading.Event()
        _CountingPipeline.release = threading.Event()

        results = {}

        def first():
            results["first"] = asyncio.run(_CountingPipeline().run())

        thread = threading.Thread(target=first)
        thread.start()
        assert _CountingPipeline.entered.wait(timeout=10), "first run never started"

        # Second trigger arrives while the first is mid-execute.
        results["second"] = asyncio.run(_CountingPipeline().run())

        _CountingPipeline.release.set()
        thread.join(timeout=10)

        assert _CountingPipeline.executions == 1
        assert results["second"].status == ApiStatus.SKIPPED
        assert results["first"].status == ApiStatus.SUCCESS

    def test_allow_concurrent_opts_out(self, rival, integration_db):
        import asyncio

        assert _hold(rival, _ConcurrentPipeline.config.name) is True
        result = asyncio.run(_ConcurrentPipeline().run())
        assert result.status == ApiStatus.SUCCESS
        assert _ConcurrentPipeline.executions == 1

    def test_the_lock_does_not_ride_the_pooled_connection(self, rival, integration_db):
        """Session locks outlive `db.close()`, which returns to the pool.

        If the unlock were forgotten, the next pipeline to check out that
        connection would inherit the lock and wedge itself.
        """
        import asyncio

        asyncio.run(_CountingPipeline().run())
        assert not _is_held(rival, _CountingPipeline.config.name)

        # And a subsequent run still works, rather than skipping forever.
        result = asyncio.run(_CountingPipeline().run())
        assert result.status == ApiStatus.SUCCESS
        assert _CountingPipeline.executions == 2
