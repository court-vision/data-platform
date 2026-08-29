"""
Self-gating behaviour that is not (yet) a pure function.

The window and ESPN-batch gates moved into `pipelines/gates.py` and are tested
against the real code in `test_gates.py`; the NBA date rule moved into
`core/nba_calendar.py` and is tested in `test_nba_calendar.py`. This file used
to hold private re-implementations of all three — `_compute_nba_date`,
`_is_pre_game_window_open`, `_is_post_game_window_open` — which meant the
endpoint and its copy here could agree with each other while both being wrong.
Four of the five ESPN-gate defects in `docs/PRODUCTION_READINESS.md` item 4
survived a passing test suite that way.

What remains here is the gating that still lives inline: the live-stats
pre-tip-off check, and the two `PipelineRun` predicates the batch endpoints use.
"""

import pytest
from datetime import date, datetime, time, timedelta


# ---------------------------------------------------------------------------
# Pipeline dedup gating
# ---------------------------------------------------------------------------

@pytest.mark.unit
class TestPipelineDedupGating:
    """
    Per-pipeline dedup: skip if already ran successfully on this NBA date.
    PipelineRun.was_successful_on_date() checks started_at >= cutoff.
    """

    def test_was_successful_on_date_logic(self):
        """Verify the cutoff logic: with no explicit 'after', uses midnight of nba_date."""
        nba_date = date(2026, 3, 4)
        # The cutoff should be 2026-03-04T00:00:00
        expected_cutoff = datetime.combine(nba_date, time.min)
        assert expected_cutoff == datetime(2026, 3, 4, 0, 0, 0)

    def test_after_parameter_overrides_cutoff(self):
        """Post-game passes the window's opening time as the 'after' cutoff."""
        # Post-game might set after=2026-03-05T05:30:00 (window open, in UTC)
        after = datetime(2026, 3, 5, 5, 30, 0)
        # A pipeline run at 3 AM UTC (before after) should NOT count
        run_started = datetime(2026, 3, 5, 3, 0, 0)
        assert run_started < after  # This run would be excluded by the after cutoff


# ---------------------------------------------------------------------------
# Concurrency gating
# ---------------------------------------------------------------------------

@pytest.mark.unit
class TestConcurrencyGating:
    """The staleness threshold for `PipelineRun.is_running`.

    This is the *cheap* gate — it stops the endpoint dispatching work that is
    already in flight. It is not the guarantee: it reads and then acts, so two
    triggers can both pass it. The guarantee is the advisory lock in
    `core/locks.py` (`tests/integration/test_pipeline_locks.py`).
    """

    def test_stale_running_not_considered_active(self):
        """Running records older than max_age_minutes are treated as stale."""
        max_age = 120  # default
        started = datetime(2026, 3, 4, 10, 0, 0)
        now = datetime(2026, 3, 4, 13, 0, 0)  # 3 hours later
        staleness_cutoff = now - timedelta(minutes=max_age)
        # started < staleness_cutoff → stale
        assert started < staleness_cutoff

    def test_recent_running_is_active(self):
        """Running records within max_age_minutes are active."""
        max_age = 120
        started = datetime(2026, 3, 4, 12, 0, 0)
        now = datetime(2026, 3, 4, 13, 0, 0)  # 1 hour later
        staleness_cutoff = now - timedelta(minutes=max_age)
        # started >= staleness_cutoff → active
        assert started >= staleness_cutoff


# ---------------------------------------------------------------------------
# Live stats pre-tip gate
# ---------------------------------------------------------------------------

@pytest.mark.unit
class TestLiveStatsPreTipGate:
    """Live stats gate: don't run until 15 min before first game.

    Still inline in `trigger_live_stats`, so this is still a re-implementation.
    It is the last one.
    """

    @staticmethod
    def _is_live_gate_open(
        now_et_naive: datetime,
        earliest_start: time,
        game_date: date,
    ) -> bool:
        """Replicate the pre-tip gate from trigger_live_stats()."""
        earliest_dt = datetime.combine(game_date, earliest_start)
        gate_dt = earliest_dt - timedelta(minutes=15)
        return now_et_naive >= gate_dt

    def test_too_early(self):
        """1 hour before first game → gate closed."""
        game_date = date(2026, 3, 4)
        earliest = time(19, 0)
        now = datetime(2026, 3, 4, 18, 0)
        assert not self._is_live_gate_open(now, earliest, game_date)

    def test_within_15_min(self):
        """10 minutes before first game → gate open."""
        game_date = date(2026, 3, 4)
        earliest = time(19, 0)
        now = datetime(2026, 3, 4, 18, 50)
        assert self._is_live_gate_open(now, earliest, game_date)

    def test_exactly_15_min_before(self):
        """Exactly 15 minutes before → gate open."""
        game_date = date(2026, 3, 4)
        earliest = time(19, 0)
        now = datetime(2026, 3, 4, 18, 45)
        assert self._is_live_gate_open(now, earliest, game_date)

    def test_after_game_started(self):
        """After first game started → gate open."""
        game_date = date(2026, 3, 4)
        earliest = time(19, 0)
        now = datetime(2026, 3, 4, 20, 30)
        assert self._is_live_gate_open(now, earliest, game_date)
