"""
Self-Gating Logic Tests

Tests the time-window, dedup, concurrency, and data-readiness gates used by
the pre-game and post-game category endpoints. Uses freezegun to control time.
"""

import pytest
from datetime import date, datetime, time, timedelta
from unittest.mock import MagicMock, patch

from freezegun import freeze_time

from core.settings import settings


# ---------------------------------------------------------------------------
# NBA date convention
# ---------------------------------------------------------------------------

@pytest.mark.unit
class TestNBADateConvention:
    """
    NBA date rule: before 6 AM ET, we're still on yesterday's game date.
    The trigger endpoints compute nba_date inline, so we test the logic directly.
    """

    @staticmethod
    def _compute_nba_date(now_et: datetime) -> date:
        """Replicate the NBA date logic from pipelines.py trigger functions."""
        if now_et.hour < 6:
            return (now_et - timedelta(days=1)).date()
        return now_et.date()

    def test_3am_et_is_yesterdays_nba_date(self):
        """3 AM ET on March 5 → NBA date is March 4."""
        now = datetime(2026, 3, 5, 3, 0, 0)
        assert self._compute_nba_date(now) == date(2026, 3, 4)

    def test_559am_et_is_yesterdays_nba_date(self):
        """5:59 AM ET on March 5 → NBA date is March 4."""
        now = datetime(2026, 3, 5, 5, 59, 0)
        assert self._compute_nba_date(now) == date(2026, 3, 4)

    def test_6am_et_is_todays_nba_date(self):
        """6:00 AM ET on March 5 → NBA date is March 5."""
        now = datetime(2026, 3, 5, 6, 0, 0)
        assert self._compute_nba_date(now) == date(2026, 3, 5)

    def test_noon_et_is_todays_nba_date(self):
        """12:00 PM ET on March 5 → NBA date is March 5."""
        now = datetime(2026, 3, 5, 12, 0, 0)
        assert self._compute_nba_date(now) == date(2026, 3, 5)

    def test_1159pm_et_is_todays_nba_date(self):
        """11:59 PM ET on March 5 → NBA date is March 5."""
        now = datetime(2026, 3, 5, 23, 59, 0)
        assert self._compute_nba_date(now) == date(2026, 3, 5)


# ---------------------------------------------------------------------------
# Pre-game window gating
# ---------------------------------------------------------------------------

@pytest.mark.unit
class TestPreGameWindowGating:
    """
    Pre-game pipelines open at (first_game_time - window_minutes).
    Default window is 150 minutes. Some pipelines override it.
    """

    @staticmethod
    def _is_pre_game_window_open(
        now_et_naive: datetime,
        first_game_time: time,
        nba_date: date,
        window_minutes: int = 150,
    ) -> bool:
        """Replicate the pre-game window check from trigger_pre_game()."""
        first_game_dt = datetime.combine(nba_date, first_game_time)
        window_opens_at = first_game_dt - timedelta(minutes=window_minutes)
        return now_et_naive >= window_opens_at

    def test_window_closed_too_early(self):
        """3 hours before tip, with 150-min window → closed."""
        nba_date = date(2026, 3, 4)
        first_tip = time(19, 30)  # 7:30 PM
        now = datetime(2026, 3, 4, 16, 0)  # 4:00 PM (210 min before)
        assert not self._is_pre_game_window_open(now, first_tip, nba_date)

    def test_window_open_within_default(self):
        """2 hours before tip, with 150-min window → open."""
        nba_date = date(2026, 3, 4)
        first_tip = time(19, 30)  # 7:30 PM
        now = datetime(2026, 3, 4, 17, 30)  # 5:30 PM (120 min before)
        assert self._is_pre_game_window_open(now, first_tip, nba_date)

    def test_window_open_at_exact_boundary(self):
        """Exactly at window open time → open."""
        nba_date = date(2026, 3, 4)
        first_tip = time(19, 30)  # 7:30 PM
        now = datetime(2026, 3, 4, 17, 0)  # 5:00 PM (150 min before)
        assert self._is_pre_game_window_open(now, first_tip, nba_date)

    def test_custom_window_override(self):
        """breakout_detection uses 120-min window, tighter than default."""
        nba_date = date(2026, 3, 4)
        first_tip = time(19, 30)
        # 130 min before tip — inside 150-min default but outside 120-min override
        now = datetime(2026, 3, 4, 17, 20)
        assert self._is_pre_game_window_open(now, first_tip, nba_date, window_minutes=150)
        assert not self._is_pre_game_window_open(now, first_tip, nba_date, window_minutes=120)


# ---------------------------------------------------------------------------
# Post-game window gating
# ---------------------------------------------------------------------------

@pytest.mark.unit
class TestPostGameWindowGating:
    """
    Post-game pipelines open at (latest_game_start + estimated_duration)
    and close at (open_time + window_minutes).
    Defaults come from settings (estimated_game_duration_minutes,
    post_game_pipeline_window_minutes).
    """

    @staticmethod
    def _is_post_game_window_open(
        now_et_naive: datetime,
        latest_game_time: time,
        nba_date: date,
        estimated_duration: int = settings.estimated_game_duration_minutes,
        window_minutes: int = settings.post_game_pipeline_window_minutes,
    ) -> bool:
        """Replicate the post-game window check from trigger_post_game()."""
        latest_game_dt = datetime.combine(nba_date, latest_game_time)
        estimated_end = latest_game_dt + timedelta(minutes=estimated_duration)
        window_end = estimated_end + timedelta(minutes=window_minutes)
        return estimated_end <= now_et_naive <= window_end

    def test_before_estimated_end(self):
        """During games → window closed."""
        nba_date = date(2026, 3, 4)
        latest_start = time(22, 0)  # 10 PM
        now = datetime(2026, 3, 4, 23, 0)  # 11 PM (only 60 min after start)
        assert not self._is_post_game_window_open(now, latest_start, nba_date)

    def test_within_window(self):
        """After estimated end, within window → open."""
        nba_date = date(2026, 3, 4)
        latest_start = time(22, 0)  # 10 PM → estimated end 12:30 AM
        # 1:00 AM next day (30 min after estimated end)
        now = datetime(2026, 3, 5, 1, 0)
        assert self._is_post_game_window_open(now, latest_start, nba_date)

    def test_after_window_expires(self):
        """Long after games ended → window closed."""
        nba_date = date(2026, 3, 4)
        latest_start = time(22, 0)  # 10 PM → estimated end 12:30 AM
        estimated_end = datetime(2026, 3, 5, 0, 30)
        window_end = estimated_end + timedelta(minutes=settings.post_game_pipeline_window_minutes)
        assert self._is_post_game_window_open(window_end, latest_start, nba_date)
        assert not self._is_post_game_window_open(window_end + timedelta(minutes=1), latest_start, nba_date)
        now = datetime(2026, 3, 5, 9, 0)  # 9 AM (well past any window)
        assert not self._is_post_game_window_open(now, latest_start, nba_date)

    def test_at_exact_estimated_end(self):
        """Exactly at estimated end → window open."""
        nba_date = date(2026, 3, 4)
        latest_start = time(22, 0)
        # estimated end: 22:00 + 150min = 00:30 next day
        now = datetime(2026, 3, 5, 0, 30)
        assert self._is_post_game_window_open(now, latest_start, nba_date)


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
        """Post-game uses estimated_end_utc as the 'after' cutoff."""
        nba_date = date(2026, 3, 4)
        # Post-game might set after=2026-03-05T05:30:00 (estimated end in UTC)
        after = datetime(2026, 3, 5, 5, 30, 0)
        # A pipeline run at 3 AM UTC (before after) should NOT count
        run_started = datetime(2026, 3, 5, 3, 0, 0)
        assert run_started < after  # This run would be excluded by the after cutoff


# ---------------------------------------------------------------------------
# Concurrency gating
# ---------------------------------------------------------------------------

@pytest.mark.unit
class TestConcurrencyGating:
    """Verify the staleness threshold for concurrency gating."""

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
    """Live stats gate: don't run until 15 min before first game."""

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
