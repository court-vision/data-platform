"""
The write-side pairing guard (docs/PENDING_PROD_CHECKS.md #4).

`live_window_from_watermark` shipped ON without waiting for the opening-night
probe because of this function: whatever order ESPN updates
`latestScoringPeriod` and `totalPoints` in, a snapshot can no longer pair an
advanced watermark with totals that don't cover it. The probe is demoted from
gate to confirmation.
"""

import pytest

from pipelines.daily_matchup_scores import watermark_outruns_totals


def outruns(**kw):
    args = dict(
        stored_period=134, stored_score=1200.0, stored_opponent_score=1100.0,
        new_period=135, new_source="provider",
        new_score=1250.0, new_opponent_score=1150.0,
        days_since_stored=0,
    )
    args.update(kw)
    return watermark_outruns_totals(**args)


@pytest.mark.unit
class TestTheBugItPrevents:
    def test_advanced_period_with_frozen_totals_is_blocked(self):
        """The Δ>0 failure: period says 135, totals still cover through 133."""
        assert outruns(new_score=1200.0, new_opponent_score=1100.0) is True

    def test_advanced_period_with_moved_totals_writes(self):
        """Totals moving is the confirmation the period claims."""
        assert outruns() is False

    def test_one_side_moving_is_enough(self):
        assert outruns(new_score=1200.0) is False
        assert outruns(new_opponent_score=1100.0) is False


@pytest.mark.unit
class TestTheDeliberateOuts:
    def test_seeding_a_new_period_always_writes(self):
        """A 0-0 day-0 row claims nothing — this is the 20% of periods that
        don't self-seed (PENDING_PROD_CHECKS #3)."""
        assert outruns(stored_period=None, new_score=0.0, new_opponent_score=0.0) is False

    def test_same_period_refresh_always_writes(self):
        assert outruns(new_period=134, new_score=1200.0, new_opponent_score=1100.0) is False

    def test_period_behind_stored_writes(self):
        """Being ahead of ESPN (backfill) is the gate's problem, not the guard's."""
        assert outruns(new_period=133, new_score=1200.0, new_opponent_score=1100.0) is False

    def test_idle_matchup_writes_through_after_two_days(self):
        """Both rosters scoreless for two days: totals legitimately frozen."""
        assert outruns(new_score=1200.0, new_opponent_score=1100.0, days_since_stored=2) is False
        assert outruns(new_score=1200.0, new_opponent_score=1100.0, days_since_stored=1) is True

    def test_calendar_watermarks_are_exempt(self):
        """Yahoo's watermark is derived from our calendar, not ESPN's claim."""
        assert outruns(new_source="calendar", new_score=1200.0, new_opponent_score=1100.0) is False

    def test_missing_period_is_exempt(self):
        assert outruns(new_period=None, new_score=1200.0, new_opponent_score=1100.0) is False


@pytest.mark.unit
class TestDecimalTolerance:
    def test_decimal_vs_float_compares_by_value(self):
        """stored_* comes from a Peewee DecimalField; new_* from ESPN JSON."""
        from decimal import Decimal
        assert outruns(
            stored_score=Decimal("1200.00"), stored_opponent_score=Decimal("1100.00"),
            new_score=1200.0, new_opponent_score=1100.0,
        ) is True
