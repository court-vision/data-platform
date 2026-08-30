"""
The write-side pairing guard (docs/PENDING_PROD_CHECKS.md #4).

`live_window_from_watermark` shipped ON without waiting for the opening-night
probe because of this function: whatever order ESPN updates
`latestScoringPeriod` and `totalPoints` in, a snapshot can no longer pair an
advanced watermark with totals that don't cover it.

Three named scenarios below came from review (codex, data-platform#4) and are
pinned explicitly: the unseeded mid-period team, the multi-period gap, and the
category league whose win-counts sit still while its totals move.
"""

import pytest

from pipelines.daily_matchup_scores import (
    SKIP,
    WITHHOLD,
    WRITE,
    watermark_decision,
)


def decide(**kw):
    args = dict(
        stored_period=134, stored_score=1200.0, stored_opponent_score=1100.0,
        stored_categories=None,
        new_period=135, new_source="provider",
        new_score=1250.0, new_opponent_score=1150.0, new_categories=None,
        days_since_stored=0, stored_exists=True,
    )
    args.update(kw)
    return watermark_decision(**args)


def cats(pts, reb, wins=5, losses=4):
    return {
        "scoring_format": "categories",
        "you": {"pts": pts, "reb": reb},
        "opp": {"pts": pts - 40, "reb": reb - 5},
        "wins": wins, "losses": losses, "ties": 0,
    }


@pytest.mark.unit
class TestTheBugItPrevents:
    def test_advanced_period_with_frozen_totals_is_skipped(self):
        """The Δ>0 failure: period says 135, totals still cover through 133."""
        assert decide(new_score=1200.0, new_opponent_score=1100.0) == SKIP

    def test_advanced_period_with_moved_totals_writes(self):
        assert decide() == WRITE

    def test_one_side_moving_is_enough(self):
        assert decide(new_score=1200.0) == WRITE
        assert decide(new_opponent_score=1100.0) == WRITE


@pytest.mark.unit
class TestUnseededTeams:
    """Review catch #1: `stored is None` is not only the day-0 seed — a team
    first processed mid-period lands here with nonzero totals, and during a
    period-before-totals gap an unconditional write would freeze the
    overstated pairing. Only a claim-free zero seed writes; anything else
    stores totals with the watermark withheld."""

    def test_zero_seed_on_day_zero_writes(self):
        assert decide(
            stored_exists=False, new_score=0.0, new_opponent_score=0.0, day_of_matchup=0
        ) == WRITE

    def test_zero_totals_past_day_zero_withhold(self):
        """0-0 on day 1+ is not a verifiable seed: on the first night of a
        non-self-seeded period, the gate fires on the period advance and the
        totals are 0-0 precisely *because* day 1's batch hasn't landed —
        writing the advanced watermark would claim a scored day is covered."""
        assert decide(
            stored_exists=False, new_score=0.0, new_opponent_score=0.0, day_of_matchup=1
        ) == WITHHOLD

    def test_nonzero_first_write_withholds_the_watermark(self):
        assert decide(stored_exists=False) == WITHHOLD

    def test_zero_seed_with_nonzero_category_totals_withholds(self):
        """0-0 win counts with populated totals is not a day-0 seed."""
        assert decide(
            stored_exists=False, new_score=0.0, new_opponent_score=0.0,
            new_categories=cats(320.0, 118.0, wins=0, losses=0),
        ) == WITHHOLD

    def test_withheld_row_becomes_the_reference(self):
        """Next poll: movement from the withheld totals confirms the claim."""
        assert decide(stored_period=None) == WRITE

    def test_withheld_row_with_frozen_totals_keeps_waiting(self):
        assert decide(stored_period=None, new_score=1200.0, new_opponent_score=1100.0) == SKIP

    def test_withheld_row_idles_out_after_two_days(self):
        assert decide(
            stored_period=None, new_score=1200.0, new_opponent_score=1100.0,
            days_since_stored=2,
        ) == WRITE


@pytest.mark.unit
class TestMultiPeriodGaps:
    """Review catch #2: stored 134, ESPN 136, totals moved — movement only
    proves the *oldest* missing batch (day 134) landed, not day 135. Claiming
    136 would silently omit 135 from the overlay. Totals are stored, watermark
    withheld; the next poll's movement confirms the rest."""

    def test_gap_of_two_with_movement_withholds(self):
        assert decide(new_period=136) == WITHHOLD

    def test_gap_of_two_with_frozen_totals_skips(self):
        assert decide(new_period=136, new_score=1200.0, new_opponent_score=1100.0) == SKIP

    def test_gap_of_one_with_movement_writes(self):
        assert decide(new_period=135) == WRITE

    def test_the_full_recovery_sequence(self):
        """Night after an outage: withhold on first movement, write on second."""
        first = decide(new_period=136)
        assert first == WITHHOLD
        # The withheld row (period None, the new totals) is now the reference;
        # the 135-batch lands, totals move again → full watermark confirmed.
        second = decide(
            stored_period=None, stored_score=1250.0, stored_opponent_score=1150.0,
            new_period=136, new_score=1310.0, new_opponent_score=1190.0,
        )
        assert second == WRITE


@pytest.mark.unit
class TestCategoryLeagues:
    """Review catch #3: for category leagues the extractor stores categories
    WON in current_score — a small integer that sits still overnight while
    every per-category total moves. Movement is judged on the totals."""

    def test_totals_moving_with_frozen_win_counts_writes(self):
        assert decide(
            stored_score=5, stored_opponent_score=4, stored_categories=cats(320.0, 118.0),
            new_score=5, new_opponent_score=4, new_categories=cats(354.0, 129.0),
        ) == WRITE

    def test_frozen_totals_skip_even_if_plausible(self):
        assert decide(
            stored_score=5, stored_opponent_score=4, stored_categories=cats(320.0, 118.0),
            new_score=5, new_opponent_score=4, new_categories=cats(320.0, 118.0),
        ) == SKIP

    def test_opponent_side_movement_counts(self):
        stored = cats(320.0, 118.0)
        new = cats(320.0, 118.0)
        new["opp"] = {**new["opp"], "pts": new["opp"]["pts"] + 12}
        assert decide(
            stored_score=5, stored_opponent_score=4, stored_categories=stored,
            new_score=5, new_opponent_score=4, new_categories=new,
        ) == WRITE

    def test_format_flip_between_polls_withholds(self):
        """Points row stored, categories fetched (or vice versa): incomparable."""
        assert decide(new_categories=cats(354.0, 129.0)) == WITHHOLD
        assert decide(
            stored_categories=cats(320.0, 118.0),
            new_categories=None,
        ) == WITHHOLD


@pytest.mark.unit
class TestTheDeliberateOuts:
    def test_same_period_refresh_always_writes(self):
        assert decide(new_period=134, new_score=1200.0, new_opponent_score=1100.0) == WRITE

    def test_period_behind_stored_writes(self):
        """Being ahead of ESPN (backfill) is the gate's problem, not the guard's."""
        assert decide(new_period=133, new_score=1200.0, new_opponent_score=1100.0) == WRITE

    def test_idle_matchup_writes_through_after_two_days(self):
        assert decide(new_score=1200.0, new_opponent_score=1100.0, days_since_stored=2) == WRITE
        assert decide(new_score=1200.0, new_opponent_score=1100.0, days_since_stored=1) == SKIP

    def test_calendar_watermarks_are_exempt(self):
        assert decide(new_source="calendar", new_score=1200.0, new_opponent_score=1100.0) == WRITE

    def test_missing_period_is_exempt(self):
        assert decide(new_period=None, new_score=1200.0, new_opponent_score=1100.0) == WRITE


@pytest.mark.unit
class TestValueComparison:
    def test_decimal_vs_float_compares_by_value(self):
        """stored_* comes from a Peewee DecimalField; new_* from ESPN JSON."""
        from decimal import Decimal
        assert decide(
            stored_score=Decimal("1200.00"), stored_opponent_score=Decimal("1100.00"),
            new_score=1200.0, new_opponent_score=1100.0,
        ) == SKIP

    def test_jsonb_float_category_totals_compare_by_value(self):
        assert decide(
            stored_score=5, stored_opponent_score=4,
            stored_categories={"you": {"pts": 320.0}, "opp": {"pts": 280.0}, "wins": 5, "losses": 4},
            new_categories={"you": {"pts": 320}, "opp": {"pts": 280}, "wins": 5, "losses": 4},
            new_score=5, new_opponent_score=4,
        ) == SKIP


@pytest.mark.unit
class TestTheIdleDaySequence:
    """A genuinely flat day (neither roster's starters played) blocks the
    watermark but never the number: the stored totals legitimately include the
    idle day, so the displayed standing score stays correct while the *claim*
    waits for evidence. The cost is liveness — one to two evenings where the
    overlay day lags and the score doesn't tick during games — bounded by the
    idle-out and always undercount-shaped.

    Note the asymmetry that keeps this rare: movement on EITHER side unblocks,
    so "I forgot to set my lineup" doesn't trigger it (the opponent moved),
    and in category leagues any single stat by any started player unblocks.
    """

    def test_night_one_flat_day_skips_but_the_number_was_already_right(self):
        assert decide(new_period=135, new_score=1200.0, new_opponent_score=1100.0) == SKIP

    def test_night_two_movement_at_gap_two_withholds(self):
        """The guard cannot distinguish 'day B was idle-zero, one movement
        covers both days' from 'day B's batch is still pending' — so it
        stores the fresh totals and defers the claim."""
        assert decide(new_period=136) == WITHHOLD

    def test_frozen_after_the_withhold_keeps_waiting(self):
        assert decide(
            stored_period=None, stored_score=1250.0, stored_opponent_score=1150.0,
            new_period=136, new_score=1250.0, new_opponent_score=1150.0,
        ) == SKIP

    def test_next_movement_or_the_idle_out_recovers(self):
        by_movement = decide(
            stored_period=None, stored_score=1250.0, stored_opponent_score=1150.0,
            new_period=137, new_score=1290.0, new_opponent_score=1180.0,
        )
        by_timeout = decide(
            stored_period=None, stored_score=1250.0, stored_opponent_score=1150.0,
            new_period=137, new_score=1250.0, new_opponent_score=1150.0,
            days_since_stored=2,
        )
        assert by_movement == by_timeout == WRITE
