"""
Batch gate decisions (`pipelines/gates.py`).

Every test below names the defect it pins. The ESPN gate had five, and four of
them were invisible in the old shape of the code because the decision was
inline in the endpoint and the tests re-implemented it rather than calling it.

Times are constructed explicitly rather than frozen: these functions take their
clock as an argument, which is the point of extracting them.
"""

from datetime import date, datetime, time

import pytest

from pipelines.gates import (
    CENTRAL,
    espn_batch_gate,
    espn_fallback_at,
    post_game_window,
    pre_game_window,
)

NBA_DATE = date(2026, 3, 4)


def cst(year, month, day, hour, minute=0) -> datetime:
    return CENTRAL.localize(datetime(year, month, day, hour, minute))


def gate(**overrides):
    """The gate with a healthy night's defaults; override one thing per test."""
    kwargs = dict(
        now_cst=cst(2026, 3, 5, 1, 0),      # 01:00 CST, before the fallback
        nba_date=NBA_DATE,
        league_present=True,
        espn_period=135,
        baselines={1: 134},                  # one team, one day behind
        succeeded_tonight=False,
        attempts_tonight=0,
        max_attempts=3,
    )
    kwargs.update(overrides)
    return espn_batch_gate(**kwargs)


# ---------------------------------------------------------------------------
# Windows
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestPreGameWindow:
    def test_closed_before_the_window_opens(self):
        w = pre_game_window(datetime(2026, 3, 4, 16, 0), time(19, 30), NBA_DATE, 150)
        assert not w.open and w.opens_at == datetime(2026, 3, 4, 17, 0)

    def test_open_at_the_exact_boundary(self):
        assert pre_game_window(
            datetime(2026, 3, 4, 17, 0), time(19, 30), NBA_DATE, 150
        ).open

    def test_per_pipeline_override_is_tighter(self):
        now = datetime(2026, 3, 4, 17, 20)  # 130 min before a 19:30 tip
        assert pre_game_window(now, time(19, 30), NBA_DATE, 150).open
        assert not pre_game_window(now, time(19, 30), NBA_DATE, 120).open


@pytest.mark.unit
class TestPostGameWindow:
    def test_before_during_and_after(self):
        args = (time(22, 0), NBA_DATE, 150, 210)  # opens 00:30, closes 04:00
        assert post_game_window(datetime(2026, 3, 4, 23, 0), *args).state == "before"
        assert post_game_window(datetime(2026, 3, 5, 1, 0), *args).state == "open"
        assert post_game_window(datetime(2026, 3, 5, 9, 0), *args).state == "closed"

    def test_boundaries_are_inclusive(self):
        args = (time(22, 0), NBA_DATE, 150, 210)
        assert post_game_window(datetime(2026, 3, 5, 0, 30), *args).open
        assert post_game_window(datetime(2026, 3, 5, 4, 0), *args).open
        assert post_game_window(datetime(2026, 3, 5, 4, 1), *args).closed

    def test_closed_is_distinct_from_before(self):
        """The completeness sweep hangs off this distinction.

        The old code compared `estimated_end <= now <= window_end` and treated
        both failures the same, so "the night is over" was indistinguishable
        from "the night has not started".
        """
        args = (time(22, 0), NBA_DATE, 150, 210)
        assert post_game_window(datetime(2026, 3, 4, 20, 0), *args).state == "before"
        assert post_game_window(datetime(2026, 3, 5, 5, 0), *args).state == "closed"


# ---------------------------------------------------------------------------
# The ESPN batch gate
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestFallbackIsAnchoredToTheDate:
    """Defect 1: `hour > 2` is true at 21:00 as surely as at 03:00.

    The post-game window opens around 20:00 CST, so the "2:30 AM fallback" was
    in force for the whole first half of every night and `latestScoringPeriod`
    was only ever consulted between 00:00 and 02:29 CST.
    """

    def test_fallback_is_the_morning_after_the_nba_date(self):
        assert espn_fallback_at(NBA_DATE) == cst(2026, 3, 5, 2, 30)

    def test_9pm_is_not_past_the_fallback(self):
        d = gate(now_cst=cst(2026, 3, 4, 21, 0), espn_period=134, baselines={1: 134})
        assert not d.run
        assert d.reason == "period_unchanged"
        assert d.detail["past_fallback"] is False

    def test_3am_is_past_the_fallback(self):
        d = gate(now_cst=cst(2026, 3, 5, 3, 0), espn_period=134, baselines={1: 134})
        assert d.run and d.reason == "time_fallback_no_advance"


@pytest.mark.unit
class TestPerTeamBaselines:
    """Defect 2: one team's newest row was the oracle for every user."""

    def test_a_team_left_behind_by_a_partial_run_is_retried(self):
        # Team 1 was written, team 2 was not. The old gate read team 1 and
        # concluded the night was done.
        d = gate(baselines={1: 135, 2: 134})
        assert d.run and d.reason == "period_advanced"
        assert d.detail["teams_behind"] == 1

    def test_all_teams_current_means_no_run(self):
        d = gate(baselines={1: 135, 2: 135})
        assert not d.run and d.reason == "period_unchanged"

    def test_no_teams_configured_at_all(self):
        d = gate(league_present=False, baselines={})
        assert d.run and d.reason == "no_espn_league"


@pytest.mark.unit
class TestComparisonIsGreaterThan:
    """Defect 3: `!=` ran the pipeline whenever the numbers merely differed."""

    def test_espn_behind_us_does_not_trigger_a_run(self):
        # A backfill (or an ESPN rollback) leaves us ahead of ESPN. `!=` read
        # that as "advanced" and re-ran, overwriting good rows with old totals.
        d = gate(espn_period=134, baselines={1: 135})
        assert not d.run and d.reason == "period_unchanged"

    def test_espn_ahead_triggers_a_run(self):
        assert gate(espn_period=136, baselines={1: 135}).run


@pytest.mark.unit
class TestMatchupPeriodSeeding:
    """Defect 4: the gate ignored `matchup_period`, so a new one never seeded.

    Baselines are read within ESPN's *current* matchup period. A team with no
    row in that period has watermark None — the day-0 snapshot 13 of 66 periods
    never got (PENDING_PROD_CHECKS #3).
    """

    def test_unseeded_period_runs_even_when_the_period_did_not_advance(self):
        d = gate(espn_period=135, baselines={1: None})
        assert d.run and d.reason == "period_advanced"
        assert d.detail["teams_unseeded"] == 1

    def test_one_unseeded_team_among_current_ones(self):
        d = gate(espn_period=135, baselines={1: 135, 2: None})
        assert d.run
        assert d.detail["teams_unseeded"] == 1 and d.detail["teams_behind"] == 1


@pytest.mark.unit
class TestBoundedRetries:
    """Defect 5: every ESPN error returned "run", all night, every 15 minutes."""

    def test_espn_unavailable_waits_for_the_fallback(self):
        d = gate(espn_period=None)
        assert not d.run and d.reason == "espn_unavailable_waiting"

    def test_espn_unavailable_runs_once_past_the_fallback(self):
        d = gate(espn_period=None, now_cst=cst(2026, 3, 5, 3, 0))
        assert d.run and d.reason == "time_fallback_espn_unavailable"

    def test_retry_budget_is_finite(self):
        d = gate(espn_period=None, now_cst=cst(2026, 3, 5, 3, 0), attempts_tonight=3)
        assert not d.run and d.reason == "attempts_exhausted"

    def test_budget_also_bounds_a_pipeline_that_keeps_failing(self):
        # ESPN advanced, so `behind` stays true however many times the pipeline
        # fails — without a budget that is an unbounded retry loop too.
        d = gate(attempts_tonight=3)
        assert not d.run and d.reason == "attempts_exhausted"

    def test_attempts_do_not_block_a_healthy_first_run(self):
        assert gate(attempts_tonight=2).run


@pytest.mark.unit
class TestNoRerunsAfterASuccessfulNight:
    """The fallback used to short-circuit before the "already current" check.

    So from 02:30 CST the pipeline re-ran on every poll until the window
    closed, each run overwriting the snapshot it had just written — despite a
    comment claiming the period comparison prevented exactly that.
    """

    def test_success_plus_current_watermarks_stops_the_reruns(self):
        d = gate(
            now_cst=cst(2026, 3, 5, 3, 0),
            espn_period=135,
            baselines={1: 135},
            succeeded_tonight=True,
            attempts_tonight=1,
        )
        assert not d.run and d.reason == "period_unchanged_already_ran"

    def test_but_a_later_espn_advance_still_re_runs(self):
        # Success is not a reason to ignore ESPN moving on: that is what makes
        # the gate stronger than a plain "already ran today" dedup.
        d = gate(
            now_cst=cst(2026, 3, 5, 3, 0),
            espn_period=136,
            baselines={1: 135},
            succeeded_tonight=True,
            attempts_tonight=1,
        )
        assert d.run and d.reason == "period_advanced"

    def test_espn_unavailable_after_a_success_stays_quiet(self):
        d = gate(
            now_cst=cst(2026, 3, 5, 3, 0),
            espn_period=None,
            succeeded_tonight=True,
            attempts_tonight=1,
        )
        assert not d.run and d.reason == "espn_unavailable_already_ran"

    def test_no_league_configured_still_runs_only_once(self):
        d = gate(league_present=False, baselines={}, succeeded_tonight=True, attempts_tonight=1)
        assert not d.run and d.reason == "no_espn_league_already_ran"


@pytest.mark.unit
class TestReasonsAreStable:
    """Reasons are stored in nba.pipeline_batches and read back by humans."""

    def test_every_decision_carries_a_reason_and_the_numbers(self):
        for d in [
            gate(),
            gate(espn_period=None),
            gate(league_present=False, baselines={}),
            gate(baselines={1: 135}),
            gate(attempts_tonight=9),
        ]:
            assert d.reason and d.reason.islower()
            assert "attempts_tonight" in d.detail
            assert "fallback_at" in d.detail
