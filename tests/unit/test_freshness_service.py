"""
Table freshness: the due dates, the rule, the clock, and the registry-to-model
resolution.

`due_dates` and `judge` are pure functions of dates, so every verdict is tested
here without a database. `targets` is what ties a pipeline's `target_table` to a model; a
config naming a table no model declares fails here instead of on the page.
"""

from datetime import date, datetime, time, timezone

import pytest

from pipelines import PIPELINE_REGISTRY
from pipelines.config import PipelineCategory
from services import freshness_service as fs


def _target(
    category=PipelineCategory.POST_GAME,
    pipelines=("player_game_stats",),
    date_column="game_date",
    write_column="updated_at",
) -> fs.TableTarget:
    return fs.TableTarget(
        table="nba.t", pipelines=pipelines, category=category, model=None,
        date_column=date_column, write_column=write_column,
    )


# 7:00 AM ET on Thu Mar 5, 2026: last night (Mar 4) is settled, today has games.
NOW = datetime(2026, 3, 5, 7, 0)
REGULAR = fs.Clock(now_et=NOW, today=date(2026, 3, 5), settled_through=date(2026, 3, 4), phase="regular")
WRITTEN = datetime(2026, 3, 5, 8, 12)
MAR_3, MAR_4, MAR_5 = date(2026, 3, 3), date(2026, 3, 4), date(2026, 3, 5)

# The 2025-26 regular season, as the rule sees it.
OPENING, LAST_NIGHT = date(2025, 10, 21), date(2026, 4, 12)
in_regular = lambda day: OPENING <= day <= LAST_NIGHT  # noqa: E731

# Yesterday's tip and today's, as the pre-game rule needs them.
TIPS = {MAR_3: time(19, 0), MAR_4: time(19, 30), MAR_5: time(19, 0)}

# Mar 4 settled, and Mar 5's first tip has not happened by 7 AM.
DUE = fs.Due(post_game=MAR_4, pre_game=MAR_4)
NOTHING_DUE = fs.Due(post_game=None, pre_game=None)


def _judge(target, latest_date, due=DUE, written=WRITTEN):
    return fs.judge(target, latest_date=latest_date, latest_written_at=written, due=due)


def _clock(now_et: datetime) -> fs.Clock:
    return fs.clock(EASTERN.localize(now_et), season="2025-26")


EASTERN = fs.EASTERN


@pytest.mark.unit
class TestDue:
    """The due dates come off the schedule, never off what the batch wrote."""

    def test_post_game_is_the_last_settled_game_day_whatever_its_status(self):
        # No `final` flag is consulted: a batch-wide failure that never flips
        # last night's games to final must not move the mark.
        due = fs.due_dates(REGULAR, TIPS, in_regular)
        assert due.post_game == MAR_4

    def test_today_is_not_post_game_due_until_tomorrow_morning(self):
        assert fs.due_dates(REGULAR, TIPS, in_regular).post_game == MAR_4

    def test_pre_game_is_yesterday_before_todays_first_tip_and_today_after_it(self):
        assert fs.due_dates(REGULAR, TIPS, in_regular).pre_game == MAR_4
        after_tip = _clock(datetime(2026, 3, 5, 19, 5))
        assert fs.due_dates(after_tip, TIPS, in_regular).pre_game == MAR_5
        assert fs.due_dates(after_tip, TIPS, in_regular).post_game == MAR_4

    def test_a_day_without_known_start_times_is_not_due_until_it_is_over(self):
        evening = _clock(datetime(2026, 3, 5, 22, 0))
        assert fs.due_dates(evening, {**TIPS, MAR_5: None}, in_regular).pre_game == MAR_4
        next_morning = _clock(datetime(2026, 3, 6, 7, 0))
        assert fs.due_dates(next_morning, {**TIPS, MAR_5: None}, in_regular).pre_game == MAR_5

    def test_an_off_day_holds_pre_game_to_the_last_game_day(self):
        no_game_today = {MAR_3: time(19, 0), MAR_4: time(19, 30)}
        after_seven = _clock(datetime(2026, 3, 5, 20, 0))
        assert fs.due_dates(after_seven, no_game_today, in_regular).pre_game == MAR_4

    def test_preseason_game_days_are_never_due(self):
        preseason = _clock(datetime(2025, 10, 10, 12, 0))
        tips = {date(2025, 10, 3): time(19, 0), date(2025, 10, 9): time(19, 0)}
        assert fs.due_dates(preseason, tips, in_regular) == NOTHING_DUE

    def test_nothing_is_due_before_the_first_night_settles(self):
        opening_evening = _clock(datetime(2025, 10, 21, 23, 0))
        tips = {date(2025, 10, 9): time(19, 0), OPENING: time(19, 30)}
        due = fs.due_dates(opening_evening, tips, in_regular)
        assert due.post_game is None
        assert due.pre_game == OPENING  # its tip has passed

    def test_the_seasons_last_night_is_judged_the_morning_after_it(self):
        # Today is already the offseason, but last night was regular season.
        morning_after = _clock(datetime(2026, 4, 13, 7, 0))
        assert morning_after.phase == "offseason"
        due = fs.due_dates(morning_after, {LAST_NIGHT: time(15, 30)}, in_regular)
        assert due == fs.Due(post_game=LAST_NIGHT, pre_game=LAST_NIGHT)

    def test_and_stays_judged_through_it_all_summer(self):
        july = _clock(datetime(2026, 7, 1, 12, 0))
        assert fs.due_dates(july, {LAST_NIGHT: time(15, 30)}, in_regular).post_game == LAST_NIGHT

    def test_an_empty_calendar_owes_nothing(self):
        assert fs.due_dates(REGULAR, {}, in_regular) == NOTHING_DUE


@pytest.mark.unit
class TestJudge:
    def test_an_empty_nightly_table_is_empty_only_while_something_is_due(self):
        assert _judge(_target(), None, written=None) == ("empty", None)
        assert _judge(_target(), None, written=None, due=NOTHING_DUE) == ("idle", None)
        assert _judge(_target(PipelineCategory.SCHEDULED), None, written=None) == ("unjudged", None)

    def test_fresh_when_it_runs_through_the_due_date(self):
        assert _judge(_target(), MAR_4) == ("fresh", MAR_4)

    def test_a_table_that_runs_past_it_is_fresh_too(self):
        # nba.games holds the rest of the schedule.
        assert _judge(_target(), date(2026, 4, 12)) == ("fresh", MAR_4)

    def test_stale_when_behind_the_due_date(self):
        assert _judge(_target(), MAR_3) == ("stale", MAR_4)

    def test_a_business_date_that_is_all_null_is_stale(self):
        assert _judge(_target(), None) == ("stale", MAR_4)

    def test_pre_game_tables_are_held_to_their_own_due_date(self):
        injuries = _target(PipelineCategory.PRE_GAME, ("espn_injury_status",), "report_date", "created_at")
        assert _judge(injuries, MAR_4) == ("fresh", MAR_4)
        # After today's first tip, today's report is due: yesterday's is stale.
        after_tip = fs.Due(post_game=MAR_4, pre_game=MAR_5)
        assert _judge(injuries, MAR_5, due=after_tip) == ("fresh", MAR_5)
        assert _judge(injuries, MAR_4, due=after_tip) == ("stale", MAR_5)
        # While a post-game table is still held to last night.
        assert _judge(_target(), MAR_4, due=after_tip) == ("fresh", MAR_4)

    def test_nothing_is_due_before_the_first_settled_game(self):
        assert _judge(_target(), date(2025, 4, 13), due=NOTHING_DUE) == ("idle", None)
        # Opening night after tip: pre-game is due, post-game is not.
        opening = fs.Due(post_game=None, pre_game=OPENING)
        assert _judge(_target(), date(2025, 4, 13), due=opening) == ("idle", None)
        injuries = _target(PipelineCategory.PRE_GAME, ("espn_injury_status",), "report_date", "created_at")
        assert _judge(injuries, date(2025, 4, 13), due=opening) == ("stale", OPENING)

    @pytest.mark.parametrize("category", [PipelineCategory.SCHEDULED, PipelineCategory.LIVE])
    def test_scheduled_and_live_tables_have_no_nightly_cadence(self, category):
        assert _judge(_target(category), date(2026, 3, 1)) == ("unjudged", None)

    @pytest.mark.parametrize("pipeline,column", [
        ("lineup_alerts", "notification_date"),
        ("breakout_detection", "as_of_date"),
    ])
    def test_a_conditional_writer_is_not_judged_by_its_silence(self, pipeline, column):
        # A quiet night writes nothing; the table's age says nothing about health.
        quiet = _target(PipelineCategory.PRE_GAME, (pipeline,), column, "created_at")
        assert _judge(quiet, date(2026, 2, 1)) == ("unjudged", None)

    def test_a_table_without_a_business_date_is_not_judged(self):
        assert _judge(_target(date_column=None), None) == ("unjudged", None)


@pytest.mark.unit
class TestClock:
    """`settled_through` is the last game date whose 6 AM ET deadline has passed."""

    def test_before_the_morning_deadline_last_night_is_not_settled(self):
        # 4:00 AM ET on Mar 5: the post-game batch may still be running.
        clk = fs.clock(datetime(2026, 3, 5, 9, 0, tzinfo=timezone.utc), season="2025-26")
        assert (clk.today, clk.settled_through, clk.phase) == (date(2026, 3, 5), date(2026, 3, 3), "regular")

    def test_after_the_deadline_last_night_is_settled(self):
        # 7:00 AM ET on Mar 5.
        clk = fs.clock(datetime(2026, 3, 5, 12, 0, tzinfo=timezone.utc), season="2025-26")
        assert (clk.today, clk.settled_through) == (date(2026, 3, 5), date(2026, 3, 4))
        assert clk.now_et == datetime(2026, 3, 5, 7, 0)  # naive Eastern, for the tip-off comparison

    def test_the_et_date_not_the_utc_one(self):
        # 11:30 PM ET on Mar 4 is already Mar 5 in UTC.
        clk = fs.clock(datetime(2026, 3, 5, 4, 30, tzinfo=timezone.utc), season="2025-26")
        assert clk.today == date(2026, 3, 4)

    def test_phase_follows_the_calendar(self):
        assert fs.clock(datetime(2025, 10, 10, 12, tzinfo=timezone.utc), season="2025-26").phase == "preseason"
        assert fs.clock(datetime(2026, 7, 1, 12, tzinfo=timezone.utc), season="2025-26").phase == "offseason"


TARGETS = {t.table: t for t in fs.targets()}


@pytest.mark.unit
class TestTargets:
    def test_every_registered_pipeline_writes_a_table_listed_here(self):
        listed = {p for t in TARGETS.values() for p in t.pipelines}
        assert listed == set(PIPELINE_REGISTRY)

    @pytest.mark.parametrize("table", sorted(TARGETS))
    def test_a_target_table_is_a_model_with_a_write_timestamp(self, table):
        target = TARGETS[table]
        assert target.model is not None, (
            f"{table} (written by {', '.join(target.pipelines)}) is no model's table: "
            "the pipeline's target_table is misspelled, or the model is missing"
        )
        assert target.write_column, f"{table} has none of {fs.WRITE_COLUMNS}"

    @pytest.mark.parametrize("table", sorted(
        t for t, target in TARGETS.items()
        if target.category in (PipelineCategory.POST_GAME, PipelineCategory.PRE_GAME)
        and not set(target.pipelines) & fs.CONDITIONAL_WRITERS
    ))
    def test_a_nightly_table_has_a_business_date_to_judge(self, table):
        assert TARGETS[table].date_column, f"{table} has none of {fs.DATE_COLUMNS}"

    def test_games_has_two_writers_and_the_nightly_ones_category(self):
        games = TARGETS["nba.games"]
        assert games.pipelines == ("game_schedule", "game_start_times")
        assert games.category == PipelineCategory.POST_GAME
        assert (games.date_column, games.write_column) == ("game_date", "updated_at")

    def test_daily_matchup_scores_names_the_real_table(self):
        # The config said `stats_s2.daily_matchup_score` (singular) until this page.
        assert "stats_s2.daily_matchup_scores" in TARGETS

    def test_breakout_candidates_is_a_conditional_writers_table(self):
        # execute() returns without a row on a night with no prominent injury.
        assert "breakout_detection" in fs.CONDITIONAL_WRITERS
        assert TARGETS["nba.breakout_candidates"].pipelines == ("breakout_detection",)

    def test_a_reference_table_has_a_write_column_and_no_business_date(self):
        profiles = TARGETS["nba.player_profiles"]
        assert (profiles.date_column, profiles.write_column) == (None, "updated_at")
