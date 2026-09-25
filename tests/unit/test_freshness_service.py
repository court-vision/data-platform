"""
Table freshness: the rule, the clock, and the registry-to-model resolution.

`judge` is a pure function of dates, so every verdict is tested here without a
database. `targets` is what ties a pipeline's `target_table` to a model; a
config naming a table no model declares fails here instead of on the page.
"""

from datetime import date, datetime, timezone

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


REGULAR = fs.Clock(today=date(2026, 3, 5), settled_through=date(2026, 3, 4), phase="regular")
PRESEASON = fs.Clock(today=date(2026, 10, 10), settled_through=date(2026, 10, 9), phase="preseason")
WRITTEN = datetime(2026, 3, 5, 8, 12)
MAR_4 = date(2026, 3, 4)


def _judge(target, latest_date, last_game_date=MAR_4, clock=REGULAR, written=WRITTEN):
    return fs.judge(
        target, latest_date=latest_date, latest_written_at=written,
        last_game_date=last_game_date, clock=clock,
    )


@pytest.mark.unit
class TestJudge:
    def test_an_empty_nightly_table_is_empty_only_while_something_is_due(self):
        assert _judge(_target(), None, written=None) == ("empty", None)
        # Nothing is due in preseason, and a playoff table has no nightly cadence.
        assert _judge(_target(), None, written=None, clock=PRESEASON) == ("idle", None)
        assert _judge(_target(PipelineCategory.SCHEDULED), None, written=None) == ("unjudged", None)

    def test_fresh_when_it_runs_through_the_last_settled_game(self):
        assert _judge(_target(), MAR_4) == ("fresh", MAR_4)

    def test_a_table_that_runs_past_it_is_fresh_too(self):
        # nba.games holds the rest of the schedule.
        assert _judge(_target(), date(2026, 4, 12)) == ("fresh", MAR_4)

    def test_stale_when_behind_the_last_settled_game(self):
        assert _judge(_target(), date(2026, 3, 3)) == ("stale", MAR_4)

    def test_a_business_date_that_is_all_null_is_stale(self):
        assert _judge(_target(), None) == ("stale", MAR_4)

    def test_pre_game_tables_are_held_to_the_same_date(self):
        injuries = _target(PipelineCategory.PRE_GAME, ("espn_injury_status",), "report_date", "created_at")
        assert _judge(injuries, MAR_4) == ("fresh", MAR_4)
        assert _judge(injuries, date(2026, 3, 2)) == ("stale", MAR_4)

    def test_nothing_is_due_outside_the_regular_season(self):
        assert _judge(_target(), date(2025, 4, 13), clock=PRESEASON) == ("idle", None)
        offseason = fs.Clock(today=date(2026, 7, 1), settled_through=date(2026, 6, 30), phase="offseason")
        assert _judge(_target(), date(2026, 4, 12), clock=offseason) == ("idle", None)

    def test_nothing_is_due_before_the_first_settled_game(self):
        # Opening week: in season, but no final game has passed its deadline yet.
        assert _judge(_target(), date(2025, 4, 13), last_game_date=None) == ("idle", None)

    @pytest.mark.parametrize("category", [PipelineCategory.SCHEDULED, PipelineCategory.LIVE])
    def test_scheduled_and_live_tables_have_no_nightly_cadence(self, category):
        assert _judge(_target(category), date(2026, 3, 1)) == ("unjudged", None)

    def test_a_conditional_writer_is_not_judged_by_its_silence(self):
        alerts = _target(PipelineCategory.PRE_GAME, ("lineup_alerts",), "notification_date", "created_at")
        assert _judge(alerts, date(2026, 2, 1)) == ("unjudged", None)

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

    def test_a_reference_table_has_a_write_column_and_no_business_date(self):
        profiles = TARGETS["nba.player_profiles"]
        assert (profiles.date_column, profiles.write_column) == (None, "updated_at")
