"""
The NBA game date (`core/nba_calendar.py`) and the writer/reader split it closes.

`docs/PRODUCTION_READINESS.md` item 4: `nba.live_player_stats.game_date` was
written on a **Central** 6 AM cutoff and read on an **Eastern** one. These tests
pin the hour where those two rules disagree, so the split cannot come back by
someone reintroducing a local cutoff.
"""

from datetime import date, datetime, timedelta

import pytest
import pytz

from core.nba_calendar import EASTERN, nba_date_et
from pipelines.context import PipelineContext

CENTRAL = pytz.timezone("US/Central")


def et(hour, minute=0, day=5) -> datetime:
    return EASTERN.localize(datetime(2026, 3, day, hour, minute))


def _central_rule(now_cst: datetime) -> date:
    """The rule every pipeline used to carry its own copy of."""
    if now_cst.hour < 6:
        return (now_cst - timedelta(days=1)).date()
    return now_cst.date()


@pytest.mark.unit
class TestTheRule:
    @pytest.mark.parametrize("hour,expected", [
        (0, date(2026, 3, 4)),
        (3, date(2026, 3, 4)),
        (5, date(2026, 3, 4)),
        (6, date(2026, 3, 5)),
        (12, date(2026, 3, 5)),
        (23, date(2026, 3, 5)),
    ])
    def test_six_am_eastern_rolls_the_day(self, hour, expected):
        assert nba_date_et(et(hour)) == expected

    def test_5_59_is_still_last_night(self):
        assert nba_date_et(et(5, 59)) == date(2026, 3, 4)

    def test_accepts_any_timezone(self):
        """A caller holding UTC or Central must get the same answer."""
        moment = et(6, 30)
        assert nba_date_et(moment.astimezone(pytz.utc)) == date(2026, 3, 5)
        assert nba_date_et(moment.astimezone(CENTRAL)) == date(2026, 3, 5)

    def test_naive_is_read_as_eastern(self):
        assert nba_date_et(datetime(2026, 3, 5, 5, 59)) == date(2026, 3, 4)


@pytest.mark.unit
class TestTheSplitThisCloses:
    def test_the_two_rules_disagree_for_exactly_one_hour(self):
        """06:00–06:59 ET is the window where writer and reader diverged."""
        disagreements = [
            minute
            for minute in range(24 * 60)
            if nba_date_et(et(minute // 60, minute % 60))
            != _central_rule(et(minute // 60, minute % 60).astimezone(CENTRAL))
        ]
        assert disagreements, "the split should be reproducible, not folklore"
        assert min(disagreements) == 6 * 60 and max(disagreements) == 6 * 60 + 59

    def test_in_that_hour_the_writer_stamped_a_date_no_reader_asks_for(self):
        moment = et(6, 30)
        assert _central_rule(moment.astimezone(CENTRAL)) == date(2026, 3, 4)
        assert nba_date_et(moment) == date(2026, 3, 5)


@pytest.mark.unit
class TestContextGameDate:
    """`ctx.game_date()` is the one accessor every pipeline goes through."""

    def test_backfill_date_wins(self):
        ctx = PipelineContext(
            "t", date_override=date(2026, 1, 1), nba_date=date(2026, 3, 4)
        )
        assert ctx.game_date() == date(2026, 1, 1)

    def test_batch_date_beats_the_clock(self):
        """A batch that straddles the cutoff must not split across two dates."""
        ctx = PipelineContext(
            "t",
            nba_date=date(2026, 3, 4),
            started_at=CENTRAL.localize(datetime(2026, 3, 5, 6, 0)),
        )
        assert ctx.game_date() == date(2026, 3, 4)

    def test_falls_back_to_the_eastern_rule(self):
        ctx = PipelineContext(
            "t", started_at=CENTRAL.localize(datetime(2026, 3, 5, 5, 30))
        )
        # 05:30 CST is 06:30 ET — the hour the two rules disagree about.
        assert ctx.game_date() == date(2026, 3, 5)
