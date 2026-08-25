"""Season-aware calendar: file selection, bounds/phase, and ESPN period → date resolution."""

from datetime import date, timedelta

import pytest

from core.settings import settings
from services import schedule_service as ss

STATIC = ss.STATIC_DIR
HAS_26_27 = (STATIC / "schedule26-27.json").exists() and (STATIC / "matchupsPerDay26-27.json").exists()


@pytest.fixture
def season(monkeypatch):
    def _use(key: str):
        monkeypatch.setattr(settings, "nba_season", key)
        ss.reset_cache()
    yield _use
    ss.reset_cache()


@pytest.mark.unit
def test_2025_26_calendar_and_espn_day_alignment(season):
    season("2025-26")
    assert ss.calendar_path("schedule").name == "schedule25-26.json"
    assert ss.calendar_path("matchupsPerDay").name == "matchupsPerDay25-26.json"
    b = ss.get_season_bounds()
    assert (b.opening_night, b.regular_season_end, b.week_count, b.espn_year) == (date(2025, 10, 21), date(2026, 4, 12), 24, 2026)
    assert b.preseason_start == date(2025, 10, 2)
    assert ss.get_max_week() == 24
    # ESPN's 2025-26 playoff round [22, 23] started at day id 154 == our merged week 22's first day
    assert ss.date_for_espn_scoring_period(154) == ss.get_matchup_by_number(22)["start_date"] == date(2026, 3, 23)
    assert ss.get_matchup_by_number(17)["game_span"] == 14
    assert ss.get_season_phase(date(2026, 1, 10)) == "regular"
    assert ss.get_season_phase(date(2025, 10, 5)) == "preseason"
    assert ss.get_season_phase(date(2026, 8, 25)) == "offseason"
    assert ss.season_day(date(2025, 10, 21)) == 1 and ss.season_day(date(2026, 8, 25)) is None
    assert ss.get_upcoming_games_on_date(date(2025, 10, 2)) != []
    assert ss.get_upcoming_games_on_date(date(2026, 8, 25)) == []


@pytest.mark.unit
def test_current_matchup_and_week_helpers(season):
    season("2025-26")
    wk = ss.get_current_matchup(date(2025, 10, 21))
    assert (wk["matchup_number"], wk["current_day_index"]) == (1, 0)
    assert ss.get_current_matchup(date(2026, 8, 25)) is None
    assert ss.get_matchup_dates(2) == (ss.get_matchup_by_number(2)["start_date"], ss.get_matchup_by_number(2)["end_date"])
    assert ss.get_matchup_dates(99) is None
    assert [w["matchup_number"] for w in ss.iter_weeks()] == list(range(1, 25))


@pytest.mark.unit
def test_espn_matchup_dates_aligned_missing_key_and_misaligned(season):
    season("2025-26")
    m = {"1": [1], "2": [2], "20": [20, 21], "21": [22, 23]}
    assert ss.espn_scoring_periods(m, 21) == [22, 23]
    assert ss.espn_scoring_periods(m, 99) == [99] and ss.espn_scoring_periods(None, 4) == [4]
    wk2 = ss.get_matchup_by_number(2)
    # regression: day id 8 must not be appended to the weekly ids (used to stretch week 2 to week 8's end)
    assert ss.get_espn_matchup_dates(m, 2, latest_scoring_period=8) == (wk2["start_date"], wk2["end_date"])
    assert (wk2["end_date"] - wk2["start_date"]).days == 6
    # two-week playoff round spans both weeks
    assert ss.get_espn_matchup_dates(m, 21, latest_scoring_period=160) == (
        ss.get_matchup_by_number(22)["start_date"], ss.get_matchup_by_number(23)["end_date"])
    # missing key with a day id → the calendar week containing that day
    day30 = ss.date_for_espn_scoring_period(30)
    wk = ss.get_current_matchup(day30)
    assert ss.get_espn_matchup_dates({}, 99, latest_scoring_period=30) == (wk["start_date"], wk["end_date"])
    # misaligned ids (ESPN says period 3 but the day is in our week 5) → self-corrects
    day = ss.get_matchup_by_number(5)["start_date"] + timedelta(days=2)
    sp = (day - ss.get_season_bounds().opening_night).days + 1
    assert ss.get_espn_matchup_dates({"3": [3]}, 3, latest_scoring_period=sp) == (
        ss.get_matchup_by_number(5)["start_date"], ss.get_matchup_by_number(5)["end_date"])
    # offseason: a day id beyond the calendar keeps the id-based range
    assert ss.get_espn_matchup_dates(m, 21, latest_scoring_period=175) == (
        ss.get_matchup_by_number(22)["start_date"], ss.get_matchup_by_number(23)["end_date"])
    assert ss.get_espn_matchup_dates({}, 99, latest_scoring_period=None) is None


@pytest.mark.unit
def test_missing_calendar_is_a_clear_error(season):
    season("2099-00")
    with pytest.raises(FileNotFoundError):
        ss.assert_calendar_available()
    with pytest.raises(FileNotFoundError):
        ss.get_current_matchup(date(2099, 11, 1))


@pytest.mark.unit
@pytest.mark.skipif(not HAS_26_27, reason="schedule26-27.json not generated yet")
def test_2026_27_calendar(season):
    season("2026-27")
    ss.assert_calendar_available()
    b = ss.get_season_bounds()
    assert (b.opening_night, b.regular_season_end, b.week_count, b.espn_year) == (date(2026, 10, 20), date(2027, 4, 11), 24, 2027)
    assert b.preseason_start == date(2026, 10, 3)
    assert ss.get_current_matchup(date(2026, 10, 20))["current_day_index"] == 0
    wk18 = ss.get_current_matchup(date(2027, 2, 22))
    assert (wk18["matchup_number"], wk18["game_span"], wk18["current_day_index"]) == (18, 14, 7)
    assert ss.get_current_matchup(date(2027, 4, 11))["matchup_number"] == 24
    assert (ss.get_matchup_by_number(23)["end_date"] - b.opening_night).days + 1 == 167
    assert ss.get_season_phase(date(2026, 8, 25)) == "offseason"
    assert ss.get_season_phase(date(2026, 10, 3)) == "preseason"
    assert ss.get_season_phase(date(2026, 10, 20)) == "regular"
    assert ss.get_season_phase(date(2027, 4, 12)) == "offseason"
