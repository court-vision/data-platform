"""ESPN extractor: matchup_start comes from the weekly ids; the day-granular latestScoringPeriod only corrects."""

from datetime import timedelta

import pytest

from core.settings import settings
from pipelines.extractors import espn as espn_mod
from services import schedule_service as ss


class _Resp:
    def __init__(self, payload):
        self._payload = payload

    def raise_for_status(self):
        pass

    def json(self):
        return self._payload


def _payload(current_period, matchup_periods, latest_scoring_period):
    return {
        "status": {"latestScoringPeriod": latest_scoring_period, "currentMatchupPeriod": current_period},
        "settings": {
            "scheduleSettings": {"matchupPeriods": matchup_periods},
            "scoringSettings": {"scoringType": "H2H_POINTS"},
        },
        "teams": [{"id": 1, "name": "Us"}, {"id": 2, "name": "Them"}],
        "schedule": [
            {"matchupPeriodId": current_period,
             "home": {"teamId": 1, "totalPoints": 10}, "away": {"teamId": 2, "totalPoints": 5}},
        ],
    }


@pytest.fixture
def calendar(monkeypatch):
    monkeypatch.setattr(settings, "nba_season", "2025-26")
    ss.reset_cache()
    yield
    ss.reset_cache()


def _matchup(monkeypatch, payload, local_period):
    monkeypatch.setattr(espn_mod.requests, "get", lambda *a, **k: _Resp(payload))
    return espn_mod.ESPNExtractor().get_matchup_data(
        league_id=1, team_name="Us", espn_s2="", swid="", year=2026, matchup_period=local_period
    )


@pytest.mark.unit
def test_aligned_week_uses_the_weekly_id_not_the_day_id(calendar, monkeypatch):
    wk2 = ss.get_matchup_by_number(2)
    # early season: currentMatchupPeriod 2, latestScoringPeriod 8 (a DAY index inside week 2)
    r = _matchup(monkeypatch, _payload(2, {"1": [1], "2": [2]}, 8), local_period=2)
    assert r["matchup_start"] == wk2["start_date"]
    assert (r["matchup_period"], r["scoring_period_id"]) == (2, 8)
    assert (r["current_score"], r["opponent_current_score"], r["opponent_team_name"]) == (10, 5, "Them")


@pytest.mark.unit
def test_two_week_playoff_round_starts_at_first_week(calendar, monkeypatch):
    r = _matchup(monkeypatch, _payload(21, {"21": [22, 23]}, 160), local_period=23)
    assert r["matchup_start"] == ss.get_matchup_by_number(22)["start_date"]
    assert r["matchup_period"] == 21  # ESPN's period wins over the local week number


@pytest.mark.unit
def test_misaligned_ids_self_correct_from_the_day_id(calendar, monkeypatch):
    day = ss.get_matchup_by_number(5)["start_date"] + timedelta(days=2)
    day_id = (day - ss.get_season_bounds().opening_night).days + 1
    r = _matchup(monkeypatch, _payload(3, {"3": [3]}, day_id), local_period=5)
    assert r["matchup_start"] == ss.get_matchup_by_number(5)["start_date"]


@pytest.mark.unit
def test_missing_schedule_settings_still_resolves(calendar, monkeypatch):
    payload = _payload(2, {}, 8)
    payload["settings"] = {}
    r = _matchup(monkeypatch, payload, local_period=2)
    assert r["matchup_start"] == ss.get_matchup_by_number(2)["start_date"]
