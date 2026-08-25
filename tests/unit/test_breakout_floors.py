"""breakout_detection: GP floors scale with the season; the lookback is bounded by opening night."""

from datetime import date

import pytest

from core.settings import settings
from pipelines import breakout_detection as bd
from services import schedule_service as ss


@pytest.mark.unit
@pytest.mark.parametrize(
    "default, max_gp, expected",
    [
        (20, None, 2),   # no season rows yet
        (20, 0, 2),
        (20, 1, 2),
        (20, 3, 2),
        (20, 5, 3),
        (20, 10, 5),
        (20, 39, 20),
        (20, 82, 20),
        (10, 5, 3),
        (10, 19, 10),
        (10, 82, 10),
    ],
)
def test_scaled_floor(default, max_gp, expected):
    assert bd.scaled_floor(default, max_gp) == expected


@pytest.mark.unit
def test_lookback_is_bounded_by_opening_night(monkeypatch):
    monkeypatch.setattr(settings, "nba_season", "2025-26")
    ss.reset_cache()
    pipeline = bd.BreakoutDetectionPipeline()
    assert pipeline._season_opening_night("2025-26") == date(2025, 10, 21)
    assert pipeline._season_opening_night("2099-00") is None   # no calendar → plain lookback
    ss.reset_cache()
