import pytest

from pipelines.transformers import (
    calculate_fantasy_points,
    minutes_to_int,
    normalize_name,
)


@pytest.mark.unit
def test_calculate_fantasy_points_uses_league_formula() -> None:
    stats = {
        "pts": 20,
        "reb": 10,
        "ast": 5,
        "stl": 2,
        "blk": 1,
        "tov": 3,
        "fgm": 8,
        "fga": 15,
        "fg3m": 3,
        "ftm": 5,
        "fta": 6,
    }

    # 20 + 10 + (5*2) + ((2+1)*4) + (3*-2) + 3 + ((8*2)-15) + (5-6) = 49
    assert calculate_fantasy_points(stats) == 49


@pytest.mark.unit
@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("PT34M56.00S", 34),
        ("34:56", 34),
        (34.9, 34),
        (34, 34),
        (None, 0),
        ("", 0),
        ("not-minutes", 0),
    ],
)
def test_minutes_to_int_handles_multiple_formats(raw, expected: int) -> None:
    assert minutes_to_int(raw) == expected


@pytest.mark.unit
def test_normalize_name_removes_diacritics_and_normalizes_case() -> None:
    assert normalize_name("  Nikola Jokić  ") == "nikola jokic"
    assert normalize_name("Luka Dončić") == "luka doncic"

