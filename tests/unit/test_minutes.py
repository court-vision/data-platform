import pytest

from pipelines.transformers import minutes_to_int
from pipelines.transformers.minutes import seconds_played


@pytest.mark.unit
@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        # PlayerGameLogs MIN: minutes as a float
        (34.2, 2052.0),
        (0.5, 30.0),
        (0.0, 0.0),
        (0, 0.0),
        (34, 2040.0),
        (float("nan"), 0.0),
        # Live BoxScore `minutes`, and the whole-minute `minutesCalculated`
        ("PT34M56.00S", 2096.0),
        ("PT00M40.00S", 40.0),
        ("PT00M00.00S", 0.0),
        ("PT25M", 1500.0),
        ("PT00M", 0.0),
        ("PT1H02M03.00S", 3723.0),
        ("PT", 0.0),
        ("PTgarbage", 0.0),
        # MM:SS
        ("34:56", 2096.0),
        ("0:40", 40.0),
        ("0:00", 0.0),
        # Numeric strings, and nothing at all
        ("0.5", 30.0),
        ("12", 720.0),
        (None, 0.0),
        ("", 0.0),
        ("  ", 0.0),
        ("not-minutes", 0.0),
        ("a:b", 0.0),
    ],
)
def test_seconds_played_reads_every_minutes_format(raw, expected: float) -> None:
    assert seconds_played(raw) == pytest.approx(expected)


@pytest.mark.unit
@pytest.mark.parametrize("raw", [0.67, "PT00M40.00S", "0:40"])
def test_a_sub_minute_appearance_is_played_time_that_truncates_to_zero(raw) -> None:
    """The case the pipelines used to drop: whole minutes say 0, the clock does not."""
    assert minutes_to_int(raw) == 0
    assert seconds_played(raw) > 0
