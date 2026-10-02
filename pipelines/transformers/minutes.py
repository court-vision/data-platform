"""
Did the player take the floor?

`minutes_to_int` (cv-core) truncates to whole minutes. That is the number the
`min` columns store, but it cannot answer whether a player played: a 40-second
appearance truncates to 0, the same as a DNP. The NBA counts that game (season
GP goes up) and whatever happened in it, so the box-score pipelines ask this
instead and skip a row only when no time was played at all.

Lives beside the cv-core shim rather than in it: only the pipelines that write
box scores ask the question, and the shim (`__init__.py`) takes no code.
"""

import math
import re
from numbers import Real
from typing import Union

# "PT34M56.00S", and the whole-minute "PT34M" that `minutesCalculated` carries.
_ISO_DURATION = re.compile(r"^PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+(?:\.\d+)?)S)?$")


def seconds_played(raw: Union[str, int, float, None]) -> float:
    """
    Seconds on the floor, from any minutes format the NBA sends.

    Handles:
    - Float minutes 0.67 -> 40.2           (PlayerGameLogs MIN)
    - ISO 8601 duration "PT00M40.00S" -> 40.0  (live BoxScore `minutes`)
    - String "0:40" -> 40.0
    - None, NaN, "" and anything unreadable -> 0.0

    Examples:
        >>> seconds_played("PT00M40.00S")
        40.0
        >>> seconds_played("34:56")
        2096.0
        >>> seconds_played(None)
        0.0
    """
    if raw is None or isinstance(raw, bool):
        return 0.0

    if isinstance(raw, Real):
        minutes = float(raw)
        return 0.0 if math.isnan(minutes) else minutes * 60

    s = str(raw).strip()
    if not s:
        return 0.0

    if s.startswith("PT"):
        match = _ISO_DURATION.match(s)
        if not match:
            return 0.0
        hours, minutes, seconds = (float(part) if part else 0.0 for part in match.groups())
        return hours * 3600 + minutes * 60 + seconds

    try:
        if ":" in s:
            minutes, seconds = s.split(":", 1)
            return int(minutes) * 60 + float(seconds)
        return float(s) * 60
    except ValueError:
        return 0.0
