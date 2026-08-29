"""
The NBA game date — one rule, one timezone.

A "game date" is the night a game belongs to, not the calendar date it ended
on: a game tipping at 22:30 ET finishes after midnight and still belongs to the
day it started. The rule everywhere is **6 AM Eastern** — before 6 AM ET we are
still on the previous night's game date.

Eastern, specifically. The API reads these dates in Eastern
(`backend/api/v1/public/live.py`, `services/matchup_days.py`,
`services/games_service.py`, `services/schedule_service.py`) while every
pipeline in this repo used to derive its own date in **Central**. The two rules
disagree for one hour a day — 06:00–06:59 ET — and in that hour the writer
stamps rows with yesterday's date while every reader asks for today's. Nothing
runs in that hour today, which is why the split survived a season; it is one
schedule change away from being live, and it is the split named in
`docs/PRODUCTION_READINESS.md` item 4.

Pipelines should not call this directly. The trigger endpoint computes the
batch's date once and hands it to every pipeline in the batch
(`PipelineContext.nba_date`), so a batch that straddles the cutoff cannot write
half its rows under one date and half under the next; `ctx.game_date()` is the
accessor. This module is the fallback for a pipeline run with no batch behind
it, and the single definition both sides share.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta

import pytz

EASTERN = pytz.timezone("US/Eastern")

# Before this hour (Eastern) we are still on the previous night's game date.
DAY_ROLLOVER_HOUR_ET = 6


def nba_date_et(now: datetime | None = None) -> date:
    """The NBA game date for a moment in time, on the 6 AM ET rule.

    `now` may be in any timezone (it is converted) or naive (read as Eastern).
    Defaults to the current time.
    """
    if now is None:
        now_et = datetime.now(EASTERN)
    elif now.tzinfo is None:
        now_et = EASTERN.localize(now)
    else:
        now_et = now.astimezone(EASTERN)

    if now_et.hour < DAY_ROLLOVER_HOUR_ET:
        return (now_et - timedelta(days=1)).date()
    return now_et.date()
