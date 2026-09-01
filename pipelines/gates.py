"""
Batch gate decisions, as pure functions.

The pre-game and post-game endpoints decide, on every cron poll, whether there
is work to do. Those decisions used to be inline in `api/v1/pipelines.py`,
where the only way to test them was to re-implement them in the test file —
which is what `tests/unit/test_gating.py` did, so a bug in the endpoint and its
copy in the test agreed with each other. Everything here takes its inputs
explicitly (clock included) and returns a decision plus a stable reason slug,
so the endpoint is an I/O shell and the tests exercise the real code.

Reason slugs are load-bearing: they go into the log line, into
`nba.pipeline_batches.reason`, and into test assertions. Treat them as an
interface.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta
from typing import Mapping, Optional

import pytz

CENTRAL = pytz.timezone("US/Central")

# The hour ESPN's nightly batch is assumed to have landed by, after which the
# gate stops waiting for `latestScoringPeriod` to advance. Anchored to the day
# *after* the NBA date, never to a bare hour-of-day.
ESPN_FALLBACK_TIME_CST = time(2, 30)


@dataclass(frozen=True)
class GateDecision:
    """Whether to run, and why — the `why` is what gets logged and stored."""

    run: bool
    reason: str
    detail: dict = field(default_factory=dict)


@dataclass(frozen=True)
class Window:
    """A batch's time window, and where `now` sits relative to it.

    `state` is "before", "open" or "closed". "closed" is the interesting one:
    it is the moment the night's last chance has passed, which is when the
    post-game endpoint checks whether anything was missed.
    """

    opens_at: datetime
    closes_at: Optional[datetime]
    state: str

    @property
    def open(self) -> bool:
        return self.state == "open"

    @property
    def closed(self) -> bool:
        return self.state == "closed"


# ---------------------------------------------------------------------------
# Pre-game
# ---------------------------------------------------------------------------


def pre_game_window(
    now_et_naive: datetime,
    first_game_time: time,
    nba_date: date,
    window_minutes: int,
) -> Window:
    """Opens `window_minutes` before the first tip and never closes.

    Pre-game work stays useful right up to tip-off (and past it, for the teams
    playing later), so there is no upper bound — the per-pipeline dedup is what
    stops it repeating.
    """
    opens_at = datetime.combine(nba_date, first_game_time) - timedelta(
        minutes=window_minutes
    )
    state = "open" if now_et_naive >= opens_at else "before"
    return Window(opens_at=opens_at, closes_at=None, state=state)


# ---------------------------------------------------------------------------
# Post-game
# ---------------------------------------------------------------------------


def post_game_window(
    now_et_naive: datetime,
    latest_game_time: time,
    nba_date: date,
    estimated_duration_minutes: int,
    window_minutes: int,
) -> Window:
    """Opens when the last game is estimated to have ended, closes `window_minutes` later."""
    opens_at = datetime.combine(nba_date, latest_game_time) + timedelta(
        minutes=estimated_duration_minutes
    )
    closes_at = opens_at + timedelta(minutes=window_minutes)

    if now_et_naive < opens_at:
        state = "before"
    elif now_et_naive <= closes_at:
        state = "open"
    else:
        state = "closed"
    return Window(opens_at=opens_at, closes_at=closes_at, state=state)


def espn_fallback_at(nba_date: date) -> datetime:
    """02:30 CST on the morning **after** `nba_date` — aware.

    Anchoring to the date is the whole point. The rule this replaces was
    `hour > 2 or (hour == 2 and minute >= 30)`, which is true at 21:00 as
    surely as at 03:00, so the fallback fired the moment the post-game window
    opened and ESPN's scoring period was never actually consulted outside
    00:00–02:29 CST.
    """
    return CENTRAL.localize(
        datetime.combine(nba_date + timedelta(days=1), ESPN_FALLBACK_TIME_CST)
    )


def espn_batch_gate(
    *,
    now_cst: datetime,
    nba_date: date,
    league_present: bool,
    espn_period: Optional[int],
    baselines: Mapping[int, Optional[int]],
    succeeded_tonight: bool,
    attempts_tonight: int,
    max_attempts: int,
) -> GateDecision:
    """Should an ESPN-gated pipeline run on this poll?

    The question is "has ESPN's nightly batch landed", and the answer is
    `latestScoringPeriod` having moved past the watermark on our newest stored
    snapshot. Everything else here is about the cases where that comparison
    cannot be made.

    Args:
        now_cst: current time, aware, Central.
        nba_date: the ET game date this batch covers; the fallback is anchored
            to the morning after it.
        league_present: any ESPN team is configured at all. When none is, there
            is nothing to gate on — but the pipeline still has Yahoo teams to
            write, so it runs once rather than every poll.
        espn_period: `status.latestScoringPeriod`, or None when the lookup
            failed or the field was absent.
        baselines: team id -> the newest watermark stored for that team **in
            ESPN's current matchup period**, or None when the team has no
            snapshot in that period yet. A None is how a new matchup period
            gets seeded and how a team a partial run missed gets retried; the
            old gate read a single team's newest row across all periods and
            used it as an oracle for everyone.
        succeeded_tonight: this pipeline already completed successfully since
            the window opened.
        attempts_tonight: runs started since the window opened, successful or
            not. Bounds retries — the old gate returned True on every ESPN
            error, so an outage meant a failing run every 15 minutes all night.
        max_attempts: retry budget for the night.

    Returns:
        GateDecision. `reason` is a stable slug; `detail` carries the numbers
        worth having in the log line.
    """
    fallback_at = espn_fallback_at(nba_date)
    past_fallback = now_cst >= fallback_at
    budget_left = attempts_tonight < max_attempts
    detail = {
        "espn_period": espn_period,
        "attempts_tonight": attempts_tonight,
        "past_fallback": past_fallback,
        "fallback_at": fallback_at.isoformat(),
    }

    def decide(run: bool, reason: str, **extra) -> GateDecision:
        return GateDecision(run=run, reason=reason, detail={**detail, **extra})

    if not league_present:
        if succeeded_tonight:
            return decide(False, "no_espn_league_already_ran")
        if not budget_left:
            return decide(False, "attempts_exhausted")
        return decide(True, "no_espn_league")

    if espn_period is None:
        # No usable signal from ESPN. Running now would hit the same API the
        # pipeline itself needs, so wait for the fallback rather than burning
        # the retry budget on a provider that is already answering badly.
        if succeeded_tonight:
            return decide(False, "espn_unavailable_already_ran")
        if not budget_left:
            return decide(False, "attempts_exhausted")
        if past_fallback:
            return decide(True, "time_fallback_espn_unavailable")
        return decide(False, "espn_unavailable_waiting")

    behind = [
        team_id
        for team_id, watermark in baselines.items()
        if watermark is None or watermark < espn_period
    ]
    unseeded = [team_id for team_id, w in baselines.items() if w is None]

    if behind:
        if not budget_left:
            return decide(
                False,
                "attempts_exhausted",
                teams_behind=len(behind),
                teams_unseeded=len(unseeded),
            )
        return decide(
            True,
            "period_advanced",
            teams_behind=len(behind),
            teams_unseeded=len(unseeded),
        )

    # Every team's stored watermark already covers ESPN's current period, so
    # the batch we are waiting for has not landed. This is also what stops the
    # re-runs a successful night used to produce: the old fallback returned
    # True before this comparison, so from 02:30 the pipeline re-ran on every
    # poll until the window closed.
    if succeeded_tonight:
        return decide(False, "period_unchanged_already_ran")
    if past_fallback and budget_left:
        return decide(True, "time_fallback_no_advance")
    if past_fallback:
        return decide(False, "attempts_exhausted")
    return decide(False, "period_unchanged")


# The preseason-market pull is only meaningful while draft prep is live:
# ESPN publishes draft ranks/ADP from late summer, and they stop mattering
# once the season is underway.
PRESEASON_MARKET_OPENS = (8, 15)    # Aug 15
PRESEASON_MARKET_CLOSES = (10, 31)  # Oct 31


def preseason_market_window(today: date) -> GateDecision:
    """Whether the preseason-market pipeline should pull today (Aug 15 – Oct 31)."""
    opens = date(today.year, *PRESEASON_MARKET_OPENS)
    closes = date(today.year, *PRESEASON_MARKET_CLOSES)
    detail = {"opens": str(opens), "closes": str(closes)}
    if opens <= today <= closes:
        return GateDecision(True, "preseason_window_open", detail)
    return GateDecision(False, "outside_preseason_window", detail)
