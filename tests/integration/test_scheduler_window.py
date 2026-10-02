"""
The scheduler window past a day, against real rows at cron-runner's cadence.

cron-runner reports every firing. Its registry (cron-runner
internal/jobs/registry.go) has live-stats every 30 seconds for 16 hours a day,
pre-game and post-game every 15 minutes for half a day each, two daily jobs
and a weekly one: about 2,000 rows a day. A week of them has to come back
whole, and small.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from api.v1 import dashboard
from db.base import db
from db.models.nba.cron_job_run import CronJobRun

pytestmark = pytest.mark.integration

NOW = datetime(2026, 3, 5, 13, 0, tzinfo=timezone.utc)  # a Thursday
SEEDED_FROM = datetime(2026, 2, 25, 0, 0)               # more than a week before
WEEK, THREE_DAYS = 168, 72

LIVE_HOURS = {*range(16, 24), *range(0, 8)}       # */30 * 16-23,0-7 * * * (with seconds)
PRE_GAME_HOURS = {*range(13, 24), 0, 1}           # 0/15 13-23,0-1 * * *
POST_GAME_HOURS = set(range(2, 14))               # 0/15 2-13 * * *

A_FAILURE = datetime(2026, 2, 27, 9, 15)          # post-game, six days back
A_RETRY = datetime(2026, 3, 1, 20, 0, 30)         # live-stats, four days back
AN_OUTAGE = datetime(2026, 3, 3, 3, 0)            # live-stats: eight polls in a row from here


def registry_firings(start: datetime, end: datetime) -> list[tuple[str, datetime]]:
    """Every (job, moment) cron-runner's registry fires in [start, end], UTC."""
    firings = []
    at = start
    while at <= end:
        on_the_minute = at.second == 0
        quarter = on_the_minute and at.minute in (0, 15, 30, 45)
        on_the_hour = on_the_minute and at.minute == 0
        if at.hour in LIVE_HOURS:
            firings.append(("live-stats", at))
        if quarter and at.hour in PRE_GAME_HOURS:
            firings.append(("pre-game", at))
        if quarter and at.hour in POST_GAME_HOURS:
            firings.append(("post-game", at))
        if on_the_hour and at.hour == 6:
            firings.append(("playoffs", at))
        if on_the_hour and at.hour == 11:
            firings.append(("preseason-market", at))
        if on_the_hour and at.hour == 12 and at.weekday() == 0:
            firings.append(("schedule-sync", at))
        at += timedelta(seconds=30)
    return firings


def _row(job: str, at: datetime) -> dict:
    outage = job == "live-stats" and AN_OUTAGE <= at < AN_OUTAGE + timedelta(minutes=4)
    failed = outage or (job, at) == ("post-game", A_FAILURE)
    return dict(
        job_name=job, triggered_at=at, completed_at=at + timedelta(seconds=2), duration_ms=2000,
        result="failure" if failed else "success",
        http_status=503 if failed else 200,
        attempts=3 if failed or (job, at) == ("live-stats", A_RETRY) else 1,
        error_message="endpoint returned status 503: upstream connect error" if failed else None,
        response_snippet='{"status": "skipped", "message": "Games haven\'t started yet"}',
    )


@pytest.fixture(scope="module")
def firings(integration_db) -> list[tuple[str, datetime]]:
    """Eight days and a bit at the registry's cadence, written once for the module."""
    db.create_tables([CronJobRun], safe=True)
    CronJobRun.delete().execute()
    seeded = registry_firings(SEEDED_FROM, NOW.replace(tzinfo=None))
    rows = [_row(job, at) for job, at in seeded]
    with db.atomic():
        for start in range(0, len(rows), 1000):
            CronJobRun.insert_many(rows[start:start + 1000]).execute()
    yield seeded
    CronJobRun.delete().execute()


def _within(firings, hours: int) -> list[tuple[str, datetime]]:
    since = (NOW - timedelta(hours=hours)).replace(tzinfo=None)
    return [(job, at) for job, at in firings if at >= since]


def test_the_seed_is_the_registrys_cadence(firings):
    one_day = _within(firings, 24)
    per_job = {job: sum(1 for name, _ in one_day if name == job) for job, _ in one_day}
    # 16 h of 30-second polls, 13 h and 12 h of quarter hours (and the one at
    # this very minute), two daily jobs.
    assert per_job == {
        "live-stats": 1920, "pre-game": 53, "post-game": 49, "playoffs": 1, "preseason-market": 1,
    }
    assert len(_within(firings, WEEK)) > 14_000


def test_a_week_comes_back_whole(firings):
    data = dashboard._build_scheduler_counts(WEEK, now=NOW)
    in_window = _within(firings, WEEK)

    assert data.truncated is False
    assert sum(bucket.runs for bucket in data.buckets) == len(in_window)
    # The oldest run counted is the window's first, seven days back.
    since = (NOW - timedelta(hours=WEEK)).replace(tzinfo=None)
    assert min(bucket.first_triggered_at for bucket in data.buckets) == since
    # Every lane reaches back, not only the one that fires most.
    for job in ("live-stats", "pre-game", "post-game", "playoffs", "preseason-market"):
        oldest = min(b.first_triggered_at for b in data.buckets if b.job_name == job)
        assert oldest == min(at for name, at in in_window if name == job), job
    assert [b.runs for b in data.buckets if b.job_name == "schedule-sync"] == [1]


def test_a_week_is_a_small_reply(firings):
    data = dashboard._build_scheduler_counts(WEEK, now=NOW)
    # 14,000 rows were megabytes. Counts and the rows a mark can open are not.
    assert len(data.buckets) < 400 and len(data.runs) < 400
    assert len(data.model_dump_json()) < 200_000


def test_three_days_and_a_week_are_different_windows(firings):
    week = dashboard._build_scheduler_counts(WEEK, now=NOW)
    three_days = dashboard._build_scheduler_counts(THREE_DAYS, now=NOW)

    assert (week.bucket_seconds, three_days.bucket_seconds) == (6300, 2700)  # the window in 96 columns
    assert sum(b.runs for b in three_days.buckets) == len(_within(firings, THREE_DAYS))
    since = (NOW - timedelta(hours=THREE_DAYS)).replace(tzinfo=None)
    assert min(b.first_triggered_at for b in three_days.buckets) == since
    assert sum(b.runs for b in three_days.buckets) < sum(b.runs for b in week.buckets)


def test_every_run_is_counted_in_the_column_it_fired_in(firings):
    data = dashboard._build_scheduler_counts(WEEK, now=NOW)
    width = timedelta(seconds=data.bucket_seconds)
    epoch = datetime(1970, 1, 1)
    for bucket in data.buckets:
        # Columns are cut from the epoch, so they stay put from poll to poll.
        assert (bucket.start - epoch) % width == timedelta(0)
        assert bucket.start <= bucket.first_triggered_at <= bucket.last_triggered_at < bucket.start + width
    keys = [(b.job_name, b.start) for b in data.buckets]
    assert len(set(keys)) == len(keys)


def test_a_run_that_failed_or_was_retried_comes_as_a_row(firings):
    data = dashboard._build_scheduler_counts(WEEK, now=NOW)

    failure, = [run for run in data.runs if run.triggered_at == A_FAILURE and run.job_name == "post-game"]
    assert (failure.result, failure.http_status, failure.attempts) == ("failure", 503, 3)
    assert failure.error_message == "endpoint returned status 503: upstream connect error"
    assert failure.response_snippet is None  # bodies stay with the status payload
    column, = [b for b in data.buckets if b.job_name == "post-game" and b.start <= A_FAILURE < b.start + timedelta(seconds=6300)]
    assert (column.failed, column.retried) == (1, 0) and column.runs > 1

    retry, = [run for run in data.runs if run.triggered_at == A_RETRY]
    assert (retry.result, retry.attempts) == ("success", 3)
    column, = [b for b in data.buckets if b.job_name == "live-stats" and b.start <= A_RETRY < b.start + timedelta(seconds=6300)]
    assert (column.failed, column.retried) == (0, 1)


def test_a_column_of_failures_is_counted_in_full_and_listed_in_part(firings):
    data = dashboard._build_scheduler_counts(WEEK, now=NOW)
    column, = [b for b in data.buckets if b.job_name == "live-stats" and b.start <= AN_OUTAGE < b.start + timedelta(seconds=6300)]
    assert column.failed == 8

    width = timedelta(seconds=6300)
    carried = [run for run in data.runs if run.job_name == "live-stats" and column.start <= run.triggered_at < column.start + width]
    troubled = [run for run in carried if run.result == "failure"]
    # The newest five of the eight, and the column's newest run of all.
    assert [run.triggered_at for run in troubled] == [
        AN_OUTAGE + timedelta(seconds=30 * i) for i in (7, 6, 5, 4, 3)
    ]
    assert carried[0].triggered_at == column.last_triggered_at and len(carried) == 6


def test_a_run_alone_in_its_column_comes_as_a_row(firings):
    data = dashboard._build_scheduler_counts(WEEK, now=NOW)
    carried = {(run.job_name, run.triggered_at) for run in data.runs}
    alone = [b for b in data.buckets if b.job_name == "playoffs"]
    assert len(alone) == 7 and all(b.runs == 1 for b in alone)
    assert all((b.job_name, b.first_triggered_at) in carried for b in alone)
    # And every column's newest run, whatever the job.
    assert all((b.job_name, b.last_triggered_at) in carried for b in data.buckets)


def test_runs_are_newest_first(firings):
    data = dashboard._build_scheduler_counts(WEEK, now=NOW)
    times = [run.triggered_at for run in data.runs]
    assert times == sorted(times, reverse=True)
