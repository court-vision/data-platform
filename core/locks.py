"""
Execution locks — a pipeline cannot double-run.

`PipelineRun.is_running()` is a check-then-act: the trigger endpoint reads the
run table, decides, and only then starts work. Two triggers 200 ms apart (an
overlapping cron poll, a manual re-trigger, a second instance) both read "not
running" and both start. Nothing downstream is safe against that — the stats
upserts are idempotent, but `daily_matchup_scores` writes a watermark and
`live_game_stats` deletes rows below one.

A PostgreSQL **advisory lock** closes it, because the check and the acquire are
the same operation. `pg_try_advisory_lock` either takes the lock or returns
false immediately; it never waits, so a contended trigger returns at once
rather than piling up worker threads.

Session-scoped, not transaction-scoped: a pipeline runs for minutes and holding
a transaction open that long would pin a snapshot and block vacuum. Session
scope means the lock must be released explicitly — and because the connection
is *pooled*, a leaked lock would ride the connection to its next user and wedge
that pipeline for the life of the process. So the unlock is in a `finally`, and
if the unlock itself fails (a broken connection, a server-side disconnect) the
connection is dropped rather than returned to the pool: ending the session is
what releases the lock server-side.

Keys are `(NAMESPACE, crc32(pipeline_name))` in Postgres's two-int lock space.
The namespace keeps us out of anyone else's advisory-lock keys, and deriving
the second int in Python (rather than `hashtext()`) keeps the key computable
from a test without a database.
"""

from __future__ import annotations

import zlib
from contextlib import contextmanager
from typing import Iterator

from db.base import db
from core.logging import get_logger

log = get_logger("locks")

# Arbitrary but fixed: "court-vision pipelines". Advisory-lock keys are int4,
# so this must stay inside the signed 32-bit range.
NAMESPACE = 0x43563031  # 'CV01'


def lock_key(name: str) -> int:
    """The int4 advisory-lock key for a pipeline name."""
    # crc32 is unsigned 32-bit; shift into the signed range Postgres expects.
    return zlib.crc32(name.encode()) - 2**31


class LockNotAcquired(Exception):
    """Raised by `pipeline_lock` when another session already holds the lock."""

    def __init__(self, name: str):
        self.name = name
        super().__init__(f"another run of '{name}' holds the execution lock")


@contextmanager
def pipeline_lock(name: str) -> Iterator[None]:
    """Hold the execution lock for `name`, or raise `LockNotAcquired`.

    Must be called on the thread that owns the pipeline's DB connection — the
    lock belongs to that session, and releasing it from another one is not
    possible.
    """
    key = lock_key(name)
    acquired = db.execute_sql(
        "SELECT pg_try_advisory_lock(%s, %s)", (NAMESPACE, key)
    ).fetchone()[0]

    if not acquired:
        raise LockNotAcquired(name)

    try:
        yield
    finally:
        _release(name, key)


def _release(name: str, key: int) -> None:
    """Release the lock; on failure, end the session so the server releases it."""
    try:
        released = db.execute_sql(
            "SELECT pg_advisory_unlock(%s, %s)", (NAMESPACE, key)
        ).fetchone()[0]
        if released:
            return
        # False means this session did not hold it — it was never taken, or
        # something released it underneath us. Nothing to drop the connection
        # for, but it should never happen.
        log.warning("pipeline_lock_not_held_at_release", pipeline=name)
    except Exception as exc:
        # The unlock statement itself failed, so the lock may still be held by
        # a session this connection is about to hand to someone else. Close the
        # underlying connection instead of returning it to the pool: the lock
        # dies with the session.
        log.error(
            "pipeline_lock_release_failed",
            pipeline=name,
            error=f"{type(exc).__name__}: {exc}",
        )
        try:
            db.manual_close()
        except Exception:
            pass
