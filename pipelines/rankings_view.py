"""Refresh of the nba.rankings materialized view (backend migration 0006).

nba.rankings is a materialized copy of nba.rankings_source and is what the
public rankings endpoint reads. It has to be refreshed after every write to
nba.player_season_stats, which is why this hangs off the season-stats pipeline
instead of being its own registry entry: the post-game endpoint dedups per
pipeline per night, so a standalone refresh pipeline that succeeded *before* a
season-stats retry would be skipped for the rest of that night and leave the
copy behind the table it mirrors.

A failure here is deliberately not fatal to the season-stats run. The backend
compares the copy's newest as_of_date with nba.player_season_stats and reads
nba.rankings_source when they differ, so a missed refresh costs request latency,
never correctness — and wedging the season-stats pipeline into a nightly retry
loop over a refresh failure is the more expensive outcome.
"""

import time

from db.base import db


def rankings_is_materialized() -> bool:
    """True when nba.rankings is a materialized view (relkind 'm'), not a plain view."""
    cursor = db.execute_sql(
        "SELECT c.relkind FROM pg_class c "
        "JOIN pg_namespace n ON n.oid = c.relnamespace "
        "WHERE n.nspname = 'nba' AND c.relname = 'rankings'"
    )
    row = cursor.fetchone()
    return bool(row) and row[0] == "m"


def refresh_rankings(log) -> bool:
    """Refresh nba.rankings. Returns True when the copy was actually refreshed."""
    if not rankings_is_materialized():
        # Migration 0006 has not reached this database yet: nba.rankings is
        # still a plain view and needs no refresh. Deploy ordering, not a fault.
        log.info("rankings_refresh_skipped", reason="not_materialized")
        return False

    started = time.perf_counter()
    try:
        # Peewee runs Postgres connections in autocommit mode, so this is not
        # inside a transaction block — which CONCURRENTLY requires.
        db.execute_sql("REFRESH MATERIALIZED VIEW CONCURRENTLY nba.rankings")
        mode = "concurrent"
    except Exception as exc:
        # CONCURRENTLY needs the unique index and an already-populated view.
        # Fall back to the blocking form rather than leave the copy stale; at
        # this row count readers block for tens of milliseconds.
        log.warning(
            "rankings_refresh_concurrent_failed",
            error=type(exc).__name__,
            detail=str(exc)[:200],
        )
        db.execute_sql("REFRESH MATERIALIZED VIEW nba.rankings")
        mode = "blocking"

    log.info(
        "rankings_refreshed",
        mode=mode,
        elapsed_ms=round((time.perf_counter() - started) * 1000),
    )
    return True
