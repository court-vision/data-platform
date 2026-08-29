"""Refresh of the nba.rankings materialized view, and its failure modes."""

from types import SimpleNamespace

import pytest

from pipelines import rankings_view


class _Log:
    """structlog-shaped recorder: log.info("event", k=v)."""

    def __init__(self):
        self.events = []

    def __getattr__(self, level):
        def record(event, **kw):
            self.events.append((level, event, kw))
        return record


@pytest.fixture
def fake_db(monkeypatch):
    """Stub db.execute_sql: `relkind` drives the probe, `fail` names statements that raise."""
    state = SimpleNamespace(relkind="m", fail=(), statements=[])

    def execute_sql(sql, *args, **kwargs):
        state.statements.append(sql)
        if sql.lstrip().upper().startswith("SELECT C.RELKIND"):
            row = (state.relkind,) if state.relkind else None
            return SimpleNamespace(fetchone=lambda: row)
        for marker in state.fail:
            if marker in sql:
                raise RuntimeError(f"cannot run: {marker}")
        return SimpleNamespace(fetchone=lambda: None)

    monkeypatch.setattr(rankings_view.db, "execute_sql", execute_sql)
    return state


@pytest.mark.unit
def test_refresh_uses_the_concurrent_form(fake_db):
    log = _Log()

    assert rankings_view.refresh_rankings(log) is True
    assert "REFRESH MATERIALIZED VIEW CONCURRENTLY nba.rankings" in fake_db.statements
    assert [(e, kw.get("mode")) for lvl, e, kw in log.events] == [("rankings_refreshed", "concurrent")]


@pytest.mark.unit
def test_a_plain_view_is_skipped_rather_than_failed(fake_db):
    """Deploy ordering: data-platform may run before the backend applies migration 0006."""
    fake_db.relkind = "v"
    log = _Log()

    assert rankings_view.refresh_rankings(log) is False
    assert not any("REFRESH" in s for s in fake_db.statements)
    assert log.events[0][1] == "rankings_refresh_skipped"


@pytest.mark.unit
def test_a_missing_relation_is_skipped_too(fake_db):
    fake_db.relkind = None

    assert rankings_view.refresh_rankings(_Log()) is False
    assert not any("REFRESH" in s for s in fake_db.statements)


@pytest.mark.unit
def test_a_failed_concurrent_refresh_falls_back_to_the_blocking_form(fake_db):
    fake_db.fail = ("CONCURRENTLY",)
    log = _Log()

    assert rankings_view.refresh_rankings(log) is True
    assert fake_db.statements[-1] == "REFRESH MATERIALIZED VIEW nba.rankings"
    levels = [(lvl, e) for lvl, e, _ in log.events]
    assert levels == [("warning", "rankings_refresh_concurrent_failed"), ("info", "rankings_refreshed")]
    assert log.events[-1][2]["mode"] == "blocking"


@pytest.mark.unit
def test_a_refresh_failure_never_fails_the_season_stats_pipeline(monkeypatch):
    """A stale copy is a latency problem — the backend still reads the source view.
    A season-stats pipeline stuck retrying all night would be the worse outcome."""
    from pipelines.player_season_stats import PlayerSeasonStatsPipeline

    def boom(_log):
        raise RuntimeError("refresh exploded")

    monkeypatch.setattr("pipelines.player_season_stats.refresh_rankings", boom)
    log = _Log()

    PlayerSeasonStatsPipeline().after_execute(SimpleNamespace(log=log))

    assert [(lvl, e) for lvl, e, _ in log.events] == [("error", "rankings_refresh_failed")]
