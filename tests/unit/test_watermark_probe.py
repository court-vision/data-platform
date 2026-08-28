"""
The ESPN watermark probe (docs/PENDING_PROD_CHECKS.md #4).

It answers whether ESPN advances `latestScoringPeriod` before or after
`totalPoints` absorbs the previous day — the one assumption the live-score
watermark design rests on. Because it runs inside the post-game poll, the thing
that matters most is that it can never affect whether a pipeline runs.
"""

from types import SimpleNamespace

from freezegun import freeze_time

import pytest

from api.v1 import pipelines


def _payload(period=135, matchup_period=22, home=1234.5, away=1200.0):
    return {
        "status": {"latestScoringPeriod": period, "currentMatchupPeriod": matchup_period},
        "schedule": [
            {"matchupPeriodId": matchup_period - 1, "home": {"totalPoints": 1.0}, "away": {"totalPoints": 2.0}},
            {"matchupPeriodId": matchup_period, "home": {"totalPoints": home}, "away": {"totalPoints": away}},
        ],
    }


@pytest.fixture
def league(monkeypatch):
    monkeypatch.setattr(
        pipelines, "_first_espn_league",
        lambda: (SimpleNamespace(team_id=1), {"year": 2027, "league_id": 5, "espn_s2": "s", "swid": "w"}),
    )


@pytest.fixture
def logged(monkeypatch):
    records = []
    monkeypatch.setattr(pipelines.log, "info", lambda event, **kw: records.append((event, kw)))
    monkeypatch.setattr(pipelines.log, "warning", lambda event, **kw: records.append((event, kw)))
    return records


@pytest.mark.unit
class TestWatermarkProbe:
    def test_logs_the_period_beside_the_totals(self, league, logged, monkeypatch):
        monkeypatch.setattr(pipelines, "_fetch_espn_league", lambda li, views: _payload())
        assert pipelines.probe_espn_watermark() == 135
        event, fields = next(r for r in logged if r[0] == "espn_watermark_probe")
        assert fields["latest_scoring_period"] == 135
        assert fields["current_matchup_period"] == 22
        # Both must be on the same line, or Δ cannot be computed from the logs.
        assert fields["home_total_points"] == 1234.5
        assert fields["away_total_points"] == 1200.0

    def test_requests_both_views(self, league, logged, monkeypatch):
        seen = {}
        def fake(li, views):
            seen["views"] = views
            return _payload()
        monkeypatch.setattr(pipelines, "_fetch_espn_league", fake)
        pipelines.probe_espn_watermark()
        # mSettings alone carries no totalPoints, which is the whole measurement.
        assert "mMatchup" in seen["views"] and "mSettings" in seen["views"]

    def test_picks_the_current_matchup_period_not_the_first(self, league, logged, monkeypatch):
        monkeypatch.setattr(pipelines, "_fetch_espn_league", lambda li, views: _payload(home=99.0))
        pipelines.probe_espn_watermark()
        _, fields = next(r for r in logged if r[0] == "espn_watermark_probe")
        assert fields["home_total_points"] == 99.0

    @pytest.mark.parametrize("payload", [
        {},                                            # empty body
        {"status": {}},                                # no scoring period
        {"status": {"latestScoringPeriod": 0}},        # preseason
        {"status": {"latestScoringPeriod": 7}},        # no schedule block
    ])
    def test_degrades_on_thin_payloads(self, league, logged, monkeypatch, payload):
        monkeypatch.setattr(pipelines, "_fetch_espn_league", lambda li, views: payload)
        pipelines.probe_espn_watermark()  # must not raise

    def test_never_raises_when_espn_fails(self, league, logged, monkeypatch):
        def boom(li, views):
            raise RuntimeError("ESPN is down")
        monkeypatch.setattr(pipelines, "_fetch_espn_league", boom)
        assert pipelines.probe_espn_watermark() is None
        assert any(r[0] == "espn_watermark_probe_failed" for r in logged)

    def test_no_espn_league_is_not_an_error(self, logged, monkeypatch):
        monkeypatch.setattr(pipelines, "_first_espn_league", lambda: (None, None))
        assert pipelines.probe_espn_watermark() is None


@pytest.mark.unit
class TestGateReusesTheProbe:
    def test_gate_does_not_refetch_when_given_a_period(self, league, monkeypatch):
        """Instrumentation must not double the request rate against ESPN.

        Frozen at 01:00 CST deliberately: past 02:30 the gate returns True before
        it fetches anything, so a naive "no calls were made" assertion would pass
        without ever reaching the code under test.
        """
        import db.models.stats.daily_matchup_score as dms

        reached = []
        monkeypatch.setattr(
            pipelines, "_first_espn_league",
            lambda: reached.append("looked-up") or (SimpleNamespace(team_id=1), {"league_id": 5}),
        )
        fetches = []
        monkeypatch.setattr(pipelines, "_fetch_espn_league",
                            lambda li, views: fetches.append(views) or _payload())

        class _NoBaseline:
            # The gate builds `where(DailyMatchupScore.team_id == ...)` and
            # `order_by(DailyMatchupScore.date.desc())`, so both must resolve.
            team_id = SimpleNamespace(__eq__=lambda self, other: True)
            date = SimpleNamespace(desc=lambda: None)

            @classmethod
            def select(cls):
                return cls()
            def where(self, *_): return self
            def order_by(self, *_): return self
            def first(self): return None
        monkeypatch.setattr(dms, "DailyMatchupScore", _NoBaseline)

        with freeze_time("2026-03-05T07:00:00Z"):  # 01:00 CST, before the fallback
            pipelines._espn_scoring_period_advanced(known_period=135)

        assert reached, "gate short-circuited before the fetch path — the test proves nothing"
        assert fetches == [], "gate refetched despite being handed the probe's value"

    def test_gate_still_fetches_when_no_period_is_supplied(self, league, monkeypatch):
        """The reuse path must not have removed the gate's own ability to look."""
        import db.models.stats.daily_matchup_score as dms

        fetches = []
        monkeypatch.setattr(pipelines, "_fetch_espn_league",
                            lambda li, views: fetches.append(views) or _payload())

        class _NoBaseline:
            # The gate builds `where(DailyMatchupScore.team_id == ...)` and
            # `order_by(DailyMatchupScore.date.desc())`, so both must resolve.
            team_id = SimpleNamespace(__eq__=lambda self, other: True)
            date = SimpleNamespace(desc=lambda: None)

            @classmethod
            def select(cls):
                return cls()
            def where(self, *_): return self
            def order_by(self, *_): return self
            def first(self): return None
        monkeypatch.setattr(dms, "DailyMatchupScore", _NoBaseline)

        with freeze_time("2026-03-05T07:00:00Z"):
            pipelines._espn_scoring_period_advanced()

        assert fetches == ["mSettings"], f"expected one mSettings fetch, got {fetches}"
