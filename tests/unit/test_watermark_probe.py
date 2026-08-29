"""
The ESPN watermark probe (docs/PENDING_PROD_CHECKS.md #4).

It answers whether ESPN advances `latestScoringPeriod` before or after
`totalPoints` absorbs the previous day — the one assumption the live-score
watermark design rests on. Because it runs inside the post-game poll, the thing
that matters most is that it can never affect whether a pipeline runs.
"""

from datetime import date
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
        observed = pipelines.probe_espn_watermark()
        assert observed.latest_scoring_period == 135
        # The gate needs the matchup period too — it scopes the stored
        # watermarks it compares against to the period ESPN is currently on.
        assert observed.current_matchup_period == 22
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


@pytest.fixture
def stored(monkeypatch):
    """Stand in for the two DB reads the gate makes, so it stays a unit test."""
    def _install(baselines):
        monkeypatch.setattr(pipelines, "_espn_team_ids", lambda: list(baselines))
        monkeypatch.setattr(
            pipelines, "_espn_baselines", lambda ids, period: dict(baselines)
        )
    return _install


@pytest.mark.unit
class TestGateReusesTheProbe:
    def test_gate_does_not_refetch_when_given_an_observation(self, league, stored, monkeypatch):
        """Instrumentation must not double the request rate against ESPN."""
        reached = []
        monkeypatch.setattr(
            pipelines, "_first_espn_league",
            lambda: reached.append("looked-up") or (SimpleNamespace(team_id=1), {"league_id": 5}),
        )
        fetches = []
        monkeypatch.setattr(pipelines, "_fetch_espn_league",
                            lambda li, views: fetches.append(views) or _payload())
        stored({1: 134})

        with freeze_time("2026-03-05T07:00:00Z"):  # 01:00 CST, before the fallback
            decision = pipelines._espn_scoring_period_advanced(
                date(2026, 3, 4), pipelines.EspnObservation(135, 22)
            )

        assert reached, "gate short-circuited before the fetch path — the test proves nothing"
        assert fetches == [], "gate refetched despite being handed the probe's value"
        assert decision.run and decision.reason == "period_advanced"

    def test_gate_still_fetches_when_no_observation_is_supplied(self, league, stored, monkeypatch):
        """The reuse path must not have removed the gate's own ability to look."""
        fetches = []
        monkeypatch.setattr(pipelines, "_fetch_espn_league",
                            lambda li, views: fetches.append(views) or _payload())
        stored({1: 134})

        with freeze_time("2026-03-05T07:00:00Z"):
            decision = pipelines._espn_scoring_period_advanced(date(2026, 3, 4))

        assert fetches == ["mSettings"], f"expected one mSettings fetch, got {fetches}"
        assert decision.run

    def test_espn_unreachable_waits_rather_than_running(self, league, stored, monkeypatch):
        """An outage used to mean a failing run every 15 minutes all night.

        The gate and the pipeline call the same ESPN API, so a gate that cannot
        reach it has no reason to believe the pipeline could.
        """
        def boom(li, views):
            raise RuntimeError("ESPN is down")
        monkeypatch.setattr(pipelines, "_fetch_espn_league", boom)
        stored({1: 134})

        with freeze_time("2026-03-05T07:00:00Z"):  # 01:00 CST
            decision = pipelines._espn_scoring_period_advanced(date(2026, 3, 4))

        assert not decision.run
        assert decision.reason == "espn_unavailable_waiting"

    def test_matchup_period_scopes_the_stored_watermarks(self, league, monkeypatch):
        """The period ESPN reports is the period the baselines are read from."""
        monkeypatch.setattr(pipelines, "_fetch_espn_league", lambda li, views: _payload())
        monkeypatch.setattr(pipelines, "_espn_team_ids", lambda: [1])
        seen = {}
        def baselines(ids, period):
            seen["period"] = period
            return {1: 134}
        monkeypatch.setattr(pipelines, "_espn_baselines", baselines)

        with freeze_time("2026-03-05T07:00:00Z"):
            pipelines._espn_scoring_period_advanced(date(2026, 3, 4))

        assert seen["period"] == 22
