"""
`pipelines.lineup_snapshots`: which days it captures, when it waits, and how it
treats cookies — with a fake extractor (fixture payloads), a fake store (an
in-memory dict of team-days) and fake teams. The calendar is the real 2025-26
one (`NBA_SEASON` in tests/conftest.py), so ESPN day N ↔ date arithmetic is
exercised for real.

The fixtures are a public league's day 60 (three teams) and its
`mTeam,mSettings,mMatchup` read; `status.latestScoringPeriod` is overridden
per test to put ESPN on the day each case needs.
"""

import copy
import json
from datetime import date
from pathlib import Path
from types import SimpleNamespace

import pytest

from core.crypto import CredentialDecryptionError
from pipelines import lineup_snapshots as module
from pipelines.base import DataNotReady
from pipelines.context import PipelineContext
from pipelines.extractors.espn import ProviderAuthError
from pipelines.lineup_snapshots import GAP_FILL_PERIODS, LeagueKey, LineupSnapshotsPipeline, WriteResult
from services import schedule_service

FIXTURES = Path(__file__).parent.parent / "fixtures"
ROSTERS = json.loads((FIXTURES / "espn_league_rosters_period.json").read_text())
DISCOVERY = json.loads((FIXTURES / "espn_league_discovery.json").read_text())
SEASON = 2026          # the fixtures' seasonId; tests pin settings.espn_year to it
LEAGUE = 993431466
KEY = LeagueKey(provider_league_id=str(LEAGUE), season=SEASON)
GAME_DATE = schedule_service.date_for_espn_scoring_period(60)   # ESPN day 60 of 2025-26


# ---- fakes --------------------------------------------------------------------------


def team_row(team_id, league_id=LEAGUE, year=SEASON, provider="espn", name="Flaggin it", connection=None):
    info = {"provider": provider, "league_id": league_id, "year": year, "team_name": name, "espn_s2": f"s2-{team_id}", "swid": f"{{{team_id}}}"}
    return SimpleNamespace(team_id=team_id, league_info=json.dumps(info), provider_connection_id=connection)


class FakeExtractor:
    """Answers the discovery read with the fixture at `latest`, a day read with the roster fixture."""

    def __init__(self, latest=61, final=167, refuse=()):
        self.latest, self.final, self.refuse = latest, final, set(refuse)
        self.calls = []

    def get_league(self, league_id, espn_s2, swid, year, views, scoring_period_id=None):
        self.calls.append((league_id, espn_s2, year, tuple(views), scoring_period_id))
        if espn_s2 in self.refuse:
            raise ProviderAuthError("refused", status_code=401)
        if scoring_period_id is None:
            payload = copy.deepcopy(DISCOVERY)
            payload["status"]["latestScoringPeriod"] = self.latest
            payload["status"]["finalScoringPeriod"] = self.final
            return payload
        payload = copy.deepcopy(ROSTERS)
        payload["scoringPeriodId"] = scoring_period_id
        return payload


class FakeStore:
    def __init__(self, stored_periods=()):
        self.days = {}          # (key, period) -> list[TeamDayRecord]
        self.writes = []        # (key, period, nba_date, replace, source)
        for period in stored_periods:
            self.days[(KEY, period)] = ["stale"]

    def newest_period(self, key):
        periods = [p for (k, p) in self.days if k == key]
        return max(periods) if periods else None

    def write_day(self, key, scoring_period_id, nba_date, records, *, replace, source, pipeline_run_id):
        self.writes.append((key, scoring_period_id, nba_date, replace, source))
        existed = (key, scoring_period_id) in self.days
        self.days[(key, scoring_period_id)] = records
        drift = [{"provider_team_id": r.lineup.provider_team_id, "scoring_period_id": scoring_period_id,
                  "players_before": 14, "players_after": len(r.lineup.players), "changed": 0}
                 for r in records] if (existed and replace) else []
        return WriteResult(stored=0 if (existed and not replace) else len(records), drift=drift)

    def fantasy_points_logged(self, nba_date, espn_player_ids):
        return 123.5


@pytest.fixture
def pipeline(monkeypatch):
    from core.settings import settings

    monkeypatch.setattr(settings, "espn_year", SEASON)
    monkeypatch.setattr(module.credential_service, "hydrate", lambda team, payload: payload)
    p = LineupSnapshotsPipeline()
    p.espn_extractor = FakeExtractor()
    p.store = FakeStore()
    return p


def teams(monkeypatch, *rows):
    monkeypatch.setattr(module, "_load_teams", lambda: list(rows))


def ctx(**kwargs):
    kwargs.setdefault("nba_date", GAME_DATE)
    return PipelineContext(pipeline_name="lineup_snapshots", **kwargs)


# ---- which days -----------------------------------------------------------------------


@pytest.mark.unit
class TestNightlyCapture:
    def test_captures_the_finished_day_once_espn_rolled(self, pipeline, monkeypatch):
        teams(monkeypatch, team_row(1))
        pipeline.store = FakeStore(stored_periods=range(46, 60))   # through day 59
        pipeline.espn_extractor = FakeExtractor(latest=61)         # ESPN is on day 61: day 60 is done
        c = ctx()
        pipeline.execute(c)
        assert [w[1] for w in pipeline.store.writes] == [60]
        key, period, nba_date, replace, source = pipeline.store.writes[0]
        assert (key, nba_date, replace, source) == (KEY, GAME_DATE, False, "pipeline")
        assert c.records_processed == 3 and c.records_failed == 0
        # the day read asked ESPN for that day with the roster views
        assert pipeline.espn_extractor.calls[-1] == (LEAGUE, "s2-1", SEASON, ("mTeam", "mRoster"), 60)

    def test_fills_every_missing_day_ascending_within_the_lookback(self, pipeline, monkeypatch):
        teams(monkeypatch, team_row(1))
        pipeline.store = FakeStore(stored_periods=[55])
        pipeline.espn_extractor = FakeExtractor(latest=61)
        pipeline.execute(ctx())
        assert [w[1] for w in pipeline.store.writes] == [56, 57, 58, 59, 60]

    def test_lookback_is_bounded_when_nothing_is_stored(self, pipeline, monkeypatch):
        teams(monkeypatch, team_row(1))
        pipeline.espn_extractor = FakeExtractor(latest=61)
        pipeline.execute(ctx())
        assert [w[1] for w in pipeline.store.writes] == list(range(61 - GAP_FILL_PERIODS, 61))

    def test_never_past_the_seasons_last_day(self, pipeline, monkeypatch):
        teams(monkeypatch, team_row(1))
        pipeline.store = FakeStore(stored_periods=[165])
        pipeline.espn_extractor = FakeExtractor(latest=175, final=167)   # playoffs: L runs past final
        pipeline.execute(ctx(nba_date=schedule_service.date_for_espn_scoring_period(167)))
        assert [w[1] for w in pipeline.store.writes] == [166, 167]

    def test_nothing_missing_is_a_quiet_success(self, pipeline, monkeypatch):
        teams(monkeypatch, team_row(1))
        pipeline.store = FakeStore(stored_periods=range(46, 61))   # day 60 already stored
        pipeline.espn_extractor = FakeExtractor(latest=61)
        c = ctx()
        pipeline.execute(c)
        assert pipeline.store.writes == [] and c.records_processed == 0 and c.records_failed == 0
        assert len(pipeline.espn_extractor.calls) == 1          # the discovery read only

    def test_waits_while_espn_is_still_on_the_game_day(self, pipeline, monkeypatch):
        teams(monkeypatch, team_row(1))
        pipeline.store = FakeStore(stored_periods=range(46, 60))
        pipeline.espn_extractor = FakeExtractor(latest=60)         # ESPN has not rolled past day 60
        with pytest.raises(DataNotReady):
            pipeline.execute(ctx())
        assert pipeline.store.writes == []

    def test_preseason_is_skipped_not_waited_for(self, pipeline, monkeypatch, alerts):
        teams(monkeypatch, team_row(1))
        pipeline.espn_extractor = FakeExtractor(latest=0)
        c = ctx()
        pipeline.execute(c)
        assert c.skip_reasons == {"no_scoring_period": 1} and pipeline.store.writes == []

    def test_outside_the_regular_season_is_skipped(self, pipeline, monkeypatch):
        teams(monkeypatch, team_row(1))
        c = ctx(nba_date=date(2026, 10, 1))    # a preseason date: no ESPN day
        pipeline.execute(c)
        assert c.skip_reasons == {"outside_regular_season": 1}
        assert pipeline.espn_extractor.calls == []


# ---- backfill ---------------------------------------------------------------------------


@pytest.mark.unit
class TestBackfill:
    def test_date_override_replaces_exactly_that_day(self, pipeline, monkeypatch):
        teams(monkeypatch, team_row(1))
        pipeline.store = FakeStore(stored_periods=[59, 60])
        pipeline.espn_extractor = FakeExtractor(latest=90)
        c = ctx(date_override=GAME_DATE)
        pipeline.execute(c)
        assert pipeline.store.writes == [(KEY, 60, GAME_DATE, True, "backfill")]
        assert c.records_processed == 3

    def test_a_day_espn_has_not_finished_is_refused(self, pipeline, monkeypatch):
        teams(monkeypatch, team_row(1))
        pipeline.espn_extractor = FakeExtractor(latest=60)
        c = ctx(date_override=GAME_DATE)
        pipeline.execute(c)
        assert pipeline.store.writes == [] and c.skip_reasons == {"day_not_finished": 1}


# ---- leagues and cookies -------------------------------------------------------------------


@pytest.mark.unit
class TestLeaguesAndCredentials:
    def test_one_capture_per_league_whatever_the_team_count(self, pipeline, monkeypatch):
        teams(monkeypatch, team_row(1), team_row(2, name="Steve's Smart Team"), team_row(3, league_id=555))
        pipeline.store = FakeStore(stored_periods=range(46, 60))
        pipeline.espn_extractor = FakeExtractor(latest=61)
        pipeline.execute(ctx())
        by_league = {}
        for key, period, *_ in pipeline.store.writes:
            by_league.setdefault(key.provider_league_id, []).append(period)
        assert by_league[str(LEAGUE)] == [60]                                # one capture, not one per team
        assert by_league["555"] == list(range(61 - GAP_FILL_PERIODS, 61))     # a league with nothing stored yet
        discovery_reads = [c for c in pipeline.espn_extractor.calls if c[4] is None]
        assert [(c[0], c[1]) for c in discovery_reads] == [(LEAGUE, "s2-1"), (555, "s2-3")]

    def test_refused_cookies_fall_through_to_the_next_team(self, pipeline, monkeypatch):
        teams(monkeypatch, team_row(1), team_row(2))
        pipeline.store = FakeStore(stored_periods=range(46, 60))
        pipeline.espn_extractor = FakeExtractor(latest=61, refuse={"s2-1"})
        c = ctx()
        pipeline.execute(c)
        assert [c_[1] for c_ in pipeline.espn_extractor.calls] == ["s2-1", "s2-2", "s2-2"]
        assert c.records_processed == 3 and c.records_failed == 0

    def test_every_cookie_refused_skips_the_league(self, pipeline, monkeypatch):
        teams(monkeypatch, team_row(1), team_row(2))
        pipeline.espn_extractor = FakeExtractor(latest=61, refuse={"s2-1", "s2-2"})
        c = ctx()
        pipeline.execute(c)
        assert c.skip_reasons == {"league_unreadable": 1} and c.records_failed == 0

    def test_undecryptable_credentials_fall_through(self, pipeline, monkeypatch):
        def hydrate(team, payload):
            if team.team_id == 1:
                raise CredentialDecryptionError("bad key")
            return payload

        monkeypatch.setattr(module.credential_service, "hydrate", hydrate)
        teams(monkeypatch, team_row(1, connection=7), team_row(2))
        pipeline.store = FakeStore(stored_periods=range(46, 60))
        pipeline.espn_extractor = FakeExtractor(latest=61)
        pipeline.execute(ctx())
        assert [c_[1] for c_ in pipeline.espn_extractor.calls] == ["s2-2", "s2-2"]

    def test_yahoo_and_other_seasons_are_skipped_with_a_reason(self, pipeline, monkeypatch):
        teams(monkeypatch, team_row(1, provider="yahoo"), team_row(2, year=SEASON - 1))
        c = ctx()
        pipeline.execute(c)
        assert c.skip_reasons == {"provider_unsupported": 1, "season_mismatch": 1}
        assert pipeline.espn_extractor.calls == []

    def test_one_leagues_failure_leaves_the_others_captured(self, pipeline, monkeypatch):
        teams(monkeypatch, team_row(1), team_row(3, league_id=555))
        pipeline.store = FakeStore(stored_periods=range(46, 60))
        extractor = FakeExtractor(latest=61)
        real = extractor.get_league

        def flaky(league_id, *args, **kwargs):
            if league_id == 555 and kwargs.get("scoring_period_id"):
                raise RuntimeError("boom")
            return real(league_id, *args, **kwargs)

        extractor.get_league = flaky
        pipeline.espn_extractor = extractor
        c = ctx()
        pipeline.execute(c)
        assert [w[0].provider_league_id for w in pipeline.store.writes] == [str(LEAGUE)]
        assert c.records_processed == 3 and c.failure_reasons == {"RuntimeError": 1}

    def test_every_league_failing_is_a_failed_run(self, pipeline, monkeypatch):
        teams(monkeypatch, team_row(1))
        pipeline.espn_extractor = SimpleNamespace(get_league=lambda **kw: (_ for _ in ()).throw(RuntimeError("down")))
        with pytest.raises(RuntimeError):
            pipeline.execute(ctx())


# ---- what gets stored ---------------------------------------------------------------------


@pytest.mark.unit
class TestRecords:
    def test_each_team_carries_its_matchup_and_opponent(self, pipeline, monkeypatch):
        teams(monkeypatch, team_row(1))
        pipeline.store = FakeStore(stored_periods=range(46, 60))
        pipeline.espn_extractor = FakeExtractor(latest=61)
        pipeline.execute(ctx())
        records = pipeline.store.days[(KEY, 60)]
        week = schedule_service.get_current_matchup(GAME_DATE)["matchup_number"]
        assert [r.lineup.provider_team_id for r in records] == [1, 2, 3]
        assert all(r.matchup_period_id == week for r in records)     # one-week matchups mid-season
        opponents = {r.lineup.provider_team_id: r.opponent_provider_team_id for r in records}
        assert opponents[1] is not None and opponents[opponents[1]] == 1 if opponents[1] in opponents else True
