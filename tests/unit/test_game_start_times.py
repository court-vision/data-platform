"""game_start_times: source selection, preseason/placeholder/non-NBA filters, static fallback."""

from datetime import date, time
from types import SimpleNamespace

import pytest
import requests

from core.settings import settings
from pipelines import game_start_times as gst
from pipelines.context import PipelineContext
from utils import nba_cdn

SEASON = "2025-26"
NBA_TEAMS = ("LAL", "GSW", "OKC", "HOU", "NYK")


def _game(game_id, dt, home, away, status=1, home_score=None, away_score=None):
    return {
        "gameId": game_id,
        "gameStatus": status,
        "gameDateTimeEst": dt,
        "homeTeam": {"teamTricode": home, "score": home_score},
        "awayTeam": {"teamTricode": away, "score": away_score},
    }


def _feed(season=SEASON):
    return {
        "leagueSchedule": {
            "seasonYear": season,
            "gameDates": [
                {"gameDate": "10/05/2025 00:00:00", "games": [
                    _game("0012500010", "2025-10-05T15:00:00Z", "LAL", "GSW"),          # preseason
                    _game("0012500011", "2025-10-05T12:00:00Z", "NYK", "MEL"),          # preseason vs non-NBA
                ]},
                {"gameDate": "10/21/2025 00:00:00", "games": [
                    _game("0022500001", "2025-10-21T19:30:00Z", "OKC", "HOU"),          # regular season
                    _game("0022500002", "2025-10-21T22:00:00Z", "LAL", "GSW", 3, 110, 100),  # final
                ]},
                {"gameDate": "12/09/2025 00:00:00", "games": [
                    _game("0052500101", "2025-12-09T19:00:00Z", "", ""),                # Cup placeholder
                ]},
            ],
        }
    }


class FakeNBATeam:
    id = "id"

    @staticmethod
    def select(*_args, **_kwargs):
        return [SimpleNamespace(id=t) for t in NBA_TEAMS]


@pytest.fixture
def db(monkeypatch):
    """No Postgres: fake the team lookup and record upserts in memory."""
    upserts: dict[str, dict] = {}

    class FakeGame:
        @staticmethod
        def upsert_game(game_id, game_data):
            upserts[game_id] = game_data

    monkeypatch.setattr(gst, "NBATeam", FakeNBATeam)
    monkeypatch.setattr(gst, "Game", FakeGame)
    monkeypatch.setattr(settings, "nba_season", SEASON)
    return upserts


@pytest.fixture
def sources(monkeypatch):
    """Control both feed sources; returns a recorder of which were called."""
    calls = {"cdn": 0, "static": 0}
    state = {"cdn": _feed(), "static": _feed()}

    def fetch(season, timeout=30):
        calls["cdn"] += 1
        assert season == SEASON
        if isinstance(state["cdn"], Exception):
            raise state["cdn"]
        return state["cdn"]

    def load(season):
        calls["static"] += 1
        assert season == SEASON
        if isinstance(state["static"], Exception):
            raise state["static"]
        return state["static"]

    monkeypatch.setattr(nba_cdn, "fetch_league_schedule", fetch)
    monkeypatch.setattr(nba_cdn, "load_static_schedule", load)
    return SimpleNamespace(calls=calls, state=state)


def _run(**options):
    ctx = PipelineContext("game_start_times", options=options)
    gst.GameStartTimesPipeline().execute(ctx)
    return ctx


@pytest.mark.unit
def test_cdn_default_writes_regular_season_only(db, sources):
    ctx = _run()
    assert sources.calls == {"cdn": 1, "static": 0}
    assert set(db) == {"0022500001", "0022500002"}
    assert ctx.records_processed == 2
    g = db["0022500001"]
    assert (g["game_date"], g["start_time_et"], g["season"], g["status"]) == (date(2025, 10, 21), time(19, 30), SEASON, "scheduled")
    assert (g["home_team_id"], g["away_team_id"]) == ("OKC", "HOU") and "home_score" not in g
    final = db["0022500002"]
    assert (final["status"], final["home_score"], final["away_score"]) == ("final", 110, 100)


@pytest.mark.unit
def test_include_preseason_refused_outside_development_mode(db, sources, monkeypatch):
    monkeypatch.setattr(settings, "development_mode", False)
    with pytest.raises(RuntimeError, match="DEVELOPMENT_MODE"):
        _run(include_preseason=True)
    assert db == {}


@pytest.mark.unit
def test_include_preseason_in_development_mode_keeps_nba_filter(db, sources, monkeypatch):
    monkeypatch.setattr(settings, "development_mode", True)
    ctx = _run(include_preseason=True)
    assert set(db) == {"0012500010", "0022500001", "0022500002"}   # MEL game and placeholder still skipped
    assert ctx.records_processed == 3


@pytest.mark.unit
def test_cdn_failure_falls_back_to_static(db, sources):
    sources.state["cdn"] = requests.ConnectionError("cdn down")
    _run(source="cdn")
    assert sources.calls == {"cdn": 1, "static": 1}
    assert set(db) == {"0022500001", "0022500002"}


@pytest.mark.unit
def test_cdn_wrong_season_falls_back_to_static(db, sources):
    sources.state["cdn"] = ValueError("feed is the '2024-25' schedule, expected '2025-26'")
    _run()
    assert sources.calls == {"cdn": 1, "static": 1}
    assert len(db) == 2


@pytest.mark.unit
def test_static_source_never_touches_the_cdn(db, sources):
    _run(source="static")
    assert sources.calls == {"cdn": 0, "static": 1}
    assert len(db) == 2


@pytest.mark.unit
def test_static_missing_is_a_clear_error(db, sources):
    sources.state["cdn"] = requests.HTTPError("403")
    sources.state["static"] = FileNotFoundError("no schedule_raw2025-2026.json")
    with pytest.raises(FileNotFoundError):
        _run()


@pytest.mark.unit
def test_unknown_source_rejected(db, sources):
    with pytest.raises(ValueError):
        _run(source="ftp")
    assert sources.calls == {"cdn": 0, "static": 0}


# ---- utils.nba_cdn ----------------------------------------------------------


class _Resp:
    def __init__(self, payload, status=200):
        self._payload, self.status_code = payload, status

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(f"{self.status_code}")

    def json(self):
        return self._payload


@pytest.mark.unit
def test_fetch_league_schedule_sends_browser_headers_and_checks_season(monkeypatch):
    seen = {}

    def fake_get(url, headers=None, timeout=None):
        seen.update(url=url, headers=headers, timeout=timeout)
        return _Resp(_feed("2026-27"))

    monkeypatch.setattr(nba_cdn.requests, "get", fake_get)
    assert nba_cdn.fetch_league_schedule("2026-27")["leagueSchedule"]["seasonYear"] == "2026-27"
    assert seen["url"] == nba_cdn.NBA_CDN_SCHEDULE_URL and seen["timeout"] == 30
    assert seen["headers"]["Host"] == "cdn.nba.com"
    assert "Chrome/131" in seen["headers"]["User-Agent"]
    with pytest.raises(ValueError, match="2026-27"):
        nba_cdn.fetch_league_schedule("2025-26")
    with pytest.raises(ValueError):
        nba_cdn.fetch_league_schedule("2026-2027")


@pytest.mark.unit
def test_fetch_league_schedule_raises_on_http_error(monkeypatch):
    monkeypatch.setattr(nba_cdn.requests, "get", lambda *a, **k: _Resp({}, status=403))
    with pytest.raises(requests.HTTPError):
        nba_cdn.fetch_league_schedule("2026-27")


@pytest.mark.unit
def test_static_schedule_path_and_missing_file():
    assert nba_cdn.static_schedule_path("2026-27").name == "schedule_raw2026-2027.json"
    assert nba_cdn.static_schedule_path("2025-26") == nba_cdn.STATIC_DIR / "schedule_raw2025-2026.json"
    with pytest.raises(FileNotFoundError):
        nba_cdn.load_static_schedule("2099-00")


@pytest.mark.unit
@pytest.mark.skipif(not nba_cdn.static_schedule_path("2025-26").exists(), reason="no 2025-26 raw feed")
def test_load_static_schedule_reads_checked_in_feed():
    data = nba_cdn.load_static_schedule("2025-26")
    assert data["leagueSchedule"]["seasonYear"] == "2025-26"
    assert data["leagueSchedule"]["gameDates"]


@pytest.mark.unit
def test_headers_for_url_derives_host():
    assert nba_cdn.headers_for_url("https://stats.nba.com/stats/x")["Host"] == "stats.nba.com"
    assert nba_cdn.headers_for_url("https://cdn.nba.com/static/json/liveData/scoreboard")["Host"] == "cdn.nba.com"
