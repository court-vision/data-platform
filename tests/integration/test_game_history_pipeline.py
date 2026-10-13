"""game_history against Postgres: whole seasons of games and lines, in place on
a re-run, without touching what the schedule wrote; the postseason only when it
is on. Also the two model rules it leans on: a game's status never moves back,
and fantasy readers never see the history or the playoffs."""

from __future__ import annotations

from datetime import date, time, timedelta

import pytest

from db.models.nba.games import Game
from db.models.nba.player_game_stats import PlayerGameStats
from db.models.nba.player_profiles import PlayerProfile
from db.models.nba.players import Player
from pipelines.game_history import GameHistoryPipeline
from pipelines.player_rolling_stats import PlayerRollingStatsPipeline
from db.models.nba.player_rolling_stats import PlayerRollingStats
from schemas.common import ApiStatus

pytestmark = pytest.mark.integration


def _line(game_id, team, matchup, player_id, name, pts, game_date="2026-04-25T00:00:00", **extra):
    row = {
        "GAME_ID": game_id, "GAME_DATE": game_date, "MATCHUP": matchup,
        "PLAYER_ID": player_id, "PLAYER_NAME": name, "TEAM_ABBREVIATION": team,
        "MIN": 30.5, "PTS": pts, "REB": 8, "AST": 3, "STL": 1, "BLK": 2, "TOV": 2,
        "FGM": 8, "FGA": 15, "FG3M": 1, "FG3A": 3, "FTM": 3, "FTA": 4,
        "OREB": 3, "DREB": 5, "PF": 4, "PLUS_MINUS": 6,
    }
    row.update(extra)
    return row


PLAYOFF_GAME = [
    _line("0042500101", "OKC", "OKC vs. MEM", 1628983, "Shai Gilgeous-Alexander", 35),
    _line("0042500101", "OKC", "OKC vs. MEM", 1631096, "Chet Holmgren", 18),
    _line("0042500101", "MEM", "MEM @ OKC", 1628991, "Jaren Jackson Jr.", 22),
]
OLD_SEASON = [
    _line("0021200001", "BOS", "BOS @ NOH", 201143, "Al Horford", 14, game_date="2012-10-31T00:00:00"),
    _line("0021200001", "NOH", "NOH vs. BOS", 101108, "Chris Paul Namesake", 20, game_date="2012-10-31T00:00:00"),
]


def _pipeline(payloads: dict[tuple[str, str], list[dict]]) -> GameHistoryPipeline:
    pipeline = GameHistoryPipeline()
    calls = []

    def fake(season, season_type):
        calls.append((season, season_type))
        key = (season, season_type)
        if key not in payloads:
            raise RuntimeError(f"unexpected call {key}")
        return payloads[key]

    pipeline.nba_extractor.get_season_game_logs = fake
    pipeline.calls = calls
    return pipeline


@pytest.fixture(autouse=True)
def no_pause(monkeypatch):
    monkeypatch.setattr("pipelines.game_history.PAUSE_BETWEEN_CALLS", 0)


def test_a_season_type_writes_its_games_players_and_lines():
    Player.create(id=1628983, name="Shai Gilgeous-Alexander", name_normalized="shai gilgeous-alexander", espn_id=4278073)
    pipeline = _pipeline({("2025-26", "Playoffs"): PLAYOFF_GAME})

    result = pipeline._run_sync(options={"seasons": ["2025-26"], "season_types": ["playoffs"]})

    assert result.status == ApiStatus.SUCCESS and result.records_processed == 3
    game = Game.get_by_id("0042500101")
    assert (game.home_team_id, game.away_team_id, game.home_score, game.away_score) == ("OKC", "MEM", 53, 22)
    assert (game.status, game.season, game.season_type) == ("final", "2025-26", "playoffs")
    lines = list(PlayerGameStats.select().order_by(PlayerGameStats.player))
    assert {(l.season, l.season_type, l.game_id, l.game_date) for l in lines} == {
        ("2025-26", "playoffs", "0042500101", date(2026, 4, 25))
    }
    assert (lines[0].oreb, lines[0].dreb, lines[0].pf, lines[0].plus_minus) == (3, 5, 4, 6)
    # A player already known keeps his ESPN id; new ones come in without one.
    assert Player.get_by_id(1628983).espn_id == 4278073
    assert Player.get_by_id(1631096).espn_id is None


def test_a_rerun_updates_in_place_and_keeps_what_the_schedule_wrote():
    Game.create(game_id="0042500101", game_date=date(2026, 4, 25), season="2025-26", home_team_id="OKC",
                away_team_id="MEM", status="scheduled", start_time_et=time(19, 30))
    _pipeline({("2025-26", "Playoffs"): PLAYOFF_GAME})._run_sync(
        options={"seasons": ["2025-26"], "season_types": ["playoffs"]})
    corrected = [dict(row, PTS=36) if row["PLAYER_ID"] == 1628983 else row for row in PLAYOFF_GAME]

    _pipeline({("2025-26", "Playoffs"): corrected})._run_sync(
        options={"seasons": ["2025-26"], "season_types": ["playoffs"]})

    assert PlayerGameStats.select().count() == 3
    assert PlayerGameStats.get(PlayerGameStats.player == 1628983).pts == 36
    game = Game.get_by_id("0042500101")
    assert (game.status, game.home_score, game.start_time_et) == ("final", 54, time(19, 30))


def test_an_old_season_lands_under_todays_team():
    result = _pipeline({("2012-13", "Regular Season"): OLD_SEASON})._run_sync(
        options={"seasons": ["2012-13"], "season_types": ["regular"]})

    assert result.status == ApiStatus.SUCCESS
    game = Game.get_by_id("0021200001")
    assert (game.home_team_id, game.season, game.season_type) == ("NOP", "2012-13", "regular")
    assert {l.team_id for l in PlayerGameStats.select()} == {"BOS", "NOP"}


def test_a_neutral_site_game_keeps_each_teams_score():
    """The schedule says DET hosted; the box scores (Mexico City) say DAL did."""
    Game.create(game_id="0022500147", game_date=date(2025, 11, 1), season="2025-26", home_team_id="DET",
                away_team_id="DAL", status="scheduled", start_time_et=time(19, 0))
    lines = [
        _line("0022500147", "DAL", "DAL vs. DET", 1, "Dal One", 110, game_date="2025-11-01T00:00:00"),
        _line("0022500147", "DET", "DET @ DAL", 2, "Det One", 122, game_date="2025-11-01T00:00:00"),
    ]

    _pipeline({("2025-26", "Regular Season"): lines})._run_sync(
        options={"seasons": ["2025-26"], "season_types": ["regular"]})

    game = Game.get_by_id("0022500147")
    assert (game.home_team_id, game.home_score, game.away_team_id, game.away_score) == ("DET", 122, "DAL", 110)


def test_a_neutral_site_game_whose_lines_both_claim_home_is_stored():
    """The 2025-26 Cup semifinal in Las Vegas: both teams' lines say "vs."."""
    Game.create(game_id="0022501229", game_date=date(2025, 12, 13), season="2025-26", home_team_id="ORL",
                away_team_id="NYK", status="final", home_score=None, away_score=None)
    lines = [
        _line("0022501229", "NYK", "NYK vs. ORL", 1, "Nyk One", 132, game_date="2025-12-13T00:00:00"),
        _line("0022501229", "ORL", "ORL vs. NYK", 2, "Orl One", 120, game_date="2025-12-13T00:00:00"),
    ]

    result = _pipeline({("2025-26", "Regular Season"): lines})._run_sync(
        options={"seasons": ["2025-26"], "season_types": ["regular"]})

    assert result.records_processed == 2
    game = Game.get_by_id("0022501229")
    assert (game.home_team_id, game.home_score, game.away_team_id, game.away_score) == ("ORL", 120, "NYK", 132)


def test_preseason_and_all_star_lines_are_never_stored():
    """No SeasonType returns every type for a whole season; whatever comes, only these load."""
    rows = PLAYOFF_GAME + [
        _line("0012500001", "OKC", "OKC vs. MEM", 1628983, "Shai Gilgeous-Alexander", 20),
        _line("0032500001", "OKC", "OKC vs. MEM", 1628983, "Shai Gilgeous-Alexander", 20),
    ]

    result = _pipeline({("2025-26", "Playoffs"): rows})._run_sync(
        options={"seasons": ["2025-26"], "season_types": ["playoffs"]})

    assert result.records_processed == 3
    assert {l.season_type for l in PlayerGameStats.select()} == {"playoffs"}
    assert not Game.select().where(Game.game_id.in_(["0012500001", "0032500001"])).exists()


def test_a_failed_call_is_counted_and_the_rest_still_load():
    pipeline = _pipeline({("2025-26", "Playoffs"): PLAYOFF_GAME})  # PlayIn raises

    result = pipeline._run_sync(options={"seasons": ["2025-26"], "season_types": ["play_in", "playoffs"]})

    assert result.records_failed == 1 and result.records_processed == 3


class TestPostseasonMode:
    def _season_with_last_regular_night(self, last: date) -> None:
        Game.create(game_id="0022500999", game_date=last, season="2025-26", home_team_id="BOS",
                    away_team_id="NYK", status="final")

    def test_idle_while_the_regular_season_is_on(self, monkeypatch):
        monkeypatch.setattr("pipelines.game_history.settings.nba_season", "2025-26")
        self._season_with_last_regular_night(date(2026, 4, 12))
        pipeline = _pipeline({})

        result = pipeline._run_sync(date_override=date(2026, 4, 12))

        assert result.status == ApiStatus.SUCCESS and pipeline.calls == []

    def test_loads_this_seasons_play_in_and_playoffs_once_it_is_over(self, monkeypatch):
        monkeypatch.setattr("pipelines.game_history.settings.nba_season", "2025-26")
        self._season_with_last_regular_night(date(2026, 4, 12))
        pipeline = _pipeline({("2025-26", "PlayIn"): [], ("2025-26", "Playoffs"): PLAYOFF_GAME})

        result = pipeline._run_sync(date_override=date(2026, 4, 25))

        assert pipeline.calls == [("2025-26", "PlayIn"), ("2025-26", "Playoffs")]
        assert result.records_processed == 3

    def test_a_failed_call_fails_the_nightly_run_so_it_alerts(self, monkeypatch):
        monkeypatch.setattr("pipelines.game_history.settings.nba_season", "2025-26")
        self._season_with_last_regular_night(date(2026, 4, 12))
        pipeline = _pipeline({("2025-26", "Playoffs"): PLAYOFF_GAME})   # PlayIn raises

        result = pipeline._run_sync(date_override=date(2026, 4, 25))

        assert result.status == ApiStatus.ERROR and "2025-26 play_in" in (result.error or "")
        assert PlayerGameStats.select().count() == 3   # the playoffs still landed


def test_a_finished_game_never_goes_back_to_scheduled():
    """2026-09-28: a stale schedule copy reset 411 finished games."""
    Game.create(game_id="0022500500", game_date=date(2026, 2, 20), season="2025-26", home_team_id="BOS",
                away_team_id="NYK", status="final", home_score=110, away_score=101)

    Game.upsert_game("0022500500", {"status": "scheduled", "start_time_et": time(19, 0)})

    game = Game.get_by_id("0022500500")
    assert (game.status, game.home_score, game.start_time_et) == ("final", 110, time(19, 0))
    assert game.season_type == "regular"
    Game.upsert_game("0022500501", {"game_date": date(2026, 2, 21), "season": "2025-26",
                                    "home_team_id": "LAL", "away_team_id": "GSW", "status": "scheduled"})
    Game.upsert_game("0022500501", {"status": "in_progress"})
    assert Game.get_by_id("0022500501").status == "in_progress"


def test_fantasy_readers_leave_out_the_playoffs():
    """player_rolling_stats over a window that holds a regular-season game and a playoff game."""
    night = date(2026, 4, 25)
    Player.create(id=1628983, name="Shai Gilgeous-Alexander", name_normalized="shai gilgeous-alexander")
    PlayerProfile.create(player=1628983)
    Game.create(game_id="0022500998", game_date=night - timedelta(days=13), season="2025-26",
                home_team_id="OKC", away_team_id="MEM", status="final")
    PlayerGameStats.upsert_game_stats(1628983, night - timedelta(days=13), {"fpts": 40, "pts": 30}, team_id="OKC",
                                      game_id="0022500998")
    _pipeline({("2025-26", "Playoffs"): PLAYOFF_GAME})._run_sync(
        options={"seasons": ["2025-26"], "season_types": ["playoffs"]})

    PlayerRollingStatsPipeline()._run_sync(date_override=night)

    row = PlayerRollingStats.get((PlayerRollingStats.player == 1628983) & (PlayerRollingStats.window_days == 14))
    assert (row.gp, float(row.pts)) == (1, 30.0)
    regular = PlayerGameStats.get(PlayerGameStats.game == "0022500998")
    assert (regular.season, regular.season_type) == ("2025-26", "regular")
