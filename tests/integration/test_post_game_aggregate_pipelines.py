from __future__ import annotations

from datetime import date, timedelta

import pytest

from db.models.nba.games import Game
from db.models.nba.player_game_stats import PlayerGameStats
from db.models.nba.player_rolling_stats import PlayerRollingStats
from db.models.nba.player_season_stats import PlayerSeasonStats
from db.models.nba.players import Player
from db.models.nba.team_stats import TeamStats
from pipelines.player_rolling_stats import PlayerRollingStatsPipeline
from pipelines.player_season_stats import PlayerSeasonStatsPipeline
from pipelines.team_stats import TeamStatsPipeline
from schemas.common import ApiStatus


@pytest.mark.integration
def test_player_season_stats_filters_unchanged_gp(integration_db) -> None:
    target_date = date(2026, 2, 20)
    prev_date = target_date - timedelta(days=1)
    season = "2025-26"

    Player.upsert_player(player_id=1, name="Unchanged Player")
    Player.upsert_player(player_id=2, name="Changed Player")

    # Prior snapshots used for GP-delta detection.
    PlayerSeasonStats.upsert_season_stats(
        player_id=1,
        as_of_date=prev_date,
        season=season,
        team_id="BOS",
        stats={
            "gp": 10,
            "fpts": 200,
            "pts": 80,
            "reb": 30,
            "ast": 20,
            "stl": 5,
            "blk": 2,
            "tov": 12,
            "min": 280,
            "fgm": 30,
            "fga": 70,
            "fg3m": 10,
            "fg3a": 25,
            "ftm": 10,
            "fta": 12,
            "rost_pct": 40.0,
        },
    )
    PlayerSeasonStats.upsert_season_stats(
        player_id=2,
        as_of_date=prev_date,
        season=season,
        team_id="NYK",
        stats={
            "gp": 9,
            "fpts": 180,
            "pts": 70,
            "reb": 25,
            "ast": 22,
            "stl": 4,
            "blk": 1,
            "tov": 10,
            "min": 255,
            "fgm": 28,
            "fga": 65,
            "fg3m": 8,
            "fg3a": 21,
            "ftm": 6,
            "fta": 8,
            "rost_pct": 25.0,
        },
    )

    pipeline = PlayerSeasonStatsPipeline()
    pipeline.espn_extractor.get_player_data = lambda: {
        "unchanged player": {"rost_pct": 41.5},
        "changed player": {"rost_pct": 29.8},
    }
    pipeline.nba_extractor.get_league_leaders = lambda *_: [
        {
            "PLAYER_ID": 1,
            "PLAYER": "Unchanged Player",
            "TEAM": "BOS",
            "GP": 10,
            "PTS": 90,
            "REB": 33,
            "AST": 23,
            "STL": 6,
            "BLK": 3,
            "TOV": 14,
            "FGM": 32,
            "FGA": 74,
            "FG3M": 11,
            "FG3A": 28,
            "FTM": 12,
            "FTA": 14,
            "MIN": 300,
        },
        {
            "PLAYER_ID": 2,
            "PLAYER": "Changed Player",
            "TEAM": "NYK",
            "GP": 10,
            "PTS": 100,
            "REB": 50,
            "AST": 40,
            "STL": 10,
            "BLK": 5,
            "TOV": 20,
            "FGM": 35,
            "FGA": 80,
            "FG3M": 15,
            "FG3A": 35,
            "FTM": 20,
            "FTA": 25,
            "MIN": 320,
        },
    ]

    result = pipeline._run_sync(date_override=target_date)
    assert result.status == ApiStatus.SUCCESS

    new_rows = list(
        PlayerSeasonStats.select().where(PlayerSeasonStats.as_of_date == target_date)
    )
    assert len(new_rows) == 1
    row = new_rows[0]
    assert row.player_id == 2
    assert row.gp == 10
    assert float(row.rost_pct) == pytest.approx(29.8, abs=1e-4)


@pytest.mark.integration
def test_season_stats_survive_espn_being_unavailable(integration_db) -> None:
    """ESPN supplies ownership, the NBA API supplies the actual season totals.

    ESPN publishes one season at a time and 404s the rest, so it is unreachable
    for weeks around the rollover. Losing the totals for that whole window
    because an enrichment lookup failed is the wrong trade.
    """
    import requests

    target_date = date(2026, 2, 14)
    Player.upsert_player(player_id=1, name="Only Player")

    pipeline = PlayerSeasonStatsPipeline()

    def espn_is_down():
        raise requests.HTTPError("404 Client Error:  for url: .../seasons/2027/...")

    pipeline.espn_extractor.get_player_data = espn_is_down
    pipeline.nba_extractor.get_league_leaders = lambda *_: [
        {
            "PLAYER_ID": 1, "PLAYER": "Only Player", "TEAM": "BOS", "GP": 10,
            "PTS": 90, "REB": 33, "AST": 23, "STL": 6, "BLK": 3, "TOV": 14,
            "FGM": 32, "FGA": 74, "FG3M": 11, "FG3A": 28, "FTM": 12, "FTA": 14,
            "MIN": 300,
        },
    ]

    result = pipeline._run_sync(date_override=target_date)

    assert result.status == ApiStatus.SUCCESS
    row = PlayerSeasonStats.get(PlayerSeasonStats.as_of_date == target_date)
    assert row.player_id == 1 and row.gp == 10 and row.pts == 90
    # Null, not 0: 0 would claim ESPN told us nobody owns him.
    assert row.rost_pct is None


@pytest.mark.integration
def test_player_rolling_stats_computes_window_percentages_from_totals(integration_db) -> None:
    target_date = date(2026, 2, 20)
    Player.upsert_player(player_id=201939, name="Stephen Curry")

    # Two games inside L7 and one extra game inside L14/L30.
    PlayerGameStats.upsert_game_stats(
        player_id=201939,
        game_date=target_date - timedelta(days=1),
        team_id="GSW",
        stats={
            "fpts": 50,
            "pts": 30,
            "reb": 5,
            "ast": 6,
            "stl": 2,
            "blk": 0,
            "tov": 3,
            "min": 35,
            "fgm": 5,
            "fga": 10,
            "fg3m": 2,
            "fg3a": 5,
            "ftm": 4,
            "fta": 5,
        },
    )
    PlayerGameStats.upsert_game_stats(
        player_id=201939,
        game_date=target_date - timedelta(days=3),
        team_id="GSW",
        stats={
            "fpts": 40,
            "pts": 25,
            "reb": 4,
            "ast": 7,
            "stl": 1,
            "blk": 1,
            "tov": 2,
            "min": 33,
            "fgm": 3,
            "fga": 4,
            "fg3m": 1,
            "fg3a": 3,
            "ftm": 2,
            "fta": 3,
        },
    )
    PlayerGameStats.upsert_game_stats(
        player_id=201939,
        game_date=target_date - timedelta(days=10),
        team_id="GSW",
        stats={
            "fpts": 30,
            "pts": 20,
            "reb": 3,
            "ast": 5,
            "stl": 1,
            "blk": 0,
            "tov": 1,
            "min": 30,
            "fgm": 4,
            "fga": 8,
            "fg3m": 1,
            "fg3a": 2,
            "ftm": 1,
            "fta": 2,
        },
    )

    pipeline = PlayerRollingStatsPipeline()
    result = pipeline._run_sync(date_override=target_date)
    assert result.status == ApiStatus.SUCCESS

    rows = list(
        PlayerRollingStats.select().where(PlayerRollingStats.as_of_date == target_date)
    )
    assert len(rows) == 3  # L7, L14, L30

    l7 = (
        PlayerRollingStats.select()
        .where(
            (PlayerRollingStats.as_of_date == target_date)
            & (PlayerRollingStats.window_days == 7)
            & (PlayerRollingStats.player_id == 201939)
        )
        .first()
    )
    assert l7 is not None
    assert l7.gp == 2
    # FG% computed from totals: (5+3)/(10+4) = 8/14 = 0.5714
    assert float(l7.fg_pct) == pytest.approx(0.5714, abs=1e-4)
    assert float(l7.fpts) == pytest.approx(45.0, abs=1e-4)



# ---------------------------------------------------------------------------
# Readiness: the season dashboards against the night's game log
# ---------------------------------------------------------------------------

NIGHT = date(2026, 4, 8)
SEASON = "2025-26"

_ZERO_LINE = {
    "fpts": 20, "pts": 10, "reb": 4, "ast": 3, "stl": 1, "blk": 0, "tov": 2,
    "min": 30, "fgm": 4, "fga": 9, "fg3m": 1, "fg3a": 3, "ftm": 1, "fta": 2,
}


def _leader(player_id: int, team: str, gp: int) -> dict:
    return {
        "PLAYER_ID": player_id, "PLAYER": f"Player {player_id}", "TEAM": team, "GP": gp,
        "PTS": 90, "REB": 33, "AST": 23, "STL": 6, "BLK": 3, "TOV": 14,
        "FGM": 32, "FGA": 74, "FG3M": 11, "FG3A": 28, "FTM": 12, "FTA": 14, "MIN": 300,
    }


def _game(game_id: str, game_date: date, home: str, away: str, status: str = "final") -> None:
    Game.create(
        game_id=game_id, game_date=game_date, season=SEASON,
        home_team_id=home, away_team_id=away, status=status,
        home_score=110 if status == "final" else None,
        away_score=104 if status == "final" else None,
    )


def _played(player_id: int, team: str, game_id: str | None, game_date: date = NIGHT) -> None:
    Player.upsert_player(player_id=player_id, name=f"Player {player_id}")
    PlayerGameStats.upsert_game_stats(
        player_id=player_id, game_date=game_date, team_id=team,
        stats=dict(_ZERO_LINE), game_id=game_id,
    )


def _season_row(player_id: int, team: str, gp: int, as_of: date) -> None:
    PlayerSeasonStats.upsert_season_stats(
        player_id=player_id, as_of_date=as_of, season=SEASON, team_id=team,
        stats={**_ZERO_LINE, "gp": gp, "rost_pct": 10.0},
    )


@pytest.mark.integration
def test_season_stats_wait_for_every_player_in_the_nights_game_log(integration_db) -> None:
    """A partial LeagueLeaders update fails the run, and the retry completes it."""
    _game("0022501180", NIGHT, "BOS", "NYK")
    _game("0022501185", NIGHT, "PHX", "LAL")
    _played(1, "BOS", "0022501180")   # early game
    _played(2, "PHX", "0022501185")   # late game
    _season_row(1, "BOS", 63, NIGHT - timedelta(days=2))
    _season_row(2, "PHX", 63, NIGHT - timedelta(days=2))

    pipeline = PlayerSeasonStatsPipeline()
    pipeline.espn_extractor.get_player_data = lambda: {}

    # First poll: the early game has reached the dashboard, the late one has not.
    pipeline.nba_extractor.get_league_leaders = lambda *_: [
        _leader(1, "BOS", 64), _leader(2, "PHX", 63),
    ]
    result = pipeline._run_sync(nba_date=NIGHT)

    assert result.status == ApiStatus.ERROR
    assert "1 of 2 players" in result.error
    assert "Data not ready yet" in result.error
    assert not PlayerSeasonStats.select().where(PlayerSeasonStats.as_of_date == NIGHT).exists()

    # Next poll: the dashboard has caught up.
    pipeline.nba_extractor.get_league_leaders = lambda *_: [
        _leader(1, "BOS", 64), _leader(2, "PHX", 64),
    ]
    result = pipeline._run_sync(nba_date=NIGHT)

    assert result.status == ApiStatus.SUCCESS
    rows = {
        row.player_id: row.gp
        for row in PlayerSeasonStats.select().where(PlayerSeasonStats.as_of_date == NIGHT)
    }
    assert rows == {1: 64, 2: 64}

    # A re-run of a night already written finds nothing new and is not held.
    result = pipeline._run_sync(nba_date=NIGHT)
    assert result.status == ApiStatus.SUCCESS
    assert result.records_processed == 0


@pytest.mark.integration
def test_team_stats_wait_for_every_team_that_played_that_night(integration_db) -> None:
    """A dashboard missing the late game fails the run before any team is written."""
    # Before the night: BOS and NYK two finals each, PHX and LAL one each.
    _game("0022501100", NIGHT - timedelta(days=3), "BOS", "NYK")
    _game("0022501110", NIGHT - timedelta(days=2), "NYK", "BOS")
    _game("0022501120", NIGHT - timedelta(days=2), "PHX", "LAL")
    # Not regular season, and not this season: neither counts.
    _game("0062500001", NIGHT - timedelta(days=1), "PHX", "LAL")
    Game.create(
        game_id="0022401100", game_date=date(2025, 4, 1), season="2024-25",
        home_team_id="PHX", away_team_id="LAL", status="final",
        home_score=101, away_score=99,
    )
    # The night: the early game is final in the schedule; the late one is
    # still "scheduled" there, and only the game log says it was played.
    _game("0022501180", NIGHT, "BOS", "NYK")
    _game("0022501185", NIGHT, "PHX", "LAL", status="scheduled")
    _played(1, "BOS", "0022501180")
    _played(2, "NYK", "0022501180")
    _played(3, "PHX", "0022501185")
    _played(4, "LAL", "0022501185")

    def dashboard(gp: dict[str, int]) -> list[dict]:
        return [
            {"TEAM_ABBREVIATION": team, "TEAM_NAME": team, "GP": games, "W": 1, "L": games - 1}
            for team, games in gp.items()
        ]

    pipeline = TeamStatsPipeline()

    pipeline.nba_extractor.get_team_stats = lambda *_: dashboard(
        {"BOS": 3, "NYK": 3, "PHX": 1, "LAL": 1, "DEN": 2}
    )
    result = pipeline._run_sync(nba_date=NIGHT)

    assert result.status == ApiStatus.ERROR
    assert "2 team(s)" in result.error
    assert "LAL, PHX" in result.error
    assert "Data not ready yet" in result.error
    assert TeamStats.select().count() == 0

    pipeline.nba_extractor.get_team_stats = lambda *_: dashboard(
        {"BOS": 3, "NYK": 3, "PHX": 2, "LAL": 2, "DEN": 2}
    )
    result = pipeline._run_sync(nba_date=NIGHT)

    assert result.status == ApiStatus.SUCCESS
    rows = {
        row.team_id: row.gp
        for row in TeamStats.select().where(TeamStats.as_of_date == NIGHT)
    }
    assert rows == {"BOS": 3, "NYK": 3, "PHX": 2, "LAL": 2, "DEN": 2}


@pytest.mark.integration
def test_team_stats_are_not_held_on_a_day_without_games(integration_db) -> None:
    """A manual run the morning after: today's games are scheduled, nothing is played."""
    _game("0022501120", NIGHT - timedelta(days=1), "PHX", "LAL")
    _game("0022501185", NIGHT, "PHX", "LAL", status="scheduled")

    pipeline = TeamStatsPipeline()
    pipeline.nba_extractor.get_team_stats = lambda *_: [
        {"TEAM_ABBREVIATION": "PHX", "GP": 1, "W": 1, "L": 0},
        {"TEAM_ABBREVIATION": "LAL", "GP": 1, "W": 0, "L": 1},
    ]
    result = pipeline._run_sync(nba_date=NIGHT)

    assert result.status == ApiStatus.SUCCESS
    assert TeamStats.select().where(TeamStats.as_of_date == NIGHT).count() == 2


@pytest.mark.integration
def test_the_cup_final_holds_neither_pipeline(integration_db) -> None:
    """The game log carries the Cup final; the season dashboards never count it.

    2025-12-16 left 18 such rows (NYK and SAS). Waiting on those players would
    fail both pipelines on every poll of that night.
    """
    _game("0062500001", NIGHT, "NYK", "SAS")
    _played(1, "NYK", "0062500001")
    _played(2, "SAS", "0062500001")
    _season_row(1, "NYK", 25, NIGHT - timedelta(days=3))
    _season_row(2, "SAS", 25, NIGHT - timedelta(days=3))

    season = PlayerSeasonStatsPipeline()
    season.espn_extractor.get_player_data = lambda: {}
    season.nba_extractor.get_league_leaders = lambda *_: [
        _leader(1, "NYK", 25), _leader(2, "SAS", 25),
    ]
    assert season._run_sync(nba_date=NIGHT).status == ApiStatus.SUCCESS

    teams = TeamStatsPipeline()
    teams.nba_extractor.get_team_stats = lambda *_: [
        {"TEAM_ABBREVIATION": "NYK", "GP": 25, "W": 18, "L": 7},
        {"TEAM_ABBREVIATION": "SAS", "GP": 25, "W": 17, "L": 8},
    ]
    assert teams._run_sync(nba_date=NIGHT).status == ApiStatus.SUCCESS
