from __future__ import annotations

from datetime import date, timedelta

import pytest

from db.models.nba.player_game_stats import PlayerGameStats
from db.models.nba.player_rolling_stats import PlayerRollingStats
from db.models.nba.player_season_stats import PlayerSeasonStats
from db.models.nba.players import Player
from pipelines.player_rolling_stats import PlayerRollingStatsPipeline
from pipelines.player_season_stats import PlayerSeasonStatsPipeline
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

