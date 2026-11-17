from __future__ import annotations

from datetime import date

import pandas as pd
import pytest

from db.models.nba.games import Game
from db.models.nba.player_game_stats import PlayerGameStats
from db.models.nba.players import Player
from pipelines.base import DataNotReady
from pipelines.context import PipelineContext
from pipelines.player_game_stats import PlayerGameStatsPipeline
from schemas.common import ApiStatus


def _stats_df(game_id: str = "0022500001") -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "GAME_ID": game_id,
                "PLAYER_ID": 201939,
                "PLAYER_NAME": "Stephen Curry",
                "TEAM_ABBREVIATION": "GSW",
                "MIN": "34:12",
                "PTS": 30,
                "REB": 5,
                "AST": 6,
                "STL": 2,
                "BLK": 0,
                "TOV": 3,
                "FGM": 10,
                "FGA": 20,
                "FG3M": 5,
                "FG3A": 12,
                "FTM": 5,
                "FTA": 6,
            }
        ]
    )


def _seed_expected_game(game_date: date, game_id: str) -> None:
    Game.create(
        game_id=game_id,
        game_date=game_date,
        season="2025-26",
        home_team_id="GSW",
        away_team_id="LAL",
        status="final",
        home_score=120,
        away_score=115,
    )


@pytest.mark.integration
def test_player_game_stats_pipeline_fails_on_incomplete_games(integration_db) -> None:
    game_date = date(2026, 2, 14)
    _seed_expected_game(game_date, "0022500001")
    _seed_expected_game(game_date, "0022500002")

    pipeline = PlayerGameStatsPipeline()
    pipeline.espn_extractor.get_player_data = lambda: {}
    pipeline.nba_extractor.get_game_logs = lambda *_: _stats_df(game_id="0022500001")

    result = pipeline._run_sync(date_override=game_date)

    assert result.status == ApiStatus.ERROR
    assert result.error is not None
    assert "Data not ready yet" in result.error
    assert PlayerGameStats.select().count() == 0


@pytest.mark.integration
def test_a_game_log_that_has_not_landed_is_waiting_not_a_crash(integration_db, alerts) -> None:
    """Both of its readiness checks: some of the night's games missing, and all of them."""
    game_date = date(2026, 2, 14)
    _seed_expected_game(game_date, "0022500001")
    _seed_expected_game(game_date, "0022500002")

    pipeline = PlayerGameStatsPipeline()
    pipeline.espn_extractor.get_player_data = lambda: {}
    pipeline.nba_extractor.get_game_logs = lambda *_: _stats_df(game_id="0022500001")

    result = pipeline._run_sync(nba_date=game_date)

    assert result.status == ApiStatus.ERROR
    assert alerts.keys() == ["pipeline_waiting:player_game_stats"]
    assert alerts.events[0].severity == "warning"

    pipeline.nba_extractor.get_game_logs = lambda *_: pd.DataFrame()
    with pytest.raises(DataNotReady, match="no stats"):
        pipeline.execute(PipelineContext("player_game_stats", nba_date=game_date))


@pytest.mark.integration
def test_player_game_stats_pipeline_upserts_idempotently(integration_db) -> None:
    game_date = date(2026, 2, 14)
    _seed_expected_game(game_date, "0022500001")

    pipeline = PlayerGameStatsPipeline()
    pipeline.espn_extractor.get_player_data = lambda: {
        "stephen curry": {"espn_id": 3975, "rost_pct": 99.1}
    }
    pipeline.nba_extractor.get_game_logs = lambda *_: _stats_df(game_id="0022500001")

    first = pipeline._run_sync(date_override=game_date)
    second = pipeline._run_sync(date_override=game_date)

    assert first.status == ApiStatus.SUCCESS
    assert second.status == ApiStatus.SUCCESS
    assert Player.select().count() == 1
    assert PlayerGameStats.select().count() == 1

    row = PlayerGameStats.select().first()
    assert row is not None
    assert row.player_id == 201939
    assert row.team_id == "GSW"
    assert row.game_date == game_date
    assert row.fpts == 53
    # The payload's GAME_ID is stored, so readers identify the fixture by the
    # game rather than inferring it from (game_date, team).
    assert row.game_id == "0022500001"


@pytest.mark.integration
def test_a_game_the_schedule_has_not_caught_up_with_still_lands(integration_db) -> None:
    """The stats pipeline can outrun the schedule pipeline.

    `game_id` is a foreign key, so storing an id `nba.games` has never heard of
    would fail the whole row. The line is worth more than the label: it lands
    keyed by date, and a later run promotes it once the game exists.
    """
    game_date = date(2026, 2, 14)
    _seed_expected_game(game_date, "0022500001")

    pipeline = PlayerGameStatsPipeline()
    pipeline.espn_extractor.get_player_data = lambda: {
        "stephen curry": {"espn_id": 3975, "rost_pct": 99.1}
    }
    # The box score names a game the schedule does not have yet.
    pipeline.nba_extractor.get_game_logs = lambda *_: _stats_df(game_id="0022500001")
    Game.delete().execute()

    result = pipeline._run_sync(date_override=game_date)

    assert result.status == ApiStatus.SUCCESS
    row = PlayerGameStats.select().first()
    assert row is not None and row.game_id is None
    assert row.game_date == game_date, "the box score survived without its game"


@pytest.mark.integration
def test_a_sub_minute_appearance_is_a_stored_game(integration_db) -> None:
    """Forty seconds truncates to 0 minutes and is still a game the NBA counts.

    The season GP goes up for it, so a missing row here is a game the season
    table has and the game log does not. A player who did not play (no minutes
    at all) is still left out.
    """
    game_date = date(2026, 2, 14)
    _seed_expected_game(game_date, "0022500001")

    stats = pd.concat(
        [_stats_df(game_id="0022500001")] * 3, ignore_index=True
    ).astype({"MIN": object})
    counting = ["PTS", "REB", "AST", "STL", "BLK", "TOV", "FGM", "FGA", "FG3M", "FG3A", "FTM", "FTA"]
    # PlayerGameLogs reports MIN as minutes, a float: forty seconds, with a three.
    stats.loc[1, ["PLAYER_ID", "PLAYER_NAME", "MIN"]] = [1628969, "Mikal Bridges", 40 / 60]
    stats.loc[1, counting] = 0
    stats.loc[1, ["PTS", "FGM", "FGA", "FG3M", "FG3A"]] = [3, 1, 1, 1, 1]
    stats.loc[2, ["PLAYER_ID", "PLAYER_NAME", "MIN"]] = [203110, "Draymond Green", 0.0]
    stats.loc[2, counting] = 0

    pipeline = PlayerGameStatsPipeline()
    pipeline.espn_extractor.get_player_data = lambda: {}
    pipeline.nba_extractor.get_game_logs = lambda *_: stats

    result = pipeline._run_sync(date_override=game_date)

    assert result.status == ApiStatus.SUCCESS
    stored = {row.player_id: row for row in PlayerGameStats.select()}
    assert set(stored) == {201939, 1628969}, "the cameo is in, the DNP is out"

    cameo = stored[1628969]
    assert cameo.min == 0
    assert (cameo.pts, cameo.fg3m, cameo.fpts) == (3, 1, 5)
    assert cameo.game_id == "0022500001"


@pytest.mark.integration
def test_a_playoff_night_is_not_held_for_games_this_pipeline_does_not_fetch(integration_db) -> None:
    """game_history stores play-in and playoff games; this pipeline's call returns none of them."""
    game_date = date(2026, 4, 25)
    _seed_expected_game(game_date, "0042500101")

    pipeline = PlayerGameStatsPipeline()
    pipeline.espn_extractor.get_player_data = lambda: {}
    pipeline.nba_extractor.get_game_logs = lambda *_: pd.DataFrame()

    result = pipeline._run_sync(date_override=game_date)

    assert result.status == ApiStatus.SUCCESS
    assert PlayerGameStats.select().count() == 0


def _night_with_tacko_fall(game_id: str = "0022500001") -> pd.DataFrame:
    """Curry's line plus Tacko Fall's: the 2026-11-16 box score that failed every run."""
    stats = pd.concat([_stats_df(game_id=game_id)] * 2, ignore_index=True)
    stats.loc[1, ["PLAYER_ID", "PLAYER_NAME", "TEAM_ABBREVIATION", "MIN"]] = [1629605, "Tacko Fall", "LAL", "2:00"]
    return stats


TACKO_ESPN_ID = 3904625
ESPN_FOR_THE_NIGHT = {
    "stephen curry": {"espn_id": 3975, "rost_pct": 99.1},
    "tacko fall": {"espn_id": TACKO_ESPN_ID, "rost_pct": 0.1},
}


@pytest.mark.integration
def test_an_espn_id_another_row_holds_does_not_fail_the_night(integration_db, alerts) -> None:
    """nba.players.espn_id is unique; a row the box score never touches can hold the id.

    2026-11-17: ESPN listed Tacko Fall under 3904625, another row already held it,
    and the IntegrityError failed every post-game run, so the night never landed.
    Now his line lands without the id, the holder keeps it, and ops hears once.
    """
    game_date = date(2026, 2, 14)
    _seed_expected_game(game_date, "0022500001")
    # The live box score wrote his row during the game (no ESPN id there).
    Player.create(id=1629605, name="Tacko Fall", name_normalized="tacko fall")
    Player.create(id=99, name="Stale Row", name_normalized="stale row", espn_id=TACKO_ESPN_ID)

    pipeline = PlayerGameStatsPipeline()
    pipeline.espn_extractor.get_player_data = lambda: ESPN_FOR_THE_NIGHT
    pipeline.nba_extractor.get_game_logs = lambda *_: _night_with_tacko_fall()

    first = pipeline._run_sync(date_override=game_date)
    second = pipeline._run_sync(date_override=game_date)

    assert first.status == ApiStatus.SUCCESS
    assert second.status == ApiStatus.SUCCESS
    assert {row.player_id for row in PlayerGameStats.select()} == {201939, 1629605}
    assert Player.get_by_id(201939).espn_id == 3975, "everyone else still gets his id"
    assert Player.get_by_id(1629605).espn_id is None
    assert Player.get_by_id(99).espn_id == TACKO_ESPN_ID, "a name match never takes an id off another row"

    assert alerts.keys() == [f"espn_id_taken:{TACKO_ESPN_ID}"], "once, not every retry"
    event = alerts.events[0]
    assert event.severity == "warning"
    assert event.fields["player_id"] == 1629605
    assert event.fields["held_by_player_id"] == 99


@pytest.mark.integration
def test_clearing_the_wrong_rows_id_lets_the_next_run_give_it_to_him(integration_db, alerts) -> None:
    """The cleanup the alert asks for: null the holder's espn_id, and the next run claims it."""
    game_date = date(2026, 2, 14)
    _seed_expected_game(game_date, "0022500001")
    Player.create(id=99, name="Stale Row", name_normalized="stale row", espn_id=TACKO_ESPN_ID)

    pipeline = PlayerGameStatsPipeline()
    pipeline.espn_extractor.get_player_data = lambda: ESPN_FOR_THE_NIGHT
    pipeline.nba_extractor.get_game_logs = lambda *_: _night_with_tacko_fall()

    assert pipeline._run_sync(date_override=game_date).status == ApiStatus.SUCCESS
    assert Player.get_by_id(1629605).espn_id is None, "a player new to nba.players lands too"

    Player.update(espn_id=None).where(Player.id == 99).execute()
    assert pipeline._run_sync(date_override=game_date).status == ApiStatus.SUCCESS

    assert Player.get_by_id(1629605).espn_id == TACKO_ESPN_ID
    assert PlayerGameStats.select().count() == 2
