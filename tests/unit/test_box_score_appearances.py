"""
Who gets a box-score row: anyone who took the floor, however briefly.

Both writers used to skip a player whose minutes truncated to 0, which is a DNP
and also a 40-second appearance. The NBA counts the second one as a game played
(season GP goes up) and counts whatever happened in it, so it is stored, with
`min` 0. The settled pipeline (`nba.player_game_stats`) and the live one
(`nba.live_player_stats`) apply the same rule, so they agree about who played.

The model writers are captured rather than run: these tests are about which
rows the pipelines choose to write.
"""

from __future__ import annotations

from datetime import date

import pandas as pd
import pytest

from db.models.nba import LiveGameScoreSnapshot, LivePlayerStats, Player, PlayerGameStats
from db.models.nba.games import Game
from pipelines.context import PipelineContext
from pipelines.live_game_stats import LiveGameStatsPipeline
from pipelines.player_game_stats import PlayerGameStatsPipeline

GAME_DATE = date(2026, 4, 12)
GAME_ID = "0022501225"


# ---------------------------------------------------------------------------
# Settled: PlayerGameLogs -> nba.player_game_stats
# ---------------------------------------------------------------------------


def _game_log(player_id: int, name: str, minutes, **stats) -> dict:
    """One PlayerGameLogs row. MIN is minutes as a float."""
    line = dict.fromkeys(
        ("PTS", "REB", "AST", "STL", "BLK", "TOV", "FGM", "FGA", "FG3M", "FG3A", "FTM", "FTA"), 0
    )
    line.update(stats)
    return {
        "GAME_ID": GAME_ID,
        "PLAYER_ID": player_id,
        "PLAYER_NAME": name,
        "TEAM_ABBREVIATION": "NYK",
        "MIN": minutes,
        **line,
    }


@pytest.fixture
def settled_rows(monkeypatch):
    """Run PlayerGameStatsPipeline over a frame; return the rows it stored, by player."""
    stored: dict[int, dict] = {}

    monkeypatch.setattr(Game, "get_games_on_date", classmethod(lambda cls, _d: [Game(game_id=GAME_ID)]))
    monkeypatch.setattr(Player, "upsert_player", classmethod(lambda cls, **kw: None))
    monkeypatch.setattr(
        PlayerGameStats,
        "upsert_game_stats",
        classmethod(lambda cls, player_id, stats, **kw: stored.__setitem__(player_id, stats)),
    )

    def run(rows: list[dict]) -> tuple[dict[int, dict], PipelineContext]:
        pipeline = PlayerGameStatsPipeline()
        pipeline.espn_extractor.get_player_data = lambda: {}
        pipeline.nba_extractor.get_game_logs = lambda *_: pd.DataFrame(rows)
        ctx = PipelineContext("player_game_stats", date_override=GAME_DATE)
        pipeline.execute(ctx)
        return stored, ctx

    return run


@pytest.mark.unit
def test_settled_stores_a_forty_second_appearance_with_its_stats(settled_rows) -> None:
    stored, ctx = settled_rows(
        [
            _game_log(1628969, "Mikal Bridges", 40 / 60, PTS=3, FGM=1, FGA=1, FG3M=1, FG3A=1),
            _game_log(1628973, "Jalen Brunson", 34.2, PTS=30, AST=6, FGM=10, FGA=20, FTM=10, FTA=10),
        ]
    )

    assert set(stored) == {1628969, 1628973}
    assert ctx.records_processed == 2

    cameo = stored[1628969]
    assert cameo["min"] == 0, "whole minutes, truncated like every other row"
    assert (cameo["pts"], cameo["fg3m"], cameo["fgm"], cameo["fga"]) == (3, 1, 1, 1)
    assert cameo["fpts"] == 5  # 3 pts + 1 three + (2*1 - 1)
    assert stored[1628973]["min"] == 34


@pytest.mark.unit
def test_settled_stores_a_sub_minute_appearance_with_an_empty_line(settled_rows) -> None:
    """A streak-keeping cameo records nothing, and is still a game played."""
    stored, _ = settled_rows([_game_log(1628969, "Mikal Bridges", 0.05)])

    assert stored[1628969]["min"] == 0
    assert stored[1628969]["fpts"] == 0


@pytest.mark.unit
@pytest.mark.parametrize("minutes", [None, float("nan"), "", 0.0, 0, "0:00"])
def test_settled_still_skips_a_player_who_did_not_play(settled_rows, minutes) -> None:
    stored, ctx = settled_rows(
        [
            _game_log(1628969, "Did Not Play", minutes),
            _game_log(1628973, "Jalen Brunson", 34.2, PTS=30),
        ]
    )

    assert set(stored) == {1628973}
    assert ctx.records_processed == 1


# ---------------------------------------------------------------------------
# Live: BoxScore -> nba.live_player_stats
# ---------------------------------------------------------------------------


def _live_player(
    person_id: int,
    name: str,
    minutes: str,
    minutes_calculated: str,
    status: str = "ACTIVE",
    **stats,
) -> dict:
    """One live BoxScore player. `minutes` keeps the seconds; `minutesCalculated` does not."""
    first, family = name.split(" ", 1)
    return {
        "status": status,
        "personId": person_id,
        "firstName": first,
        "familyName": family,
        "played": "1" if minutes != "PT00M00.00S" else "0",
        "statistics": {"minutes": minutes, "minutesCalculated": minutes_calculated, **stats},
    }


class _NoRows:
    """Stands in for a DELETE query: accepts the filter, removes nothing."""

    def where(self, *_):
        return self

    def execute(self):
        return 0


@pytest.fixture
def live_rows(monkeypatch):
    """Run LiveGameStatsPipeline over one game's players; return the rows it stored, by player."""
    stored: dict[int, dict] = {}

    monkeypatch.setattr(LivePlayerStats, "delete", classmethod(lambda cls: _NoRows()))
    monkeypatch.setattr(LiveGameScoreSnapshot, "delete", classmethod(lambda cls: _NoRows()))
    monkeypatch.setattr(LiveGameScoreSnapshot, "record_snapshot", classmethod(lambda cls, **kw: None))
    monkeypatch.setattr(Player, "upsert_player", classmethod(lambda cls, **kw: None))
    monkeypatch.setattr(
        LivePlayerStats,
        "upsert_live_stats",
        classmethod(lambda cls, player_id, stats, **kw: stored.__setitem__(player_id, stats)),
    )

    def run(home: list[dict], away: list[dict] | None = None) -> tuple[dict[int, dict], PipelineContext]:
        pipeline = LiveGameStatsPipeline()
        pipeline.nba_extractor.get_scoreboard_games = lambda _d: [
            {"game_id": GAME_ID, "game_status": 2, "period": 4, "game_clock": "PT00M40.00S"}
        ]
        pipeline.nba_extractor.get_live_box_score = lambda _id: {
            "homeTeam": {"players": home},
            "awayTeam": {"players": away or []},
        }
        ctx = PipelineContext("live_game_stats", nba_date=GAME_DATE)
        pipeline.execute(ctx)
        return stored, ctx

    return run


@pytest.mark.unit
def test_live_stores_a_forty_second_appearance_with_its_stats(live_rows) -> None:
    stored, ctx = live_rows(
        home=[
            _live_player(
                1628969, "Mikal Bridges", "PT00M40.00S", "PT00M",
                points=3, fieldGoalsMade=1, fieldGoalsAttempted=1,
                threePointersMade=1, threePointersAttempted=1,
            ),
            _live_player(1628973, "Jalen Brunson", "PT25M01.00S", "PT25M", points=21, assists=8),
        ]
    )

    assert set(stored) == {1628969, 1628973}
    assert ctx.records_processed == 2

    cameo = stored[1628969]
    assert cameo["min"] == 0, "whole minutes, truncated like every other row"
    assert (cameo["pts"], cameo["fg3m"], cameo["fgm"], cameo["fga"]) == (3, 1, 1, 1)
    assert cameo["fpts"] == 5  # the same line scores the same live and settled
    assert stored[1628973]["min"] == 25


@pytest.mark.unit
def test_live_still_skips_players_who_have_not_played(live_rows) -> None:
    stored, ctx = live_rows(
        home=[
            # Dressed, never checked in: ACTIVE with no time on the clock.
            _live_player(1, "Bench Warmer", "PT00M00.00S", "PT00M"),
            # Not dressed.
            _live_player(2, "Street Clothes", "PT00M00.00S", "PT00M", status="INACTIVE"),
            _live_player(1628973, "Jalen Brunson", "PT25M01.00S", "PT25M", points=21),
        ],
        away=[
            # No statistics block at all.
            {"status": "ACTIVE", "personId": 3, "firstName": "No", "familyName": "Line"},
        ],
    )

    assert set(stored) == {1628973}
    assert ctx.records_processed == 1


@pytest.mark.unit
def test_live_falls_back_to_whole_minutes_when_the_clock_field_is_missing(live_rows) -> None:
    """Without `minutes` only `minutesCalculated` is left: whole minutes still count."""
    player = _live_player(1628973, "Jalen Brunson", "PT25M01.00S", "PT25M", points=21)
    del player["statistics"]["minutes"]

    stored, _ = live_rows(home=[player])

    assert stored[1628973]["min"] == 25
