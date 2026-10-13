"""
When the live-stats pipeline stops re-reading a finished game.

cron-runner polls live-stats every 30 s until 4 AM ET. Until 2026-10 every
poll after the last final fetched every box score again and rewrote every row
(about 138 rows a run, half of all DB execution time in prod). A game is now
*settled*, and skipped, once a successful run has stored its box score at
least `live_stats_settle_minutes` after the scoreboard first called it final
and none of its rows is still live (`pipelines/live_game_stats.py`,
`stored_final_games`).

What must not happen is a game going quiet with stats that are not its final
ones. The case that matters is a row marked final by the endpoint's finalize
helpers, which set `game_status` and `last_updated` without reading a box
score: by its timestamp alone that row looks final and freshly written.

These run the real pipeline against Postgres, one poll at a time under
freeze_time, with the NBA extractor faked.
"""

from __future__ import annotations

from datetime import date

import pytest
from freezegun import freeze_time

from core.settings import settings
from db.base import db
from db.models.nba.live_game_score_snapshots import LiveGameScoreSnapshot
from db.models.nba.live_player_stats import LivePlayerStats
from db.models.nba.players import Player
from pipelines.live_game_stats import (
    STORED_FINAL_GAMES_SQL,
    LiveGameStatsPipeline,
    live_rows_open,
    stored_final_games,
    stored_final_night,
)
from schemas.common import ApiStatus

GAME_DATE = date(2026, 3, 4)
KNICKS = "0022500901"
LAKERS = "0022500902"
BRUNSON = 1628973
HART = 1628404


def _player(person_id: int, name: str, points: int, status: str = "ACTIVE") -> dict:
    first, family = name.split(" ", 1)
    return {
        "status": status,
        "personId": person_id,
        "firstName": first,
        "familyName": family,
        "statistics": {"minutes": "PT30M00.00S", "minutesCalculated": "PT30M", "points": points},
    }


class Night:
    """The NBA as the pipeline sees it, poll by poll.

    `games` maps game id -> {"status": scoreboard status, "players": box score
    players or None when the box score is unavailable, "raises": bool}.
    """

    def __init__(self, *game_ids: str):
        self.games = {game_id: {"status": 2, "players": [], "raises": False} for game_id in game_ids}
        self.box_reads: list[tuple[str, str]] = []

    def poll(self, at: str):
        pipeline = LiveGameStatsPipeline()
        pipeline.nba_extractor.get_scoreboard_games = lambda _d: [
            {
                "game_id": game_id,
                "game_status": game["status"],
                "period": 4,
                "game_clock": "",
                "home_team": "NYK",
                "away_team": "BOS",
                "home_score": 101,
                "away_score": 99,
            }
            for game_id, game in self.games.items()
        ]

        def box_score(game_id):
            self.box_reads.append((at, game_id))
            game = self.games[game_id]
            if game["raises"]:
                raise RuntimeError("NBA live BoxScore connection error")
            if game["players"] is None:
                return None
            return {
                "gameStatus": game["status"],
                "homeTeam": {"players": game["players"]},
                "awayTeam": {"players": []},
            }

        pipeline.nba_extractor.get_live_box_score = box_score
        with freeze_time(at):
            return pipeline._run_sync(nba_date=GAME_DATE)

    def reads_at(self, at: str) -> list[str]:
        return [game_id for when, game_id in self.box_reads if when == at]


def _points(player_id: int) -> int:
    return LivePlayerStats.get(LivePlayerStats.player == player_id).pts


@pytest.fixture(autouse=True)
def fifteen_minutes(monkeypatch):
    monkeypatch.setattr(settings, "live_stats_settle_minutes", 15)


@pytest.mark.integration
def test_a_final_game_is_read_until_the_margin_passes_then_left_alone(integration_db):
    night = Night(KNICKS)
    night.games[KNICKS]["players"] = [_player(BRUNSON, "Jalen Brunson", 10)]
    assert night.poll("2026-03-05T02:00:00").status == ApiStatus.SUCCESS
    assert _points(BRUNSON) == 10

    # The scoreboard calls it final at 03:00. A box score fetched in the same
    # poll can still be behind, and a stat can be corrected after the buzzer:
    # inside the margin the game is read on every poll as before.
    night.games[KNICKS]["status"] = 3
    night.games[KNICKS]["players"] = [_player(BRUNSON, "Jalen Brunson", 20)]
    night.poll("2026-03-05T03:00:00")
    assert KNICKS not in stored_final_games(GAME_DATE).settled

    night.games[KNICKS]["players"] = [_player(BRUNSON, "Jalen Brunson", 22)]
    night.poll("2026-03-05T03:10:00")
    assert _points(BRUNSON) == 22
    assert KNICKS not in stored_final_games(GAME_DATE).settled

    # The first poll past the margin stores it once more, and that settles it.
    night.poll("2026-03-05T03:15:30")
    assert night.reads_at("2026-03-05T03:15:30") == [KNICKS]
    stored = stored_final_games(GAME_DATE)
    assert stored.settled == {KNICKS} and stored.complete

    # From here the game is left alone: no box score, no snapshot, no write.
    snapshots = LiveGameScoreSnapshot.select().count()
    night.games[KNICKS]["players"] = [_player(BRUNSON, "Jalen Brunson", 99)]
    result = night.poll("2026-03-05T03:16:00")

    assert result.status == ApiStatus.SUCCESS
    assert (result.records_processed, result.records_skipped) == (0, 1)
    assert night.reads_at("2026-03-05T03:16:00") == []
    assert LiveGameScoreSnapshot.select().count() == snapshots
    assert _points(BRUNSON) == 22


@pytest.mark.integration
def test_a_game_marked_final_without_its_box_score_is_not_settled(integration_db):
    """The finalize helpers mark rows final without reading a box score."""
    night = Night(KNICKS)
    night.games[KNICKS]["players"] = [_player(BRUNSON, "Jalen Brunson", 10)]
    night.poll("2026-03-05T02:00:00")

    # Final on the scoreboard, but the box score cannot be had.
    night.games[KNICKS]["status"] = 3
    night.games[KNICKS]["players"] = None
    night.poll("2026-03-05T03:00:00")
    with freeze_time("2026-03-05T03:00:05"):
        # What the endpoint does when the scoreboard says every game is over.
        assert LivePlayerStats.finalize_stale_games(GAME_DATE) == 1

    # Final and freshly touched, with the mid-game line of 10 points.
    row = LivePlayerStats.get(LivePlayerStats.player == BRUNSON)
    assert (row.game_status, row.pts) == (3, 10)

    # Past the margin, a poll that still cannot read the box score settles nothing.
    night.poll("2026-03-05T03:20:00")
    assert night.reads_at("2026-03-05T03:20:00") == [KNICKS]
    assert stored_final_games(GAME_DATE).settled == frozenset()

    # The first poll that reads it stores the final line, and only then is it settled.
    night.games[KNICKS]["players"] = [_player(BRUNSON, "Jalen Brunson", 25)]
    night.poll("2026-03-05T03:21:00")
    assert _points(BRUNSON) == 25
    assert stored_final_games(GAME_DATE).settled == {KNICKS}

    night.poll("2026-03-05T03:22:00")
    assert night.reads_at("2026-03-05T03:22:00") == []


@pytest.mark.integration
def test_a_final_taken_back_starts_the_margin_again(integration_db):
    night = Night(KNICKS)
    night.games[KNICKS]["players"] = [_player(BRUNSON, "Jalen Brunson", 10)]
    night.poll("2026-03-05T02:00:00")
    night.games[KNICKS].update(status=3, players=[_player(BRUNSON, "Jalen Brunson", 20)])
    night.poll("2026-03-05T03:00:00")

    # A review puts time back on the clock. The scoreboard is read twice a
    # poll, so the endpoint's finalize can still mark these in-play rows final.
    night.games[KNICKS]["status"] = 2
    night.poll("2026-03-05T03:14:00")
    with freeze_time("2026-03-05T03:14:05"):
        assert LivePlayerStats.finalize_stale_games(GAME_DATE) == 1
    assert stored_final_games(GAME_DATE).settled == frozenset()

    # Final again at 03:16, a quarter of an hour after the first final. The
    # margin runs from this one.
    night.games[KNICKS].update(status=3, players=[_player(BRUNSON, "Jalen Brunson", 23)])
    night.poll("2026-03-05T03:16:00")
    assert stored_final_games(GAME_DATE).settled == frozenset()

    night.poll("2026-03-05T03:31:00")
    assert stored_final_games(GAME_DATE).settled == {KNICKS}
    assert _points(BRUNSON) == 23
    night.poll("2026-03-05T03:32:00")
    assert night.reads_at("2026-03-05T03:32:00") == []


@pytest.mark.integration
def test_a_failed_run_does_not_settle_what_it_wrote(integration_db, alerts):
    night = Night(KNICKS, LAKERS)
    night.games[KNICKS].update(status=3, players=[_player(BRUNSON, "Jalen Brunson", 20)])
    night.games[LAKERS].update(status=3, players=[_player(HART, "Josh Hart", 8)])
    night.poll("2026-03-05T03:00:00")

    # Past the margin the Knicks box score is stored, then the Lakers' read
    # blows up and fails the run. Nothing that run wrote is trusted.
    night.games[LAKERS]["raises"] = True
    assert night.poll("2026-03-05T03:20:00").status == ApiStatus.ERROR
    assert stored_final_games(GAME_DATE).settled == frozenset()

    night.games[LAKERS]["raises"] = False
    night.poll("2026-03-05T03:21:00")
    assert sorted(night.reads_at("2026-03-05T03:21:00")) == [KNICKS, LAKERS]
    assert stored_final_games(GAME_DATE).complete


@pytest.mark.integration
def test_a_player_who_left_the_box_score_holds_the_game_until_his_row_is_final(integration_db):
    """A row the final box score no longer writes is left as it was."""
    night = Night(KNICKS)
    night.games[KNICKS]["players"] = [
        _player(BRUNSON, "Jalen Brunson", 10),
        _player(HART, "Josh Hart", 2),
    ]
    night.poll("2026-03-05T02:00:00")

    # Hart's appearance is taken off the sheet; his row stays live.
    night.games[KNICKS].update(status=3, players=[_player(BRUNSON, "Jalen Brunson", 20)])
    night.poll("2026-03-05T02:30:00")
    night.poll("2026-03-05T02:46:00")

    stored = stored_final_games(GAME_DATE)
    assert stored.settled == frozenset() and stored.open_rows and not stored.complete

    # The 30-minute stale finalize marks his row final, and the game settles on
    # the next poll's read; a stale row does not hold the night open for good.
    with freeze_time("2026-03-05T02:46:05"):
        assert LivePlayerStats.finalize_stale_by_time(GAME_DATE, stale_minutes=30) == 1
    night.poll("2026-03-05T02:47:00")
    stored = stored_final_games(GAME_DATE)
    assert stored.settled == {KNICKS} and stored.complete


@pytest.mark.integration
def test_a_game_that_never_tips_keeps_the_night_open(integration_db):
    """Each game settles on its own; the night is complete only when all of them have."""
    night = Night(KNICKS, LAKERS)
    night.games[KNICKS].update(status=3, players=[_player(BRUNSON, "Jalen Brunson", 20)])
    night.games[LAKERS].update(status=1, players=None)  # postponed, still listed
    night.poll("2026-03-05T03:00:00")
    night.poll("2026-03-05T03:20:00")

    stored = stored_final_games(GAME_DATE)
    assert stored.seen == {KNICKS, LAKERS}
    assert stored.settled == {KNICKS}
    assert not stored.complete

    night.poll("2026-03-05T03:21:00")
    assert night.reads_at("2026-03-05T03:21:00") == [LAKERS]


@pytest.mark.integration
def test_another_date_has_nothing_settled(integration_db):
    night = Night(KNICKS)
    night.games[KNICKS].update(status=3, players=[_player(BRUNSON, "Jalen Brunson", 20)])
    night.poll("2026-03-05T03:00:00")
    night.poll("2026-03-05T03:20:00")
    assert stored_final_games(GAME_DATE).complete

    other = stored_final_games(date(2026, 3, 5))
    assert other.seen == frozenset() and not other.complete


@pytest.mark.integration
def test_live_rows_open_is_any_row_of_the_date_not_yet_final(integration_db):
    night = Night(KNICKS, LAKERS)
    night.games[KNICKS].update(status=3, players=[_player(BRUNSON, "Jalen Brunson", 20)])
    night.games[LAKERS]["players"] = [_player(HART, "Josh Hart", 8)]
    night.poll("2026-03-05T03:00:00")

    assert live_rows_open(GAME_DATE)
    assert stored_final_night(GAME_DATE) is None
    assert not live_rows_open(date(2026, 3, 5))

    # Both final and past the margin: no live row, and the night is complete.
    night.games[LAKERS]["status"] = 3
    night.poll("2026-03-05T03:01:00")
    night.poll("2026-03-05T03:20:00")
    assert not live_rows_open(GAME_DATE)
    assert stored_final_night(GAME_DATE).complete


# One night shaped like 2026-10-12 in prod, the night that measured these
# queries: five games, a poll every 30 s from tip to 4 AM ET (1,376 runs and
# 6,880 snapshots; prod had 6,884), and each game's ~28 rows final.
POLLS = 1376
GAMES = [f"00225009{g:02d}" for g in range(1, 6)]
NIGHT_STARTS = "2026-03-04 23:00"


def _seed_a_full_night() -> None:
    """Every game goes final at poll 400 + 100 * g; its rows' last writer reads it 32 polls on."""
    db.execute_sql(
        """
        INSERT INTO nba.pipeline_runs
            (id, pipeline_name, started_at, completed_at, status, records_processed)
        SELECT md5('run' || i)::uuid, 'live_game_stats',
               %(starts)s::timestamp + i * interval '30 seconds',
               %(starts)s::timestamp + i * interval '30 seconds' + interval '8 seconds',
               'success', 140
        FROM generate_series(1, %(polls)s) i
        """,
        {"starts": NIGHT_STARTS, "polls": POLLS},
    )
    db.execute_sql(
        """
        INSERT INTO nba.live_game_score_snapshots
            (game_id, game_date, home_team, away_team, home_score, away_score,
             period, game_clock, game_status, captured_at, pipeline_run_id)
        SELECT '00225009' || lpad(g::text, 2, '0'), %(game_date)s, 'NYK', 'BOS', 101, 99,
               4, NULL, CASE WHEN i > 400 + 100 * g THEN 3 ELSE 2 END,
               %(starts)s::timestamp + i * interval '30 seconds',
               md5('run' || i)::uuid
        FROM generate_series(1, %(polls)s) i, generate_series(1, 5) g
        """,
        {"starts": NIGHT_STARTS, "polls": POLLS, "game_date": GAME_DATE},
    )
    players = [(g, 100 * g + p) for g in range(1, 6) for p in range(28)]
    Player.insert_many(
        [{"id": pid, "name": f"Player {pid}", "name_normalized": f"player {pid}"} for _, pid in players]
    ).execute()
    writer = db.execute_sql(
        "SELECT g, md5('run' || (432 + 100 * g))::uuid FROM generate_series(1, 5) g"
    ).fetchall()
    writer = {g: run_id for g, run_id in writer}
    LivePlayerStats.insert_many(
        [
            {
                "player": pid,
                "game_id": GAMES[g - 1],
                "game_date": GAME_DATE,
                "game_status": 3,
                "pts": 10,
                "pipeline_run_id": writer[g],
            }
            for g, pid in players
        ]
    ).execute()
    db.execute_sql("ANALYZE nba.pipeline_runs, nba.live_game_score_snapshots, nba.live_player_stats")


@pytest.mark.integration
def test_a_full_night_settles_with_a_plan_far_under_the_jit_threshold(integration_db):
    """The query runs on most polls of a night; it must stay too cheap to JIT-compile.

    Postgres JIT-compiles a plan whose estimated cost passes jit_above_cost
    (100,000 by default, and in prod). The first version of the query asked a
    correlated EXISTS per game, which the planner priced at 102,576 on prod's
    2026-10-12 data: every call then paid about 58 ms of compilation for a
    10 ms query. The bound here is a tenth of the threshold.
    """
    _seed_a_full_night()

    stored = stored_final_games(GAME_DATE)
    assert stored.seen == stored.settled == frozenset(GAMES)
    assert stored.complete

    (plan,) = db.execute_sql(
        "EXPLAIN (FORMAT JSON) " + STORED_FINAL_GAMES_SQL,
        {"game_date": GAME_DATE, "settle_minutes": settings.live_stats_settle_minutes},
    ).fetchone()
    cost = plan[0]["Plan"]["Total Cost"]
    assert cost < 10_000, f"plan cost {cost:,.0f}: the planner may JIT-compile it"
