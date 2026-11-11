"""
Where a live poll gets a game's clock, status and score, and what it costs.

The scoreboard file (one file for the night) moves about once in ten minutes:
the median gap between changes was 600 s in every game of 2026-11-09, and a
live sample on 2026-11-11 saw it change once in 12 minutes while the box scores
changed on nearly every 10 s read. Every row's period, clock and status and
every score snapshot were the scoreboard's, so the stat lines were 30 s old and
the clock beside them up to ten minutes; a game stayed "scheduled" for minutes
after its tip and "live" for minutes after its final.

They are now the box score's, read in the same poll. The scoreboard still says
which games there are and stands in when a box score cannot be had, but never
to put the game back behind what is already stored.

The same polls are also what a run costs: the night's box scores are read
together rather than in turn, and a game is written in three statements rather
than three a player.

These run the real pipeline against Postgres, one poll at a time under
freeze_time, with the NBA extractor faked.
"""

from __future__ import annotations

import threading
from datetime import date, datetime

import pytest
from freezegun import freeze_time

from core.settings import settings
from db.base import db
from db.models.nba.live_game_score_snapshots import LatestSnapshot, LiveGameScoreSnapshot
from db.models.nba.live_player_stats import LivePlayerStats
from db.models.nba.players import Player
from pipelines.live_game_stats import (
    LiveGameStatsPipeline,
    box_score_lines,
    ensure_players,
    store_game,
    stored_final_games,
)
from schemas.common import ApiStatus

pytestmark = pytest.mark.integration

GAME_DATE = date(2026, 3, 4)
KNICKS = "0022500901"
LAKERS = "0022500902"
SUNS = "0022500903"
BRUNSON = 1628973
HART = 1628404
JAMES = 2544
BOOKER = 1626164


def _player(person_id: int, name: str, points: int = 0, minutes: str = "PT30M00.00S") -> dict:
    first, family = name.split(" ", 1)
    return {
        "status": "ACTIVE",
        "personId": person_id,
        "firstName": first,
        "familyName": family,
        "statistics": {"minutes": minutes, "minutesCalculated": "PT30M", "points": points},
    }


def _listed(game_id: str, status: int, period: int, clock: str, home: int, away: int) -> dict:
    """One game of the scoreboard file, as the extractor hands it over."""
    return {
        "game_id": game_id, "game_status": status, "period": period, "game_clock": clock,
        "home_team": "NYK", "away_team": "BOS", "home_score": home, "away_score": away,
    }


def _box(status: int, period: int, clock: str, home: int, away: int, players: list[dict]) -> dict:
    """One game's live box score file."""
    return {
        "gameStatus": status, "period": period, "gameClock": clock,
        "homeTeam": {"teamTricode": "NYK", "score": home, "players": players},
        "awayTeam": {"teamTricode": "BOS", "score": away, "players": []},
    }


class Feed:
    """The two NBA files as one poll reads them: `scoreboard` (a list of `_listed`)
    and `boxes` (game id -> `_box`, None for a box score that cannot be had, or
    an Exception its read raises)."""

    def __init__(self):
        self.scoreboard: list[dict] = []
        self.boxes: dict[str, dict | None | Exception] = {}
        self.read: list[str] = []
        self.handed: list[list] = []

    def poll(self, at: str):
        pipeline = LiveGameStatsPipeline()
        pipeline.nba_extractor.get_scoreboard_games = lambda _d: [dict(game) for game in self.scoreboard]

        def box_score(game_id):
            self.read.append(game_id)
            box = self.boxes.get(game_id)
            if isinstance(box, Exception):
                raise box
            return box

        pipeline.nba_extractor.get_live_box_score = box_score
        with freeze_time(at):
            return pipeline._run_sync(nba_date=GAME_DATE)


def _row(player_id: int) -> LivePlayerStats:
    return LivePlayerStats.get(LivePlayerStats.player == player_id)


def _snapshots(game_id: str) -> list[tuple]:
    return [
        (s.game_status, s.period, s.game_clock, s.home_score, s.away_score)
        for s in LiveGameScoreSnapshot.select()
        .where(LiveGameScoreSnapshot.game_id == game_id)
        .order_by(LiveGameScoreSnapshot.captured_at, LiveGameScoreSnapshot.id)
    ]


@pytest.fixture(autouse=True)
def fifteen_minutes(monkeypatch):
    monkeypatch.setattr(settings, "live_stats_settle_minutes", 15)


# ---------------------------------------------------------------------------
# The clock, the status and the score are the box score's
# ---------------------------------------------------------------------------


def test_a_row_and_its_snapshot_carry_the_box_scores_clock_not_the_scoreboards(integration_db):
    feed = Feed()
    # The scoreboard last moved at the end of the first quarter.
    feed.scoreboard = [_listed(KNICKS, 2, 1, "PT00M00.00S", 28, 25)]
    feed.boxes[KNICKS] = _box(2, 2, "PT04M41.00S", 49, 51, [_player(BRUNSON, "Jalen Brunson", 14)])

    assert feed.poll("2026-03-05T01:00:00").status == ApiStatus.SUCCESS

    row = _row(BRUNSON)
    assert (row.game_status, row.period, row.game_clock, row.pts) == (2, 2, "PT04M41.00S", 14)
    assert _snapshots(KNICKS) == [(2, 2, "PT04M41.00S", 49, 51)]
    snapshot = LiveGameScoreSnapshot.get()
    assert (snapshot.home_team, snapshot.away_team) == ("NYK", "BOS")


def test_a_game_is_live_from_its_box_scores_tip_not_the_scoreboards(integration_db):
    """The scoreboard said "scheduled" for up to ten minutes into every game."""
    feed = Feed()
    feed.scoreboard = [_listed(KNICKS, 1, 0, "", 0, 0)]
    feed.boxes[KNICKS] = _box(2, 1, "PT09M30.00S", 6, 4, [_player(BRUNSON, "Jalen Brunson", 4, "PT02M30.00S")])

    result = feed.poll("2026-03-05T00:13:00")

    assert (result.records_processed, result.records_skipped) == (1, 0)
    assert (_row(BRUNSON).game_status, _row(BRUNSON).period) == (2, 1)
    assert _snapshots(KNICKS) == [(2, 1, "PT09M30.00S", 6, 4)]


def test_a_game_is_final_from_its_box_scores_final_and_settles_from_then(integration_db):
    """And it is still read until the scoreboard agrees: that is the only word on a game not read."""
    feed = Feed()
    feed.scoreboard = [_listed(KNICKS, 2, 4, "PT05M00.00S", 95, 90)]
    feed.boxes[KNICKS] = _box(2, 4, "PT00M08.00S", 108, 106, [_player(BRUNSON, "Jalen Brunson", 30)])
    feed.poll("2026-03-05T03:00:00")

    # The buzzer. The scoreboard will say so in a few minutes.
    feed.boxes[KNICKS] = _box(3, 4, "", 110, 106, [_player(BRUNSON, "Jalen Brunson", 32)])
    feed.poll("2026-03-05T03:00:30")
    row = _row(BRUNSON)
    assert (row.game_status, row.game_clock, row.pts) == (3, None, 32)
    assert _snapshots(KNICKS)[-1] == (3, 4, None, 110, 106)

    # Past the margin, counted from the box score's final, the game is settled...
    feed.poll("2026-03-05T03:16:00")
    assert stored_final_games(GAME_DATE).settled == {KNICKS}

    # ...and read all the same while the scoreboard has it in play.
    feed.read.clear()
    feed.poll("2026-03-05T03:16:30")
    assert feed.read == [KNICKS]

    feed.scoreboard = [_listed(KNICKS, 3, 4, "", 110, 106)]
    feed.read.clear()
    result = feed.poll("2026-03-05T03:17:00")
    assert feed.read == [] and (result.records_processed, result.records_skipped) == (0, 1)


def test_the_scoreboards_final_counts_for_a_box_score_that_has_not_said_so(integration_db):
    """The status is the later of the two, so nothing is called live for longer than before."""
    feed = Feed()
    feed.scoreboard = [_listed(KNICKS, 3, 4, "", 110, 106)]
    feed.boxes[KNICKS] = _box(2, 4, "PT00M00.00S", 110, 106, [_player(BRUNSON, "Jalen Brunson", 32)])

    feed.poll("2026-03-05T03:00:00")

    assert _row(BRUNSON).game_status == 3
    assert _snapshots(KNICKS) == [(3, 4, "PT00M00.00S", 110, 106)]


def test_a_box_score_without_the_games_state_leaves_the_scoreboards(integration_db):
    """Each field on its own presence: a file with only its players changes nothing else."""
    feed = Feed()
    feed.scoreboard = [_listed(KNICKS, 2, 3, "PT07M00.00S", 70, 66)]
    feed.boxes[KNICKS] = {
        "homeTeam": {"players": [_player(BRUNSON, "Jalen Brunson", 20)], "score": 74},  # one score only
        "awayTeam": {"players": []},
    }

    feed.poll("2026-03-05T02:00:00")

    row = _row(BRUNSON)
    assert (row.game_status, row.period, row.game_clock) == (2, 3, "PT07M00.00S")
    assert _snapshots(KNICKS) == [(2, 3, "PT07M00.00S", 70, 66)]


# ---------------------------------------------------------------------------
# A box score that cannot be had
# ---------------------------------------------------------------------------


def test_before_the_tip_the_scoreboard_is_still_the_snapshot(integration_db):
    """No box score exists until about half an hour before the tip; the 0-0 polls are how a game is seen."""
    feed = Feed()
    feed.scoreboard = [_listed(KNICKS, 1, 0, "", 0, 0)]
    feed.boxes[KNICKS] = None

    for at in ("2026-03-04T23:00:00", "2026-03-04T23:00:30"):
        result = feed.poll(at)
        assert result.status == ApiStatus.SUCCESS and not result.partial
        assert result.records_skipped == 1

    assert _snapshots(KNICKS) == [(1, None, None, 0, 0)] * 2
    assert stored_final_games(GAME_DATE).seen == {KNICKS}


def test_a_missed_box_score_does_not_put_the_game_back_where_the_scoreboard_has_it(integration_db):
    """The scoreboard's word is minutes older than what the last box score stored: its point would
    run the margin line backward, and its "in play" after a final would start the settle margin again."""
    feed = Feed()
    feed.scoreboard = [_listed(KNICKS, 2, 1, "PT00M00.00S", 28, 25)]
    feed.boxes[KNICKS] = _box(2, 2, "PT04M41.00S", 49, 51, [_player(BRUNSON, "Jalen Brunson", 14)])
    feed.poll("2026-03-05T01:00:00")

    feed.boxes[KNICKS] = None
    result = feed.poll("2026-03-05T01:00:30")

    assert result.partial, "a tipped game without its box score is a failed read, as before"
    assert _snapshots(KNICKS) == [(2, 2, "PT04M41.00S", 49, 51)]
    # The line is left as it was, not marked with the older clock.
    assert (_row(BRUNSON).period, _row(BRUNSON).game_clock, _row(BRUNSON).pts) == (2, "PT04M41.00S", 14)

    # Once the scoreboard has caught up with the store, its snapshot is taken again.
    feed.scoreboard = [_listed(KNICKS, 2, 2, "PT02M00.00S", 55, 53)]
    feed.poll("2026-03-05T01:01:00")
    assert _snapshots(KNICKS)[-1] == (2, 2, "PT02M00.00S", 55, 53)


def test_a_missed_box_score_does_not_wind_the_clock_back(integration_db):
    """Level on period and score the scoreboard can still be the older word, and its clock says so."""
    feed = Feed()
    feed.scoreboard = [_listed(KNICKS, 2, 2, "PT06M00.00S", 49, 51)]
    feed.boxes[KNICKS] = _box(2, 2, "PT04M41.00S", 49, 51, [_player(BRUNSON, "Jalen Brunson", 14)])
    feed.poll("2026-03-05T01:00:00")

    feed.boxes[KNICKS] = None
    feed.poll("2026-03-05T01:00:30")
    assert _snapshots(KNICKS) == [(2, 2, "PT04M41.00S", 49, 51)]

    # The scoreboard's clock has run past the stored one: it is the newer word again.
    feed.scoreboard = [_listed(KNICKS, 2, 2, "PT04M10.00S", 49, 51)]
    feed.poll("2026-03-05T01:01:00")
    assert _snapshots(KNICKS)[-1] == (2, 2, "PT04M10.00S", 49, 51)


def test_a_missed_box_score_after_the_final_keeps_the_settle_margin_running(integration_db):
    feed = Feed()
    feed.scoreboard = [_listed(KNICKS, 2, 4, "PT05M00.00S", 95, 90)]
    feed.boxes[KNICKS] = _box(3, 4, "", 110, 106, [_player(BRUNSON, "Jalen Brunson", 32)])
    feed.poll("2026-03-05T03:00:00")

    feed.boxes[KNICKS] = None                       # one read lost; the scoreboard still says in play
    feed.poll("2026-03-05T03:10:00")
    assert [status for status, *_ in _snapshots(KNICKS)] == [3]

    feed.boxes[KNICKS] = _box(3, 4, "", 110, 106, [_player(BRUNSON, "Jalen Brunson", 32)])
    feed.poll("2026-03-05T03:15:30")
    assert stored_final_games(GAME_DATE).settled == {KNICKS}


def test_a_tipped_game_with_no_box_score_counts_as_a_failed_read_whatever_the_scoreboard_says(integration_db):
    """The scoreboard says "scheduled" for minutes after the tip; the store knows the game is on."""
    feed = Feed()
    feed.scoreboard = [_listed(KNICKS, 1, 0, "", 0, 0)]
    feed.boxes[KNICKS] = _box(2, 1, "PT09M30.00S", 6, 4, [_player(BRUNSON, "Jalen Brunson", 4)])
    feed.poll("2026-03-05T00:13:00")

    feed.boxes[KNICKS] = None
    result = feed.poll("2026-03-05T00:13:30")

    assert result.partial and result.records_skipped == 0
    assert _snapshots(KNICKS) == [(2, 1, "PT09M30.00S", 6, 4)]


def test_latest_by_game_is_each_games_newest_snapshot(integration_db):
    for minute, (game_id, status, period, clock, home, away) in enumerate([
        (KNICKS, 2, 1, "PT03M00.00S", 10, 8), (KNICKS, 2, 2, "PT04M41.00S", 50, 48), (LAKERS, 1, None, None, 0, 0),
    ]):
        LiveGameScoreSnapshot.create(
            game_id=game_id, game_date=GAME_DATE, home_team="NYK", away_team="BOS",
            home_score=home, away_score=away, period=period, game_clock=clock, game_status=status,
            captured_at=datetime(2026, 3, 5, 1, minute),
        )

    latest = LiveGameScoreSnapshot.latest_by_game([KNICKS, LAKERS, SUNS])

    assert latest == {KNICKS: LatestSnapshot(2, 2, 98, "PT04M41.00S"), LAKERS: LatestSnapshot(1, 0, 0, None)}
    assert latest[KNICKS].ahead_of(_listed(KNICKS, 2, 1, "", 28, 25))
    assert latest[KNICKS].ahead_of(_listed(KNICKS, 1, 2, "", 50, 48))
    assert not latest[KNICKS].ahead_of(_listed(KNICKS, 2, 2, "", 50, 48))
    assert not latest[LAKERS].ahead_of(_listed(LAKERS, 1, 0, "", 0, 0))
    assert LiveGameScoreSnapshot.latest_by_game([]) == {}


# ---------------------------------------------------------------------------
# What a poll costs: the reads together, a game in three statements
# ---------------------------------------------------------------------------


def _three_games(feed: Feed) -> None:
    feed.scoreboard = [_listed(game_id, 2, 2, "PT05M00.00S", 50, 48) for game_id in (KNICKS, LAKERS, SUNS)]
    feed.boxes = {
        KNICKS: _box(2, 2, "PT04M41.00S", 49, 51, [_player(BRUNSON, "Jalen Brunson", 14)]),
        LAKERS: _box(2, 3, "PT11M02.00S", 60, 58, [_player(JAMES, "LeBron James", 21)]),
        SUNS: _box(2, 1, "PT01M10.00S", 22, 30, [_player(BOOKER, "Devin Booker", 9)]),
    }


def test_a_nights_box_scores_are_read_together(integration_db):
    """In turn, the third read would never start while the first two wait for it."""
    feed = Feed()
    _three_games(feed)
    boxes = dict(feed.boxes)
    on_the_wire = threading.Barrier(3, timeout=5)

    class Held(dict):
        def get(self, game_id):
            on_the_wire.wait()  # every read is out before any comes back
            return boxes[game_id]

    feed.boxes = Held()

    result = feed.poll("2026-03-05T01:00:00")

    assert result.status == ApiStatus.SUCCESS and result.records_processed == 3
    assert sorted(feed.read) == [KNICKS, LAKERS, SUNS]
    assert {(_row(p).game_id, _row(p).period) for p in (BRUNSON, JAMES, BOOKER)} == {
        (KNICKS, 2), (LAKERS, 3), (SUNS, 1),
    }


def test_one_worker_reads_them_in_turn(integration_db, monkeypatch):
    monkeypatch.setattr(settings, "live_box_score_workers", 1)
    feed = Feed()
    _three_games(feed)
    threads = set()
    boxes = dict(feed.boxes)

    class Watched(dict):
        def get(self, game_id):
            threads.add(threading.current_thread().name)
            return boxes[game_id]

    feed.boxes = Watched()

    assert feed.poll("2026-03-05T01:00:00").records_processed == 3
    assert feed.read == [KNICKS, LAKERS, SUNS]
    assert len(threads) == 1 and not any(name.startswith("live-box-score") for name in threads)


def test_one_games_failed_read_still_stores_the_others_and_fails_the_run(integration_db, alerts):
    """Read in turn, the games after the failure were not read at all that poll."""
    feed = Feed()
    _three_games(feed)
    feed.boxes[KNICKS] = RuntimeError("NBA live BoxScore connection error")

    result = feed.poll("2026-03-05T01:00:00")

    assert result.status == ApiStatus.ERROR and "connection error" in result.error
    assert (_row(JAMES).pts, _row(BOOKER).pts) == (21, 9)
    assert LivePlayerStats.select().where(LivePlayerStats.game_id == KNICKS).count() == 0
    assert _snapshots(KNICKS) == []


def test_a_game_is_written_in_three_statements_however_many_played(integration_db, monkeypatch):
    """It was three a player: the player, a read of his line, the write."""
    lines = box_score_lines(_box(2, 2, "PT04M41.00S", 49, 51, [_player(1000 + n, f"Player {n}", n) for n in range(26)]))
    state = _listed(KNICKS, 2, 2, "PT04M41.00S", 49, 51)
    # The first poll of a game also adds its players: one statement more, once.
    assert store_game(KNICKS, GAME_DATE, state, lines, None) == 26

    statements: list[str] = []
    execute_sql = db.execute_sql

    def counted(sql, *args, **kwargs):
        statements.append(sql.split("(")[0].strip())
        return execute_sql(sql, *args, **kwargs)

    monkeypatch.setattr(db, "execute_sql", counted)
    assert store_game(KNICKS, GAME_DATE, state, lines, None) == 26
    monkeypatch.undo()

    assert statements == [
        'INSERT INTO "nba"."live_game_score_snapshots"',
        'SELECT "t1"."id", "t1"."name" FROM "nba"."players" AS "t1" WHERE',
        'INSERT INTO "nba"."live_player_stats"',
    ]
    assert LivePlayerStats.select().count() == 26 and LiveGameScoreSnapshot.select().count() == 2


def test_a_games_snapshot_and_lines_are_written_together_or_not_at_all(integration_db):
    """One transaction: a line pointing at a player who cannot be stored takes the snapshot with it."""
    lines = box_score_lines(_box(2, 2, "PT04M41.00S", 49, 51, [_player(BRUNSON, "Jalen Brunson", 14)]))
    lines[0]["pts"] = 10 ** 9  # more than the column holds

    with pytest.raises(Exception):
        store_game(KNICKS, GAME_DATE, _listed(KNICKS, 2, 2, "PT04M41.00S", 49, 51), lines, None)

    assert LiveGameScoreSnapshot.select().count() == 0
    assert LivePlayerStats.select().count() == 0 and Player.select().count() == 0


def test_every_line_is_rewritten_on_every_poll_changed_or_not(integration_db):
    """The run id and time a poll leaves on a line are how a settled game is told and a stuck one finalized."""
    feed = Feed()
    feed.scoreboard = [_listed(KNICKS, 2, 2, "PT05M00.00S", 50, 48)]
    feed.boxes[KNICKS] = _box(2, 2, "PT04M41.00S", 49, 51, [_player(BRUNSON, "Jalen Brunson", 14)])
    feed.poll("2026-03-05T01:00:00")
    first = _row(BRUNSON)

    feed.poll("2026-03-05T01:00:30")
    second = _row(BRUNSON)

    assert second.id == first.id and second.pts == 14
    assert second.pipeline_run_id != first.pipeline_run_id
    assert (first.last_updated, second.last_updated) == (
        datetime(2026, 3, 5, 1, 0, 0), datetime(2026, 3, 5, 1, 0, 30),
    )


# ---------------------------------------------------------------------------
# The players the lines point at
# ---------------------------------------------------------------------------


def test_ensure_players_adds_the_new_renames_the_renamed_and_leaves_the_rest(integration_db):
    then = datetime(2026, 3, 1)
    with freeze_time(then):
        Player.create(id=BRUNSON, name="Jalen Brunson", name_normalized="jalen brunson", espn_id=3934672,
                      position="G", created_at=then)
        Player.create(id=HART, name="Joshua Hart", name_normalized="joshua hart", espn_id=3062679, created_at=then)

    with freeze_time("2026-03-05T01:00:00"):
        written = ensure_players({BRUNSON: "Jalen Brunson", HART: "Josh Hart", JAMES: " LeBron James"})

    assert written == 2
    brunson, hart, james = (Player.get_by_id(pid) for pid in (BRUNSON, HART, JAMES))
    # Untouched.
    assert (brunson.updated_at, brunson.espn_id, brunson.position) == (then, 3934672, "G")
    # Renamed, and everything else about him kept.
    assert (hart.name, hart.name_normalized, hart.espn_id) == ("Josh Hart", "josh hart", 3062679)
    assert (hart.created_at, hart.updated_at) == (then, datetime(2026, 3, 5, 1))
    # New, as Player.upsert_player would have made him.
    assert (james.name, james.name_normalized, james.espn_id, james.position) == (
        " LeBron James", "lebron james", None, None,
    )
    assert ensure_players({}) == 0
    assert ensure_players({BRUNSON: "Jalen Brunson"}) == 0


def test_upsert_lines_fills_what_a_line_leaves_out_and_keeps_the_last_of_a_player_listed_twice(integration_db):
    Player.create(id=BRUNSON, name="Jalen Brunson", name_normalized="jalen brunson")

    written = LivePlayerStats.upsert_lines(
        KNICKS,
        [
            {"player_id": BRUNSON, "game_date": GAME_DATE, "pts": 10},
            {"player_id": BRUNSON, "game_date": GAME_DATE, "pts": 12, "game_status": 2, "period": 1},
        ],
    )

    row = _row(BRUNSON)
    assert written == 1
    assert (row.pts, row.reb, row.game_status, row.period, row.game_clock) == (12, 0, 2, 1, None)
    assert LivePlayerStats.upsert_lines(KNICKS, []) == 0
