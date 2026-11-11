"""
One live-stats poll, without a database: what it takes as each game's state,
how it reads the night's box scores, and what one bad read costs.

The scoreboard file moves about once in ten minutes and the box scores every
few seconds (pipelines/live_game_stats.py), so a game's clock, period, status
and score are its box score's; the scoreboard fills in what a box score does
not carry. The box scores are read together, and each game is stored as its
read comes back.

The writers are captured rather than run. The same polls against Postgres are
tests/integration/test_live_stats_box_score_state.py.
"""

from __future__ import annotations

import threading
from datetime import date

import pytest

from core.settings import settings
from db.models.nba import LiveGameScoreSnapshot, LivePlayerStats
from db.models.nba.live_game_score_snapshots import LatestSnapshot, clock_seconds
from pipelines import live_game_stats
from pipelines.availability import WriteResult
from pipelines.context import PipelineContext
from pipelines.live_game_stats import LiveGameStatsPipeline, StoredFinalGames, game_state

pytestmark = pytest.mark.unit

GAME_DATE = date(2026, 3, 4)
KNICKS, LAKERS, SUNS = "0022500901", "0022500902", "0022500903"


def _listed(game_id: str = KNICKS, status: int = 2, period: int = 1, clock: str = "PT00M00.00S",
            home: int = 28, away: int = 25) -> dict:
    return {
        "game_id": game_id, "game_status": status, "period": period, "game_clock": clock,
        "home_team": "NYK", "away_team": "BOS", "home_score": home, "away_score": away,
    }


def _box(status: int = 2, period: int = 2, clock: str = "PT04M41.00S", home: int = 49, away: int = 51,
         players: list[dict] | None = None) -> dict:
    return {
        "gameStatus": status, "period": period, "gameClock": clock,
        "homeTeam": {"teamTricode": "NYK", "score": home, "players": players or []},
        "awayTeam": {"teamTricode": "BOS", "score": away, "players": []},
    }


def _player(person_id: int, points: int = 10) -> dict:
    return {
        "status": "ACTIVE", "personId": person_id, "firstName": "Player", "familyName": str(person_id),
        "statistics": {"minutes": "PT20M00.00S", "minutesCalculated": "PT20M", "points": points},
    }


# ---------------------------------------------------------------------------
# game_state: the box score's word, the scoreboard's for the rest
# ---------------------------------------------------------------------------


def test_the_box_score_is_the_games_clock_period_status_and_score() -> None:
    state = game_state(_listed(), _box())

    assert state == {
        "game_id": KNICKS, "game_status": 2, "period": 2, "game_clock": "PT04M41.00S",
        "home_team": "NYK", "away_team": "BOS", "home_score": 49, "away_score": 51,
    }


def test_without_a_box_score_the_scoreboard_is_the_game() -> None:
    listed = _listed()
    assert game_state(listed, None) == listed
    assert game_state(listed, {}) == listed
    assert game_state(listed, None) is not listed


@pytest.mark.parametrize(
    ("listed", "box", "status"),
    [
        (1, 2, 2),   # tipped: the scoreboard says "scheduled" for minutes yet
        (2, 3, 3),   # the buzzer
        (3, 2, 3),   # a box score that has not said final yet
        (1, 1, 1),
        (2, None, 2),
        (3, "Final", 3),
    ],
)
def test_the_status_is_the_later_of_the_two(listed, box, status) -> None:
    assert game_state(_listed(status=listed), {**_box(), "gameStatus": box})["game_status"] == status


def test_the_final_box_score_clears_the_clock() -> None:
    state = game_state(_listed(status=2, period=4, clock="PT05M00.00S"), _box(3, 4, "", 110, 106))
    assert (state["game_status"], state["game_clock"], state["home_score"], state["away_score"]) == (3, "", 110, 106)


def test_a_field_the_box_score_lacks_stays_the_scoreboards() -> None:
    """Each on its own presence, bar the two scores: one team's points from each file is no margin at all."""
    box = {"homeTeam": {"score": 74, "players": []}, "awayTeam": {"players": []}}
    state = game_state(_listed(period=3, clock="PT07M00.00S", home=70, away=66), box)

    assert (state["game_status"], state["period"], state["game_clock"]) == (2, 3, "PT07M00.00S")
    assert (state["home_score"], state["away_score"]) == (70, 66)
    assert (state["home_team"], state["away_team"]) == ("NYK", "BOS")


def test_a_snapshot_is_ahead_of_an_older_word_on_the_game() -> None:
    stored = LatestSnapshot(game_status=2, period=2, score=100)
    assert stored.ahead_of(_listed(status=1, period=2, home=50, away=50))
    assert stored.ahead_of(_listed(status=2, period=1, home=50, away=50))
    assert stored.ahead_of(_listed(status=2, period=2, home=49, away=50))
    assert not stored.ahead_of(_listed(status=2, period=2, home=50, away=50))
    assert not stored.ahead_of(_listed(status=3, period=4, home=110, away=106))
    # Before the tip the scoreboard has no period and no score.
    assert not LatestSnapshot(1, 0, 0).ahead_of({"game_status": 1, "period": 0, "home_score": None})


@pytest.mark.parametrize(
    ("clock", "seconds"),
    [("PT06M30.00S", 390.0), ("PT00M04.70S", 4.7), ("PT12M00.00S", 720.0), ("PT00M00.00S", 0.0),
     ("", None), (None, None), ("Final", None), (390, None)],
)
def test_a_game_clock_is_the_seconds_left_in_its_period(clock, seconds) -> None:
    assert clock_seconds(clock) == seconds


def test_level_in_play_the_clock_says_which_word_is_the_older() -> None:
    """A stretch with no score: the scoreboard can match the stored period and points and still be minutes behind."""
    stored = LatestSnapshot(game_status=2, period=3, score=150, game_clock="PT06M30.00S")
    level = {"status": 2, "period": 3, "home": 76, "away": 74}

    assert stored.ahead_of(_listed(clock="PT08M00.00S", **level))
    assert not stored.ahead_of(_listed(clock="PT06M30.00S", **level))
    assert not stored.ahead_of(_listed(clock="PT05M10.00S", **level))
    # A clock that cannot be read settles nothing, whichever side lacks it.
    assert not stored.ahead_of(_listed(clock="", **level))
    assert not LatestSnapshot(2, 3, 150).ahead_of(_listed(clock="PT08M00.00S", **level))
    # Nor does a final's: its snapshots have to keep coming for the game to settle.
    final = LatestSnapshot(3, 4, 216, "PT00M00.00S")
    assert not final.ahead_of(_listed(status=3, period=4, clock="PT00M01.20S", home=110, away=106))


# ---------------------------------------------------------------------------
# A poll
# ---------------------------------------------------------------------------


class _NoRows:
    """Stands in for a DELETE query: accepts the filter, removes nothing."""

    def where(self, *_):
        return self

    def execute(self):
        return 0


class Poll:
    """LiveGameStatsPipeline.execute over a faked NBA, its writes captured."""

    def __init__(self, monkeypatch):
        self.scoreboard: list[dict] = []
        self.boxes: dict = {}
        self.read: list[str] = []
        self.stored: dict[str, tuple[dict, list[dict]]] = {}      # game id -> (state, lines)
        self.scoreboard_snapshots: list[dict] = []
        self.handed: list[list] = []
        self.latest: dict[str, LatestSnapshot] = {}
        self.latest_reads = 0

        monkeypatch.setattr(LivePlayerStats, "delete", classmethod(lambda cls: _NoRows()))
        monkeypatch.setattr(LiveGameScoreSnapshot, "delete", classmethod(lambda cls: _NoRows()))
        monkeypatch.setattr(
            live_game_stats, "stored_final_games",
            lambda _d, settle_minutes=None: StoredFinalGames(frozenset(), frozenset(), False),
        )
        monkeypatch.setattr(live_game_stats, "store_game", self._store_game)
        monkeypatch.setattr(
            LiveGameScoreSnapshot, "record_snapshot",
            classmethod(lambda cls, game_meta, **kw: self.scoreboard_snapshots.append(game_meta)),
        )
        monkeypatch.setattr(LiveGameScoreSnapshot, "latest_by_game", classmethod(self._latest_by_game))
        monkeypatch.setattr(
            live_game_stats, "record_boxscore",
            lambda game_date, boxes, **kw: self.handed.append(boxes) or WriteResult(),
        )

    def _store_game(self, game_id, game_date, state, lines, run_id) -> int:
        self.stored[game_id] = (dict(state), lines)
        return len(lines)

    def _latest_by_game(self, _cls, game_ids):
        self.latest_reads += 1
        return {game_id: self.latest[game_id] for game_id in game_ids if game_id in self.latest}

    def box_score(self, game_id):
        self.read.append(game_id)
        box = self.boxes.get(game_id)
        if isinstance(box, Exception):
            raise box
        return box

    def run(self) -> PipelineContext:
        pipeline = LiveGameStatsPipeline()
        pipeline.nba_extractor.get_scoreboard_games = lambda _d: [dict(game) for game in self.scoreboard]
        pipeline.nba_extractor.get_live_box_score = self.box_score
        ctx = PipelineContext("live_game_stats", nba_date=GAME_DATE)
        pipeline.execute(ctx)
        return ctx


@pytest.fixture
def poll(monkeypatch) -> Poll:
    return Poll(monkeypatch)


def _three_games(poll: Poll) -> None:
    poll.scoreboard = [_listed(game_id) for game_id in (KNICKS, LAKERS, SUNS)]
    poll.boxes = {
        KNICKS: _box(2, 2, "PT04M41.00S", 49, 51, [_player(1)]),
        LAKERS: _box(2, 3, "PT11M02.00S", 60, 58, [_player(2)]),
        SUNS: _box(3, 4, "", 110, 106, [_player(3)]),
    }


def test_each_game_is_stored_under_its_own_box_scores_state(poll) -> None:
    _three_games(poll)

    ctx = poll.run()

    assert ctx.records_processed == 3
    assert {game_id: (state["game_status"], state["period"], state["game_clock"])
            for game_id, (state, _) in poll.stored.items()} == {
        KNICKS: (2, 2, "PT04M41.00S"), LAKERS: (2, 3, "PT11M02.00S"), SUNS: (3, 4, ""),
    }
    assert [line["player_id"] for _, lines in poll.stored.values() for line in lines].count(1) == 1
    # No snapshot of the scoreboard's, and no look at the stored ones, while every box score is in.
    assert poll.scoreboard_snapshots == [] and poll.latest_reads == 0


def test_the_nights_box_scores_are_read_together(poll) -> None:
    """In turn, the third read would never start while the first two wait for it."""
    _three_games(poll)
    boxes = dict(poll.boxes)
    on_the_wire = threading.Barrier(3, timeout=5)
    readers = set()

    class Held(dict):
        def get(self, game_id):
            readers.add(threading.current_thread().name)
            on_the_wire.wait()  # every read is out before any comes back
            return boxes[game_id]

    poll.boxes = Held()

    assert poll.run().records_processed == 3
    assert sorted(poll.read) == [KNICKS, LAKERS, SUNS]
    assert len(readers) == 3 and all(name.startswith("live-box-score") for name in readers)


def test_the_writes_stay_on_the_polls_own_thread(poll, monkeypatch) -> None:
    """Peewee's connection belongs to the thread that runs the pipeline; only the reads leave it."""
    _three_games(poll)
    writers = set()
    store = poll._store_game

    def watched(*args):
        writers.add(threading.current_thread())
        return store(*args)

    monkeypatch.setattr(live_game_stats, "store_game", watched)
    poll.run()

    assert writers == {threading.current_thread()}


def test_one_worker_reads_them_in_turn_on_the_polls_thread(poll, monkeypatch) -> None:
    monkeypatch.setattr(settings, "live_box_score_workers", 1)
    _three_games(poll)
    boxes = dict(poll.boxes)
    readers = set()

    class Watched(dict):
        def get(self, game_id):
            readers.add(threading.current_thread())
            return boxes[game_id]

    poll.boxes = Watched()

    assert poll.run().records_processed == 3
    assert poll.read == [KNICKS, LAKERS, SUNS]
    assert readers == {threading.current_thread()}


def test_a_slow_box_score_holds_up_no_other_games_write(poll, monkeypatch) -> None:
    """Each game is stored as its read comes back; they are handed on in the scoreboard's order all the same."""
    _three_games(poll)
    poll.scoreboard = [_listed(game_id, status=1) for game_id in (KNICKS, LAKERS, SUNS)]
    poll.boxes[LAKERS] = _box(1, 0, "", 0, 0, [])       # the file is up before the tip
    boxes = dict(poll.boxes)
    others_stored = threading.Event()
    store = poll._store_game

    def counted(*args):
        written = store(*args)
        if len(poll.stored) == 2:
            others_stored.set()
        return written

    monkeypatch.setattr(live_game_stats, "store_game", counted)

    class SlowKnicks(dict):
        def get(self, game_id):
            if game_id == KNICKS:
                assert others_stored.wait(5), "the other two games waited for this read"
            return boxes[game_id]

    poll.boxes = SlowKnicks()
    poll.run()

    assert list(poll.stored) == [LAKERS, SUNS, KNICKS] or list(poll.stored) == [SUNS, LAKERS, KNICKS]
    (handed,) = poll.handed
    assert [(game_id, tipped) for game_id, _, tipped in handed] == [(KNICKS, True), (LAKERS, False), (SUNS, True)]
    assert handed[0][1] is boxes[KNICKS]


@pytest.mark.parametrize("workers", [8, 1])
def test_one_games_failed_read_still_stores_the_others_then_fails_the_run(poll, monkeypatch, workers) -> None:
    """Read in turn, the games after the failure were not read at all that poll. The run
    still fails, as it did: nothing a failed run wrote counts toward a settled game."""
    monkeypatch.setattr(settings, "live_box_score_workers", workers)
    _three_games(poll)
    poll.boxes[KNICKS] = RuntimeError("NBA live BoxScore connection error")

    with pytest.raises(RuntimeError, match="connection error"):
        poll.run()

    assert sorted(poll.stored) == [LAKERS, SUNS]
    (handed,) = poll.handed
    assert [game_id for game_id, _, _ in handed] == [LAKERS, SUNS]


def test_before_the_tip_a_game_with_no_box_score_keeps_its_scoreboard_snapshot(poll) -> None:
    poll.scoreboard = [_listed(KNICKS, 1, 0, "", 0, 0), _listed(LAKERS, 1, 0, "", 0, 0)]
    poll.latest = {KNICKS: LatestSnapshot(1, 0, 0)}     # LAKERS has never been seen

    ctx = poll.run()

    assert [snapshot["game_id"] for snapshot in poll.scoreboard_snapshots] == [KNICKS, LAKERS]
    assert (ctx.skip_reasons, ctx.records_failed) == ({"not_started": 2}, 0)
    assert poll.latest_reads == 1, "one read of the stored snapshots a poll, however many games lack a box score"
    assert poll.handed == []


def test_a_missed_box_score_never_puts_a_game_back_where_the_scoreboard_has_it(poll) -> None:
    """Its snapshot would run the margin line backward, or restart the settle margin after a final."""
    poll.scoreboard = [_listed(KNICKS, 2, 1, "PT00M00.00S", 28, 25), _listed(LAKERS, 1, 0, "", 0, 0)]
    poll.latest = {KNICKS: LatestSnapshot(2, 2, 100), LAKERS: LatestSnapshot(2, 1, 10)}

    ctx = poll.run()

    assert poll.scoreboard_snapshots == []
    # Both have tipped by the store's account, so both are failed reads, not games yet to start.
    assert ctx.failure_reasons == {"live_box_score_unavailable": 2} and ctx.records_skipped == 0


def test_a_missed_box_score_takes_the_scoreboards_snapshot_once_it_has_caught_up(poll) -> None:
    poll.scoreboard = [_listed(KNICKS, 2, 2, "PT02M00.00S", 55, 53)]
    poll.latest = {KNICKS: LatestSnapshot(2, 2, 100)}

    ctx = poll.run()

    assert [snapshot["home_score"] for snapshot in poll.scoreboard_snapshots] == [55]
    assert ctx.failure_reasons == {"live_box_score_unavailable": 1}
