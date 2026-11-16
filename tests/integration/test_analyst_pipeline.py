"""
The analyst in shadow against Postgres (the Live rankings plan, step 4): who a
morning reads and what each read is shown, what becomes of an answer, the
second projection and boards its notes make, what a later run does not ask
again, and where a run stops.

The morning is the live projection's own (tests/integration/test_live_projection_pipeline.py):
Nov 9, DEN and BOS four games in and six to go, the star out since the third
with the feed's return date two games off. On top of it sit the stored news,
the morning's boards and ESPN's, and a backend that is scripted: no model is
called here.
"""

from __future__ import annotations

import threading
from datetime import date, datetime, time, timedelta, timezone

import pytest
import pytz

from core.settings import settings
from db.base import db
from db.models.nba import (
    AnalystRead,
    DailyRanking,
    NewsItem,
    NewsItemPlayer,
    PlayerHistory,
    PlayerProfile,
    PlayerProjection,
    ProjectionAdjustment,
)
from db.models.nba.availability import AvailabilityReport, PlayerAvailability
from db.models.nba.daily_rankings import BOARD_CATEGORIES, BOARD_POINTS, BOARDS, SOURCE_CV, SOURCE_ESPN
from db.models.nba.games import Game
from db.models.nba.player_game_stats import PlayerGameStats
from db.models.nba.players import Player
from db.models.nba.projection_adjustments import SCOPE_ANALYST
from pipelines import analyst, live_projection
from pipelines.analyst import SHADOW_BOARDS, SHADOW_SOURCE, AnalystPipeline, due_reason
from pipelines.live_projection import SOURCE, LiveProjectionPipeline
from schemas.common import ApiStatus
from services import analyst_digest, analyst_inputs
from services.analyst_client import Month, ReadResult
from services.live_projection_inputs import load_coefficients
from services.live_projection_model import expand
from services.projection_model import season_label, season_start
from services.valuation_client import BoardPlace, DailyBoards, DailyRank

pytestmark = pytest.mark.integration

SEASON = settings.nba_season
YEAR = season_start(SEASON)
LAST_SEASON = season_label(YEAR - 1)
PRESEASON = date(YEAR, 10, 20)
PLAYED = [date(YEAR, 11, d) for d in (1, 3, 5, 7)]
TODAY = date(YEAR, 11, 9)
AHEAD = [TODAY + timedelta(days=2 * i) for i in range(6)]
# Six in the morning, Eastern
NOW = datetime(TODAY.year, TODAY.month, TODAY.day, 11, 0, tzinfo=timezone.utc)
EASTERN = pytz.timezone("US/Eastern")

STAR, STARTER, BENCH, WAITING, GONE, CELTIC, UNPROJECTED = 201, 202, 203, 204, 205, 301, 302
EVERYONE = (STAR, STARTER, BENCH, WAITING, GONE, CELTIC, UNPROJECTED)
PER_MINUTE = {"pts": 0.66, "reb": 0.2, "ast": 0.15, "stl": 0.03, "blk": 0.02, "tov": 0.06,
              "fgm": 0.24, "fga": 0.5, "fg3m": 0.06, "fg3a": 0.16, "ftm": 0.12, "fta": 0.15}
MODELS = [PlayerProfile, PlayerHistory, PlayerProjection, DailyRanking, AnalystRead]

# The boards everybody sees this morning, ESPN's, and the ones the backend builds on the shadow projection
LIVE_ORDER = (STAR, CELTIC, STARTER, GONE, BENCH, WAITING, UNPROJECTED)
ESPN_ORDER = {STAR: 1, STARTER: 3, GONE: 4, BENCH: 5, WAITING: 6, UNPROJECTED: 7, CELTIC: 40}
SHADOW_ORDER = (CELTIC, STARTER, STAR, GONE, BENCH, WAITING, UNPROJECTED)

COST = 14_000
NOTHING = {"decision": "none", "reason": "Nothing a box score cannot say."}


@pytest.fixture(scope="module", autouse=True)
def analyst_tables(integration_db, adjustments_table):
    db.create_tables(MODELS, safe=True)
    yield


def _game_id(n: int) -> str:
    return f"002{YEAR % 100:02d}{n:05d}"


def _box(pid: int, team: str, game: int, day: date, minutes: int) -> None:
    stats = {k: int(round(v * minutes)) for k, v in PER_MINUTE.items()}
    PlayerGameStats.create(player=pid, team=team, game=_game_id(game), game_date=day, min=minutes, fpts=0, **stats)


def _stamp(when: datetime) -> datetime:
    """A moment as the tables keep it: UTC, without a zone."""
    return when.astimezone(timezone.utc).replace(tzinfo=None)


def _news(pid: int, kind: str, headline: str, *, ago: timedelta = timedelta(hours=4),
          source: str = "espn_player_news") -> str:
    """A stored item about a player, fetched `ago` before the morning's clock. Returns its ref."""
    at = _stamp(NOW - ago)
    item = NewsItem.create(source=source, source_item_id=f"{pid}-{headline}", published_at=at, fetched_at=at,
                           headline=headline, url=f"https://news.example/{pid}/{kind}", kind=kind)
    NewsItemPlayer.create(news_item=item, player=pid, linked_by="espn_id")
    return f"news:{item.id}"


def _board(source: str, ranks: dict[int, int], day: date = TODAY) -> None:
    for board in BOARDS:
        DailyRanking.replace_day(day, SEASON, source, board,
                                 [{"player_id": pid, "rank": rank} for pid, rank in ranks.items()])


def _clock(monkeypatch, hour: int = 6, now: datetime = NOW) -> None:
    for module in (live_projection, analyst):
        monkeypatch.setattr(module, "now_eastern", lambda: datetime(TODAY.year, TODAY.month, TODAY.day, hour, 0))
        monkeypatch.setattr(module, "today_et", lambda now=None: TODAY)
    monkeypatch.setattr(analyst, "utc_now", lambda: now)


def _live(force: bool = False) -> None:
    result = LiveProjectionPipeline()._run_sync(options={"force": True} if force else None)
    assert result.status == ApiStatus.SUCCESS


@pytest.fixture(autouse=True)
def morning(analyst_tables, clean_integration_tables, monkeypatch):
    db.execute_sql("""
        TRUNCATE TABLE nba.analyst_reads, nba.daily_rankings, nba.projection_adjustments, nba.player_projections,
                       nba.player_history, nba.player_profiles
        RESTART IDENTITY CASCADE
    """)
    stamp = datetime.utcnow()
    roster = ((STAR, "Star", "DEN", 34), (STARTER, "Starter", "DEN", 30), (BENCH, "Bench", "DEN", 12),
              (WAITING, "Waiting", "DEN", 28), (GONE, "Gone", "BOS", 30), (CELTIC, "Celtic", "BOS", 32),
              (UNPROJECTED, "Unprojected", "BOS", 18))
    for pid, name, team, minutes in roster:
        Player.create(id=pid, name=name, name_normalized=name.lower())
        PlayerProfile.create(player=pid, team=team, updated_at=stamp)
        if pid != UNPROJECTED:
            line = {k: round(v * minutes, 2) for k, v in PER_MINUTE.items()} | {"min": float(minutes)}
            PlayerProjection.record_projection(player_id=pid, season=SEASON, as_of_date=PRESEASON, line=line,
                                               projected_gp=70, source="cv")
    PlayerHistory.create(player_id=BENCH, season=LAST_SEASON, player_name="Bench", team="DEN", age=20.4,
                         from_year=YEAR - 1, gp=60, min=700)

    for n, day in enumerate(PLAYED + AHEAD):
        Game.create(game_id=_game_id(n), game_date=day, season=SEASON, home_team="DEN", away_team="BOS",
                    status="final" if day < TODAY else "scheduled")
    for n, day in enumerate(PLAYED):
        if n < 2:
            _box(STAR, "DEN", n, day, 34)          # the star has missed the last two
        _box(STARTER, "DEN", n, day, 30)
        _box(BENCH, "DEN", n, day, 12 if n < 2 else 24)
        _box(CELTIC, "BOS", n, day, 32)
        _box(UNPROJECTED, "BOS", n, day, 18)

    # The feed ruled the star out three hours ago, back for the third game from here
    PlayerAvailability.create(player=STAR, game_date=TODAY, verdict="out", reason="calf",
                              expected_return=AHEAD[2], changed_at=_stamp(NOW - timedelta(hours=3)))
    ProjectionAdjustment.record(GONE, SEASON, kind="injury", games=0, note="Achilles", author="test")
    _clock(monkeypatch)
    load_coefficients.cache_clear()
    _live()
    _board(SOURCE_CV, {pid: rank for rank, pid in enumerate(LIVE_ORDER, start=1)})
    _board(SOURCE_ESPN, ESPN_ORDER, day=TODAY - timedelta(days=1))
    yield


@pytest.fixture
def refs(morning) -> dict[int, str]:
    """The stored news of the morning: an injury report on the star and a lineup note on his
    fill-in overnight, an older injury item on the Celtic, and a rumor about the starter."""
    return {
        STAR: _news(STAR, "injury", "Star (calf) will be re-evaluated in two weeks"),
        BENCH: _news(BENCH, "lineup", "Bench will start while Star is out", ago=timedelta(hours=5)),
        CELTIC: _news(CELTIC, "injury", "Celtic (ankle) is day to day", ago=timedelta(days=5)),
        STARTER: _news(STARTER, "rumor", "Starter is drawing trade interest", ago=timedelta(hours=2)),
    }


class Backend:
    """The backend's analyst routes, scripted: an answer per player, and the month's ledger."""

    def __init__(self, answers: dict | None = None, month: Month | None = None):
        self.answers = answers or {}
        self.bodies: list[dict] = []
        self.month_asked = 0
        self._month = month or _month()
        self._lock = threading.Lock()

    def month(self, correlation_id=None) -> Month:
        self.month_asked += 1
        return self._month

    def read(self, body, correlation_id=None) -> ReadResult:
        with self._lock:
            self.bodies.append(dict(body))
        answer = self.answers.get(body["player_id"], NOTHING)
        if isinstance(answer, ReadResult):
            return answer
        return ReadResult(outcome="read", proposal=answer, model="test-model", prompt_version="analyst-1",
                          input_tokens=2_000, output_tokens=300, cost_micros=COST)

    @property
    def asked(self) -> list[int]:
        return sorted(body["player_id"] for body in self.bodies)

    def body(self, pid: int) -> dict:
        return next(body for body in self.bodies if body["player_id"] == pid)


class Boards:
    """The backend's daily boards, scripted."""

    def __init__(self, boards: DailyBoards | None = None):
        self.boards = boards or _boards()
        self.calls: list[bool] = []

    def daily_boards(self, correlation_id=None, shadow=False) -> DailyBoards:
        self.calls.append(shadow)
        return self.boards


def _month(*, enabled: bool = True, spent: int = 0) -> Month:
    return Month(available=True, enabled=enabled, spent_micros=spent, cap_micros=40_000_000,
                 read_max_micros=300_000, model="test-model", prompt_version="analyst-1")


def _boards(order=SHADOW_ORDER, *, as_of: date = TODAY, live_as_of: date | None = TODAY) -> DailyBoards:
    players = tuple(
        DailyRank(player_id=pid, line_source="live", season_gp=4, injury_status=None,
                  points=BoardPlace(rank=rank, value=40.0 - rank, score=900.0 - rank, games=60.0),
                  categories=BoardPlace(rank=rank, value=1.0, score=9.0 - rank, games=60.0))
        for rank, pid in enumerate(order, start=1)
    )
    return DailyBoards(available=True, season=SEASON, as_of=as_of, in_season=True, model="cv_live_shadow",
                       live_as_of=live_as_of, players=players)


def _run(answers: dict | None = None, *, month: Month | None = None, boards: DailyBoards | None = None):
    """One run of the analyst against a scripted backend."""
    pipeline = AnalystPipeline()
    pipeline.client = backend = Backend(answers, month)
    pipeline.valuation_client = valuation = Boards(boards)
    return pipeline._run_sync(), backend, valuation


def _read_at(when: datetime = NOW) -> None:
    """Date the reads so far as the morning's clock has them: a test's clock is not the wall's."""
    AnalystRead.update(created_at=_stamp(when)).execute()


def _reads() -> dict[int, AnalystRead]:
    return {r.player_id: r for r in AnalystRead.select().order_by(AnalystRead.id)}


def _order() -> list[int]:
    """The players read, in the order the morning took them."""
    return [r.player_id for r in AnalystRead.select().order_by(AnalystRead.id)]


def _notes() -> dict[int, ProjectionAdjustment]:
    return {r.player_id: r for r in ProjectionAdjustment.active_for(SEASON, SCOPE_ANALYST)}


def _rows(source: str, day: date = TODAY) -> dict[int, PlayerProjection]:
    return {
        r.player_id: r for r in PlayerProjection.select().where(
            (PlayerProjection.source == source) & (PlayerProjection.as_of_date == day))
    }


def _later(refs: dict[int, str], **over) -> dict:
    """The read of the star's report: back for the fifth game from here, not the third."""
    return {"decision": "write", "kind": "injury_current", "confidence": "likely",
            "reason": "Re-evaluated in two weeks is not back in two weeks.",
            "return_date": AHEAD[4].isoformat(), "return_earliest": AHEAD[3].isoformat(),
            "return_latest": AHEAD[5].isoformat(), "review_date": (TODAY + timedelta(days=7)).isoformat(),
            "cites": [refs[STAR]]} | over


def _starts(refs: dict[int, str], **over) -> dict:
    """The read of the fill-in's lineup note: a starter's minutes until the star is back."""
    return {"decision": "write", "kind": "role", "confidence": "likely",
            "reason": "He starts while Star is out.", "minutes": 28, "usage": 1.1,
            "until_player_id": STAR, "cites": [refs[BENCH]]} | over


# ---- a morning ------------------------------------------------------------------------------


def test_a_morning_reads_the_short_list_and_logs_every_read(refs):
    assert due_reason(SEASON, TODAY) is None

    result, backend, _ = _run({STAR: _later(refs), BENCH: _starts(refs)})

    assert result.status == ApiStatus.SUCCESS and result.records_failed == 0
    # The starter has only a rumor and the rookie nothing: a rule alone picks nobody.
    # The rest are taken by the strongest rule that picked them: a status change, a new item, a rank gap
    assert backend.asked == [STAR, BENCH, CELTIC] and _order() == [STAR, BENCH, CELTIC]

    star = backend.body(STAR)
    assert (star["season"], star["as_of"], star["name"], star["team"]) == (SEASON, TODAY.isoformat(), "Star", "DEN")
    assert star["reasons"] == [
        "The availability feed now has him out; it had no report on him before",
        "1 new item since his last read: injury",
    ]
    assert star["projection"]["out_now"] is True and star["projection"]["return_date"] == AHEAD[2].isoformat()
    assert star["projection"]["missed_in_a_row"] == 2 and star["projection"]["team_games_left"] == 6
    assert star["availability"] == {"verdict": "out", "for_game_on": TODAY.isoformat(), "reason": "calf",
                                    "expected_return": AHEAD[2].isoformat()}
    assert star["items"] == [{"ref": refs[STAR], "kind": "injury", "citable": True, "source": "espn_player_news",
                              "at": f"{TODAY.isoformat()} 02:00",
                              "text": "Star (calf) will be re-evaluated in two weeks"}]
    assert [g["date"] for g in star["recent_games"]] == [PLAYED[1].isoformat(), PLAYED[0].isoformat()]
    assert star["bounds"]["review_by"] == (TODAY + timedelta(days=21)).isoformat()
    assert star["bounds"]["season_end"] == AHEAD[-1].isoformat()
    assert "own_note" not in star and "owner_note" not in star

    bench = backend.body(BENCH)
    assert bench["reasons"][0] == "1 new item since his last read: lineup"
    assert bench["reasons"][1].startswith("Star, who plays 34") and bench["reasons"][1].endswith("is now out")
    mates = {t["player_id"]: t for t in bench["teammates"]}
    assert set(mates) == {STAR, STARTER, WAITING}
    assert mates[STAR]["out"] is True and mates[STAR]["status"] == "out"
    assert mates[STAR]["expected_return"] == AHEAD[2].isoformat() and mates[STARTER]["out"] is False
    # What he plays already, plus what the star left behind: the ceiling is the cap on anyone's minutes
    assert bench["bounds"]["minutes_max"] == 38.0

    assert backend.body(CELTIC)["reasons"] == ["Ranked 2 here and 40 by ESPN"]

    reads = _reads()
    assert {pid: r.outcome for pid, r in reads.items()} == {STAR: "note", BENCH: "note", CELTIC: "none"}
    assert [r["rule"] for r in reads[STAR].reasons] == ["status_changed", "new_item"]
    assert [r["rule"] for r in reads[BENCH].reasons] == ["new_item", "teammate_change"]
    star_read = reads[STAR]
    assert (star_read.season, star_read.as_of_date, star_read.model, star_read.prompt_version) == (
        SEASON, TODAY, "test-model", "analyst-1")
    assert (star_read.input_tokens, star_read.output_tokens, star_read.cost_micros) == (2_000, 300, COST)
    assert star_read.summary == "Re-evaluated in two weeks is not back in two weeks."
    assert star_read.proposal["return_date"] == AHEAD[4].isoformat() and len(star_read.fingerprint) == 64
    assert star_read.pipeline_run_id is not None and star_read.disagrees is None
    assert reads[CELTIC].note_id is None and reads[CELTIC].summary == "Nothing a box score cannot say."

    notes = _notes()
    assert set(notes) == {STAR, BENCH} and star_read.note_id == notes[STAR].id
    star_note = notes[STAR]
    assert (star_note.author, star_note.kind, star_note.confidence) == ("analyst", "injury_current", "likely")
    assert (star_note.return_date, star_note.return_earliest, star_note.return_latest) == (
        AHEAD[4], AHEAD[3], AHEAD[5])
    assert star_note.review_date == TODAY + timedelta(days=7)
    assert star_note.note == "Re-evaluated in two weeks is not back in two weeks."
    assert star_note.source_url == f"https://news.example/{STAR}/injury"
    (evidence,) = star_note.evidence
    assert (evidence["type"], f"news:{evidence['id']}", evidence["kind"]) == ("news", refs[STAR], "injury")
    assert evidence["headline"] == "Star (calf) will be re-evaluated in two weeks"
    bench_note = notes[BENCH]
    assert (float(bench_note.minutes), float(bench_note.usage), bench_note.depends_on_player_id) == (28.0, 1.1, STAR)
    # No review date came back: two weeks, inside the three it may run
    assert bench_note.review_date == TODAY + timedelta(days=14)
    # The notes written by hand are another scope, and nothing touched them
    assert ProjectionAdjustment.active_for(SEASON, "season") == []
    assert [r.player_id for r in ProjectionAdjustment.active_for(SEASON)] == [GONE]

    assert due_reason(SEASON, TODAY) == "the analyst has run this morning"


def test_its_notes_move_a_second_projection_and_second_boards_and_nothing_else(refs):
    live_before = {pid: (r.id, r.raw) for pid, r in _rows(SOURCE).items()}
    boards_before = DailyRanking.ranks_on(TODAY, SOURCE_CV, BOARD_POINTS)

    result, _, valuation = _run({STAR: _later(refs), BENCH: _starts(refs)})

    assert result.status == ApiStatus.SUCCESS
    notes = _notes()
    live, shadow = _rows(SOURCE), _rows(SHADOW_SOURCE)
    assert {pid: (r.id, r.raw) for pid, r in live.items()} == live_before
    assert set(shadow) == set(EVERYONE)

    # The star: the feed's return date on the projection everybody sees, the note's on the second
    assert live[STAR].raw["parts"]["known"] == {"out_next": True, "out_for_season": False,
                                                "return_date": AHEAD[2].isoformat()}
    assert shadow[STAR].raw["parts"]["known"] == {"out_next": True, "out_for_season": False,
                                                  "return_date": AHEAD[4].isoformat(), "note": notes[STAR].id}
    games = expand(shadow[STAR].raw["timeline"], AHEAD)
    assert [g.chance for g in games[:4]] == [0.0] * 4 and all(g.chance > 0.5 for g in games[4:])
    assert shadow[STAR].raw["games"] < live[STAR].raw["games"]

    # His fill-in: the note's minutes for as long as the star is out, which is now two games longer
    role = shadow[BENCH].raw["parts"]["note"]
    assert (role["id"], role["minutes"], role["usage"], role["depends_on"]) == (notes[BENCH].id, 28.0, 1.1, STAR)
    assert role["through"] == AHEAD[3].isoformat()
    fill_in = expand(shadow[BENCH].raw["timeline"], AHEAD)
    assert [(g.minutes, g.usage) for g in fill_in[:4]] == [(28.0, 1.1)] * 4
    assert "note" not in live[BENCH].raw["parts"]
    # A player on the other team plays the same games and minutes on both (his share of the offense
    # moves a hair: the model centers every team's missing possessions on the league's)
    assert [run[:5] for run in shadow[CELTIC].raw["timeline"]] == [run[:5] for run in live[CELTIC].raw["timeline"]]
    assert shadow[CELTIC].raw["timeline"][0][5] == pytest.approx(live[CELTIC].raw["timeline"][0][5], abs=0.02)
    assert shadow[STAR].pipeline_run_id != live[STAR].pipeline_run_id

    # The boards are the backend's, built on the second projection, and stored apart
    assert valuation.calls == [True]
    assert DailyRanking.ranks_on(TODAY, SOURCE_CV, BOARD_POINTS) == boards_before
    for board in (BOARD_POINTS, BOARD_CATEGORIES):
        assert DailyRanking.ranks_on(TODAY, SHADOW_BOARDS, board) == {
            pid: rank for rank, pid in enumerate(SHADOW_ORDER, start=1)}
    row = DailyRanking.get((DailyRanking.source == SHADOW_BOARDS) & (DailyRanking.board == BOARD_POINTS)
                           & (DailyRanking.player == STAR))
    assert (row.season, row.model, float(row.value), float(row.games)) == (SEASON, "cv_live_shadow", 37.0, 60.0)
    assert result.records_processed == 3 + len(EVERYONE) + 2 * len(EVERYONE)


def test_a_player_whose_facts_have_not_changed_is_not_read_again(refs):
    _run({STAR: _later(refs), BENCH: _starts(refs)})
    _read_at()
    first_run = {r.pipeline_run_id for r in _rows(SHADOW_SOURCE).values()}

    again, backend, valuation = _run()

    # The Celtic's rank gap and the fill-in's teammate still pick them; what a read would see has not moved
    assert again.status == ApiStatus.SUCCESS and backend.bodies == [] and again.records_skipped == 2
    assert AnalystRead.select().count() == 3 and set(_notes()) == {STAR, BENCH}
    # The second projection and boards are the morning's, built again
    assert set(_rows(SHADOW_SOURCE)) == set(EVERYONE)
    assert not first_run & {r.pipeline_run_id for r in _rows(SHADOW_SOURCE).values()}
    assert valuation.calls == [True] and DailyRanking.stored(TODAY, SHADOW_BOARDS) == 2 * len(EVERYONE)


def test_a_new_item_is_read_and_the_rest_of_the_morning_is_not(refs, monkeypatch):
    _run({STAR: _later(refs), BENCH: _starts(refs)})
    _read_at()

    newer = _news(CELTIC, "injury", "Celtic (ankle) is out at least a week", ago=timedelta(hours=-1))
    _clock(monkeypatch, hour=8, now=NOW + timedelta(hours=2))
    again, backend, _ = _run({CELTIC: {
        "decision": "write", "kind": "injury_current", "confidence": "possible", "reason": "Out a week.",
        "return_date": AHEAD[3].isoformat(), "cites": [newer],
    }})

    assert again.status == ApiStatus.SUCCESS and backend.asked == [CELTIC]
    body = backend.body(CELTIC)
    assert body["reasons"] == ["1 new item since his last read: injury", "Ranked 2 here and 40 by ESPN"]
    assert [item["ref"] for item in body["items"]] == [newer, refs[CELTIC]]
    assert set(_notes()) == {STAR, BENCH, CELTIC} and _notes()[CELTIC].return_date == AHEAD[3]
    assert AnalystRead.select().where(AnalystRead.player == CELTIC).count() == 2
    assert _rows(SHADOW_SOURCE)[CELTIC].raw["parts"]["known"]["return_date"] == AHEAD[3].isoformat()


def test_a_morning_reads_no_more_than_its_cap_strongest_reason_first(refs, monkeypatch):
    monkeypatch.setattr(settings, "analyst_max_reads", 1)

    result, backend, _ = _run({STAR: _later(refs), BENCH: _starts(refs)})

    assert result.status == ApiStatus.SUCCESS and backend.asked == [STAR] and result.records_skipped == 2
    assert set(_reads()) == {STAR} and set(_notes()) == {STAR}

    # A run later the same morning takes the next
    _read_at()
    _, backend, _ = _run({BENCH: _starts(refs)})
    assert backend.asked == [BENCH] and set(_notes()) == {STAR, BENCH}


# ---- what becomes of an answer ------------------------------------------------------------------


def test_a_proposal_outside_its_bounds_or_without_a_source_is_refused_and_logged(refs):
    result, backend, _ = _run({
        STAR: _later(refs, cites=["news:999999"]),                      # an item the read was never shown
        BENCH: _starts(refs, minutes=45),                               # more than there is to give
        CELTIC: {"decision": "write", "kind": "role", "reason": "Rumored to lose minutes.", "minutes": 20,
                 "cites": []},                                         # no source at all
    })

    assert result.status == ApiStatus.SUCCESS and backend.asked == [STAR, BENCH, CELTIC]
    reads = _reads()
    assert {r.outcome for r in reads.values()} == {"refused"} and _notes() == {}
    assert reads[STAR].summary == "Refused: it cites items the read was not shown: news:999999"
    assert reads[BENCH].summary == "Refused: 45 minutes is over its bound of 38"
    assert reads[CELTIC].summary == (
        "Refused: it cites no stored injury, availability, lineup or transaction item")
    # What came back is kept as it came back
    assert reads[BENCH].proposal["minutes"] == 45 and reads[STAR].cost_micros == COST
    # With nothing written the second projection is the first
    live, shadow = _rows(SOURCE), _rows(SHADOW_SOURCE)
    assert {pid: r.raw["timeline"] for pid, r in shadow.items()} == {pid: r.raw["timeline"] for pid, r in live.items()}

    # A refusal is an answer: the same facts are not put to it again
    _read_at()
    _, backend, _ = _run()
    assert backend.bodies == []


def test_a_rumor_backs_nothing(refs):
    # The starter's only item is a rumor: cited, it is not a source; and he is not on the list at all
    result, backend, _ = _run()
    assert STARTER not in backend.asked

    _news(STARTER, "lineup", "Starter moves to the bench", ago=timedelta(hours=-1))
    _read_at()
    AnalystRead.delete().execute()
    _, backend, _ = _run({STARTER: {"decision": "write", "kind": "role", "reason": "Trade talk.", "minutes": 22,
                                    "cites": [refs[STARTER]]}})

    assert STARTER in backend.asked
    shown = {item["ref"]: item for item in backend.body(STARTER)["items"]}
    assert shown[refs[STARTER]]["citable"] is False
    read = _reads()[STARTER]
    assert read.outcome == "refused" and "cites no stored injury" in read.summary and STARTER not in _notes()


def test_a_note_written_by_hand_wins_and_the_analyst_only_says_it_disagrees(refs):
    # James read the same report this morning as the fourth game from here
    written = EASTERN.localize(datetime.combine(TODAY, time(5, 0))).astimezone(pytz.utc).replace(tzinfo=None)
    owner = ProjectionAdjustment.record(
        STAR, SEASON, scope="season", kind="injury_current", note="read off the report", author="james",
        return_date=AHEAD[3], review_date=TODAY + timedelta(days=14))
    ProjectionAdjustment.update(created_at=written).where(ProjectionAdjustment.id == owner.id).execute()
    _live(force=True)

    result, backend, _ = _run({STAR: _later(refs, against_owner_note="The report reads as a week longer.")})

    assert result.status == ApiStatus.SUCCESS
    body = backend.body(STAR)
    assert body["owner_note"]["return_date"] == AHEAD[3].isoformat()
    assert body["owner_note"]["standing"]["state"] == "in_force"
    assert body["projection"]["return_date"] == AHEAD[3].isoformat()
    # Its own note is stored beside his, in its own scope; his is not superseded
    read = _reads()[STAR]
    assert read.outcome == "note" and read.disagrees == "The report reads as a week longer."
    assert _notes()[STAR].return_date == AHEAD[4]
    assert [r.id for r in ProjectionAdjustment.active_for(SEASON, "season")] == [owner.id]
    # ... and the second projection reads his
    live, shadow = _rows(SOURCE), _rows(SHADOW_SOURCE)
    assert shadow[STAR].raw["parts"]["known"] == live[STAR].raw["parts"]["known"]
    assert shadow[STAR].raw["parts"]["known"]["note"] == owner.id
    assert shadow[STAR].raw["timeline"] == live[STAR].raw["timeline"]


def test_its_own_note_comes_due_and_is_kept_or_retired(refs):
    # Yesterday's notes: the star's is due to be read again this morning, the fill-in's is not
    yesterday = _stamp(NOW - timedelta(days=1))
    for pid, fields in (
        (STAR, dict(kind="injury_current", return_date=AHEAD[4], review_date=TODAY,
                    evidence=[{"type": "news", "id": 77, "kind": "injury", "headline": "an older report"}])),
        (BENCH, dict(kind="role", minutes=28, depends_on_player=STAR, review_date=TODAY + timedelta(days=5))),
    ):
        note = ProjectionAdjustment.record(pid, SEASON, scope=SCOPE_ANALYST, author="analyst", confidence="likely",
                                           note="yesterday's read", **fields)
        ProjectionAdjustment.update(created_at=yesterday).where(ProjectionAdjustment.id == note.id).execute()
    old_star, old_bench = _notes()[STAR], _notes()[BENCH]

    result, backend, _ = _run({
        STAR: {"decision": "keep", "reason": "Nothing new: still two weeks.",
               "review_date": (TODAY + timedelta(days=6)).isoformat(), "cites": [refs[STAR]]},
        BENCH: {"decision": "retire", "reason": "The coach said he goes back to the bench."},
    })

    assert result.status == ApiStatus.SUCCESS
    # A note that is due is the strongest reason there is
    assert _order() == [STAR, BENCH, CELTIC]
    star = backend.body(STAR)
    assert star["reasons"][0] == "His note is due to be read again today"
    assert star["own_note"]["return_date"] == AHEAD[4].isoformat() and star["own_note"]["cites"] == ["news:77"]
    assert star["own_note"]["written"] == (TODAY - timedelta(days=1)).isoformat()

    reads = _reads()
    assert (reads[STAR].outcome, reads[BENCH].outcome) == ("unchanged", "retired")
    kept = _notes()[STAR]
    # Kept: the same note carried on to a new review date, resting on what it did and what it was shown now
    assert kept.id != old_star.id and reads[STAR].note_id == kept.id
    assert (kept.return_date, kept.review_date, kept.note) == (AHEAD[4], TODAY + timedelta(days=6), "yesterday's read")
    assert [f"{e['type']}:{e['id']}" for e in kept.evidence] == ["news:77", refs[STAR]]
    assert ProjectionAdjustment.get_by_id(old_star.id).superseded_by_id == kept.id
    # Retired: withdrawn, and the read is left with no note
    assert BENCH not in _notes() and reads[BENCH].note_id is None
    assert ProjectionAdjustment.get_by_id(old_bench.id).retired_at is not None
    assert "note" not in _rows(SHADOW_SOURCE)[BENCH].raw["parts"]

    # Neither is read again on what it was just shown
    _read_at()
    _, backend, _ = _run()
    assert STAR not in backend.asked and BENCH not in backend.asked


def test_a_report_from_the_availability_log_is_a_source(refs):
    report = AvailabilityReport.create(
        player=CELTIC, game_date=TODAY, source="nba_report", status="out", reason="ankle sprain",
        expected_return=AHEAD[2], confidence=90, observed_at=_stamp(NOW - timedelta(hours=1)))
    # The box score's own reports are facts the projection reads already, not news
    AvailabilityReport.create(player=CELTIC, game_date=PLAYED[-1], source="box_score", status="active",
                              confidence=100, observed_at=_stamp(NOW - timedelta(hours=30)))

    _, backend, _ = _run({CELTIC: {"decision": "write", "kind": "injury_current", "reason": "The league's report.",
                                   "return_date": AHEAD[2].isoformat(), "cites": [f"report:{report.id}"]}})

    items = backend.body(CELTIC)["items"]
    assert [item["ref"] for item in items] == [f"report:{report.id}", refs[CELTIC]]
    assert items[0]["text"] == f"Out for {TODAY.isoformat()}. ankle sprain. Expected back {AHEAD[2].isoformat()}"
    assert (items[0]["kind"], items[0]["citable"], items[0]["source"]) == ("availability", True, "nba_report")
    (evidence,) = _notes()[CELTIC].evidence
    assert (evidence["type"], evidence["id"], evidence["source"]) == ("report", report.id, "nba_report")


# ---- where a run stops --------------------------------------------------------------------------


def test_with_the_months_budget_used_up_nothing_is_asked_and_the_boards_are_still_built(refs):
    result, backend, valuation = _run(month=_month(spent=39_900_000))

    assert result.status == ApiStatus.SUCCESS and backend.bodies == [] and result.records_skipped == 3
    assert AnalystRead.select().count() == 0
    assert set(_rows(SHADOW_SOURCE)) == set(EVERYONE) and valuation.calls == [True]
    assert DailyRanking.stored(TODAY, SHADOW_BOARDS) == 2 * len(EVERYONE)


def test_a_budget_that_runs_out_mid_morning_stops_the_asking(refs, monkeypatch):
    monkeypatch.setattr(analyst, "READ_WORKERS", 1)

    result, backend, _ = _run({STAR: _later(refs), BENCH: ReadResult(outcome="budget")})

    assert result.status == ApiStatus.SUCCESS and backend.asked == [STAR, BENCH]
    assert set(_reads()) == {STAR} and result.records_skipped == 2
    # The morning is done as far as it could be: tomorrow's budget, or next month's, takes the rest
    assert due_reason(SEASON, TODAY) == "the analyst has run this morning"


def test_switched_off_on_the_backend_it_does_nothing(refs):
    result, backend, valuation = _run(month=_month(enabled=False))

    assert result.status == ApiStatus.SUCCESS and result.records_skipped == 1
    assert backend.bodies == [] and valuation.calls == []
    assert _rows(SHADOW_SOURCE) == {} and DailyRanking.stored(TODAY, SHADOW_BOARDS) == 0


def test_a_backend_that_cannot_be_asked_fails_the_run_and_the_next_one_reads_again(refs):
    result, backend, valuation = _run(month=Month.unavailable("timeout"))
    assert result.status == ApiStatus.ERROR and "timeout" in result.error
    assert backend.bodies == [] and valuation.calls == [] and AnalystRead.select().count() == 0

    down = ReadResult.unavailable("http_503")
    result, backend, valuation = _run({STAR: down, BENCH: down, CELTIC: down})

    assert result.status == ApiStatus.ERROR and "could not be asked for the last 3 reads (http_503)" in result.error
    reads = _reads()
    assert {r.outcome for r in reads.values()} == {"failed"} and len(reads) == 3
    assert reads[STAR].summary == "The backend could not be asked (http_503)."
    # What there is was still built, on the notes there are
    assert set(_rows(SHADOW_SOURCE)) == set(EVERYONE) and valuation.calls == [True]
    assert due_reason(SEASON, TODAY) is None

    # The next tick starts it again, and a read that did not complete is asked again
    result, backend, _ = _run({STAR: _later(refs)})
    assert result.status == ApiStatus.SUCCESS and backend.asked == [STAR, BENCH, CELTIC]
    assert _reads()[STAR].outcome == "note" and AnalystRead.select().count() == 6


def test_an_answer_that_is_no_proposal_or_was_declined_is_logged_and_asked_again(refs):
    result, _, _ = _run({
        STAR: ReadResult(outcome="malformed", detail="the answer was cut off", model="test-model", cost_micros=9_000),
        BENCH: ReadResult(outcome="declined", model="test-model", cost_micros=8_000),
    })

    assert result.status == ApiStatus.SUCCESS and result.partial is True and result.records_failed == 1
    reads = _reads()
    assert (reads[STAR].outcome, reads[STAR].summary) == (
        "failed", "The answer was not a proposal: the answer was cut off.")
    assert (reads[BENCH].outcome, reads[BENCH].summary) == ("declined", "The model declined the request.")
    assert (reads[STAR].cost_micros, reads[BENCH].cost_micros) == (9_000, 8_000) and _notes() == {}

    _read_at()
    _, backend, _ = _run({STAR: _later(refs), BENCH: _starts(refs)})
    assert backend.asked == [STAR, BENCH] and set(_notes()) == {STAR, BENCH}


@pytest.mark.parametrize("boards, why", [
    (DailyBoards.unavailable("http_500"), "could not be asked for the shadow boards (http_500)"),
    (_boards(live_as_of=TODAY - timedelta(days=1)), "not built on this morning's shadow projection"),
    (_boards(live_as_of=None), "not built on this morning's shadow projection"),
    (_boards(as_of=TODAY - timedelta(days=1), live_as_of=TODAY - timedelta(days=1)),
     "not built on this morning's shadow projection"),
])
def test_boards_that_are_not_this_mornings_shadow_are_not_stored(refs, boards, why):
    result, backend, _ = _run({STAR: _later(refs)}, boards=boards)

    assert result.status == ApiStatus.ERROR and why in result.error
    assert DailyRanking.stored(TODAY, SHADOW_BOARDS) == 0
    # The reads were made and are kept; the next tick builds the boards without asking again
    assert backend.asked == [STAR, BENCH, CELTIC] and set(_notes()) == {STAR}
    assert due_reason(SEASON, TODAY) is None
    _read_at()
    result, backend, _ = _run()
    assert result.status == ApiStatus.SUCCESS and backend.bodies == []
    assert DailyRanking.stored(TODAY, SHADOW_BOARDS) == 2 * len(EVERYONE)


def test_it_waits_for_the_mornings_projection_and_boards(refs, monkeypatch):
    PlayerProjection.delete().where(PlayerProjection.source == SOURCE).execute()
    assert due_reason(SEASON, TODAY) == "this morning's live projection is not stored yet"
    result, backend, valuation = _run()
    assert result.status == ApiStatus.SUCCESS and result.records_skipped == 1
    assert backend.month_asked == 0 and backend.bodies == [] and valuation.calls == []

    _live()
    DailyRanking.delete().where(DailyRanking.source == SOURCE_CV).execute()
    assert due_reason(SEASON, TODAY) == "this morning's boards are not stored yet"

    # Past midnight Eastern the day the backend builds for is over
    _clock(monkeypatch, hour=1)
    result, backend, _ = _run()
    assert result.status == ApiStatus.SUCCESS and result.records_skipped == 1 and backend.month_asked == 0


def test_the_second_projection_is_kept_a_week(refs):
    for days in (3, 8):
        PlayerProjection.record_projection(player_id=STAR, season=SEASON, as_of_date=TODAY - timedelta(days=days),
                                           line={"min": 30}, source=SHADOW_SOURCE)
        PlayerProjection.record_projection(player_id=STAR, season=SEASON, as_of_date=TODAY - timedelta(days=days),
                                           line={"min": 30}, source=SOURCE)

    _run()

    kept = {(r.source, r.as_of_date) for r in PlayerProjection.select().where(
        (PlayerProjection.player == STAR) & (PlayerProjection.as_of_date < TODAY)
        & PlayerProjection.source.in_([SOURCE, SHADOW_SOURCE]))}
    assert kept == {(SHADOW_SOURCE, TODAY - timedelta(days=3)), (SOURCE, TODAY - timedelta(days=3)),
                    (SOURCE, TODAY - timedelta(days=8))}


# ---- what the screen is built from ----------------------------------------------------------------


def test_the_feed_before_and_now_and_what_counts_as_new():
    # The starter was questionable for last night's game and is out for today's
    PlayerAvailability.create(player=STARTER, game_date=PLAYED[-1], verdict="questionable",
                              changed_at=_stamp(NOW - timedelta(days=2, hours=3)))
    PlayerAvailability.create(player=STARTER, game_date=TODAY, verdict="out", reason="knee",
                              changed_at=_stamp(NOW - timedelta(hours=1)))
    # The Celtic's verdict flipped during the day of one game; the fill-in's is too old to be today's
    PlayerAvailability.create(player=CELTIC, game_date=TODAY, verdict="available", previous_verdict="out",
                              changed_at=_stamp(NOW - timedelta(hours=2)))
    PlayerAvailability.create(player=BENCH, game_date=TODAY - timedelta(days=4), verdict="out")

    feed = analyst_inputs.load_feed(TODAY)

    assert set(feed) == {STAR, STARTER, CELTIC}
    assert (feed[STARTER].verdict, feed[STARTER].before, feed[STARTER].reason) == ("out", "questionable", "knee")
    assert (feed[CELTIC].verdict, feed[CELTIC].before) == ("available", "out")
    assert (feed[STAR].verdict, feed[STAR].before, feed[STAR].expected_return) == ("out", None, AHEAD[2])
    assert feed[STAR].changed_at == NOW - timedelta(hours=3)

    # The same story from two sources is shown once; an item older than two weeks is not shown
    first = _news(STAR, "injury", "Star out two weeks")
    NewsItem.update(content_hash="same-story").where(NewsItem.id == int(first.split(":")[1])).execute()
    second = _news(STAR, "injury", "Star (calf) out two weeks", ago=timedelta(hours=6), source="rotowire_rss")
    NewsItem.update(content_hash="same-story").where(NewsItem.id == int(second.split(":")[1])).execute()
    _news(STAR, "injury", "Star tweaks a calf in camp", ago=timedelta(days=20))
    items = analyst_inputs.load_items(NOW)
    assert [item.ref for item in items[STAR]] == [first]
    assert items[STAR][0].seen == NOW - timedelta(hours=4) and items[STAR][0].evidence["url"].endswith("/injury")


# ---- the digest -----------------------------------------------------------------------------------


def test_the_digest_is_the_morning_as_the_editor_shows_it(refs):
    empty = analyst_digest.build(SEASON, None, _month(), TODAY)
    assert (empty.as_of, empty.days, empty.reads, empty.notes_live, empty.shadow_as_of) == (None, [], [], 0, None)
    assert empty.in_shadow is True and empty.spend.month_cap_usd == 40.0

    _run({STAR: _later(refs), BENCH: _starts(refs, minutes=45)})

    digest = analyst_digest.build(SEASON, None, _month(spent=1_250_000), TODAY)

    assert (digest.season, digest.as_of, digest.days) == (SEASON, TODAY, [TODAY])
    assert digest.counts == {"note": 1, "refused": 1, "none": 1} and digest.cost_usd == pytest.approx(0.042)
    assert (digest.notes_live, digest.shadow_as_of) == (1, TODAY)
    assert (digest.spend.available, digest.spend.enabled, digest.spend.month_spent_usd) == (True, True, 1.25)
    assert (digest.spend.model, digest.spend.prompt_version) == ("test-model", "analyst-1")

    reads = {r.player_id: r for r in digest.reads}
    star = reads[STAR]
    assert (star.name, star.team, star.outcome, star.note_live, star.owner_note) == ("Star", "DEN", "note", True, False)
    assert [r.rule for r in star.reasons] == ["status_changed", "new_item"]
    assert star.summary == "Re-evaluated in two weeks is not back in two weeks." and star.cost_usd == 0.014
    assert (star.note.scope, star.note.author, star.note.state) == ("analyst", "analyst", "live")
    assert (star.note.return_date, star.note.return_earliest) == (AHEAD[4], AHEAD[3])
    assert [f"news:{e.id}" for e in star.note.evidence] == [refs[STAR]]
    assert star.note.standing.state == "in_force"
    # What the note moved: fewer games on the second projection, and two places on its board
    assert star.live.games > star.shadow.games and star.live.minutes == star.shadow.minutes
    assert (star.live.points_rank, star.shadow.points_rank) == (1, 3)
    assert (star.live.categories_rank, star.shadow.categories_rank) == (1, 3)

    bench = reads[BENCH]
    assert bench.outcome == "refused" and bench.note is None and bench.note_live is False
    assert bench.summary == "Refused: 45 minutes is over its bound of 38"
    assert reads[CELTIC].outcome == "none" and reads[CELTIC].note is None

    # The widest moves between the two points boards, whoever was read
    assert [(m.player_id, m.live.points_rank, m.shadow.points_rank) for m in digest.moved] == [
        (STAR, 1, 3), (CELTIC, 2, 1), (STARTER, 3, 2)]
    assert digest.moved[0].name == "Star" and digest.moved[2].team == "DEN"

    # A day asked for by name, and a day with no reads
    assert analyst_digest.build(SEASON, TODAY, _month(), TODAY).counts == digest.counts
    before = analyst_digest.build(SEASON, TODAY - timedelta(days=1), _month(), TODAY)
    assert (before.as_of, before.reads, before.days) == (TODAY - timedelta(days=1), [], [TODAY])


def test_the_digest_says_when_a_note_has_been_replaced_or_stands_under_one_written_by_hand(refs):
    _run({STAR: _later(refs)})
    first = _notes()[STAR]
    ProjectionAdjustment.record(STAR, SEASON, scope="season", kind="injury_current", note="mine", author="james",
                                return_date=AHEAD[3], review_date=TODAY + timedelta(days=7))
    ProjectionAdjustment.record(STAR, SEASON, scope=SCOPE_ANALYST, kind="injury_current", note="a later read",
                                author="analyst", return_date=AHEAD[5], review_date=TODAY + timedelta(days=7))

    star = next(r for r in analyst_digest.build(SEASON, TODAY, Month.unavailable("timeout"), TODAY).reads
                if r.player_id == STAR)

    assert (star.note.id, star.note.state, star.note_live, star.owner_note) == (first.id, "superseded", False, True)
    assert star.note.standing is None
    spend = analyst_digest.build(SEASON, TODAY, Month.unavailable("timeout"), TODAY).spend
    assert (spend.available, spend.enabled, spend.month_cap_usd) == (False, False, 0.0)
