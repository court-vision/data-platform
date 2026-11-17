"""
The live-projection pipeline against Postgres: what a morning reads (box
scores, the schedule, the preseason projection, where players are, what is
known about an absence), what it stores under source `cv_live`, when it waits
for last night, and what a later tick or a forced run does to a stored day.

The morning is Nov 9, 2025. DEN and BOS have met four times, two days apart,
the last on the 7th, and meet six more times from the 9th.

The season's notes (the Live rankings plan, step 3) are read the same morning:
the last tests here are a note's return date against the feed's, minutes that
hold until a teammate is back, and the notes that are no longer applied.
"""

from __future__ import annotations

from datetime import date, datetime, time, timedelta

import pytest
import pytz

from core.settings import settings
from db.base import db
from db.models.nba import PlayerHistory, PlayerProfile, PlayerProjection, ProjectionAdjustment
from db.models.nba.availability import PlayerAvailability
from db.models.nba.games import Game
from db.models.nba.player_game_stats import PlayerGameStats
from db.models.nba.players import Player
from db.models.pipeline_run import PipelineRun
from pipelines import live_projection
from pipelines.live_projection import BOX_SCORES_PIPELINE, SOURCE, LiveProjectionPipeline, stored_on
from schemas.common import ApiStatus
from services.live_notes import ENDED, IN_FORCE, REVIEW_DUE
from services.live_projection_inputs import DEFAULT_TIP, EASTERN, load_coefficients, load_inputs, tips_of
from services.live_projection_model import LINE_KEYS, Availability, expand
from services.projection_model import season_label, season_start

pytestmark = pytest.mark.integration

SEASON = settings.nba_season
YEAR = season_start(SEASON)
LAST_SEASON = season_label(YEAR - 1)
PRESEASON = date(YEAR, 10, 20)
PLAYED = [date(YEAR, 11, d) for d in (1, 3, 5, 7)]
TODAY = date(YEAR, 11, 9)
AHEAD = [TODAY + timedelta(days=2 * i) for i in range(6)]
LAST_NIGHT = PLAYED[-1]

STAR, STARTER, BENCH, WAITING, GONE, CELTIC, UNPROJECTED = 201, 202, 203, 204, 205, 301, 302
PER_MINUTE = {"pts": 0.66, "reb": 0.2, "ast": 0.15, "stl": 0.03, "blk": 0.02, "tov": 0.06,
              "fgm": 0.24, "fga": 0.5, "fg3m": 0.06, "fg3a": 0.16, "ftm": 0.12, "fta": 0.15}
MODELS = [PlayerProfile, PlayerHistory, PlayerProjection]


@pytest.fixture(scope="module", autouse=True)
def projection_tables(integration_db, adjustments_table):
    db.create_tables(MODELS, safe=True)
    yield


def _game_id(n: int) -> str:
    return f"002{YEAR % 100:02d}{n:05d}"


def _box(pid: int, team: str, game: int, day: date, minutes: int) -> None:
    stats = {k: int(round(v * minutes)) for k, v in PER_MINUTE.items()}
    PlayerGameStats.create(player=pid, team=team, game=_game_id(game), game_date=day, min=minutes, fpts=0, **stats)


def _project(pid: int, minutes: float, games: int = 70, source: str = "cv") -> None:
    line = {k: round(v * minutes, 2) for k, v in PER_MINUTE.items()} | {"min": minutes}
    PlayerProjection.record_projection(player_id=pid, season=SEASON, as_of_date=PRESEASON, line=line,
                                       projected_gp=games, source=source)


@pytest.fixture(autouse=True)
def morning(projection_tables, clean_integration_tables, monkeypatch):
    db.execute_sql("""
        TRUNCATE TABLE nba.projection_adjustments, nba.player_projections, nba.player_history, nba.player_profiles
        RESTART IDENTITY CASCADE
    """)
    now = datetime.utcnow()
    roster = ((STAR, "Star", "DEN", 34), (STARTER, "Starter", "DEN", 30), (BENCH, "Bench", "DEN", 12),
              (WAITING, "Waiting", "DEN", 28), (GONE, "Gone", "BOS", 30), (CELTIC, "Celtic", "BOS", 32),
              (UNPROJECTED, "Unprojected", "BOS", 18))
    for pid, name, team, minutes in roster:
        Player.create(id=pid, name=name, name_normalized=name.lower())
        PlayerProfile.create(player=pid, team=team, updated_at=now)
        if pid != UNPROJECTED:
            _project(pid, float(minutes))
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

    # The feed has the star out with a return date; the curated notes write one season off
    PlayerAvailability.create(player=STAR, game_date=TODAY, verdict="out", expected_return=AHEAD[2])
    ProjectionAdjustment.record(GONE, SEASON, kind="injury", games=0, note="Achilles", author="test")
    _clock(monkeypatch, 6)
    load_coefficients.cache_clear()
    yield


def _clock(monkeypatch, hour: int, day: date = TODAY) -> None:
    monkeypatch.setattr(live_projection, "now_eastern", lambda: datetime(day.year, day.month, day.day, hour, 0))
    monkeypatch.setattr(live_projection, "today_et", lambda now=None: day)


def _run(force: bool = False):
    return LiveProjectionPipeline()._run_sync(options={"force": True} if force else None)


def _rows(day: date = TODAY) -> dict[int, PlayerProjection]:
    return {
        r.player_id: r
        for r in PlayerProjection.select().where(
            (PlayerProjection.source == SOURCE) & (PlayerProjection.as_of_date == day))
    }


def test_the_inputs_are_one_morning_as_the_database_has_it():
    inputs = load_inputs(SEASON, TODAY)

    assert len(inputs.lines) == 18 and inputs.newest_line == LAST_NIGHT
    assert {l.team for l in inputs.lines} == {"DEN", "BOS"} and inputs.lines[0].game_id.startswith("002")
    assert inputs.ahead == {"DEN": AHEAD, "BOS": AHEAD}
    assert (inputs.prior_source, inputs.prior_as_of) == ("cv", PRESEASON)
    assert set(inputs.priors) == {STAR, STARTER, BENCH, WAITING, GONE, CELTIC}
    assert inputs.priors[STAR].minutes == 34.0 and inputs.priors[STAR].per_game["reb"] == pytest.approx(6.8)
    assert inputs.teams[WAITING] == "DEN" and inputs.teams[GONE] == "BOS"
    # Last season's age plus a year
    assert inputs.ages[BENCH] == pytest.approx(21.4)
    star, gone = inputs.known[STAR], inputs.known[GONE]
    assert (star.out_next, star.return_date, star.out_for_season) == (True, AHEAD[2], False)
    assert (gone.out_next, gone.return_date, gone.out_for_season) == (False, None, True)
    assert set(inputs.known) == {STAR, GONE}


def test_a_morning_stores_a_line_per_game_left_for_everyone():
    result = _run()

    assert result.status == ApiStatus.SUCCESS and result.records_processed == 7
    rows = _rows()
    assert set(rows) == {STAR, STARTER, BENCH, WAITING, GONE, CELTIC, UNPROJECTED}
    assert stored_on(SEASON, TODAY) == 7

    star = rows[STAR]
    assert star.season == SEASON and star.pipeline_run_id is not None
    assert star.raw["team"] == "DEN" and star.raw["prior"] == "cv"
    assert star.raw["version"] == load_coefficients().version
    assert set(star.raw["rates"]) == set(LINE_KEYS)
    # Out until the return date the feed gives: nothing in the first two games, then his share
    games = expand(star.raw["timeline"], AHEAD)
    assert [g.chance for g in games[:2]] == [0.0, 0.0] and all(0.5 < g.chance < 1.0 for g in games[2:])
    assert star.raw["games"] == pytest.approx(sum(g.chance for g in games), abs=0.05)
    assert star.projected_gp == round(star.raw["games"]) and 2 <= star.projected_gp <= 4
    assert star.raw["parts"]["known"] == {"out_next": True, "out_for_season": False,
                                          "return_date": AHEAD[2].isoformat()}
    assert float(star.min) == pytest.approx(34.0, abs=1.5) and float(star.pts) > 15

    # The fill-in plays more while the star is out than after he is back
    bench = expand(rows[BENCH].raw["timeline"], AHEAD)
    assert bench[0].minutes > bench[-1].minutes and float(rows[BENCH].min) > 12
    # The season written off: no games, and still the line he would play
    assert rows[GONE].projected_gp == 0 and rows[GONE].raw["games"] == 0.0 and float(rows[GONE].min) == 30.0
    # Not yet played: on his team's schedule along the curve of players who have not debuted
    assert rows[WAITING].raw["parts"]["not_yet_played"] is True and 0 < rows[WAITING].raw["games"] < 3
    # Nothing projects him: this season's games against the pool's line
    assert rows[UNPROJECTED].raw["parts"]["prior_minutes"] is None
    assert float(rows[UNPROJECTED].min) == pytest.approx(18.0, abs=0.5)
    # The preseason snapshots are untouched
    assert PlayerProjection.select().where(PlayerProjection.source == "cv").count() == 6


def test_the_days_projection_is_kept_and_a_forced_run_rebuilds_it():
    _run()
    first = {pid: r.id for pid, r in _rows().items()}
    # A snapshot row left for a player the model no longer returns
    Player.create(id=999, name="Ghost", name_normalized="ghost")
    PlayerProjection.record_projection(player_id=999, season=SEASON, as_of_date=TODAY, line={"min": 10},
                                       source=SOURCE)

    again = _run()
    assert again.status == ApiStatus.SUCCESS and again.records_processed == 0
    assert 999 in _rows()

    PlayerAvailability.update(expected_return=AHEAD[4]).where(PlayerAvailability.player == STAR).execute()
    forced = _run(force=True)

    assert forced.status == ApiStatus.SUCCESS and forced.records_processed == 7
    rows = _rows()
    assert set(rows) == set(first) and rows[STARTER].id == first[STARTER]
    assert rows[STAR].raw["parts"]["known"]["return_date"] == AHEAD[4].isoformat()
    assert rows[STAR].raw["games"] < 2


def test_it_waits_for_last_nights_box_scores_until_late_morning(monkeypatch):
    PlayerGameStats.delete().where(PlayerGameStats.game_date == LAST_NIGHT).execute()

    waiting = _run()
    assert waiting.status == ApiStatus.ERROR and LAST_NIGHT.isoformat() in (waiting.error or "")
    assert stored_on(SEASON, TODAY) == 0

    _clock(monkeypatch, live_projection.WAIT_UNTIL_HOUR_ET)
    assert _run().status == ApiStatus.SUCCESS and stored_on(SEASON, TODAY) == 7


def _rebuild():
    """What a note saved in the projections editor runs."""
    return LiveProjectionPipeline()._run_sync(options={"rebuild": True})


def test_a_notes_rebuild_before_last_night_is_in_stores_nothing_for_the_morning_to_keep(monkeypatch):
    # Saved after the day turned, before the box scores landed: a snapshot built now
    # would be kept by the morning's run all day, short of last night's games
    PlayerGameStats.delete().where(PlayerGameStats.game_date == LAST_NIGHT).execute()

    early = _rebuild()
    assert early.status == ApiStatus.SUCCESS and early.records_processed == 0
    assert stored_on(SEASON, TODAY) == 0

    # The same rebuild once the morning has stopped waiting builds the day
    _clock(monkeypatch, live_projection.WAIT_UNTIL_HOUR_ET)
    assert _rebuild().records_processed == 7 and stored_on(SEASON, TODAY) == 7


def test_a_notes_rebuild_replaces_a_stored_morning():
    _run()
    PlayerAvailability.update(expected_return=AHEAD[4]).where(PlayerAvailability.player == STAR).execute()
    # The stored morning passed the wait; a batch started since does not hold the rebuild
    PipelineRun.create(pipeline_name=BOX_SCORES_PIPELINE, started_at=datetime.utcnow(), status="running")

    rebuilt = _rebuild()
    assert rebuilt.status == ApiStatus.SUCCESS and rebuilt.records_processed == 7
    assert _rows()[STAR].raw["parts"]["known"]["return_date"] == AHEAD[4].isoformat()


def test_it_waits_for_a_box_score_batch_that_is_not_done_and_a_forced_run_does_not():
    started = datetime.utcnow() - timedelta(minutes=2)
    PipelineRun.create(pipeline_name=BOX_SCORES_PIPELINE, started_at=started, status="running")

    waiting = _run()
    assert waiting.status == ApiStatus.ERROR and "box-score batch" in (waiting.error or "")
    assert stored_on(SEASON, TODAY) == 0

    assert _run(force=True).status == ApiStatus.SUCCESS and stored_on(SEASON, TODAY) == 7


def test_a_box_score_batch_left_marked_running_holds_the_projection_until_a_later_one_gets_through():
    long_ago = datetime.utcnow() - timedelta(hours=5)
    PipelineRun.create(pipeline_name=BOX_SCORES_PIPELINE, started_at=long_ago, status="running")

    waiting = _run()
    assert waiting.status == ApiStatus.ERROR and "never finished" in (waiting.error or "")
    assert stored_on(SEASON, TODAY) == 0

    now = datetime.utcnow()
    PipelineRun.create(pipeline_name=BOX_SCORES_PIPELINE, started_at=now, completed_at=now, status="success")
    assert _run().status == ApiStatus.SUCCESS and stored_on(SEASON, TODAY) == 7


def test_box_scores_read_while_their_batch_was_writing_are_not_projected_from(monkeypatch):
    read = live_projection.load_inputs

    def read_while_the_batch_runs(season, day):
        inputs = read(season, day)
        now = datetime.utcnow()
        PipelineRun.create(pipeline_name=BOX_SCORES_PIPELINE, started_at=now, completed_at=now, status="success")
        return inputs

    monkeypatch.setattr(live_projection, "load_inputs", read_while_the_batch_runs)
    waiting = _run()
    assert waiting.status == ApiStatus.ERROR and "was writing while" in (waiting.error or "")
    assert stored_on(SEASON, TODAY) == 0

    # The next tick reads with nothing writing under it
    monkeypatch.setattr(live_projection, "load_inputs", read)
    assert _run().status == ApiStatus.SUCCESS and stored_on(SEASON, TODAY) == 7


@pytest.mark.parametrize("force", [False, True], ids=["tick", "forced"])
def test_nothing_is_stored_before_the_day_turns(monkeypatch, force):
    _clock(monkeypatch, 1)
    result = _run(force=force)
    assert result.status == ApiStatus.SUCCESS
    assert (result.records_processed, result.records_skipped) == (0, 1)
    assert stored_on(SEASON, TODAY) == 0


def test_with_no_game_played_or_none_left_there_is_nothing_to_project():
    PlayerGameStats.delete().execute()
    assert _run().status == ApiStatus.SUCCESS and stored_on(SEASON, TODAY) == 0


def test_espns_projection_is_the_prior_where_court_vision_has_none():
    PlayerProjection.delete().where(PlayerProjection.source == "cv").execute()
    _project(STARTER, 31.0, source="espn")

    inputs = load_inputs(SEASON, TODAY)
    assert inputs.prior_source == "espn" and set(inputs.priors) == {STARTER}
    assert _run().status == ApiStatus.SUCCESS and _rows()[STARTER].raw["prior"] == "espn"


def test_a_verdict_that_is_old_or_not_out_says_nothing_about_today():
    PlayerAvailability.delete().execute()
    PlayerAvailability.create(player=STAR, game_date=TODAY - timedelta(days=3), verdict="out", expected_return=AHEAD[3])
    PlayerAvailability.create(player=STARTER, game_date=TODAY - timedelta(days=1), verdict="out")
    PlayerAvailability.create(player=STARTER, game_date=TODAY, verdict="active")
    PlayerAvailability.create(player=BENCH, game_date=TODAY - timedelta(days=1), verdict="out",
                              expected_return=TODAY - timedelta(days=1))

    known = load_inputs(SEASON, TODAY).known
    # The star's verdict is three days old; the starter's newest says he plays
    assert STAR not in known and STARTER not in known
    # Out as of yesterday, with a return date that has already passed: out of the next game, no date
    assert (known[BENCH].out_next, known[BENCH].return_date) == (True, None)


# ---- the season's notes ------------------------------------------------------------------


def _note(pid: int, *, written: datetime, **fields) -> ProjectionAdjustment:
    """A season note saved at `written`, Eastern. The table keeps UTC."""
    base = dict(scope="season", kind="injury_current", note="read off a report", author="test",
                review_date=TODAY + timedelta(days=14))
    record = ProjectionAdjustment.record(pid, SEASON, **(base | fields))
    saved = EASTERN.localize(written).astimezone(pytz.utc).replace(tzinfo=None)
    ProjectionAdjustment.update(created_at=saved).where(ProjectionAdjustment.id == record.id).execute()
    return record


THIS_MORNING = datetime.combine(TODAY, time(5, 0))


def test_a_notes_return_date_is_believed_over_the_feeds():
    # The feed has the star back for the third game from here; the note read the report as the fifth
    note = _note(STAR, written=THIS_MORNING, return_date=AHEAD[4], return_earliest=AHEAD[3],
                 return_latest=AHEAD[5], confidence="likely")

    inputs = load_inputs(SEASON, TODAY)
    assert inputs.known_before_notes[STAR].return_date == AHEAD[2]
    assert inputs.known[STAR] == Availability(out_next=True, return_date=AHEAD[4], out_now=True)
    assert inputs.standings[STAR].state == IN_FORCE and inputs.role_notes == {}

    assert _run().status == ApiStatus.SUCCESS
    star = _rows()[STAR]
    games = expand(star.raw["timeline"], AHEAD)
    assert [g.chance for g in games[:4]] == [0.0] * 4 and all(0.5 < g.chance < 1.0 for g in games[4:])
    assert star.raw["parts"]["known"] == {"out_next": True, "out_for_season": False,
                                          "return_date": AHEAD[4].isoformat(), "note": note.id}
    # The line he plays when he plays is the one he had: the note took games
    assert float(star.min) == pytest.approx(34.0, abs=1.5)
    # The preseason adjustment that writes a season off is another scope, and still read
    assert _rows()[GONE].projected_gp == 0 and "note" not in _rows()[GONE].raw["parts"]["known"]


def test_a_note_can_say_he_is_out_when_no_box_score_or_feed_does():
    # The starter played last night and nothing has ruled him out: only the note knows
    _note(STARTER, written=datetime.combine(LAST_NIGHT, time(23, 30)), games=0, kind="injury_current")

    assert _run().status == ApiStatus.SUCCESS
    starter = _rows()[STARTER]
    assert (starter.projected_gp, starter.raw["games"]) == (0, 0.0)
    assert starter.raw["parts"]["known"]["out_for_season"] is True
    # He keeps the line he would play
    assert float(starter.min) == pytest.approx(30.0, abs=1.5)


def test_minutes_on_a_note_hold_until_the_teammate_it_depends_on_is_back():
    note = _note(BENCH, written=THIS_MORNING, kind="role", minutes=30, usage=1.15, depends_on_player=STAR)

    inputs = load_inputs(SEASON, TODAY)
    assert inputs.role_notes[BENCH].minutes == 30.0 and inputs.role_notes[BENCH].depends_on == STAR
    assert BENCH not in inputs.known                      # a role note says nothing about his own games

    assert _run().status == ApiStatus.SUCCESS
    bench = _rows()[BENCH]
    games = expand(bench.raw["timeline"], AHEAD)
    # The two games the star is sure to miss are the note's, exactly; from his return they slide to the model's
    assert [(g.minutes, g.usage) for g in games[:2]] == [(30.0, 1.15)] * 2
    assert games[2].minutes < 30.0 and games[-1].minutes < games[2].minutes
    stored = bench.raw["parts"]["note"]
    assert (stored["id"], stored["minutes"], stored["usage"], stored["depends_on"]) == (note.id, 30.0, 1.15, STAR)
    assert stored["through"] == AHEAD[1].isoformat()
    assert "known" not in bench.raw["parts"]


def test_a_note_past_its_review_date_or_spent_by_a_game_is_not_applied():
    # Nobody has read the starter's note again since its review date
    _note(STARTER, written=datetime.combine(PLAYED[0], time(9, 0)), return_date=AHEAD[3], minutes=20,
          review_date=TODAY - timedelta(days=1))
    # The Celtic's said out for the season, and he has played three games since it was written
    _note(CELTIC, written=datetime.combine(PLAYED[1], time(9, 0)), games=0)
    # The bench player's minutes ran through the last game played
    _note(BENCH, written=datetime.combine(PLAYED[2], time(9, 0)), kind="role", minutes=30, until_date=LAST_NIGHT)

    inputs = load_inputs(SEASON, TODAY)
    assert {pid: s.state for pid, s in inputs.standings.items()} == {
        STARTER: REVIEW_DUE, CELTIC: ENDED, BENCH: ENDED}
    assert "read again since" in inputs.standings[STARTER].reasons[0]
    assert f"he played on {LAST_NIGHT.isoformat()}" in inputs.standings[CELTIC].reasons[0]
    assert set(inputs.notes) == {STARTER, CELTIC, BENCH} and inputs.role_notes == {}
    assert inputs.known == inputs.known_before_notes

    with_notes = {pid: (r.projected_gp, r.raw["timeline"]) for pid, r in (_run() and _rows()).items()}
    ProjectionAdjustment.delete().where(ProjectionAdjustment.scope == "season").execute()
    _run(force=True)
    assert {pid: (r.projected_gp, r.raw["timeline"]) for pid, r in _rows().items()} == with_notes


def test_a_note_is_spent_only_by_a_game_that_tipped_after_it_was_written():
    Game.update(start_time_et=time(22, 0)).where(Game.game_date == LAST_NIGHT).execute()

    # Written at nine with him out, and he played the late game after all
    _note(CELTIC, written=datetime.combine(LAST_NIGHT, time(21, 0)), return_date=AHEAD[3])
    inputs = load_inputs(SEASON, TODAY)
    assert inputs.last_played[CELTIC] == datetime.combine(LAST_NIGHT, time(22, 0))
    assert (inputs.standings[CELTIC].state, CELTIC in inputs.known) == (ENDED, False)

    # Read again after the game he was hurt in: that game is not one he came back for
    _note(CELTIC, written=datetime.combine(LAST_NIGHT, time(23, 45)), return_date=AHEAD[3])
    inputs = load_inputs(SEASON, TODAY)
    assert inputs.standings[CELTIC].games is True
    assert inputs.known[CELTIC] == Availability(return_date=AHEAD[3], out_now=True)

    # The editor reads the same tips for a few players without the season's box scores:
    # a game with no start time on the schedule is taken to have tipped at seven
    assert tips_of([CELTIC, STAR, WAITING], SEASON, TODAY) == {
        CELTIC: datetime.combine(LAST_NIGHT, time(22, 0)), STAR: datetime.combine(PLAYED[1], DEFAULT_TIP)}
    assert {pid: inputs.last_played[pid] for pid in (CELTIC, STAR)} == tips_of([CELTIC, STAR], SEASON, TODAY)
