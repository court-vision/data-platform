"""
The availability resolver (pipelines.transformers.availability): pure, so every
rule from the plan's § 2 is a list of reports in, one verdict out.
"""

from datetime import date, datetime, timedelta, timezone

import pytest

from pipelines.transformers.availability import (
    GameContext,
    Report,
    StoredVerdict,
    is_reaction_event,
    names_game_day,
    next_row,
    resolve,
    should_record,
)

pytestmark = pytest.mark.unit

TUESDAY = date(2026, 10, 20)
T0 = datetime(2026, 10, 20, 16, 0, tzinfo=timezone.utc)   # noon ET on the game day
GAME = GameContext(game_date=TUESDAY, game_id="0022600002", day_words=("Knicks", "New York"))


def rep(source, status, minutes=0, **kw):
    at = T0 + timedelta(minutes=minutes)
    return Report(source=source, status=status, observed_at=at, source_time=kw.pop("source_time", at), **kw)


def verdict_of(*reports, game=GAME):
    v = resolve(list(reports), game)
    return v.verdict if v else None


# ---- ranking ----------------------------------------------------------------------


def test_box_score_inactive_is_final_out_and_beats_everything():
    v = resolve([rep("nba_report", "probable"), rep("espn_injuries", "questionable"),
                 rep("nba_boxscore", "inactive", minutes=-30, reason="INACTIVE_INJURY")], GAME)
    assert (v.verdict, v.final, v.decided_by, v.confidence) == ("out", True, "nba_boxscore", 100)


def test_box_score_active_is_final_available_and_overrides_the_espn_floor():
    v = resolve([rep("espn_fantasy", "out"), rep("nba_boxscore", "active")], GAME)
    assert (v.verdict, v.final) == ("available", True)


def test_nba_report_beats_espn_even_when_older():
    assert verdict_of(rep("nba_report", "doubtful", minutes=-120), rep("espn_injuries", "questionable")) == "doubtful"


def test_within_a_source_the_newest_source_time_wins():
    old = rep("nba_report", "questionable", minutes=-60)
    new = rep("nba_report", "out", minutes=-5)
    assert verdict_of(new, old) == "out"


def test_espn_fantasy_maps_through_when_it_is_the_only_source():
    assert verdict_of(rep("espn_fantasy", "doubtful")) == "doubtful"
    assert verdict_of(rep("espn_fantasy", "available")) == "available"


def test_news_claim_is_the_last_resort():
    v = resolve([rep("news_claim", "out", confidence=55)], GAME)
    assert (v.verdict, v.confidence) == ("out", 55)
    assert verdict_of(rep("news_claim", "out"), rep("espn_fantasy", "questionable")) == "questionable"


def test_no_report_with_a_game_is_no_row():
    assert resolve([], GAME) is None


# ---- the ESPN floor (decision 3) -------------------------------------------------------


def test_espn_fantasy_out_is_a_floor_under_a_better_nba_report():
    v = resolve([rep("nba_report", "questionable"), rep("espn_fantasy", "out", reason="OUT")], GAME)
    assert (v.verdict, v.decided_by, v.final) == ("out", "espn_fantasy", False)


def test_the_floor_keeps_the_out_reports_own_source_when_it_already_says_out():
    v = resolve([rep("nba_report", "out"), rep("espn_fantasy", "out")], GAME)
    assert v.decided_by == "nba_report"


# ---- ESPN injuries: Out / Day-To-Day ---------------------------------------------------


def test_espn_out_with_a_later_return_date_is_out():
    assert verdict_of(rep("espn_injuries", "out", expected_return=date(2026, 11, 1))) == "out"


def test_espn_out_with_a_return_date_by_the_game_is_questionable():
    assert verdict_of(rep("espn_injuries", "out", expected_return=TUESDAY)) == "questionable"


@pytest.mark.parametrize("comment", [
    "Brunson (ankle) won't play Tuesday against the Celtics.",
    "Brunson has been ruled out for Tuesday's game.",
    "Brunson will not play against the Knicks.",
    "Brunson will sit out tonight's game for rest.",
])
def test_day_to_day_with_a_wont_play_blurb_that_names_the_game_is_out(comment):
    assert verdict_of(rep("espn_injuries", "questionable", detail=comment)) == "out"


@pytest.mark.parametrize("comment", [
    "Brunson (ankle) won't play Thursday against the Bulls.",          # a later game
    "Brunson is probable for Tuesday's game.",                          # no won't-play phrase
    "Brunson is listed as questionable against the Knicks.",
])
def test_day_to_day_otherwise_stays_questionable(comment):
    assert verdict_of(rep("espn_injuries", "questionable", detail=comment)) == "questionable"


def test_tonight_names_the_game_only_when_written_on_the_game_day():
    game = GameContext(game_date=TUESDAY)
    sunday = datetime(2026, 10, 18, 22, 0, tzinfo=timezone.utc)
    assert names_game_day("will sit out tonight", T0, game)
    assert not names_game_day("will sit out tonight", sunday, game)
    # 11 PM ET Monday is 03:00Z Tuesday: the ET date decides, not the UTC one.
    late_monday = datetime(2026, 10, 21, 3, 0, tzinfo=timezone.utc) - timedelta(days=1)
    assert not names_game_day("will sit out tonight", late_monday, game)


# ---- no game: roster-level status -------------------------------------------------------


def test_no_game_keeps_the_roster_level_status_without_a_game_id():
    off_day = GameContext(game_date=date(2026, 10, 22), has_game=False)
    v = resolve([rep("espn_injuries", "out", game_id=None, expected_return=date(2026, 11, 1))], off_day)
    assert (v.verdict, v.game_id) == ("out", None)


# ---- report log idempotency ---------------------------------------------------------------


def test_should_record_only_on_a_status_or_reason_change():
    first = rep("espn_injuries", "questionable", reason="Ankle")
    assert should_record(None, first)
    assert not should_record(first, rep("espn_injuries", "questionable", minutes=2, reason="Ankle", detail="new words"))
    assert should_record(first, rep("espn_injuries", "out", minutes=2, reason="Ankle"))
    assert should_record(first, rep("espn_injuries", "questionable", minutes=2, reason="Knee"))


# ---- verdict row and events -----------------------------------------------------------------


def test_first_row_takes_the_previous_games_verdict_and_counts_as_a_change():
    v = resolve([rep("nba_report", "doubtful")], GAME)
    row, changed = next_row(None, v, T0, verdict_at_previous_game="out")
    assert changed and row["previous_verdict"] == "out" and row["changed_at"] == T0


def test_a_verdict_change_moves_changed_at_and_previous_verdict():
    current = StoredVerdict(verdict="questionable", previous_verdict=None, changed_at=T0)
    later = T0 + timedelta(minutes=30)
    row, changed = next_row(current, resolve([rep("nba_report", "out")], GAME), later)
    assert changed and row["previous_verdict"] == "questionable" and row["changed_at"] == later


def test_same_verdict_with_new_detail_is_an_update_not_a_change():
    current = StoredVerdict(verdict="out", previous_verdict="questionable", changed_at=T0)
    later = T0 + timedelta(minutes=30)
    row, changed = next_row(current, resolve([rep("nba_report", "out", detail="Left Knee; Sprain")], GAME), later)
    assert not changed
    assert (row["previous_verdict"], row["changed_at"], row["updated_at"]) == ("questionable", T0, later)
    assert row["detail"] == "Left Knee; Sprain"


@pytest.mark.parametrize("verdict,expected", [
    ("out", True), ("doubtful", True), ("available", True),
    ("questionable", False), ("probable", False), ("unknown", False),
])
def test_reaction_events(verdict, expected):
    assert is_reaction_event(True, verdict) is expected
    assert is_reaction_event(False, verdict) is False
