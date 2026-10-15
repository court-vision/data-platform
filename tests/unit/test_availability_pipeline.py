"""
The availability pipeline's pure parts (pipelines/availability.py): when each
source is due, how each feed becomes observations, and which verdict changes
would reach the reactor. The writes are tests/integration/test_availability_writes.py.
"""

from __future__ import annotations

from collections import Counter
from datetime import date, datetime, time, timedelta, timezone

import pytest

from pipelines.availability import (
    Due,
    PlayerIndex,
    Slate,
    SlateGame,
    VerdictChange,
    boxscore_observations,
    due_sources,
    espn_injury_observations,
    fantasy_observations,
    poll_everything,
    reaction_events,
)
from pipelines.extractors.espn_injuries import EspnInjury
from pipelines.transformers.availability import (
    ET,
    GameContext,
    Report,
    boxscore_players,
    boxscore_tipped,
    fantasy_status,
    resolve,
    team_day_words,
)

pytestmark = pytest.mark.unit

GAME_DAY = date(2026, 10, 27)   # a Tuesday
OFF_DAY = date(2026, 10, 28)
NOW = datetime(2026, 10, 27, 16, 0, tzinfo=timezone.utc)


def et(day: date, hh: int, mm: int) -> datetime:
    return datetime.combine(day, time(hh, mm), tzinfo=ET)


def slate(*tips: tuple[str, str, str, time | None], day: date = GAME_DAY) -> Slate:
    return Slate(day, tuple(
        SlateGame(game_id, home, away, datetime.combine(day, tip, tzinfo=ET) if tip else None)
        for game_id, home, away, tip in tips
    ))


NIGHT = slate(("0022600101", "NYK", "BOS", time(19, 30)), ("0022600102", "LAL", "GSW", time(22, 0)))


# ---- due rules ----------------------------------------------------------------------------


def _ticks(day: date):
    """Every tick cron-runner fires on an ET day: each even minute."""
    start = et(day, 0, 0)
    return [start + timedelta(minutes=m) for m in range(0, 24 * 60, 2)]


def test_an_off_day_polls_espn_injuries_every_ten_minutes_and_fantasy_hourly():
    dues = [due_sources(t, Slate(OFF_DAY)) for t in _ticks(OFF_DAY)]
    assert sum(d.espn_injuries for d in dues) == 144
    assert sum(d.espn_fantasy for d in dues) == 24
    assert not any(d.boxscore_games for d in dues)
    assert due_sources(et(OFF_DAY, 14, 10), Slate(OFF_DAY)) == Due(espn_injuries=True)
    assert not due_sources(et(OFF_DAY, 14, 2), Slate(OFF_DAY)).any


def test_a_game_day_polls_fantasy_every_fifteen_minutes_from_eleven():
    dues = {t.time(): due_sources(t, NIGHT) for t in _ticks(GAME_DAY)}
    fantasy = [at for at, d in dues.items() if d.espn_fantasy]
    assert [at for at in fantasy if at < time(11)] == [time(h, 0) for h in range(11)]
    assert [at for at in fantasy if time(11) <= at < time(12)] == [time(11, 0), time(11, 16), time(11, 30), time(11, 46)]
    assert len(fantasy) == 11 + 13 * 4


def test_espn_injuries_runs_every_tick_from_ninety_minutes_before_any_tip_until_it():
    assert not due_sources(et(GAME_DAY, 17, 58), NIGHT).espn_injuries   # 92 min before 7:30
    assert due_sources(et(GAME_DAY, 18, 0), NIGHT).espn_injuries         # the 10-minute slot anyway
    assert due_sources(et(GAME_DAY, 18, 2), NIGHT).espn_injuries
    assert due_sources(et(GAME_DAY, 19, 28), NIGHT).espn_injuries
    # Between the tips the 10:00 PM game's window has opened.
    assert due_sources(et(GAME_DAY, 20, 32), NIGHT).espn_injuries
    assert not due_sources(et(GAME_DAY, 22, 2), NIGHT).espn_injuries   # every tip has passed


def test_a_games_box_score_is_due_from_one_hundred_minutes_before_its_tip_until_it():
    assert due_sources(et(GAME_DAY, 17, 48), NIGHT).boxscore_games == ()
    assert due_sources(et(GAME_DAY, 17, 50), NIGHT).boxscore_games == ("0022600101",)
    assert due_sources(et(GAME_DAY, 19, 28), NIGHT).boxscore_games == ("0022600101",)
    assert due_sources(et(GAME_DAY, 20, 20), NIGHT).boxscore_games == ("0022600102",)
    assert due_sources(et(GAME_DAY, 19, 30), NIGHT).boxscore_games == ()   # tipped: live-stats has it


def test_a_game_without_a_start_time_opens_no_window():
    no_time = slate(("0022600103", "MIA", "ORL", None))
    assert due_sources(et(GAME_DAY, 18, 2), no_time) == Due()
    assert due_sources(et(GAME_DAY, 12, 16), no_time).espn_fantasy   # still a game day


def test_the_rules_hold_in_utc_across_the_dst_switch():
    # Sunday 2026-11-01: EST from 2 AM. 7:30 PM EST is 00:30Z on the 2nd.
    day = date(2026, 11, 1)
    sunday = slate(("0022600200", "NYK", "BOS", time(19, 30)), day=day)
    assert due_sources(datetime(2026, 11, 1, 23, 0, tzinfo=timezone.utc), sunday).boxscore_games == ("0022600200",)
    assert due_sources(datetime(2026, 11, 2, 0, 30, tzinfo=timezone.utc), sunday).boxscore_games == ()


def test_run_polls_every_source_and_every_game_not_yet_tipped():
    late = slate(("0022600101", "NYK", "BOS", time(19, 30)), ("0022600102", "LAL", "GSW", time(22, 0)),
                 ("0022600103", "MIA", "ORL", None))
    due = poll_everything(et(GAME_DAY, 20, 0), late)
    assert (due.espn_injuries, due.espn_fantasy) == (True, True)
    assert due.boxscore_games == ("0022600102", "0022600103")


def test_due_round_trips_through_the_run_options():
    due = Due(espn_injuries=True, boxscore_games=("0022600101",))
    assert due.options() == {"sources": ["espn_injuries"], "boxscore_games": ["0022600101"]}
    assert Due.from_options(due.options()) == due
    assert Due.from_options({}) is None


# ---- feed vocabulary -------------------------------------------------------------------------


@pytest.mark.parametrize("raw,injured,expected", [
    ("OUT", True, "out"), ("SUSPENSION", False, "out"), ("INJURY_RESERVE", True, "out"),
    ("DOUBTFUL", True, "doubtful"), ("QUESTIONABLE", True, "questionable"),
    ("DAY_TO_DAY", True, "questionable"), ("PROBABLE", True, "probable"), ("ACTIVE", False, "available"),
    (None, True, "out"), (None, False, "available"), ("WEIRD_NEW_WORD", True, None),
])
def test_fantasy_status(raw, injured, expected):
    assert fantasy_status(injured, raw) == expected


def _box_player(pid, status="ACTIVE", reason=None, description=None):
    entry = {"personId": pid, "status": status}
    if reason:
        entry["notPlayingReason"] = reason
    if description:
        entry["notPlayingDescription"] = description
    return entry


BOX = {
    "gameStatus": 1,
    "homeTeam": {"teamTricode": "NYK", "players": [
        _box_player(1628973),
        _box_player(1628404, "INACTIVE", "INACTIVE_INJURY", "Left Knee; Soreness"),
        _box_player(1629011, "ACTIVE", "DND_ILLNESS", "Illness"),
        _box_player(1630173, "ACTIVE", "DNP_COACH"),
        {"status": "ACTIVE"},                                   # no personId: skipped
    ]},
    "awayTeam": {"teamTricode": "BOS", "players": [_box_player(1628369, "INACTIVE", "INACTIVE_GLEAGUE_TWOWAY")]},
}


def test_box_score_players_and_why_they_sit():
    players = {p.player_id: p for p in boxscore_players(BOX)}
    assert set(players) == {1628973, 1628404, 1629011, 1630173, 1628369}
    assert (players[1628973].status, players[1628973].team) == ("active", "NYK")
    assert (players[1628404].status, players[1628404].reason, players[1628404].detail) == (
        "inactive", "INACTIVE_INJURY", "Left Knee; Soreness")
    assert players[1629011].status == "inactive"       # on the list, did not dress
    assert players[1630173].status == "active"         # a coach's DNP was available
    assert (players[1628369].status, players[1628369].team) == ("inactive", "BOS")


def test_box_score_tipped():
    assert not boxscore_tipped(BOX)
    assert boxscore_tipped({**BOX, "gameStatus": 2}) and boxscore_tipped({**BOX, "gameStatus": 3})
    assert not boxscore_tipped({})


def test_team_day_words():
    assert team_day_words("NYK") == ("New York", "Knicks")
    assert team_day_words(None) == () and team_day_words("XXX") == ()


def test_the_opponent_names_the_game_unless_the_blurb_names_another_day():
    game = GameContext(game_date=GAME_DAY, game_id="0022600101", day_words=team_day_words("BOS"))
    written = NOW - timedelta(days=1)

    def verdict(blurb):
        return resolve([Report("espn_injuries", "questionable", NOW, source_time=written, detail=blurb)], game).verdict

    assert verdict("Brunson won't play against Boston.") == "out"
    assert verdict("Brunson won't play Thursday against the Celtics.") == "questionable"
    assert verdict("Brunson won't play Tuesday or Thursday against the Celtics.") == "out"


# ---- observations ------------------------------------------------------------------------------

INDEX = PlayerIndex(
    by_espn_id={3934672: 1628973, 3133628: 1628404, 4066354: 1629011},
    by_name={"josh hart": 1628404, "mitchell robinson": 1629011},
    team_of={1628973: "NYK", 1628404: "NYK", 1629011: "NYK", 1630173: "NYK", 1628369: "BOS"},
)


def _entry(espn_id, name, status="questionable", team="NYK", **kw):
    return EspnInjury(
        item_id=f"n{espn_id}", espn_id=espn_id, name=name, team=team, status=status,
        espn_status="Day-To-Day", fantasy_status="GTD", reason=kw.get("reason", "Ankle"),
        detail=kw.get("detail"), long_comment=None, expected_return=kw.get("expected_return"),
        source_time=kw.get("source_time", NOW - timedelta(hours=3)),
    )


def _whole_list(*entries):
    """The named entries plus one per other team, so the list counts as whole."""
    others = ["ATL", "BKN", "CHA", "CHI", "CLE", "DAL", "DEN", "DET", "HOU", "IND", "LAC",
              "MEM", "MIA", "MIL", "MIN", "NOP", "OKC", "ORL", "PHI", "PHX"]
    return [*entries, *(_entry(9_000_000 + i, f"Somebody {i}", team=t) for i, t in enumerate(others))]


def test_espn_injuries_observes_each_listed_player_once_and_counts_the_unmatched():
    skips = Counter()
    entries = [_entry(3934672, "Jalen Brunson"), _entry(3934672, "Jalen Brunson"), _entry(1, "Josh Hart"),
               _entry(2, "Nobody Known")]
    observed = espn_injury_observations(entries, {}, INDEX, NOW, skips)
    assert [(o.player_id, o.report.status, o.team) for o in observed] == [
        (1628973, "questionable", "NYK"), (1628404, "questionable", "NYK")]   # Hart by name
    assert skips == Counter({"unmatched_player": 1})
    assert observed[0].report.source_time == NOW - timedelta(hours=3)


def test_a_player_the_whole_list_stops_naming_is_observed_healthy():
    earlier = {1629011: Report("espn_injuries", "out", NOW - timedelta(hours=1)),
               1630173: Report("espn_injuries", "available", NOW - timedelta(hours=1))}
    skips = Counter()
    observed = espn_injury_observations(_whole_list(_entry(3934672, "Jalen Brunson")), earlier, INDEX, NOW, skips)
    healthy = [o for o in observed if o.report.status == "available"]
    assert [(o.player_id, o.report.source_time) for o in healthy] == [(1629011, None)]   # 1630173 was already


def test_a_short_list_drops_only_players_whose_team_it_still_lists():
    earlier = {1629011: Report("espn_injuries", "out", NOW - timedelta(hours=1)),     # NYK, listed below
               1628369: Report("espn_injuries", "out", NOW - timedelta(hours=1))}     # BOS, missing
    skips = Counter()
    observed = espn_injury_observations([_entry(3934672, "Jalen Brunson")], earlier, INDEX, NOW, skips)
    assert [(o.player_id, o.report.status) for o in observed if o.report.status == "available"] == [(1629011, "available")]
    assert skips["espn_injuries_team_missing"] == 1


def test_fantasy_observes_the_hurt_and_only_a_return_for_the_healthy():
    players = {
        "jalen brunson": {"espn_id": 3934672, "injured": True, "injury_status": "DAY_TO_DAY"},
        "josh hart": {"espn_id": 3133628, "injured": False, "injury_status": "ACTIVE"},          # healthy all day
        "mitchell robinson": {"espn_id": 4066354, "injured": False, "injury_status": "ACTIVE"},  # back today
        "suspended guy": {"espn_id": 5, "injured": False, "injury_status": "SUSPENSION"},       # unmatched
        "healthy stranger": {"espn_id": 6, "injured": False, "injury_status": "ACTIVE"},        # unmatched, fine
        "odd one": {"espn_id": 7, "injured": True, "injury_status": "SOMETHING_NEW"},
    }
    earlier = {1629011: Report("espn_fantasy", "out", NOW - timedelta(hours=1))}
    skips = Counter()
    observed = fantasy_observations(players, earlier, INDEX, NOW, skips)
    assert [(o.player_id, o.report.status) for o in observed] == [(1628973, "questionable"), (1629011, "available")]
    assert skips == Counter({"unmatched_player": 1, "espn_fantasy_unknown_status": 1})


def test_fantasy_gives_a_suspension_its_reason():
    players = {"josh hart": {"espn_id": 3133628, "injured": False, "injury_status": "SUSPENSION"}}
    (observed,) = fantasy_observations(players, {}, INDEX, NOW, Counter())
    assert (observed.report.status, observed.report.reason) == ("out", "Suspension")


def test_before_the_tip_only_inactive_is_news():
    players = boxscore_players(BOX)
    observed = boxscore_observations("0022600101", players, False, {}, NOW)
    assert sorted((o.player_id, o.report.status) for o in observed) == [
        (1628369, "inactive"), (1628404, "inactive"), (1629011, "inactive")]
    assert all(o.report.game_id == "0022600101" and o.report.source == "nba_boxscore" for o in observed)


def test_before_the_tip_an_active_takes_back_an_inactive():
    earlier = {1628973: Report("nba_boxscore", "inactive", NOW - timedelta(minutes=10), game_id="0022600101")}
    observed = boxscore_observations("0022600101", boxscore_players(BOX), False, earlier, NOW)
    assert (1628973, "active") in {(o.player_id, o.report.status) for o in observed}


def test_from_the_tip_every_listed_player_is_news():
    observed = boxscore_observations("0022600101", boxscore_players(BOX), True, {}, NOW)
    assert len(observed) == 5
    assert {o.team for o in observed} == {"NYK", "BOS"}


# ---- reactions -------------------------------------------------------------------------------------


def test_reactions_are_out_doubtful_or_available_changes_for_games_not_tipped():
    def change(pid, verdict, changed=True, game_id="0022600102"):
        return VerdictChange(pid, verdict, None, "espn_injuries", game_id, changed)

    before_tips = et(GAME_DAY, 19, 45)   # the 7:30 game has tipped, the 10:00 has not
    verdicts = [
        change(1, "out"), change(2, "doubtful"), change(3, "available"),
        change(4, "questionable"),                      # not a reaction verdict
        change(5, "out", changed=False),                # re-observed, not changed
        change(6, "out", game_id="0022600101"),         # tipped
        change(7, "out", game_id=None),                 # no game today
        change(8, "out", game_id="0022699999"),         # a game we cannot place
    ]
    assert [v.player_id for v in reaction_events(verdicts, NIGHT, before_tips)] == [1, 2, 3]


def test_slate_context_names_the_opponent():
    ctx = NIGHT.context("BOS")
    assert (ctx.has_game, ctx.game_id, ctx.day_words) == (True, "0022600101", ("New York", "Knicks"))
    assert NIGHT.context("MIA").has_game is False
    box_only = NIGHT.context("MIA", game_id="0042600101")   # a playoff game nba.games does not have yet
    assert (box_only.has_game, box_only.game_id) == (True, "0042600101")
