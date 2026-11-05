"""
The Availability page's reads against Postgres (services/availability_scorecard.py):
the report log, the slate and the players in, the scorecard out. Which outcome
each pattern of reports gets is tested in tests/unit/test_availability_scorecard.py;
this is the window, the dates it scores, and what each call is told about its player.

Wednesday Nov 4 has Knicks-Celtics at 7:30 PM ET (00:30 UTC) and Lakers-Warriors
at 10 (03:00 UTC); it is 8 PM ET, between the two. Tuesday's only game tipped
before the pipeline first polled the date, so Tuesday is not scored.
"""

from __future__ import annotations

from datetime import date, datetime, time

import pytest

from db.models.nba.availability import AvailabilityReport
from db.models.nba.games import Game
from db.models.nba.player_profiles import PlayerProfile
from db.models.nba.players import Player
from pipelines.transformers import normalize_name
from services.availability_scorecard import build_scorecard

pytestmark = pytest.mark.integration

TUESDAY, WEDNESDAY = date(2026, 11, 3), date(2026, 11, 4)
NOW = datetime.fromisoformat("2026-11-05T01:00:00+00:00")
KNICKS_CELTICS, LAKERS_WARRIORS, TUESDAY_GAME = "0022600151", "0022600152", "0022600140"
BRUNSON, HART, TATUM, JAMES, CURRY, NOBODY = 1628973, 1628404, 1628369, 2544, 201939, 1641000
PLAYERS = {
    BRUNSON: ("Jalen Brunson", "NYK"),
    HART: ("Josh Hart", "NYK"),
    TATUM: ("Jayson Tatum", "BOS"),
    JAMES: ("LeBron James", "LAL"),
    CURRY: ("Stephen Curry", "GSW"),
    NOBODY: ("Free Agent", None),
}
CONFIDENCE = {"nba_boxscore": 100, "espn_injuries": 80, "espn_fantasy": 70}


def report(pid: int, day: date, source: str, status: str, at: str, game_id: str | None = None) -> None:
    AvailabilityReport.create(
        player=pid, game_date=day, source=source, status=status, game_id=game_id,
        observed_at=datetime.fromisoformat(at), confidence=CONFIDENCE[source],
    )


@pytest.fixture
def log(integration_db):
    for pid, (name, team) in PLAYERS.items():
        Player.create(id=pid, name=name, name_normalized=normalize_name(name))
        if team:
            PlayerProfile.create(player=pid, team=team, to_year=None)
    Game.create(game_id=TUESDAY_GAME, game_date=TUESDAY, season="2026-27", home_team="NYK", away_team="MIA",
                start_time_et=time(19, 30))
    Game.create(game_id=KNICKS_CELTICS, game_date=WEDNESDAY, season="2026-27", home_team="NYK", away_team="BOS",
                start_time_et=time(19, 30))
    Game.create(game_id=LAKERS_WARRIORS, game_date=WEDNESDAY, season="2026-27", home_team="LAL", away_team="GSW",
                start_time_et=time(22, 0))

    # Tuesday: first polled at 10 PM ET, after its 7:30 tip.
    report(BRUNSON, TUESDAY, "espn_injuries", "out", "2026-11-04T03:00:00")
    report(BRUNSON, TUESDAY, "espn_fantasy", "out", "2026-11-04T03:00:00")

    # Wednesday's first polls, at 6 AM ET.
    report(HART, WEDNESDAY, "espn_injuries", "out", "2026-11-04T11:00:00")
    report(HART, WEDNESDAY, "espn_fantasy", "out", "2026-11-04T11:00:00")
    report(NOBODY, WEDNESDAY, "espn_injuries", "out", "2026-11-04T11:00:00")
    # Brunson is ruled out at 3 PM ET; ESPN fantasy follows at 4:15.
    report(BRUNSON, WEDNESDAY, "espn_injuries", "out", "2026-11-04T20:00:00")
    report(BRUNSON, WEDNESDAY, "espn_fantasy", "out", "2026-11-04T21:15:00")
    # Tatum is listed out and plays.
    report(TATUM, WEDNESDAY, "espn_injuries", "out", "2026-11-04T22:00:00")
    # Curry's game has not tipped.
    report(CURRY, WEDNESDAY, "espn_fantasy", "out", "2026-11-04T23:00:00")
    # The Knicks-Celtics box score at the tip.
    for pid, status in ((BRUNSON, "inactive"), (HART, "inactive"), (TATUM, "active")):
        report(pid, WEDNESDAY, "nba_boxscore", status, "2026-11-05T00:31:00", game_id=KNICKS_CELTICS)
    # LeBron sits a game in which no source named him.
    report(JAMES, WEDNESDAY, "nba_boxscore", "inactive", "2026-11-05T03:01:00", game_id=LAKERS_WARRIORS)


def test_scores_the_covered_dates_and_names_each_player(log):
    card = build_scorecard(2, now=NOW)

    assert (card.since, card.until, card.available) == (TUESDAY, WEDNESDAY, True)
    assert [(d.game_date, d.games, d.covered, d.calls) for d in card.dates] == [
        (WEDNESDAY, 2, True, 4),
        (TUESDAY, 1, False, 0),
    ]
    assert card.dates[0].first_poll == datetime.fromisoformat("2026-11-04T11:00:00+00:00")
    assert card.dates[0].first_tip == datetime.fromisoformat("2026-11-05T00:30:00+00:00")

    # By tip, and within a game the false outs first.
    assert [(c.name, c.team, c.outcome) for c in card.calls] == [
        ("Jayson Tatum", "BOS", "false_out"),
        ("Jalen Brunson", "NYK", "us_first"),
        ("Josh Hart", "NYK", "carried"),
        ("Stephen Curry", "GSW", "pending"),
    ]
    brunson = card.calls[1]
    assert (brunson.game_id, brunson.lead_minutes, brunson.ours_by) == (KNICKS_CELTICS, 75.0, "espn_injuries")

    s = card.summary
    assert (s.races, s.us_first, s.carried, s.pending, s.false_outs, s.false_outs_floor) == (1, 1, 1, 1, 1, 0)
    assert s.first_share == 1.0
    assert not s.gate_met  # Tatum's false out was ours


def test_a_date_espn_fantasy_never_read_before_the_tip_is_not_scored(log):
    AvailabilityReport.delete().where(
        (AvailabilityReport.game_date == WEDNESDAY) & (AvailabilityReport.source == "espn_fantasy")
    ).execute()

    card = build_scorecard(2, now=NOW)

    assert [(d.game_date, d.covered) for d in card.dates] == [(WEDNESDAY, False), (TUESDAY, False)]
    assert card.calls == []
    assert card.summary.races == 0


def test_an_empty_window_is_not_available(log):
    card = build_scorecard(1, now=datetime.fromisoformat("2026-11-20T18:00:00+00:00"))

    assert not card.available
    assert card.dates == []
    assert card.calls == []
