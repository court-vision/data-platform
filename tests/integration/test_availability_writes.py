"""
The availability pipeline against Postgres (pipelines/availability.py): what a
tick writes to nba.availability_reports and nba.player_availability, what a
quiet one leaves alone, live-stats' hand-off of its box scores, and the copy
into nba.player_injuries.

The extractors are faked; everything under them runs for real, one tick at a
time under freeze_time. GAME_DAY is a Tuesday with Knicks-Celtics at 7:30 PM ET
(23:30 UTC) and Lakers-Warriors at 10.
"""

from __future__ import annotations

from datetime import date, datetime, time, timezone
from typing import NamedTuple, Optional

import psycopg2
import pytest
from freezegun import freeze_time

from core.locks import NAMESPACE, lock_key
from core.settings import settings
from db.models.nba.availability import AvailabilityReport, PlayerAvailability
from db.models.nba.games import Game
from db.models.nba.player_injuries import PlayerInjury
from db.models.nba.player_profiles import PlayerProfile
from db.models.nba.players import Player
from pipelines.availability import (
    WRITE_LOCK,
    AvailabilityPipeline,
    record_boxscore,
    write_injuries_compat,
    write_transaction,
)
from pipelines.extractors.espn_injuries import EspnInjury
from pipelines.live_game_stats import LiveGameStatsPipeline
from pipelines.transformers import normalize_name
from schemas.common import ApiStatus
from schemas.pipeline import PipelineResult

pytestmark = pytest.mark.integration

GAME_DAY = date(2026, 10, 27)
KNICKS_CELTICS = "0022600101"
LAKERS_WARRIORS = "0022600102"

BRUNSON, HART, TATUM, JAMES, CURRY = 1628973, 1628404, 1628369, 2544, 201939
PLAYERS = {
    BRUNSON: ("Jalen Brunson", 3934672, "NYK"),
    HART: ("Josh Hart", 3062679, "NYK"),
    TATUM: ("Jayson Tatum", 4065648, "BOS"),
    JAMES: ("LeBron James", 1966, "LAL"),
    CURRY: ("Stephen Curry", 3975, "GSW"),
}
BOTH_ESPN = ("espn_injuries", "espn_fantasy")


def _utc(at: str) -> datetime:
    return datetime.fromisoformat(at).replace(tzinfo=timezone.utc)


def _game(game_id: str, day: date, home: str, away: str, tip: time) -> None:
    Game.create(game_id=game_id, game_date=day, season="2026-27", home_team=home, away_team=away, start_time_et=tip)


@pytest.fixture
def night(integration_db):
    for pid, (name, espn_id, team) in PLAYERS.items():
        Player.create(id=pid, name=name, name_normalized=normalize_name(name), espn_id=espn_id)
        PlayerProfile.create(player=pid, team=team, to_year=None)
    _game(KNICKS_CELTICS, GAME_DAY, "NYK", "BOS", time(19, 30))
    _game(LAKERS_WARRIORS, GAME_DAY, "LAL", "GSW", time(22, 0))
    return Feeds()


class Tick(NamedTuple):
    result: PipelineResult
    skips: dict[str, int]
    failures: dict[str, int]


class Feeds:
    """ESPN and the NBA as the pipeline sees them; `tick` runs one poll of the given sources."""

    def __init__(self):
        self.injuries: list[EspnInjury] = []
        self.fantasy: dict[str, dict] = {}
        self.boxes: dict[str, dict] = {}
        self.down: set[str] = set()

    def tick(self, at: str, sources=BOTH_ESPN, boxscore_games: tuple[str, ...] = ()) -> Tick:
        pipeline = AvailabilityPipeline()
        pipeline.injuries_extractor.extract = self._feed("espn_injuries", lambda: (list(self.injuries), 0))
        pipeline.espn_extractor.get_player_data = self._feed("espn_fantasy", lambda: dict(self.fantasy))
        pipeline.nba_extractor.get_live_box_score = self._feed("nba_boxscore", self.boxes.get)
        seen = []
        pipeline.after_execute = seen.append
        with freeze_time(at):
            result = pipeline._run_sync(
                options={"sources": list(sources), "boxscore_games": list(boxscore_games)},
            )
        assert result.status == ApiStatus.SUCCESS, result.error
        ctx = seen[0]
        return Tick(result, dict(ctx.skip_reasons), dict(ctx.failure_reasons))

    def _feed(self, source: str, read):
        def call(*args):
            if source in self.down:
                raise ConnectionError(f"{source} is down")
            return read(*args)
        return call


def listed(pid: int, status: str = "out", *, reason: str = "Knee", detail: Optional[str] = None,
           written: str = "2026-10-27T14:00:00", returns: Optional[date] = None) -> EspnInjury:
    """An entry on ESPN's injury list."""
    name, espn_id, team = PLAYERS[pid]
    return EspnInjury(
        item_id=f"note-{pid}", espn_id=espn_id, name=name, team=team, status=status,
        espn_status="Out" if status == "out" else "Day-To-Day", fantasy_status=None,
        reason=reason, detail=detail or f"{name} ({reason.lower()}) is {'out' if status == 'out' else 'day-to-day'}.",
        long_comment=None,
        expected_return=returns, source_time=_utc(written),
    )


def fantasy(**statuses: str) -> dict[str, dict]:
    """ESPN fantasy's player data, by last name: OUT, DAY_TO_DAY, ACTIVE, ..."""
    out = {}
    for pid, (name, espn_id, _) in PLAYERS.items():
        status = statuses.get(name.split()[-1].lower())
        if status:
            out[normalize_name(name)] = {
                "espn_id": espn_id, "rost_pct": 90.0, "injured": status != "ACTIVE", "injury_status": status,
            }
    return out


def box(game_status: int, home: list[dict], away: list[dict] = (), *, teams=("NYK", "BOS")) -> dict:
    """A cdn.nba.com live box score."""
    return {
        "gameStatus": game_status,
        "homeTeam": {"teamTricode": teams[0], "players": list(home)},
        "awayTeam": {"teamTricode": teams[1], "players": list(away)},
    }


def on_box(pid: int, status: str = "ACTIVE", reason: Optional[str] = None,
           description: Optional[str] = None, points: int = 0) -> dict:
    name = PLAYERS[pid][0] if pid in PLAYERS else "Unknown Player"
    first, family = name.split(" ", 1)
    entry = {
        "personId": pid, "status": status, "firstName": first, "familyName": family,
        "statistics": {"minutes": "PT20M00.00S" if status == "ACTIVE" and not reason else "PT00M00.00S",
                       "minutesCalculated": "PT20M", "points": points},
    }
    if reason:
        entry["notPlayingReason"] = reason
    if description:
        entry["notPlayingDescription"] = description
    return entry


def reports(pid: Optional[int] = None) -> list[tuple]:
    q = AvailabilityReport.select().order_by(AvailabilityReport.id)
    if pid is not None:
        q = q.where(AvailabilityReport.player == pid)
    return [(r.player_id, r.source, r.status) for r in q]


def verdict(pid: int, day: date = GAME_DAY) -> Optional[PlayerAvailability]:
    return PlayerAvailability.get_or_none((PlayerAvailability.player == pid) & (PlayerAvailability.game_date == day))


def aware(value: datetime) -> datetime:
    return value if value.tzinfo else value.replace(tzinfo=timezone.utc)


def injury_rows(day: date = GAME_DAY) -> dict[int, tuple]:
    return {
        row.player_id: (row.status, row.injury_type, row.injury_detail)
        for row in PlayerInjury.select().where(PlayerInjury.report_date == day)
    }


# ---- the log and the verdicts ---------------------------------------------------------


def test_a_tick_logs_what_the_feeds_say_and_a_quiet_one_writes_nothing(night):
    night.injuries = [
        listed(BRUNSON, "out", detail="Brunson (knee) will not play Tuesday against Boston."),
        listed(TATUM, "questionable", reason="Ankle",
               detail="Tatum (ankle) is questionable for Tuesday's game against New York."),
    ]
    night.fantasy = fantasy(brunson="OUT", tatum="DAY_TO_DAY", hart="ACTIVE", curry="ACTIVE")

    first = night.tick("2026-10-27T16:00:00")  # noon ET

    assert first.result.records_processed == 4
    assert sorted(reports()) == sorted([
        (BRUNSON, "espn_injuries", "out"), (TATUM, "espn_injuries", "questionable"),
        (BRUNSON, "espn_fantasy", "out"), (TATUM, "espn_fantasy", "questionable"),
    ])
    brunson, tatum = verdict(BRUNSON), verdict(TATUM)
    assert (brunson.verdict, brunson.decided_by, brunson.game_id, brunson.final) == (
        "out", "espn_injuries", KNICKS_CELTICS, False)
    assert (brunson.reason, brunson.detail, brunson.confidence) == (
        "Knee", "Brunson (knee) will not play Tuesday against Boston.", 80)
    assert (tatum.verdict, tatum.game_id) == ("questionable", KNICKS_CELTICS)
    assert verdict(HART) is None and verdict(CURRY) is None  # healthy and never listed: no row
    # Brunson going out is what the reactor would hear; questionable is not (decision 8).
    assert first.skips == {"reaction_not_posted": 1}

    rows = {pid: (aware(verdict(pid).changed_at), aware(verdict(pid).updated_at)) for pid in (BRUNSON, TATUM)}
    quiet = night.tick("2026-10-27T16:10:00")

    assert (quiet.result.records_processed, quiet.skips) == (0, {})
    assert len(reports()) == 4
    assert {pid: (aware(verdict(pid).changed_at), aware(verdict(pid).updated_at)) for pid in (BRUNSON, TATUM)} == rows


def test_a_player_espn_stops_naming_is_available_again(night):
    night.injuries = [listed(BRUNSON), listed(HART, "questionable", reason="Hamstring"),
                      listed(TATUM, reason="Ankle")]
    night.fantasy = fantasy(brunson="OUT")
    night.tick("2026-10-27T16:00:00")

    # Brunson is off ESPN's list and healthy on fantasy. Hart keeps the Knicks on
    # a list this short; the Celtics are off it, so Tatum's absence proves nothing.
    night.injuries = [listed(HART, "questionable", reason="Hamstring")]
    night.fantasy = fantasy(brunson="ACTIVE")
    tick = night.tick("2026-10-27T17:00:00")

    assert tick.result.records_processed == 2
    assert reports(BRUNSON)[-2:] == [(BRUNSON, "espn_injuries", "available"), (BRUNSON, "espn_fantasy", "available")]
    row = verdict(BRUNSON)
    assert (row.verdict, row.previous_verdict, row.decided_by) == ("available", "out", "espn_injuries")
    assert aware(row.changed_at) == _utc("2026-10-27T17:00:00")
    assert verdict(TATUM).verdict == "out"
    assert tick.skips == {"espn_injuries_team_missing": 1, "reaction_not_posted": 1}


def test_a_new_note_is_logged_but_moves_changed_at_only_when_it_moves_the_verdict(night):
    note = "Hart (hamstring) is questionable for Tuesday's game against Boston."
    night.injuries = [listed(HART, "questionable", reason="Hamstring", detail=note)]
    night.tick("2026-10-27T15:00:00")
    first_change = aware(verdict(HART).changed_at)

    night.injuries = [listed(HART, "questionable", reason="Hamstring", written="2026-10-27T16:30:00",
                             detail=note + " Coach Mike Brown called him a game-time decision.")]
    updated = night.tick("2026-10-27T16:40:00")

    row = verdict(HART)
    assert updated.result.records_processed == 1
    assert (row.verdict, aware(row.changed_at), aware(row.updated_at)) == (
        "questionable", first_change, _utc("2026-10-27T16:40:00"))
    assert "game-time decision" in row.detail

    # Still Day-To-Day on ESPN, but the note now rules him out for tonight.
    night.injuries = [listed(HART, "questionable", reason="Hamstring", written="2026-10-27T21:00:00",
                             detail="Hart (hamstring) won't play Tuesday against Boston.")]
    ruled_out = night.tick("2026-10-27T21:10:00")

    row = verdict(HART)
    assert (row.verdict, row.previous_verdict, aware(row.changed_at)) == (
        "out", "questionable", _utc("2026-10-27T21:10:00"))
    assert ruled_out.skips == {"reaction_not_posted": 1}


def test_an_injury_carried_over_from_the_last_game_is_not_news(night):
    # Sunday: Brunson was out for the Knicks' last game, and has been since Friday.
    _game("0022600090", date(2026, 10, 25), "ORL", "NYK", time(18, 0))
    PlayerAvailability.create(
        player=BRUNSON, game_date=date(2026, 10, 25), game_id="0022600090", verdict="out",
        decided_by="espn_injuries", changed_at=_utc("2026-10-23T20:00:00"),
    )
    night.injuries = [listed(BRUNSON), listed(HART, reason="Hamstring")]

    tick = night.tick("2026-10-27T10:02:00")  # the date's first poll

    brunson, hart = verdict(BRUNSON), verdict(HART)
    assert (brunson.verdict, brunson.previous_verdict, aware(brunson.changed_at)) == (
        "out", "out", _utc("2026-10-23T20:00:00"))
    assert (hart.verdict, hart.previous_verdict, aware(hart.changed_at)) == (
        "out", "available", _utc("2026-10-27T10:02:00"))
    assert tick.skips == {"reaction_not_posted": 1}  # Hart's, not Brunson's


def test_the_box_scores_inactive_list_is_final_before_the_tip(night):
    night.injuries = [listed(HART, "questionable", reason="Hamstring")]
    night.tick("2026-10-27T16:00:00")

    night.boxes[KNICKS_CELTICS] = box(1, [
        on_box(BRUNSON),  # before the tip ACTIVE may only mean the list is not in yet
        on_box(HART, "INACTIVE", "INACTIVE_INJURY", "Left Hamstring; Strain"),
    ], [on_box(TATUM)])
    tick = night.tick("2026-10-27T22:30:00", sources=(), boxscore_games=(KNICKS_CELTICS,))

    assert tick.result.records_processed == 1
    assert reports(HART)[-1] == (HART, "nba_boxscore", "inactive")
    row = verdict(HART)
    assert (row.verdict, row.final, row.decided_by, row.game_id, row.confidence) == (
        "out", True, "nba_boxscore", KNICKS_CELTICS, 100)
    assert (row.reason, row.detail, row.previous_verdict) == ("INACTIVE_INJURY", "Left Hamstring; Strain", "questionable")
    assert verdict(BRUNSON) is None and verdict(TATUM) is None
    assert tick.skips == {"reaction_not_posted": 1}
    # player_injuries gets the box score's word, not its code for a body part.
    assert injury_rows()[HART] == ("Out", None, "Left Hamstring; Strain")


def test_a_box_score_not_up_yet_is_skipped_and_a_dead_feed_costs_only_its_own_reports(night):
    night.down = {"espn_injuries"}
    night.fantasy = fantasy(brunson="OUT", hart="SUSPENSION")
    night.fantasy["somebody nobody knows"] = {"espn_id": 1, "rost_pct": 0.0, "injured": True, "injury_status": "OUT"}
    night.fantasy["a new word"] = {"espn_id": 2, "rost_pct": 0.0, "injured": True, "injury_status": "ON_LEAVE"}

    tick = night.tick("2026-10-27T22:30:00", boxscore_games=(KNICKS_CELTICS,))

    assert tick.result.partial and tick.failures == {"espn_injuries_unavailable": 1}
    assert tick.result.records_processed == 2
    assert (verdict(BRUNSON).verdict, verdict(BRUNSON).decided_by) == ("out", "espn_fantasy")
    assert (verdict(HART).verdict, verdict(HART).reason) == ("out", "Suspension")
    assert tick.skips == {
        "boxscore_not_up": 1, "espn_fantasy_unknown_status": 1, "unmatched_player": 1, "reaction_not_posted": 2,
    }


# ---- live-stats' hand-off ---------------------------------------------------------------


def _live_poll(at: str, game: dict) -> PipelineResult:
    pipeline = LiveGameStatsPipeline()
    pipeline.nba_extractor.get_scoreboard_games = lambda _d: [{
        "game_id": KNICKS_CELTICS, "game_status": game["gameStatus"], "period": 1, "game_clock": "PT08M00.00S",
        "home_team": "NYK", "away_team": "BOS", "home_score": 12, "away_score": 10,
    }]
    pipeline.nba_extractor.get_live_box_score = lambda _game_id: game
    with freeze_time(at):
        return pipeline._run_sync(nba_date=GAME_DAY)


def test_live_stats_hands_over_its_box_scores_from_the_tip(night):
    game = box(2, [
        on_box(BRUNSON, points=8),
        on_box(HART, "INACTIVE", "INACTIVE_INJURY", "Left Hamstring; Strain"),
    ], [
        on_box(TATUM, "ACTIVE", "DNP_COACH", "Coach's Decision"),
        on_box(1641700, "ACTIVE", "DND_ILLNESS", "Illness"),  # a two-way player nba.players lacks
    ])

    result = _live_poll("2026-10-28T00:00:00", game)  # 8 PM ET

    assert result.status == ApiStatus.SUCCESS and not result.partial
    assert sorted(reports()) == sorted([
        (BRUNSON, "nba_boxscore", "active"), (HART, "nba_boxscore", "inactive"), (TATUM, "nba_boxscore", "active"),
    ])
    assert (verdict(BRUNSON).verdict, verdict(BRUNSON).final) == ("available", True)
    assert (verdict(HART).verdict, verdict(HART).reason) == ("out", "INACTIVE_INJURY")
    # A coach's DNP dressed: he was available.
    assert (verdict(TATUM).verdict, AvailabilityReport.get(AvailabilityReport.player == TATUM).reason) == (
        "available", "DNP_COACH")

    # The next poll reads the same lists: nothing new to log.
    _live_poll("2026-10-28T00:01:00", game)
    assert len(reports()) == 3


def test_record_boxscore_trusts_a_pre_tip_active_only_to_take_back_an_inactive(night):
    now = _utc("2026-10-27T22:40:00")
    inactive = box(1, [on_box(HART, "INACTIVE", "INACTIVE_INJURY", "Left Hamstring; Strain")])
    assert record_boxscore(GAME_DAY, [(KNICKS_CELTICS, inactive, False)], now=now, run_id=None).reports == 1

    back = box(1, [on_box(HART), on_box(BRUNSON)])
    result = record_boxscore(GAME_DAY, [(KNICKS_CELTICS, back, False)], now=_utc("2026-10-27T22:50:00"), run_id=None)

    assert result.reports == 1
    assert reports() == [(HART, "nba_boxscore", "inactive"), (HART, "nba_boxscore", "active")]
    assert [(c.player_id, c.verdict, c.changed) for c in result.verdicts] == [(HART, "available", True)]


# ---- nba.player_injuries ----------------------------------------------------------------


def test_player_injuries_gets_the_verdicts_and_clears_a_player_once_both_feeds_have_reported(night):
    PlayerInjury.create(player=CURRY, report_date=date(2026, 10, 26), status="Out", injury_type="Ankle")
    PlayerInjury.create(player=JAMES, report_date=date(2026, 10, 24), status="Questionable")
    night.injuries = [listed(BRUNSON, detail="Brunson (knee) will not play Tuesday.")]

    night.tick("2026-10-27T16:00:00", sources=("espn_injuries",))
    # Curry is on no list today, but fantasy has not reported: it may yet have him out.
    assert injury_rows() == {BRUNSON: ("Out", "Knee", "Brunson (knee) will not play Tuesday.")}

    night.fantasy = fantasy(brunson="OUT")
    night.tick("2026-10-27T16:15:00", sources=("espn_fantasy",))
    assert injury_rows() == {
        BRUNSON: ("Out", "Knee", "Brunson (knee) will not play Tuesday."),
        CURRY: ("Available", None, None),
        JAMES: ("Available", None, None),
    }

    # The copy is idempotent: the next run has nothing to write.
    assert write_injuries_compat(GAME_DAY, run_id=None) == 0


def test_with_the_copy_off_player_injuries_is_left_alone(night, monkeypatch):
    monkeypatch.setattr(settings, "availability_writes_injuries", False)
    night.injuries = [listed(BRUNSON)]
    night.tick("2026-10-27T16:00:00")
    assert verdict(BRUNSON).verdict == "out"
    assert injury_rows() == {}


# ---- the write lock ---------------------------------------------------------------------


@pytest.fixture
def rival(integration_db):
    """A second database session, standing in for the live-stats pipeline's."""
    conn = psycopg2.connect(settings.database_url)
    conn.autocommit = True
    yield conn
    conn.close()


def _try_write_lock(conn) -> bool:
    with conn.cursor() as cur:
        cur.execute("SELECT pg_try_advisory_xact_lock(%s, %s)", (NAMESPACE, lock_key(WRITE_LOCK)))
        return cur.fetchone()[0]


def test_writes_hold_the_availability_lock_until_they_commit(rival):
    with write_transaction():
        assert _try_write_lock(rival) is False
    # Transaction-scoped: gone with the commit, never left on the pooled connection.
    assert _try_write_lock(rival) is True
