"""Notes in season: when a note is in force, and what the live projection does
with one that is.

A note has a games part (a return date, or a season written off) and a role
part (minutes, a share of the offense), and each ends on its own.
"""

from datetime import date, datetime, timedelta

import pytest

from services.live_notes import ENDED, IN_FORCE, REVIEW_DUE, Note, RoleNote, holds, role_note, standing
from services.live_projection_model import (
    BUCKETS,
    HORIZON,
    VOLUME_KEYS,
    Availability,
    BoxLine,
    Coefficients,
    Prior,
    build_morning,
    play_chances,
    project,
)

pytestmark = pytest.mark.unit

D = date(2026, 11, 16)
WRITTEN = datetime(2026, 11, 16, 9, 0)           # a Monday morning, Eastern
STAR, FILL_IN, STARTER = 1, 3, 2


def _day(n: int) -> date:
    return D + timedelta(days=n)


def _note(**over) -> Note:
    base = dict(id=7, player_id=STAR, kind="injury_current", written=WRITTEN, review_date=_day(14))
    return Note(**(base | over))


def _tip(day: date, hour: int = 19) -> datetime:
    return datetime(day.year, day.month, day.day, hour, 0)


# ---- how a note stands ----------------------------------------------------------------


class TestStanding:
    def test_a_return_date_holds_until_he_plays(self):
        note = _note(return_date=_day(22))
        # His last game was the night before it was written: that is the game he was hurt in
        out = standing(note, _day(1), {STAR: _tip(_day(-1))})
        assert (out.state, out.games, out.role, out.reasons) == (IN_FORCE, True, False, ())
        # He played after it was written: the note said he would not, and is spent
        back = standing(note, _day(4), {STAR: _tip(_day(3))})
        assert (back.state, back.games) == (ENDED, False) and "he played on 2026-11-19" in back.reasons[0]

    def test_a_note_written_the_night_he_was_hurt_is_not_spent_by_that_game(self):
        late = _note(return_date=_day(22), written=datetime(2026, 11, 16, 23, 30))
        assert standing(late, _day(1), {STAR: _tip(D)}).games is True
        # Written that morning, with him playing that night after all
        early = _note(return_date=_day(22))
        assert standing(early, _day(1), {STAR: _tip(D)}).games is False

    def test_a_return_date_that_passes_with_him_still_out_is_no_longer_believed(self):
        note = _note(return_date=_day(5))
        assert standing(note, _day(5), {}).games is True
        late = standing(note, _day(6), {})
        assert (late.state, late.games) == (ENDED, False) and "passed with him still out" in late.reasons[0]
        # A season written off has no date to pass
        assert standing(_note(games=0), _day(13), {}).games is True

    def test_the_role_part_ends_at_its_date_or_when_the_teammate_plays(self):
        note = _note(player_id=FILL_IN, kind="role", minutes=32.0, until_date=_day(10), depends_on=STAR)
        assert standing(note, _day(10), {STAR: _tip(_day(-1))}).role is True
        dated = standing(note, _day(11), {})
        assert (dated.state, dated.role) == (ENDED, False) and "ran through 2026-11-26" in dated.reasons[0]
        teammate = standing(note, _day(6), {STAR: _tip(_day(5))})
        assert teammate.role is False and "teammate it depended on played on 2026-11-21" in teammate.reasons[0]
        # His own games do not end a role note: he is the one playing the role
        assert standing(note, _day(6), {FILL_IN: _tip(_day(5))}).role is True

    def test_each_part_ends_on_its_own(self):
        # Back on a minutes limit: the return date is spent by his first game, the limit runs on
        note = _note(kind="injury_return", return_date=_day(4), minutes=24.0, until_date=_day(18), review_date=_day(20))
        out = standing(note, _day(6), {STAR: _tip(_day(4))})
        assert (out.state, out.games, out.role) == (IN_FORCE, False, True)
        assert len(out.reasons) == 1

    def test_every_note_expires_at_its_review_date(self):
        note = _note(games=0, minutes=30.0)
        assert standing(note, _day(14), {}).in_force
        due = standing(note, _day(15), {})
        assert (due.state, due.games, due.role) == (REVIEW_DUE, False, False)
        assert "read again since 2026-11-30" in due.reasons[0]

    def test_a_note_says_which_parts_it_has(self):
        assert _note(return_date=_day(3)).has_games_part and not _note(return_date=_day(3)).has_role_part
        assert _note(games=0).out_for_season and not _note(games=40).has_games_part
        assert _note(usage=1.1).has_role_part


# ---- how far the role part holds in the games ahead ----------------------------------------


class TestHolds:
    DAYS = [_day(2 * i) for i in range(6)]

    def test_with_no_end_it_holds_all_the_way(self):
        assert holds(RoleNote(1, "role", minutes=30.0), self.DAYS) == [1.0] * 6

    def test_through_its_date_and_not_after(self):
        note = RoleNote(1, "role", minutes=30.0, until_date=self.DAYS[2])
        assert holds(note, self.DAYS) == [1.0, 1.0, 1.0, 0.0, 0.0, 0.0]

    def test_until_the_teammate_plays_is_the_chance_he_has_not_yet(self):
        note = RoleNote(1, "role", minutes=30.0, depends_on=STAR)
        # Out for three games for certain, then back four nights in five
        assert holds(note, self.DAYS, [0.0, 0.0, 0.0, 0.8, 0.8, 0.8]) == pytest.approx(
            [1.0, 1.0, 1.0, 0.2, 0.04, 0.008])
        # Whichever comes first
        both = RoleNote(1, "role", minutes=30.0, depends_on=STAR, until_date=self.DAYS[1])
        assert holds(both, self.DAYS, [0.0] * 6) == [1.0, 1.0, 0.0, 0.0, 0.0, 0.0]


# ---- the model prices it ---------------------------------------------------------------------


PER_MINUTE = {"pts": 0.66, "reb": 0.2, "ast": 0.15, "stl": 0.03, "blk": 0.02, "tov": 0.06,
              "fgm": 0.24, "fga": 0.5, "fg3m": 0.06, "fg3a": 0.16, "ftm": 0.12, "fta": 0.15}
HEALTHY = (0.0,) + (0.9,) * HORIZON
RETURNING = (0.0,) + tuple(min(0.1 * k, 0.8) for k in range(1, HORIZON + 1))
COEFFS = Coefficients(
    plays=(HEALTHY,) + (RETURNING,) * (BUCKETS - 1), played_once_back=(0.8,) * BUCKETS,
    lean_prior_share=0.0, lean_attendance=0.0, mean_prior_share=0.7, mean_attendance=0.85,
    minutes_prior_games=0.0, minutes_half_life=0.0, situation_slope=0.3, own_slope_ridge=0.0,
    usage_slope=0.01, development={},
    prior_weights={k: 400.0 for k in VOLUME_KEYS} | {"fg2": 300.0, "fg3": 300.0, "ft": 150.0},
)


def _prior(minutes: float) -> Prior:
    return Prior(minutes=minutes, games=70.0, per_game={k: v * minutes for k, v in PER_MINUTE.items()})


def _line(pid, game, day, minutes) -> BoxLine:
    return BoxLine(player_id=pid, team="AAA", game_id=game, game_date=day, minutes=minutes,
                   stats={k: v * minutes for k, v in PER_MINUTE.items()})


def _morning():
    """AAA after eight games, everyone healthy: the star at 34 a night, a starter at 30, a bench player at 12."""
    lines = []
    for g in range(8):
        day = _day(-16 + 2 * g)
        lines += [_line(STAR, f"a{g}", day, 34.0), _line(STARTER, f"a{g}", day, 30.0), _line(FILL_IN, f"a{g}", day, 12.0)]
    days = [_day(2 * i) for i in range(10)]
    priors = {STAR: _prior(34.0), STARTER: _prior(30.0), FILL_IN: _prior(12.0)}
    return build_morning(lines, priors, {"AAA": days}, D), days


class TestTheModelReadsANote:
    def test_a_note_can_say_he_is_out_before_any_box_score_does(self):
        # He played last night and was hurt in it: no missed game yet, and a note with a return date
        days = [_day(2 * i) for i in range(6)]
        known = Availability(out_now=True, return_date=days[3])
        assert play_chances(0, False, days, 1.0, COEFFS, known, as_of=D) == [0.0, 0.0, 0.0, 0.8, 0.8, 0.8]
        # Without the note the same date is a stale one on a player who is playing
        assert play_chances(0, False, days, 1.0, COEFFS, Availability(return_date=days[3]), as_of=D) == [0.9] * 6
        # With no date he comes back along the curve of a player who has missed a game
        assert play_chances(0, False, days, 1.0, COEFFS, Availability(out_now=True), as_of=D) == pytest.approx(
            [0.1, 0.2, 0.3, 0.4, 0.5, 0.6])

    def test_the_stars_return_date_moves_his_games_and_his_teammates_minutes(self):
        morning, days = _morning()
        plain = project(morning, COEFFS)
        out = project(morning, COEFFS, {STAR: Availability(out_now=True, return_date=days[4])})

        assert [g.chance for g in out[STAR].games_ahead] == [0.0] * 4 + [0.8] * 6
        assert out[STAR].games == pytest.approx(4.8) and plain[STAR].games == pytest.approx(9.0)
        # What he does per minute is what it was: the injury took games, not skill
        assert out[STAR].rates == plain[STAR].rates
        # The bench player gets some of the 34 minutes while he is out
        assert out[FILL_IN].games_ahead[0].minutes > plain[FILL_IN].games_ahead[0].minutes + 5

    def test_minutes_on_a_note_replace_the_models_while_it_holds(self):
        morning, days = _morning()
        known = {STAR: Availability(out_now=True, return_date=days[4])}
        note = RoleNote(note_id=9, kind="role", minutes=30.0, usage=1.2, depends_on=STAR)
        plain = project(morning, COEFFS, known)
        out = project(morning, COEFFS, known, {FILL_IN: note})

        games = out[FILL_IN].games_ahead
        # The four games the star is sure to miss: the note's minutes and share, exactly
        assert [(g.minutes, g.usage) for g in games[:4]] == [(30.0, 1.2)] * 4
        # From the day he is expected back the note holds by the chance he is not: a fifth, then a twenty-fifth
        model = plain[FILL_IN].games_ahead
        assert games[4].minutes == pytest.approx(0.2 * 30.0 + 0.8 * model[4].minutes)
        assert games[5].minutes == pytest.approx(0.04 * 30.0 + 0.96 * model[5].minutes)
        assert games[9].minutes == pytest.approx(model[9].minutes, abs=0.01)
        # His chance of playing is his own
        assert [g.chance for g in games] == [g.chance for g in model]
        # The line he is stored with is the average over the games he plays: above the model's
        assert out[FILL_IN].minutes > plain[FILL_IN].minutes + 2
        assert out[FILL_IN].per_game["pts"] > plain[FILL_IN].per_game["pts"]
        assert out[FILL_IN].parts["note"] == {
            "id": 9, "kind": "role", "minutes": 30.0, "usage": 1.2, "until": None, "depends_on": STAR,
            "games": pytest.approx(4.2, abs=0.06), "through": days[3].isoformat(),
        }
        assert "note" not in plain[FILL_IN].parts and "note" not in out[STARTER].parts
        # A window prices it: this week is the note's line, the week after his return the model's
        week_games, week = out[FILL_IN].window(days[0], days[3])
        assert week["pts"] / week_games == pytest.approx(out[FILL_IN].rates["pts"] * 30.0 * 1.2)

    def test_a_minutes_limit_runs_to_its_date(self):
        morning, days = _morning()
        note = RoleNote(note_id=4, kind="injury_return", minutes=24.0, until_date=days[2])
        out = project(morning, COEFFS, notes={STAR: note})

        minutes = [round(g.minutes, 1) for g in out[STAR].games_ahead]
        assert minutes[:3] == [24.0] * 3 and all(m == pytest.approx(34.0, abs=1.0) for m in minutes[3:])
        assert out[STAR].parts["note"]["games"] == 3.0 and out[STAR].parts["note"]["through"] == days[2].isoformat()
        # Usage was not on the note: the model's own
        assert out[STAR].games_ahead[0].usage == project(morning, COEFFS)[STAR].games_ahead[0].usage
        # The timeline stores the two stretches apart
        assert [run[2] for run in out[STAR].timeline()] == [3, 7]

    def test_a_note_that_depends_on_a_player_from_another_team_depends_on_nothing(self):
        morning, days = _morning()
        note = RoleNote(note_id=5, kind="role", minutes=20.0, depends_on=999)
        out = project(morning, COEFFS, notes={FILL_IN: note})
        assert [g.minutes for g in out[FILL_IN].games_ahead] == [20.0] * 10

    def test_role_note_carries_the_role_part_only(self):
        note = _note(player_id=FILL_IN, kind="role", minutes=32.0, usage=1.1, until_date=_day(10), depends_on=STAR,
                     return_date=_day(3))
        assert role_note(note) == RoleNote(note_id=7, kind="role", minutes=32.0, usage=1.1, until_date=_day(10),
                                           depends_on=STAR)
