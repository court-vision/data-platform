"""The live projection's pure parts: the morning built from box scores, each
game's chance of being played, minutes by situation, the per-minute line, and
the line per game left that prices any window.

Every season below is built by hand so each number can be checked on paper;
the committed coefficients file is checked for shape, not for its values.
"""

import json
from dataclasses import replace
from datetime import date, timedelta

import pytest

from services.live_projection_inputs import COEFFICIENTS_PATH
from services.live_projection_model import (
    BUCKETS,
    HORIZON,
    LINE_KEYS,
    MAX_MINUTES,
    MAX_PLAY_CHANCE,
    PRE_DEBUT,
    VOLUME_KEYS,
    Availability,
    BoxLine,
    Coefficients,
    GameAhead,
    PlayerState,
    Prior,
    bucket,
    build_morning,
    compress,
    development_of,
    expand,
    minutes_for,
    outlooks,
    play_chances,
    play_lean,
    prior_totals,
    project,
    rates_of,
    role_of,
    usage_of,
)

pytestmark = pytest.mark.unit

PER_MINUTE = {"pts": 0.66, "reb": 0.2, "ast": 0.15, "stl": 0.03, "blk": 0.02, "tov": 0.06,
              "fgm": 0.24, "fga": 0.5, "fg3m": 0.06, "fg3a": 0.16, "ftm": 0.12, "fta": 0.15}

HEALTHY = (0.0,) + (0.9,) * HORIZON
# A player who is out comes back a tenth at a time, up to four games in five
RETURNING = (0.0,) + tuple(min(0.1 * k, 0.8) for k in range(1, HORIZON + 1))


def _coeffs(**over) -> Coefficients:
    base = dict(
        plays=(HEALTHY,) + (RETURNING,) * (BUCKETS - 1),
        played_once_back=(0.8,) * BUCKETS,
        lean_prior_share=0.0, lean_attendance=0.0, mean_prior_share=0.7, mean_attendance=0.85,
        minutes_prior_games=0.0, minutes_half_life=0.0, situation_slope=0.0, own_slope_ridge=0.0,
        usage_slope=0.0, development={},
        prior_weights={k: 400.0 for k in VOLUME_KEYS} | {"fg2": 300.0, "fg3": 300.0, "ft": 150.0},
    )
    return Coefficients(**(base | over))


def _prior(minutes=30.0, games=70.0, per_minute=PER_MINUTE) -> Prior:
    return Prior(minutes=minutes, games=games, per_game={k: v * minutes for k, v in per_minute.items()})


def _line(pid, team, game, day, minutes, per_minute=PER_MINUTE) -> BoxLine:
    return BoxLine(player_id=pid, team=team, game_id=game, game_date=day, minutes=minutes,
                   stats={k: v * minutes for k, v in per_minute.items()})


D = date(2026, 11, 1)


def _day(n: int) -> date:
    return D + timedelta(days=n)


STAR, STARTER, BENCH, LATE, MOVED, NEW = 1, 2, 3, 4, 5, 7
PRIORS = {STAR: _prior(34.0), STARTER: _prior(30.0), BENCH: _prior(12.0), LATE: _prior(28.0), MOVED: _prior(26.0)}


def _aaa(star_games=(0, 1, 3), bench_minutes=(12, 12, 22, 12)) -> list[BoxLine]:
    """AAA's first four games, two days apart. The star sits out the ones not listed."""
    lines = []
    for g in range(4):
        day, game = _day(2 * g), f"a{g}"
        if g in star_games:
            lines.append(_line(STAR, "AAA", game, day, 34.0))
        lines.append(_line(STARTER, "AAA", game, day, 30.0))
        lines.append(_line(BENCH, "AAA", game, day, float(bench_minutes[g])))
    return lines


AHEAD = {"AAA": [_day(8 + 2 * i) for i in range(10)]}
AS_OF = _day(8)


class TestBuckets:
    @pytest.mark.parametrize("streak,expected", [(0, 0), (1, 1), (2, 2), (3, 3), (4, 3), (5, 4), (7, 4),
                                                 (8, 5), (12, 5), (13, 6), (20, 6), (21, 7), (60, 7)])
    def test_a_longer_absence_is_a_later_bucket(self, streak, expected):
        assert bucket(streak) == expected

    def test_possessions_are_shots_trips_to_the_line_and_turnovers(self):
        assert usage_of({"fga": 15, "fta": 5, "tov": 2}) == pytest.approx(19.2)


class TestCoefficients:
    def test_they_survive_a_round_trip_through_json(self):
        coeffs = _coeffs(situation_slope=0.04, development={"u22": 0.06}, version="t-1")
        back = Coefficients.from_json(json.loads(json.dumps(coeffs.to_json())))
        assert back == replace(coeffs, plays=back.plays, played_once_back=back.played_once_back)
        assert back.plays[0][1] == 0.9 and back.plays[3][5] == 0.5 and back.version == "t-1"

    def test_the_committed_file_has_a_curve_for_every_bucket(self):
        doc = json.loads(COEFFICIENTS_PATH.read_text())
        coeffs = Coefficients.from_json(doc)
        assert len(coeffs.plays) == BUCKETS and len(coeffs.played_once_back) == BUCKETS
        for row in coeffs.plays:
            assert len(row) == HORIZON + 1 and all(0.0 <= v <= 1.0 for v in row)
        # A healthy regular is likelier to play tonight than a game months away, and than a player who is out
        assert coeffs.plays[0][1] > 0.9 > coeffs.plays[0][40] > coeffs.plays[PRE_DEBUT][40]
        assert coeffs.plays[0][1] > coeffs.plays[1][1] > coeffs.plays[5][1]
        assert set(coeffs.prior_weights) == set(VOLUME_KEYS) | {"fg2", "fg3", "ft"}
        assert coeffs.version.startswith("live-") and doc["backtest"]["by_days_after_opening_night"]


class TestTheMorning:
    def test_a_game_is_filed_under_the_regulars_who_sat_it_out(self):
        morning = build_morning(_aaa(), PRIORS, AHEAD, AS_OF)

        bench = morning.players[BENCH]
        # The star (34 minutes established) missed the third game and nobody else did
        assert bench.vacated == [0.0, 0.0, 34.0, 0.0] and bench.minutes == [12.0, 12.0, 22.0, 12.0]
        assert bench.vacated_usage[2] == pytest.approx(PRIORS[STAR].usage) and bench.current == [True] * 4
        assert (bench.streak, bench.team, bench.team_played, bench.n) == (0, "AAA", 4, 4)
        # The regulars: the two who play 20 or more a night, both in the latest game
        assert {(m.player_id, m.streak, m.pre) for m in morning.rosters["AAA"]} == {(STAR, 0, False), (STARTER, 0, False)}
        assert bench.totals["min"] == 58.0 and bench.totals["reb"] == pytest.approx(0.2 * 58)

    def test_a_regular_who_sat_out_the_latest_games_is_on_the_roster_as_out(self):
        morning = build_morning(_aaa(star_games=(0, 1)), PRIORS, AHEAD, AS_OF)

        assert morning.players[STAR].streak == 2
        assert {(m.player_id, m.streak) for m in morning.rosters["AAA"]} == {(STAR, 2), (STARTER, 0)}
        assert morning.players[BENCH].vacated == [0.0, 0.0, 34.0, 34.0]

    def test_a_projected_player_is_out_from_the_opener_until_his_first_game(self):
        lines = _aaa() + [_line(LATE, "AAA", "a2", _day(4), 28.0), _line(LATE, "AAA", "a3", _day(6), 28.0)]
        morning = build_morning(lines, PRIORS, AHEAD, AS_OF)

        # 28 minutes were missing from the first two games, before his debut
        assert morning.players[STARTER].vacated == [28.0, 28.0, 34.0, 0.0]
        assert morning.players[LATE].pre is False and morning.players[LATE].n == 2

    def test_a_projected_player_with_no_game_yet_waits_on_the_team_he_is_on(self):
        priors = PRIORS | {NEW: _prior(31.0)}
        morning = build_morning(_aaa(), priors, AHEAD, AS_OF, teams={NEW: "AAA"})

        new = morning.players[NEW]
        assert (new.pre, new.streak, new.n, new.team, new.team_played) == (True, 4, 0, "AAA", 4)
        assert (NEW, 4, True) in {(m.player_id, m.streak, m.pre) for m in morning.rosters["AAA"]}
        assert morning.players[STARTER].vacated == [31.0, 31.0, 65.0, 31.0]
        # Without a team he is nowhere: the box scores cannot place a player who has not played
        assert NEW not in build_morning(_aaa(), priors, AHEAD, AS_OF).players

    def test_a_traded_player_is_out_for_the_old_team_until_he_plays_for_the_new_one(self):
        bbb = [_line(90 + i, "BBB", f"b{g}", day, 30.0) for g, day in enumerate((_day(1), _day(3), _day(5)))
               for i in range(2)]
        lines = _aaa() + bbb + [
            _line(MOVED, "AAA", "a0", _day(0), 26.0), _line(MOVED, "AAA", "a1", _day(2), 26.0),
            _line(MOVED, "BBB", "b2", _day(5), 26.0),
        ]
        morning = build_morning(lines, PRIORS, AHEAD | {"BBB": [_day(9)]}, AS_OF)

        moved = morning.players[MOVED]
        assert (moved.team, moved.streak, moved.current) == ("BBB", 0, [False, False, True])
        # AAA's third game (Nov 5) came before his first for BBB (Nov 6): he and the star both sat it out.
        # Its fourth came after, and he was no longer theirs to miss.
        assert morning.players[STARTER].vacated == [0.0, 0.0, 60.0, 0.0]
        assert MOVED in {m.player_id for m in morning.rosters["BBB"]}
        assert MOVED not in {m.player_id for m in morning.rosters["AAA"]}

    def test_a_player_traded_since_his_last_game_is_out_for_his_new_team(self):
        bbb = [_line(90 + i, "BBB", f"b{g}", day, 30.0) for g, day in enumerate((_day(1), _day(3), _day(7)))
               for i in range(2)]
        lines = _aaa() + bbb + [_line(MOVED, "AAA", "a0", _day(0), 26.0), _line(MOVED, "AAA", "a1", _day(2), 26.0)]
        morning = build_morning(lines, PRIORS, AHEAD, AS_OF, teams={MOVED: "BBB"})

        moved = morning.players[MOVED]
        # BBB has played twice since his last game, on Nov 3
        assert (moved.team, moved.streak, moved.current) == ("BBB", 2, [False, False])
        assert (MOVED, 2) in {(m.player_id, m.streak) for m in morning.rosters["BBB"]}
        assert morning.players[90].vacated == [0.0, 26.0, 26.0]

    def test_only_games_before_the_morning_are_played(self):
        lines = _aaa() + [_line(STAR, "AAA", "a4", AS_OF, 34.0)]
        morning = build_morning(lines, PRIORS, {"AAA": [_day(6), AS_OF, _day(10)]}, AS_OF)

        assert morning.players[STAR].n == 3 and morning.players[STAR].team_played == 4
        assert morning.ahead["AAA"] == [AS_OF, _day(10)]


class TestGames:
    DAYS = [_day(10 + 2 * i) for i in range(6)]

    def test_a_healthy_player_follows_his_curve_and_a_lean_scales_it(self):
        assert play_chances(0, False, self.DAYS, 1.0, _coeffs()) == [0.9] * 6
        assert play_chances(0, False, self.DAYS, 0.5, _coeffs()) == [0.45] * 6
        # Never a certainty
        assert play_chances(0, False, self.DAYS, 2.0, _coeffs()) == [MAX_PLAY_CHANCE] * 6

    def test_a_player_who_is_out_comes_back_along_the_curve_of_players_out_as_long(self):
        chances = play_chances(4, False, self.DAYS, 1.0, _coeffs())
        assert chances == pytest.approx([0.1, 0.2, 0.3, 0.4, 0.5, 0.6])
        assert play_chances(0, True, self.DAYS, 1.0, _coeffs()) == pytest.approx(chances)

    def test_ruled_out_of_the_next_game_he_misses_it_and_is_then_a_player_who_has_missed_one_more(self):
        chances = play_chances(0, False, self.DAYS, 1.0, _coeffs(), Availability(out_next=True))
        assert chances == pytest.approx([0.0, 0.1, 0.2, 0.3, 0.4, 0.5])

    def test_a_known_return_date_replaces_the_curve(self):
        known = Availability(out_next=True, return_date=self.DAYS[3])
        chances = play_chances(4, False, self.DAYS, 1.0, _coeffs(), known, as_of=_day(9))
        assert chances == [0.0, 0.0, 0.0, 0.8, 0.8, 0.8]
        # His own lean moves the share he plays once back
        assert play_chances(4, False, self.DAYS, 1.1, _coeffs(), known, as_of=_day(9))[3:] == pytest.approx([0.88] * 3)

    def test_a_return_date_that_has_passed_or_belongs_to_a_player_who_is_playing_is_not_believed(self):
        passed = Availability(out_next=True, return_date=_day(5))
        assert play_chances(4, False, self.DAYS, 1.0, _coeffs(), passed, as_of=_day(9)) == pytest.approx(
            [0.0, 0.1, 0.2, 0.3, 0.4, 0.5])
        # A curated date left behind on a player who is back on the floor
        stale = Availability(return_date=self.DAYS[3])
        assert play_chances(0, False, self.DAYS, 1.0, _coeffs(), stale, as_of=_day(9)) == [0.9] * 6

    def test_a_season_written_off_is_no_games(self):
        gone = Availability(out_for_season=True, return_date=self.DAYS[1])
        assert play_chances(0, True, self.DAYS, 1.0, _coeffs(), gone) == [0.0] * 6

    def test_the_lean_reads_his_projected_games_and_this_seasons_attendance(self):
        coeffs = _coeffs(lean_prior_share=0.5, lean_attendance=0.2)
        state = PlayerState(player_id=1, team="AAA", minutes=[30.0] * 10, team_played=10, prior=_prior(games=82 * 0.9))
        # 0.8 + 0.5 x (0.9 - 0.7) + 0.2 x (1.0 - 0.85), against the bucket's 0.8
        assert play_lean(state, coeffs) == pytest.approx(0.93 / 0.8)
        fragile = replace(state, prior=_prior(games=82 * 0.5), minutes=[30.0] * 5)
        assert play_lean(fragile, coeffs) == pytest.approx((0.8 - 0.1 - 0.2 * 0.35) / 0.8)
        # Nothing projects him: the bucket's own curve
        assert play_lean(replace(state, prior=None), coeffs) == 1.0
        # Bounded both ways
        assert play_lean(replace(state, prior=_prior(games=0.0), minutes=[]), _coeffs(lean_prior_share=5.0)) == 0.25

    def test_a_projection_with_no_games_leans_neither_way_on_them(self):
        """ESPN's projection can carry no games: unknown, not zero, so only
        this season's attendance leans him."""
        coeffs = _coeffs(lean_prior_share=0.5, lean_attendance=0.2)
        state = PlayerState(player_id=1, team="AAA", minutes=[30.0] * 10, team_played=10, prior=_prior(games=None))
        # 0.8 + 0.5 x 0 + 0.2 x (1.0 - 0.85), against the bucket's 0.8
        assert play_lean(state, coeffs) == pytest.approx(0.83 / 0.8)
        assert play_lean(replace(state, prior=_prior(games=0.0)), coeffs) < play_lean(state, coeffs)
        # Its shooting is counted over a full season, not over one game
        assert prior_totals(_prior(games=None))["fga"] == pytest.approx(prior_totals(_prior(games=82.0))["fga"])


class TestRole:
    def _bench(self, **over) -> PlayerState:
        base = dict(player_id=BENCH, team="AAA", minutes=[12.0, 12.0, 22.0, 12.0], vacated=[0.0, 0.0, 34.0, 0.0],
                    vacated_usage=[0.0] * 4, current=[True] * 4, prior=_prior(12.0))
        return PlayerState(**(base | over))

    def test_his_minutes_move_with_the_minutes_missing_around_him(self):
        coeffs = _coeffs(situation_slope=0.1)
        state = self._bench()
        role = role_of(state, coeffs)

        assert (role.base, role.weight, role.seen, role.slope) == (14.5, 4.0, 8.5, pytest.approx(0.1))
        # With the star out again he plays 14.5 + 0.1 x (34 - 8.5); with everyone back, less than his average
        assert minutes_for(role, state, 34.0, coeffs) == pytest.approx(17.05)
        assert minutes_for(role, state, 0.0, coeffs) == pytest.approx(13.65)
        # "As in his games so far"
        assert minutes_for(role, state, None, coeffs) == 14.5

    def test_recent_games_count_more_and_the_projection_counts_as_games(self):
        state = self._bench(minutes=[10.0, 10.0, 30.0, 30.0], vacated=[0.0] * 4)
        recent = role_of(state, _coeffs(minutes_half_life=1.0))
        assert recent.base == pytest.approx((10 * 0.125 + 10 * 0.25 + 30 * 0.5 + 30) / 1.875)
        flat = _coeffs(minutes_prior_games=4.0)
        assert minutes_for(role_of(state, flat), state, None, flat) == pytest.approx((20 * 4 + 4 * 12) / 8)

    def test_a_starter_gains_almost_nothing_and_his_own_games_can_speak(self):
        starter = self._bench(minutes=[34.0] * 4, prior=_prior(34.0))
        assert role_of(starter, _coeffs(situation_slope=0.1)).slope == pytest.approx(0.015)

        # Six games in which every vacated minute gave him exactly a quarter of one
        vacated = [0.0, 20.0, 40.0, 0.0, 20.0, 40.0]
        own = self._bench(minutes=[12.0 + v / 4 for v in vacated], vacated=vacated, vacated_usage=[0.0] * 6,
                          current=[True] * 6)
        assert role_of(own, _coeffs(situation_slope=0.1, own_slope_ridge=1e-9)).slope == pytest.approx(0.25)
        # A long ridge keeps the pooled slope
        assert role_of(own, _coeffs(situation_slope=0.1, own_slope_ridge=1e12)).slope == pytest.approx(0.1, abs=1e-6)

    def test_minutes_are_bounded_and_a_player_with_no_game_plays_his_projection(self):
        coeffs = _coeffs(situation_slope=0.13)
        state = self._bench(minutes=[30.0] * 4, vacated=[0.0] * 4)
        assert minutes_for(role_of(state, coeffs), state, 400.0, coeffs) == MAX_MINUTES
        assert minutes_for(role_of(state, coeffs), state, -400.0, coeffs) == 0.0
        waiting = PlayerState(player_id=NEW, team="AAA", prior=_prior(31.0), pre=True)
        assert role_of(waiting, coeffs) is None and minutes_for(None, waiting, 50.0, coeffs) == 31.0

    def test_games_for_another_team_carry_minutes_but_no_situation(self):
        state = self._bench(current=[False, False, True, True])
        assert role_of(state, _coeffs(situation_slope=0.1)).seen == 17.0


class TestSkill:
    def test_with_no_game_his_line_is_the_projections_per_minute(self):
        state = PlayerState(player_id=1, team="AAA", totals={k: 0.0 for k in LINE_KEYS} | {"min": 0.0},
                            prior=_prior(30.0, 70.0))
        rates = rates_of(state, _coeffs())

        for k in ("reb", "ast", "stl", "blk", "tov", "fga", "fg3a", "fta"):
            assert rates[k] == pytest.approx(PER_MINUTE[k])
        # The percentages start a little toward the league's: fifty attempts of it
        made, tried = 0.18 * 2100, 0.34 * 2100
        assert rates["fgm"] - rates["fg3m"] == pytest.approx(0.34 * (made + 26.0) / (tried + 50.0))
        assert rates["pts"] == pytest.approx(2 * (rates["fgm"] - rates["fg3m"]) + 3 * rates["fg3m"] + rates["ftm"])

    def test_each_stat_leans_on_this_season_by_its_own_weight(self):
        hot = {k: v * (2.0 if k in ("reb", "stl") else 1.0) for k, v in PER_MINUTE.items()}
        state = PlayerState(player_id=1, team="AAA", prior=_prior(30.0, 70.0),
                            totals={k: v * 400.0 for k, v in hot.items()} | {"min": 400.0})
        weights = _coeffs().prior_weights | {"reb": 400.0, "stl": 1200.0}
        rates = rates_of(state, _coeffs(prior_weights=weights))

        # 400 minutes played: halfway to this season's rebounds, a quarter of the way to its steals
        assert rates["reb"] == pytest.approx(0.2 * 1.5)
        assert rates["stl"] == pytest.approx(0.03 * 1.25)
        assert rates["ast"] == pytest.approx(0.15)

    def test_a_young_player_grows_during_the_season_in_everything_but_turnovers(self):
        coeffs = _coeffs(development={"u22": 0.06, "u24": 0.04})
        base = PlayerState(player_id=1, team="AAA", totals={k: 0.0 for k in LINE_KEYS} | {"min": 0.0},
                           prior=_prior(30.0, 70.0))
        young = rates_of(replace(base, age=21.0), coeffs)
        assert young["reb"] == pytest.approx(0.2 * 1.06) and young["tov"] == pytest.approx(0.06)
        assert [development_of(age, coeffs) for age in (21.0, 22.0, 23.5, 24.0, 27.0, None)] == [
            1.06, 1.06, 1.04, 1.04, 1.0, 1.0]

    def test_a_player_nothing_projects_starts_from_the_pool(self):
        generic = {k: v * 1000.0 for k, v in PER_MINUTE.items()} | {"min": 1000.0, "fg2m": 180.0, "fg2a": 340.0}
        state = PlayerState(player_id=9, team="AAA", totals={k: 0.0 for k in LINE_KEYS} | {"min": 0.0})
        assert rates_of(state, _coeffs(), generic)["ast"] == pytest.approx(0.15)


def _fill_in_morning():
    """AAA after eight games: the star played four and has missed four, and the
    bench player's minutes doubled while he was out."""
    lines = []
    for g in range(8):
        day, game = _day(2 * g), f"a{g}"
        if g < 4:
            lines.append(_line(STAR, "AAA", game, day, 34.0))
        lines.append(_line(STARTER, "AAA", game, day, 30.0))
        lines.append(_line(BENCH, "AAA", game, day, 12.0 if g < 4 else 24.0))
    ahead = {"AAA": [_day(16 + 2 * i) for i in range(10)]}
    return build_morning(lines, PRIORS, ahead, _day(16)), ahead["AAA"]


class TestTheProjection:
    COEFFS = _coeffs(situation_slope=0.3, usage_slope=0.01)

    def test_a_fill_ins_line_is_the_big_one_until_the_starter_is_back(self):
        morning, days = _fill_in_morning()
        back = days[4]
        out = project(morning, self.COEFFS, {STAR: Availability(out_next=True, return_date=back)})

        star, bench = out[STAR], out[BENCH]
        assert [g.chance for g in star.games_ahead] == [0.0] * 4 + [0.8] * 6
        assert star.games == pytest.approx(4.8) and star.parts["known"]["return_date"] == back.isoformat()
        # The bench player: more minutes and a larger share of the offense in the four games before
        before, after = bench.games_ahead[:4], bench.games_ahead[4:]
        assert {round(g.minutes, 6) for g in before} == {round(before[0].minutes, 6)}
        assert before[0].minutes > after[0].minutes + 5 and before[0].usage > after[0].usage
        # 34 minutes come back four nights in five: 0.3 x 34 x 0.8 fewer for him
        assert before[0].minutes - after[0].minutes == pytest.approx(0.3 * 34.0 * 0.8)
        assert [(run[2], run[3]) for run in bench.timeline()] == [(4, 0.9), (6, 0.9)]
        assert bench.timeline()[0][:2] == [days[0].isoformat(), days[3].isoformat()]

    def test_any_window_is_priced_from_the_games_in_it(self):
        morning, days = _fill_in_morning()
        out = project(morning, self.COEFFS, {STAR: Availability(out_next=True, return_date=days[4])})
        star, bench = out[STAR], out[BENCH]

        # This week the star is worth nothing, and the season is his games times his line
        assert star.window(days[0], days[3]) == (0.0, {k: 0.0 for k in LINE_KEYS})
        games, totals = star.window(days[0], days[-1])
        assert games == pytest.approx(star.games)
        for k in LINE_KEYS:
            assert totals[k] == pytest.approx(star.per_game[k] * star.games)
        # The fill-in is worth more a game this week than over the rest of the season
        week_games, week = bench.window(days[0], days[3])
        assert week_games == pytest.approx(3.6)
        assert week["pts"] / week_games > bench.per_game["pts"] > bench.window(days[4], days[-1])[1]["pts"] / 5.4
        assert bench.line().keys() == set(LINE_KEYS) | {"min"} and bench.line()["min"] == round(bench.minutes, 2)

    def test_a_league_where_nothing_changed_expects_what_it_has_seen(self):
        lines = []
        for team, base in (("AAA", 0), ("BBB", 100)):
            for g in range(6):
                for i, minutes in enumerate((34.0, 30.0, 26.0, 12.0)):
                    lines.append(_line(base + i, team, f"{team}{g}", _day(2 * g), minutes))
        priors = {base + i: _prior(m) for base in (0, 100) for i, m in enumerate((34.0, 30.0, 26.0, 12.0))}
        ahead = {team: [_day(12 + 2 * i) for i in range(8)] for team in ("AAA", "BBB")}
        morning = build_morning(lines, priors, ahead, _day(12))

        looks = outlooks(morning, _coeffs())
        # Nobody has missed a game, and one in ten will: across the league that gap is the drift, taken out
        gaps = [look.average(look.missing) for look in looks.values()]
        assert sum(gaps) / len(gaps) == pytest.approx(0.0, abs=1e-9)
        out = project(morning, _coeffs(situation_slope=0.1))
        assert out[3].minutes == pytest.approx(12.0, abs=0.5) and out[0].games == pytest.approx(7.2)

    def test_a_season_written_off_is_no_games_and_still_a_line(self):
        morning, days = _fill_in_morning()
        out = project(morning, self.COEFFS, {STAR: Availability(out_for_season=True)})
        assert out[STAR].games == 0.0 and out[STAR].per_game["pts"] > 15 and out[STAR].minutes > 30
        assert out[STAR].parts["known"]["out_for_season"] is True
        # The fill-in keeps the big line all the way
        assert len(out[BENCH].timeline()) == 1

    def test_a_team_with_no_game_left_keeps_his_line_as_it_stands(self):
        lines = _aaa()
        out = project(build_morning(lines, PRIORS, {"AAA": []}, AS_OF), _coeffs())
        assert out[STARTER].games == 0.0 and out[STARTER].games_ahead == []
        assert out[STARTER].minutes == 30.0 and out[STARTER].per_game["reb"] == pytest.approx(6.0)


class TestTheTimeline:
    def test_alike_games_are_one_run_and_a_change_starts_another(self):
        games = [GameAhead(_day(i), 0.9 if i < 3 else 0.5, 30.0 + 0.1 * i, 1.0) for i in range(6)]
        assert compress(games) == [
            [_day(0).isoformat(), _day(2).isoformat(), 3, 0.9, 30.1, 1.0],
            [_day(3).isoformat(), _day(5).isoformat(), 3, 0.5, 30.4, 1.0],
        ]

    def test_a_stored_timeline_goes_back_onto_the_schedule(self):
        games = [GameAhead(_day(2 * i), 0.9 if i < 3 else 0.5, 30.0, 1.0) for i in range(6)]
        timeline = compress(games)
        assert expand(timeline, [g.day for g in games]) == games
        # A game moved into the gap takes the run before it; one before everything takes the first
        moved = expand(timeline, [_day(-1), _day(5), _day(40)])
        assert [g.chance for g in moved] == [0.9, 0.9, 0.5]
        assert expand([], [_day(1)]) == []
