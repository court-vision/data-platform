"""Fitting and backtesting the live projection, on seasons built by hand.

The curves are checked on a team small enough to count on paper, and the whole
fit on three synthetic seasons in which one thing is known to be true: every
player rebounds half again as well as he was projected to, and keeps doing it.
"""

import random
from datetime import date, timedelta

import pytest

from services import live_projection_fit as fitting
from services.live_projection_fit import (
    After,
    build_sample,
    fit,
    fit_games,
    fpts,
    history_rows,
    last_season_ranks,
    leave_one_out,
    ages_for,
    records,
    sat_out_season,
    season_priors,
    seat_value,
)
from services.live_projection_model import BUCKETS, HORIZON, LINE_KEYS, PRE_DEBUT, BoxLine, Prior
from services.projection_model import Coefficients as SeasonCoefficients

pytestmark = pytest.mark.unit

PER_MINUTE = {"pts": 0.66, "reb": 0.2, "ast": 0.15, "stl": 0.03, "blk": 0.02, "tov": 0.06,
              "fgm": 0.24, "fga": 0.5, "fg3m": 0.06, "fg3a": 0.16, "ftm": 0.12, "fta": 0.15}
OPENER = date(2025, 10, 21)


def _line(pid, team, game, day, minutes, per_minute=PER_MINUTE) -> BoxLine:
    return BoxLine(player_id=pid, team=team, game_id=game, game_date=day, minutes=minutes,
                   stats={k: v * minutes for k, v in per_minute.items()})


def _prior(minutes, games=70.0) -> Prior:
    return Prior(minutes=minutes, games=games, per_game={k: v * minutes for k, v in PER_MINUTE.items()})


def test_a_seat_is_his_games_at_his_line_and_the_rest_at_a_replacements():
    # 50 games at 40 a night, and the 10 he misses at the replacement's 25
    assert seat_value(40.0, 50.0, 60.0, 25.0) == 2250.0
    # Missing games never pays: a player below the replacement fills his own seat either way
    assert seat_value(20.0, 30.0, 60.0, 25.0) == seat_value(20.0, 60.0, 60.0, 25.0) == 1200.0
    # More games projected than the team has left is not a debt
    assert seat_value(40.0, 62.0, 60.0, 25.0) == 2480.0


# ---- the curves, on one small league -------------------------------------------------


EVERY, BACK_THIRD, TRADED, NEVER = 1, 2, 3, 4


def _small_season():
    """AAA plays every other day for 20 games, BBB on the days between. Ten days in:
    one regular has played everything and goes on to; one has just missed two
    and comes back for the third game from here; one is traded to BBB right
    after the morning and plays all of its games; one, projected, never debuts
    this season."""
    lines = []
    for g in range(20):
        day = OPENER + timedelta(days=2 * g)
        lines.append(_line(EVERY, "AAA", f"a{g}", day, 32.0))
        lines.append(_line(90, "AAA", f"a{g}", day, 10.0))
        if g < 3 or g >= 7:
            lines.append(_line(BACK_THIRD, "AAA", f"a{g}", day, 30.0))
        if g < 5:
            lines.append(_line(TRADED, "AAA", f"a{g}", day, 28.0))
        other = day + timedelta(days=1)
        lines.append(_line(91, "BBB", f"b{g}", other, 10.0))
        if g >= 5:
            lines.append(_line(TRADED, "BBB", f"b{g}", other, 28.0))
    priors = {EVERY: _prior(32.0), BACK_THIRD: _prior(30.0), TRADED: _prior(28.0), NEVER: _prior(26.0)}
    return lines, priors


def test_a_sample_is_the_morning_and_what_came_after():
    lines, priors = _small_season()
    sample = build_sample(2025, 10, lines, priors, sat_out={NEVER: "AAA"})

    cutoff = OPENER + timedelta(days=10)
    assert sample.morning.as_of == cutoff and len(sample.morning.ahead["AAA"]) == 15
    assert sample.morning.players[EVERY].n == 5 and sample.after[EVERY] == After(15, 480.0, sample.after[EVERY].totals)
    assert sample.after[EVERY].per_game("min") == 32.0 and sample.after[EVERY].per_game("reb") == pytest.approx(6.4)
    # Out for the last two games before the morning, back for the third after it
    assert sample.morning.players[BACK_THIRD].streak == 2
    assert sample.back_on[BACK_THIRD] == OPENER + timedelta(days=14) and sample.after[BACK_THIRD].games == 13
    # The player who never debuts is placed on his team and has nothing after
    assert sample.morning.players[NEVER].pre and NEVER not in sample.after
    assert len(sample.played_on[TRADED]) == 15


def test_the_curves_count_who_played_each_game_from_the_morning(monkeypatch):
    monkeypatch.setattr(fitting, "CURVE_MIN_PLAYERS", 1)
    monkeypatch.setattr(fitting, "CURVE_SMOOTH", 0)
    lines, priors = _small_season()
    coeffs = fit_games([build_sample(2025, 10, lines, priors, sat_out={NEVER: "AAA"})])

    assert len(coeffs.plays) == BUCKETS and all(len(row) == HORIZON + 1 for row in coeffs.plays)
    # Healthy that morning: one plays every game, and the other's games for his new team fill
    # the slots between his old team's games, so a trade is not an absence. All but the first
    # slot, which closed the day before his first game there.
    assert list(coeffs.plays[0][1:16]) == [0.5] + [1.0] * 14
    # Out two games: misses two more, then plays the rest
    assert list(coeffs.plays[2][1:6]) == [0.0, 0.0, 1.0, 1.0, 1.0]
    # Has not played this season and does not: the curve that keeps a lost year off the board
    assert list(coeffs.plays[PRE_DEBUT][1:16]) == [0.0] * 15
    # Past the games anyone had left, a curve holds its last measured point
    assert coeffs.plays[2][40] == 1.0
    # Once back, both played every game they could
    assert coeffs.played_once_back[0] == 1.0 and coeffs.played_once_back[2] == 1.0


def test_a_point_measured_on_too_few_players_is_carried_from_the_one_before():
    lines, priors = _small_season()
    coeffs = fit_games([build_sample(2025, 10, lines, priors)])
    # Two healthy regulars are not thirty: the healthy curve is the share played once back, flat
    assert set(coeffs.plays[0][1:]) == {coeffs.played_once_back[0]}
    assert set(coeffs.plays[2][1:]) == {0.0}


# ---- the preseason projection of a past season -----------------------------------------


FLAT = SeasonCoefficients(curve={}, gp_beta=[0.72, 0, 0, 0, 0, 0, 0, 0, 0], weights=(7, 2, 1),
                          reg_minutes=0.0, reg_games=0.0, gp_shrink=0.0, espn_weight=0.5)


def test_history_is_rebuilt_from_box_scores_and_projects_the_next_season():
    by_season = {
        year: [_line(pid, "AAA", f"{year}-{g}", date(year, 11, 1) + timedelta(days=2 * g), minutes)
               for g in range(60) for pid, minutes in ((1, 30.0), (2, 20.0))]
        for year in (2023, 2024)
    }
    by_season[2025] = [l for l in by_season[2024] if l.player_id == 1]      # player 2 sits 2025 out ...
    by_season[2026] = by_season[2024]                                        # ... and is back in 2026
    history = history_rows(by_season, {(1, 2024): (27.0, 2018), (2, 2024): (22.0, 2023)})

    row = next(r for r in history[1] if r.season == 2024)
    assert (row.gp, row.minutes, row.age, row.from_year) == (60, 1800.0, 27.0, 2018)
    assert row.totals["reb"] == pytest.approx(0.2 * 1800)

    priors = season_priors(history, 2025, FLAT)
    assert set(priors) == {1, 2}
    assert priors[1].minutes == pytest.approx(30.0, abs=1.0) and priors[1].per_game["reb"] == pytest.approx(6.0, abs=0.3)
    assert set(priors[1].per_game) == set(LINE_KEYS)
    assert ages_for(history, 2025) == {1: 28.0, 2: 23.0}
    # Best first, by last season's fantasy points a game
    assert last_season_ranks(history, 2025) == {1: 0, 2: 1}
    assert fpts(row.totals) / row.gp > fpts(next(r for r in history[2] if r.season == 2024).totals) / 60

    assert sat_out_season(by_season, 2025) == {2: "AAA"}
    # No season after it to come back in: it cannot be told from a career that ended
    assert sat_out_season(by_season, 2026) == {}


# ---- the whole fit, on a league where one thing is true -------------------------------------


MINUTES = (34.0, 32.0, 30.0, 28.0, 26.0, 24.0, 18.0, 14.0, 10.0)


def _synthetic(year: int, rng: random.Random):
    """Two teams, 44 games. Everyone is projected at PER_MINUTE and plays to it,
    with noise, except on the glass: half again as many rebounds, all season."""
    opener = date(year, 10, 21)
    lines, priors = [], {}
    for t, team in enumerate(("AAA", "BBB")):
        for i, minutes in enumerate(MINUTES):
            priors[100 * t + i] = _prior(minutes)
        for g in range(44):
            day = opener + timedelta(days=2 * g + t)
            for i, minutes in enumerate(MINUTES):
                if rng.random() > 0.9:
                    continue
                played = minutes * rng.uniform(0.85, 1.15)
                stats = {k: v * played * rng.uniform(0.6, 1.4) * (1.5 if k == "reb" else 1.0)
                         for k, v in PER_MINUTE.items()}
                lines.append(BoxLine(player_id=100 * t + i, team=team, game_id=f"{team}{g}", game_date=day,
                                     minutes=played, stats=stats))
    return lines, priors


@pytest.fixture(scope="module")
def seasons():
    rng = random.Random(7)
    out = {}
    for year in (2023, 2024, 2025):
        lines, priors = _synthetic(year, rng)
        ranked = {pid: i for i, pid in enumerate(sorted(priors))}
        out[year] = [build_sample(year, days, lines, priors, ranked=ranked) for days in (26, 45)]
    return out


def test_where_this_seasons_games_tell_the_truth_the_fit_believes_them(seasons):
    samples = [s for group in seasons.values() for s in group]
    coeffs = fit(samples)

    weights = coeffs.prior_weights
    # Rebounds: the projection is wrong for everyone, so it gets the smallest weight on the grid
    assert weights["reb"] == fitting.GRID_VOLUME[0]
    # Assists: the projection is the truth and the games are noise around it
    assert weights["ast"] >= 800
    assert set(weights) == set(fitting.VOLUME_KEYS) | set(fitting.SHOOTING)
    assert coeffs.minutes_prior_games in fitting.GRID_MINUTES["minutes_prior_games"]
    assert coeffs.minutes_half_life in fitting.GRID_MINUTES["minutes_half_life"]
    assert len(coeffs.plays) == BUCKETS and 0.85 < coeffs.played_once_back[0] < 0.95
    # Every sample is left with its outlooks attached: what `records` and `score` read
    assert all(s.looks for s in samples) and len(records(samples)) > 60


def test_the_backtest_scores_each_season_with_coefficients_that_never_saw_it(seasons):
    result = leave_one_out(seasons)

    assert set(result) == {"26", "45"}
    for entry in result.values():
        assert entry["players"] > 20 and set(entry["by_season"]) == {"2023", "2024", "2025"}
        assert set(entry["fpts_pg_mae"]) == {"prior", "season", "blend", "live"}
        assert set(entry["per_stat_mae"]["reb"]) == {"blend", "live"}
        for key in ("board_value_blend", "board_value_live", "board_points_live", "board_rho_live",
                    "games_mae_blend", "games_mae_live"):
            assert key in entry
        # The one true thing in this league: the live line finds the rebounds faster than the blend's ten games
        assert entry["per_stat_mae"]["reb"]["live"] < entry["per_stat_mae"]["reb"]["blend"]
        assert entry["fpts_pg_mae"]["live"] < entry["fpts_pg_mae"]["prior"]
